"""
seat_guard_consumer.py
Seat Integrity Guard

Consumes the ERP site-sync topic and the check-in/check-out topic and runs the
rules in alert_rules.py against the master seat table:

  * site-sync records are checked against the master (stateless).
    A match is SILENT, a mismatch raises an alert.
  * check-in / check-out events update a running head-count per seat pool
    (stateful) which is compared with the master seat count.

Everything is persisted to MySQL for the Grafana dashboard:
  seat_master      the reference table (loaded from seat_master.csv on first run)
  site_sync_log    every ERP record with its verdict (OK or the rule that fired)
  occupancy_log    every check-in/out with the running occupancy after it
  pool_state       current + peak occupancy per pool
  alerts           one row per alert (the audit trail)
Alerts are also published to the `seat-alerts` Kafka topic (see alerts_notifier.py).

The master table is re-read every MASTER_REFRESH_SECONDS, so you can change a
seat count while the demo runs and watch the verdicts react without a restart:
    UPDATE seat_master SET seats_total = 200 WHERE location_code = '...';
"""

import csv
import json
import os
import time
from datetime import datetime

import mysql.connector
from kafka import KafkaConsumer, KafkaProducer

from alert_rules import (NEAR_CAPACITY_PCT, SEAT_VARIANCE_TOLERANCE, MasterIndex,
                         OccupancyTracker, check_site_sync)

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = "seat_guard"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SYNC_TOPIC = "seat-site-sync"
OCCUPANCY_TOPIC = "seat-occupancy"
ALERT_TOPIC = "seat-alerts"
GROUP_ID = "seat-guard-engine"

MASTER_CSV = os.getenv("MASTER_CSV", "seat_master.csv")
MASTER_REFRESH_SECONDS = float(os.getenv("MASTER_REFRESH_SECONDS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS seat_master (
        location_code VARCHAR(80) NOT NULL,
        entity VARCHAR(60) NOT NULL,
        office_type VARCHAR(60) NOT NULL,
        city VARCHAR(60) NOT NULL,
        seats_total INT NOT NULL,
        first_start_date DATE NULL,
        master_rows INT NOT NULL DEFAULT 1,
        PRIMARY KEY (location_code, entity, office_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS site_sync_log (
        event_id VARCHAR(20) PRIMARY KEY,
        event_ts DATETIME(3) NOT NULL,
        location_code VARCHAR(80) NULL,
        entity VARCHAR(60) NULL,
        office_type VARCHAR(60) NULL,
        city VARCHAR(60) NULL,
        erp_seats_total INT NULL,
        master_seats INT NULL,
        verdict VARCHAR(30) NOT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ss_ts (event_ts),
        INDEX idx_ss_verdict (verdict)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS occupancy_log (
        event_id VARCHAR(20) PRIMARY KEY,
        event_ts DATETIME(3) NOT NULL,
        event_type VARCHAR(10) NOT NULL,
        employee_id VARCHAR(12) NOT NULL,
        department VARCHAR(40) NULL,
        location_code VARCHAR(80) NULL,
        entity VARCHAR(60) NULL,
        office_type VARCHAR(60) NULL,
        city VARCHAR(60) NULL,
        occupancy_after INT NULL,
        capacity INT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_oc_ts (event_ts),
        INDEX idx_oc_pool_ts (location_code, entity, office_type, event_ts)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pool_state (
        location_code VARCHAR(80) NOT NULL,
        entity VARCHAR(60) NOT NULL,
        office_type VARCHAR(60) NOT NULL,
        city VARCHAR(60) NOT NULL,
        capacity INT NOT NULL,
        occupancy INT NOT NULL,
        peak_occupancy INT NOT NULL,
        last_event_ts DATETIME(3) NOT NULL,
        PRIMARY KEY (location_code, entity, office_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trigger_event_id VARCHAR(20) NOT NULL,
        stream VARCHAR(12) NOT NULL,
        rule_id VARCHAR(30) NOT NULL,
        severity VARCHAR(10) NOT NULL,
        event_ts DATETIME(3) NOT NULL,
        location_code VARCHAR(80) NULL,
        entity VARCHAR(60) NULL,
        office_type VARCHAR(60) NULL,
        expected_value VARCHAR(200) NULL,
        actual_value VARCHAR(200) NULL,
        detail VARCHAR(255) NULL,
        raised_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_alert (trigger_event_id, rule_id),
        INDEX idx_al_ts (event_ts),
        INDEX idx_al_rule (rule_id)
    )
    """,
]

SYNC_SQL = """
INSERT INTO site_sync_log (event_id, event_ts, location_code, entity, office_type, city, erp_seats_total, master_seats, verdict)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE master_seats = VALUES(master_seats), verdict = VALUES(verdict)
"""
OCCUPANCY_SQL = """
INSERT INTO occupancy_log (event_id, event_ts, event_type, employee_id, department, location_code, entity, office_type, city,
                           occupancy_after, capacity)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE occupancy_after = VALUES(occupancy_after), capacity = VALUES(capacity)
"""
POOL_STATE_SQL = """
INSERT INTO pool_state (location_code, entity, office_type, city, capacity, occupancy, peak_occupancy, last_event_ts)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE capacity = VALUES(capacity), occupancy = VALUES(occupancy),
                        peak_occupancy = VALUES(peak_occupancy), last_event_ts = VALUES(last_event_ts)
"""
ALERT_SQL = """
INSERT INTO alerts (trigger_event_id, stream, rule_id, severity, event_ts, location_code, entity, office_type,
                    expected_value, actual_value, detail)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE severity = VALUES(severity), expected_value = VALUES(expected_value),
                        actual_value = VALUES(actual_value), detail = VALUES(detail)
"""


def parse_ts(value):
    """ISO-8601 with +05:30 -> naive IST wall-clock DATETIME (the dashboard shows IST as written)."""
    return datetime.fromisoformat(value).replace(tzinfo=None)


def load_master_csv_if_empty(cursor):
    cursor.execute("SELECT COUNT(*) FROM seat_master")
    if cursor.fetchone()[0] > 0:
        return 0
    with open(MASTER_CSV, newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    cursor.executemany(
        "INSERT INTO seat_master (location_code, entity, office_type, city, seats_total, first_start_date, master_rows) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        [(r["location_code"], r["entity"], r["office_type"], r["city"], int(r["seats_total"]),
          r["first_start_date"] or None, int(r["master_rows"])) for r in rows],
    )
    return len(rows)


def read_master(cursor):
    cursor.execute("SELECT location_code, entity, office_type, city, seats_total FROM seat_master")
    return MasterIndex([
        {"location_code": c, "entity": e, "office_type": t, "city": city, "seats_total": s}
        for (c, e, t, city, s) in cursor.fetchall()
    ])


def alert_row(event_id, stream, ts, event, alert):
    return (event_id, stream, alert.rule_id, alert.severity, ts, event.get("location_code"), event.get("entity"),
            event.get("office_type"), alert.expected[:200], alert.actual[:200], alert.detail[:255])


def alert_message(event_id, stream, ts, event, alert):
    return {"trigger_event_id": event_id, "stream": stream, "rule_id": alert.rule_id, "severity": alert.severity,
            "event_ts": ts.isoformat(sep=" ", timespec="milliseconds"), "location_code": event.get("location_code"),
            "entity": event.get("entity"), "office_type": event.get("office_type"),
            "expected": alert.expected, "actual": alert.actual, "detail": alert.detail}


db = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD)
cursor = db.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")
for statement in SCHEMA_STATEMENTS:
    cursor.execute(statement)
cursor.execute("CREATE USER IF NOT EXISTS 'grafana'@'%' IDENTIFIED BY 'grafana'")
cursor.execute(f"GRANT SELECT ON {DB_NAME}.* TO 'grafana'@'%'")
loaded = load_master_csv_if_empty(cursor)
db.commit()
master = read_master(cursor)
db.commit()

consumer = KafkaConsumer(
    SYNC_TOPIC, OCCUPANCY_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
alert_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

tracker = OccupancyTracker()
counts = {"sync": 0, "sync_ok": 0, "occupancy": 0, "alerts": 0}

print("Seat Integrity Guard started")
print(f"   Topics : {SYNC_TOPIC}, {OCCUPANCY_TOPIC}  ->  alerts: {ALERT_TOPIC}")
print(f"   MySQL  : {DB_NAME}   master pools: {len(master)}" + (f" (loaded {loaded} from {MASTER_CSV})" if loaded else ""))
print(f"   Brackets: near-capacity >= {NEAR_CAPACITY_PCT}%   seat variance <= {SEAT_VARIANCE_TOLERANCE:.0%}")
print("=" * 78)


def process_sync(event, pending_alerts):
    ts = parse_ts(event["event_ts"])
    result = check_site_sync(event, master)
    cursor.execute(SYNC_SQL, (
        event["event_id"], ts, event.get("location_code"), event.get("entity"), event.get("office_type"),
        event.get("city"), event.get("erp_seats_total"), result.master_seats, result.verdict,
    ))
    counts["sync"] += 1
    if result.alert:
        pending_alerts.append((event, "SITE_SYNC", ts, result.alert))
    else:
        counts["sync_ok"] += 1            # matched the master: no alert, no notification


def process_occupancy(event, pending_alerts, dirty_pools):
    ts = parse_ts(event["event_ts"])
    result = tracker.apply(event, master)
    cursor.execute(OCCUPANCY_SQL, (
        event["event_id"], ts, event["event_type"], event["employee_id"], event.get("department"),
        event.get("location_code"), event.get("entity"), event.get("office_type"), event.get("city"),
        result.occupancy_after, result.capacity,
    ))
    counts["occupancy"] += 1
    if result.capacity is not None:
        dirty_pools[(event["location_code"], event["entity"], event["office_type"])] = (event["city"], result.capacity, ts)
    for alert in result.alerts:
        pending_alerts.append((event, "OCCUPANCY", ts, alert))


def flush_pool_state(dirty_pools):
    rows = []
    for key, (city, capacity, ts) in dirty_pools.items():
        rows.append((key[0], key[1], key[2], city, capacity, tracker.occupancy[key], tracker.peak[key], ts))
    if rows:
        cursor.executemany(POOL_STATE_SQL, rows)


last_master_refresh = time.monotonic()
try:
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)

        if time.monotonic() - last_master_refresh >= MASTER_REFRESH_SECONDS:
            new_master = read_master(cursor)
            db.commit()
            if new_master.pools != master.pools:
                print(f"   [master] reference table changed ({len(master)} -> {len(new_master)} pools / seat counts) - reloaded")
            master = new_master
            last_master_refresh = time.monotonic()

        if not records:
            continue

        pending_alerts, dirty_pools = [], {}
        try:
            for batch in records.values():
                for message in batch:
                    if message.topic == SYNC_TOPIC:
                        process_sync(message.value, pending_alerts)
                    else:
                        process_occupancy(message.value, pending_alerts, dirty_pools)
            flush_pool_state(dirty_pools)
            for event, stream, ts, alert in pending_alerts:
                cursor.execute(ALERT_SQL, alert_row(event["event_id"], stream, ts, event, alert))
            db.commit()
            consumer.commit()
        except mysql.connector.Error:
            db.rollback()
            raise

        for event, stream, ts, alert in pending_alerts:
            alert_producer.send(ALERT_TOPIC, value=alert_message(event["event_id"], stream, ts, event, alert))
            counts["alerts"] += 1
            print(f"ALERT [{alert.severity:<8}] {alert.rule_id:<22} {str(event.get('location_code')):<32} "
                  f"expected {alert.expected} | actual {alert.actual}")
        alert_producer.flush()

        total = counts["sync"] + counts["occupancy"]
        print(f"   processed {total:,} events  (sync {counts['sync']:,} / occupancy {counts['occupancy']:,})  "
              f"alerts so far {counts['alerts']:,}")
except KeyboardInterrupt:
    print("\nSeat Integrity Guard stopped.")
finally:
    print(f"   Totals: {counts['sync']:,} site-sync ({counts['sync_ok']:,} matched silently), "
          f"{counts['occupancy']:,} occupancy events, {counts['alerts']:,} alerts")
    consumer.close()
    alert_producer.close()
    cursor.close()
    db.close()
