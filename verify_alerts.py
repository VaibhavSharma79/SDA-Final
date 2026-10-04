"""
verify_alerts.py
Seat Integrity Guard

After a full replay, checks the `alerts` table in MySQL against expected_alerts.csv, the answer key
produced by generate_seat_data.py's independent reference engine. Prints PASS only if every expected
(event, rule) pair was raised and nothing extra was.

Note: expected_alerts.csv assumes the default brackets (90% / 5%) and an unchanged seat_master table.
If you changed NEAR_CAPACITY_PCT, SEAT_VARIANCE_TOLERANCE or edited seat_master, differences are expected.
"""

import csv
import os
import sys
from collections import Counter

import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")

with open("expected_alerts.csv", newline="", encoding="utf-8") as csv_file:
    expected = {(row["event_id"], row["rule_id"]) for row in csv.DictReader(csv_file)}

db = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database="seat_guard")
cursor = db.cursor()
cursor.execute("SELECT trigger_event_id, rule_id FROM alerts")
actual = {(event_id, rule_id) for event_id, rule_id in cursor.fetchall()}
cursor.close()
db.close()

missing, unexpected = expected - actual, actual - expected
by_rule = Counter(rule for _, rule in actual)
print(f"Expected {len(expected)} alerts, found {len(actual)} in MySQL.")
for rule, n in sorted(by_rule.items()):
    print(f"   {rule:<24} {n}")
if missing:
    print(f"MISSING ({len(missing)}): {sorted(missing)[:10]}")
if unexpected:
    print(f"UNEXPECTED ({len(unexpected)}): {sorted(unexpected)[:10]}")
print("PASS: the pipeline raised exactly the expected alerts." if not missing and not unexpected else "FAIL")
sys.exit(0 if not missing and not unexpected else 1)
