"""
alerts_notifier.py
Seat Integrity Guard

Subscribes to the `seat-alerts` topic and prints a notification line for every alert at or above
MIN_SEVERITY. Matched records never reach this topic, so a quiet terminal means the ERP agrees
with the master. Swap the print for an email / Slack / Teams webhook to go beyond the demo.

    MIN_SEVERITY=HIGH python alerts_notifier.py
"""

import json
import os

from kafka import KafkaConsumer

from alert_rules import SEVERITY_RANK

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "seat-alerts"
MIN_SEVERITY = os.getenv("MIN_SEVERITY", "LOW").upper()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset="earliest",
    group_id="seat-guard-notifier",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Notifier listening on {TOPIC} (min severity: {MIN_SEVERITY})")
shown = skipped = 0
try:
    for message in consumer:
        alert = message.value
        if SEVERITY_RANK[alert["severity"]] < SEVERITY_RANK[MIN_SEVERITY]:
            skipped += 1
            continue
        shown += 1
        print(f"*** {alert['severity']:<8} {alert['rule_id']}  {alert['location_code']} | {alert['entity']} | "
              f"{alert['office_type']}\n    {alert['detail']}   (event {alert['trigger_event_id']} @ {alert['event_ts']})")
except KeyboardInterrupt:
    print(f"\nNotifier stopped. {shown} notifications shown, {skipped} below {MIN_SEVERITY}.")
finally:
    consumer.close()
