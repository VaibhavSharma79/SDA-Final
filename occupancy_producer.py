"""Stream check-in / check-out events to Kafka, paced by bucket."""

import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "seat-occupancy"
DATA_FILE = "occupancy_events.jsonl"
SLEEP_PER_BUCKET = float(os.getenv("SLEEP_PER_BUCKET", "0.4"))

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Occupancy producer: {DATA_FILE} -> {TOPIC}  (pace: {SLEEP_PER_BUCKET}s / 5-min bucket)")
last_bucket = None
count = 0
with open(DATA_FILE, encoding="utf-8") as data_file:
    for line in data_file:
        event = json.loads(line)
        minute_of_day = int(event["event_ts"][11:13]) * 60 + int(event["event_ts"][14:16])
        bucket = minute_of_day // 5
        if last_bucket is not None and bucket != last_bucket:
            time.sleep(SLEEP_PER_BUCKET)
        event["produced_at"] = datetime.now(timezone.utc).isoformat()
        # key by seat pool: the engine keeps a running head-count per pool, so one pool = one ordered stream
        pool_key = f"{event['location_code']}|{event['entity']}|{event['office_type']}"
        producer.send(TOPIC, key=pool_key, value=event)
        last_bucket = bucket
        count += 1
        if count % 1000 == 0:
            print(f"[{event['event_ts'][11:16]}] {count:,} check-in/out events sent so far")

producer.flush()
producer.close()
print(f"Sent {count:,} check-in/out events.")
