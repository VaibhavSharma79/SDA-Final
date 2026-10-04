"""Stream ERP site-sync records (the ERP's own copy of each site's markings) to Kafka, paced by bucket."""

import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "seat-site-sync"
DATA_FILE = "site_sync_events.jsonl"
SLEEP_PER_BUCKET = float(os.getenv("SLEEP_PER_BUCKET", "0.4"))

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Site-sync producer: {DATA_FILE} -> {TOPIC}  (pace: {SLEEP_PER_BUCKET}s / 5-min bucket)")
last_bucket = None
count = 0
with open(DATA_FILE, encoding="utf-8") as data_file:
    for line in data_file:
        event = json.loads(line)
        # event_ts looks like 2026-06-01T09:14:32.418+05:30
        minute_of_day = int(event["event_ts"][11:13]) * 60 + int(event["event_ts"][14:16])
        bucket = minute_of_day // 5
        if last_bucket is not None and bucket != last_bucket:
            time.sleep(SLEEP_PER_BUCKET)
        event["produced_at"] = datetime.now(timezone.utc).isoformat()
        # key by location so records for one site stay ordered within a partition
        producer.send(TOPIC, key=str(event["location_code"]), value=event)
        last_bucket = bucket
        count += 1
        if count % 25 == 0:
            print(f"[{event['event_ts'][11:16]}] {count:,} site-sync records sent so far")

producer.flush()
producer.close()
print(f"Sent {count:,} site-sync records.")
