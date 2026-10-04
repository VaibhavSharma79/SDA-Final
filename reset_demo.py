"""
reset_demo.py - clear the results of a previous run so the next replay starts clean.

    python reset_demo.py            # empties site_sync_log, occupancy_log, pool_state, alerts
    python reset_demo.py --master   # also empties seat_master (re-loaded from seat_master.csv on the next engine start)

Restart seat_guard_consumer.py after resetting: its running head-counts live in memory.
"""

import os
import sys

import mysql.connector

db = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "localhost"), port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"), password=os.getenv("MYSQL_PASSWORD", "root"), database="seat_guard",
)
cursor = db.cursor()
tables = ["alerts", "pool_state", "occupancy_log", "site_sync_log"] + (["seat_master"] if "--master" in sys.argv else [])
for table in tables:
    cursor.execute(f"TRUNCATE TABLE {table}")
    print(f"cleared {table}")
db.commit()
cursor.close()
db.close()
