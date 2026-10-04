"""
alert_rules.py
Seat Integrity Guard

Pure, dependency-free rule logic (no Kafka, no MySQL). The streaming engine
(seat_guard_consumer.py) feeds events in and gets verdicts/alerts out, so the
rules can be unit-tested on their own.

Two kinds of rule:

  1. Reference check (stateless) on ERP site-sync records. The master seat table
     is the source of truth. A record that matches it is SILENT. A mismatch
     raises exactly one alert:
        MISSING_LOCATION_CODE, UNKNOWN_LOCATION_CODE, ENTITY_MISMATCH,
        OFFICE_TYPE_MISMATCH, CITY_MISMATCH, SEAT_COUNT_MISMATCH,
        SEAT_COUNT_VARIANCE

  2. Running-state check (stateful) on check-in / check-out events:
        ZERO_SEAT_CHECKIN, NEAR_CAPACITY, OVER_CAPACITY, DOUBLE_CHECKIN

Match key = (location_code, entity, office_type). Seats are summed per key
because the master has several rows per key.

Brackets are environment-configurable (no code change needed):
    NEAR_CAPACITY_PCT=80 python seat_guard_consumer.py
    SEAT_VARIANCE_TOLERANCE=0.10 python seat_guard_consumer.py
"""

import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

NEAR_CAPACITY_PCT = int(os.getenv("NEAR_CAPACITY_PCT", "90"))
SEAT_VARIANCE_TOLERANCE = float(os.getenv("SEAT_VARIANCE_TOLERANCE", "0.05"))

SEVERITY = {
    "MISSING_LOCATION_CODE": "HIGH",
    "UNKNOWN_LOCATION_CODE": "HIGH",
    "ENTITY_MISMATCH": "MEDIUM",
    "OFFICE_TYPE_MISMATCH": "MEDIUM",
    "CITY_MISMATCH": "MEDIUM",
    "SEAT_COUNT_MISMATCH": "HIGH",
    "SEAT_COUNT_VARIANCE": "LOW",
    "ZERO_SEAT_CHECKIN": "HIGH",
    "NEAR_CAPACITY": "LOW",
    "OVER_CAPACITY": "CRITICAL",
    "DOUBLE_CHECKIN": "HIGH",
}
SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

PoolKey = Tuple[str, str, str]


@dataclass(frozen=True)
class Alert:
    rule_id: str
    severity: str
    expected: str
    actual: str
    detail: str


@dataclass(frozen=True)
class SyncResult:
    verdict: str                      # "OK" or the rule_id that fired
    master_seats: Optional[int]       # seats for this pool in the master (None if pool not found)
    alert: Optional[Alert]


@dataclass(frozen=True)
class OccupancyResult:
    occupancy_after: Optional[int]    # None when the pool is not in the master
    capacity: Optional[int]
    alerts: List[Alert]


def _alert(rule_id: str, expected, actual, detail: str) -> Alert:
    return Alert(rule_id, SEVERITY[rule_id], str(expected), str(actual), detail)


def near_threshold(capacity: int) -> int:
    """ceil(capacity * NEAR_CAPACITY_PCT / 100) in integer arithmetic (no float drift)."""
    return -(-capacity * NEAR_CAPACITY_PCT // 100)


class MasterIndex:
    """In-memory lookup built from the master pool rows (the 'reference table' side of the join)."""

    def __init__(self, rows):
        self.pools: Dict[PoolKey, dict] = {}
        self.code_city: Dict[str, str] = {}
        self.ents_by_code = defaultdict(set)
        self.types_by_code_ent = defaultdict(set)
        for r in rows:
            key = (r["location_code"], r["entity"], r["office_type"])
            self.pools[key] = {"capacity": int(r["seats_total"]), "city": r["city"]}
            self.code_city.setdefault(r["location_code"], r["city"])
            self.ents_by_code[r["location_code"]].add(r["entity"])
            self.types_by_code_ent[(r["location_code"], r["entity"])].add(r["office_type"])

    def __len__(self):
        return len(self.pools)

    def capacity(self, key: PoolKey) -> Optional[int]:
        pool = self.pools.get(key)
        return None if pool is None else pool["capacity"]


def _identity_alert(code, entity, office_type, master: MasterIndex) -> Optional[Alert]:
    """Does (code, entity, office_type) exist in the master? First failing check wins."""
    if code is None or str(code).strip() == "":
        return _alert("MISSING_LOCATION_CODE", "non-empty location code", "<blank>", "record has no location code")
    if code not in master.code_city:
        return _alert("UNKNOWN_LOCATION_CODE", "code present in master", repr(code), "location code not found in master")
    if entity not in master.ents_by_code[code]:
        return _alert("ENTITY_MISMATCH", " / ".join(sorted(master.ents_by_code[code])), entity,
                      "entity not registered at this location code")
    if office_type not in master.types_by_code_ent[(code, entity)]:
        return _alert("OFFICE_TYPE_MISMATCH", " / ".join(sorted(master.types_by_code_ent[(code, entity)])), office_type,
                      "office type not registered for this code + entity")
    return None


def check_site_sync(event: dict, master: MasterIndex) -> SyncResult:
    """Reference check of one ERP site-sync record against the master. Silent (OK) on a match."""
    code, entity, office_type = event.get("location_code"), event.get("entity"), event.get("office_type")

    alert = _identity_alert(code, entity, office_type, master)
    if alert:
        return SyncResult(alert.rule_id, None, alert)

    if event.get("city") != master.code_city[code]:
        alert = _alert("CITY_MISMATCH", master.code_city[code], event.get("city"), "city differs from master")
        return SyncResult(alert.rule_id, master.capacity((code, entity, office_type)), alert)

    cap = master.capacity((code, entity, office_type))
    erp = int(event["erp_seats_total"])
    if erp == cap:
        return SyncResult("OK", cap, None)

    if cap == 0:
        alert = _alert("SEAT_COUNT_MISMATCH", cap, erp, "master has 0 seats, ERP reports seats")
    else:
        pct = abs(erp - cap) / cap
        if pct <= SEAT_VARIANCE_TOLERANCE:
            alert = _alert("SEAT_COUNT_VARIANCE", cap, erp,
                           f"diff {erp - cap:+d} seats ({pct:.1%}) within {SEAT_VARIANCE_TOLERANCE:.0%} bracket")
        else:
            alert = _alert("SEAT_COUNT_MISMATCH", cap, erp,
                           f"diff {erp - cap:+d} seats ({pct:.1%}) exceeds {SEAT_VARIANCE_TOLERANCE:.0%} bracket")
    return SyncResult(alert.rule_id, cap, alert)


class OccupancyTracker:
    """
    Keyed state for check-in / check-out events:
      occupancy[pool]   running head-count per seat pool   (+1 check-in, -1 check-out)
      peak[pool]        highest head-count seen
      open_at[employee] pools where the employee is currently checked in
    """

    def __init__(self):
        self.occupancy = defaultdict(int)
        self.peak = defaultdict(int)
        self.open_at = defaultdict(set)

    def apply(self, event: dict, master: MasterIndex) -> OccupancyResult:
        code, entity, office_type = event.get("location_code"), event.get("entity"), event.get("office_type")
        key: PoolKey = (code, entity, office_type)
        employee = event["employee_id"]

        unknown = _identity_alert(code, entity, office_type, master)
        if unknown:                       # pool not in master: cannot compare against seats
            return OccupancyResult(None, None, [unknown])

        cap = master.capacity(key)
        alerts: List[Alert] = []

        if event["event_type"] == "CHECK_IN":
            before = self.occupancy[key]
            self.occupancy[key] += 1
            after = self.occupancy[key]
            self.peak[key] = max(self.peak[key], after)

            elsewhere = sorted(p[0] for p in self.open_at[employee] if p != key)
            if elsewhere:
                alerts.append(_alert("DOUBLE_CHECKIN", "no open check-in elsewhere", f"still inside {elsewhere[0]}",
                                     f"{employee} checked in at {code} while still checked in at {elsewhere[0]}"))
            self.open_at[employee].add(key)

            if cap == 0:
                alerts.append(_alert("ZERO_SEAT_CHECKIN", "0 seats in master", f"occupancy {after}",
                                     "check-in at a pool with 0 master seats"))
            elif after > cap:
                alerts.append(_alert("OVER_CAPACITY", f"<= {cap}", after, f"occupancy {after}/{cap} ({after / cap:.0%})"))
            else:
                thr = near_threshold(cap)
                if before < thr <= after:   # fires once per upward crossing
                    alerts.append(_alert("NEAR_CAPACITY", f"< {thr}", after,
                                         f"occupancy {after}/{cap} ({after / cap:.0%}) crossed the "
                                         f"{NEAR_CAPACITY_PCT}% bracket"))
            return OccupancyResult(after, cap, alerts)

        # CHECK_OUT
        self.occupancy[key] = max(0, self.occupancy[key] - 1)
        self.open_at[employee].discard(key)
        return OccupancyResult(self.occupancy[key], cap, alerts)
