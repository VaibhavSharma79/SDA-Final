#!/usr/bin/env python3
"""
Synthetic ERP event generator for the seat-count master table.

Produces (in --out, default: this folder):
  seat_master.csv          Master reference table, one row per seat pool (loaded into MySQL by the engine).
  site_sync_events.jsonl   Topic 1: ERP's own copy of each site's "markings" (entity, office type,
                           city, seat total). Verified against the master table.
  occupancy_events.jsonl   Topic 2: CHECK_IN / CHECK_OUT events. Running occupancy per seat pool
                           is compared with the master seat count (stateful).
  expected_alerts.csv      Answer key: alerts a correct pipeline must raise (event_id -> rule).

Master match key = (Location Code, Entity, Office Type). Seats are SUMMED per key because the
master has several rows per key (e.g. Malad West has four Policybazaar/Corp rows).

Every alert is injected deliberately (one fault per event) and the reference engine below is
checked against the injection plan, so the answer key is exact. Same --seed => same data.

Usage:
  python generate_seat_data.py            # uses the xlsx next to this file, writes next to this file
"""
import argparse
import csv
import json
import math
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))

# ---- bracket parameters (documented in the workbook's Alert_Rules sheet) -------------------
NEAR_PCT_NUM, NEAR_PCT_DEN = 9, 10     # NEAR_CAPACITY at >= 90% of seats (integer math, no float drift)
VARIANCE_TOL = 0.05                    # seat-count difference <= 5% of master => VARIANCE (LOW), else MISMATCH (HIGH)

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
DEPARTMENTS = ["Sales", "Operations", "Customer Support", "Claims", "IT", "HR", "Finance"]
DEPT_WEIGHTS = [34, 20, 18, 12, 8, 4, 4]


# ===========================================================================================
# Master
# ===========================================================================================
def load_master(path):
    df = pd.read_excel(path, sheet_name="Active")
    coded = df[df["Location Code"].notna()]
    pools = {}
    master_rows = []
    for (code, ent, typ), g in coded.groupby(["Location Code", "Entity", "Office Type"], sort=True):
        pools[(code, ent, typ)] = {"capacity": int(g["Seats Total"].sum()), "city": g["City"].iloc[0]}
        first = g["Start Date"].min()
        master_rows.append({
            "location_code": code, "entity": ent, "office_type": typ, "city": g["City"].iloc[0],
            "seats_total": int(g["Seats Total"].sum()),
            "first_start_date": "" if pd.isna(first) else first.strftime("%Y-%m-%d"),
            "master_rows": len(g),
        })
    code_city = {}
    for (code, _, _), p in pools.items():
        assert code_city.setdefault(code, p["city"]) == p["city"], f"{code} has >1 city"
    ents_by_code = defaultdict(set)
    types_by_code_ent = defaultdict(set)
    for (code, ent, typ) in pools:
        ents_by_code[code].add(ent)
        types_by_code_ent[(code, ent)].add(typ)
    return {
        "master_rows": master_rows,
        "pools": pools,
        "code_city": code_city,
        "ents_by_code": ents_by_code,
        "types_by_code_ent": types_by_code_ent,
        "all_entities": sorted({k[1] for k in pools}),
        "all_types": sorted({k[2] for k in pools}),
        "all_cities": sorted(set(code_city.values())),
        "codes": set(code_city),
    }


def near_threshold(cap):
    return -(-cap * NEAR_PCT_NUM // NEAR_PCT_DEN)  # ceil(cap * 0.9) in integer arithmetic


def ts_iso(dt):
    return dt.isoformat(timespec="milliseconds")


# ===========================================================================================
# Topic 1: site_sync_events
# ===========================================================================================
def gen_site_sync(rng, m, day):
    events = []
    for key in sorted(m["pools"]):
        p = m["pools"][key]
        for _ in range(3):  # ERP publishes each site's record 3x a day
            secs = rng.uniform(6 * 3600, 22 * 3600)
            events.append({
                "_ts": day + timedelta(seconds=secs),
                "event_type": "SITE_SYNC",
                "source_system": "ERP",
                "location_code": key[0],
                "entity": key[1],
                "office_type": key[2],
                "city": p["city"],
                "erp_seats_total": p["capacity"],
            })

    plan = {
        "MISSING_LOCATION_CODE": 6, "UNKNOWN_LOCATION_CODE": 8, "ENTITY_MISMATCH": 8,
        "OFFICE_TYPE_MISMATCH": 6, "CITY_MISMATCH": 6, "SEAT_COUNT_MISMATCH": 8, "SEAT_COUNT_VARIANCE": 6,
    }
    idx = list(range(len(events)))
    rng.shuffle(idx)
    used = set()

    def pick(pred=lambda e: True):
        for i in idx:
            if i not in used and pred(events[i]):
                used.add(i)
                return events[i]
        raise RuntimeError("no event available for injection")

    master_codes_lower = {c.lower() for c in m["codes"]}
    for _ in range(plan["MISSING_LOCATION_CODE"]):
        pick()["location_code"] = None
    for n in range(plan["UNKNOWN_LOCATION_CODE"]):
        e = pick()
        code = e["location_code"]
        while True:
            kind = n % 3
            if kind == 0:   # one-character typo
                pos = rng.choice([i for i, ch in enumerate(code) if ch.isalnum()])
                new_ch = rng.choice([c for c in "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789" if c != code[pos].upper()])
                cand = code[:pos] + new_ch + code[pos + 1:]
            elif kind == 1:  # prefix dropped
                cand = code[3:] if code.startswith("PB_") else "X" + code
            else:            # a site that was never loaded into the master
                cand = rng.choice(["PB_MP_IND_Vijay Nagar", "PB_GJ_SUR_Adajan", "PB_PB_LUD_Ferozepur Road",
                                   "PB_WB_KOL_Salt Lake Sector V", "PB_CG_RAI_Pandri"])
            if cand.lower() not in master_codes_lower:
                e["location_code"] = cand
                break
            n += 3
    for _ in range(plan["ENTITY_MISMATCH"]):
        e = pick()
        e["entity"] = rng.choice([x for x in m["all_entities"] if x not in m["ents_by_code"][e["location_code"]]])
    for _ in range(plan["OFFICE_TYPE_MISMATCH"]):
        e = pick()
        reg = m["types_by_code_ent"][(e["location_code"], e["entity"])]
        e["office_type"] = rng.choice([x for x in m["all_types"] if x not in reg])
    for _ in range(plan["CITY_MISMATCH"]):
        e = pick()
        e["city"] = rng.choice([x for x in m["all_cities"] if x != e["city"]])
    for n in range(plan["SEAT_COUNT_MISMATCH"]):
        e = pick(lambda e: (e["erp_seats_total"] == 0) if n < 2 else (e["erp_seats_total"] >= 10))
        cap = e["erp_seats_total"]
        if cap == 0:
            e["erp_seats_total"] = rng.randint(5, 40)  # ERP thinks a zero-seat site has desks
        else:
            while True:
                v = round(cap * (1 + rng.choice([-1, 1]) * rng.uniform(0.08, 0.35)))
                if abs(v - cap) / cap > VARIANCE_TOL + 0.03 and v != cap:
                    e["erp_seats_total"] = v
                    break
    for _ in range(plan["SEAT_COUNT_VARIANCE"]):
        e = pick(lambda e: e["erp_seats_total"] >= 40)
        cap = e["erp_seats_total"]
        e["erp_seats_total"] = cap + rng.choice([-1, 1]) * rng.randint(1, (cap - 1) // 20)  # strictly < 5%

    events.sort(key=lambda e: e["_ts"])
    for i, e in enumerate(events, 1):
        e["event_id"] = f"SYN-{i:05d}"
    return events, plan


def ref_site_sync(events, m):
    """Reference rule engine for topic 1. First failing check wins."""
    alerts = []
    for e in events:
        code, ent, typ = e["location_code"], e["entity"], e["office_type"]
        rule = exp = act = detail = None
        if code is None or str(code).strip() == "":
            rule, exp, act, detail = "MISSING_LOCATION_CODE", "non-empty location code", "<blank>", "ERP record has no location code"
        elif code not in m["codes"]:
            rule, exp, act, detail = "UNKNOWN_LOCATION_CODE", "code present in master", repr(code), "location code not found in master"
        elif ent not in m["ents_by_code"][code]:
            rule, exp, act, detail = "ENTITY_MISMATCH", " / ".join(sorted(m["ents_by_code"][code])), ent, "entity not registered at this location code"
        elif typ not in m["types_by_code_ent"][(code, ent)]:
            rule, exp, act, detail = "OFFICE_TYPE_MISMATCH", " / ".join(sorted(m["types_by_code_ent"][(code, ent)])), typ, "office type not registered for this code + entity"
        elif e["city"] != m["code_city"][code]:
            rule, exp, act, detail = "CITY_MISMATCH", m["code_city"][code], e["city"], "city differs from master"
        else:
            cap = m["pools"][(code, ent, typ)]["capacity"]
            erp = e["erp_seats_total"]
            if erp != cap:
                if cap == 0:
                    rule, detail = "SEAT_COUNT_MISMATCH", "master has 0 seats, ERP reports seats"
                elif abs(erp - cap) / cap <= VARIANCE_TOL:
                    rule, detail = "SEAT_COUNT_VARIANCE", f"diff {erp - cap:+d} seats ({abs(erp - cap) / cap:.1%}) within {VARIANCE_TOL:.0%} bracket"
                else:
                    rule, detail = "SEAT_COUNT_MISMATCH", f"diff {erp - cap:+d} seats ({abs(erp - cap) / cap:.1%}) exceeds {VARIANCE_TOL:.0%} bracket"
                exp, act = str(cap), str(erp)
        if rule:
            alerts.append({"_ts": e["_ts"], "stream": "SITE_SYNC", "event_id": e["event_id"], "rule_id": rule,
                           "severity": SEVERITY[rule], "location_code": code, "entity": ent, "office_type": typ,
                           "expected": exp, "actual": act, "detail": detail})
    return alerts


# ===========================================================================================
# Topic 2: occupancy_events
# ===========================================================================================
def _clamp_gauss(rng, mu_h, sd_h, lo_h, hi_h):
    """Truncated normal (resample, don't clamp: clamping piles events up at the bounds)."""
    while True:
        v = rng.gauss(mu_h, sd_h)
        if lo_h <= v <= hi_h:
            return v * 3600


def gen_occupancy(rng, m, day):
    pools = m["pools"]
    keys = sorted(pools)
    positive = [k for k in keys if pools[k]["capacity"] > 0]
    zero = [k for k in keys if pools[k]["capacity"] == 0]

    hot = rng.sample([k for k in positive if 10 <= pools[k]["capacity"] <= 400], 10)
    breach = rng.sample([k for k in positive if 10 <= pools[k]["capacity"] <= 800 and k not in hot], 7)
    zero_hit = rng.sample(zero, 5)

    # ---- peak occupancy plan per pool -------------------------------------------------------
    peak, cat, over_k = {}, {}, {}
    for k in positive:
        cap = pools[k]["capacity"]
        thr = near_threshold(cap)
        if k in hot:
            cat[k], peak[k] = "hot", rng.randint(thr, cap)
        elif k in breach:
            over_k[k] = rng.randint(2, 8)
            cat[k], peak[k] = "breach", cap + over_k[k]
        else:
            p = int(cap * rng.uniform(0.35, 0.85))
            p = max(p, 1) if thr > 1 else 0
            cat[k], peak[k] = "normal", min(p, thr - 1)
    for k in zero_hit:
        cat[k], peak[k] = "zero", rng.randint(1, 3)

    # ---- unique employees ---------------------------------------------------------------------
    total_people = sum(peak.values()) + 40
    emp_ids = [f"E{n:06d}" for n in rng.sample(range(100000, 999999), total_people)]
    emp_iter = iter(emp_ids)

    events = []
    stays = defaultdict(list)  # pool -> [(emp, t_in_s, t_out_s, dept)]

    def add(t_s, etype, emp, dept, k):
        events.append({
            "_ts": day + timedelta(seconds=t_s + rng.random() * 0.9),
            "event_type": etype, "source_system": "ERP-WPM",
            "employee_id": emp, "department": dept,
            "location_code": k[0], "entity": k[1], "office_type": k[2], "city": pools[k]["city"],
        })

    for k in sorted(peak):
        for _ in range(peak[k]):
            emp = next(emp_iter)
            dept = rng.choices(DEPARTMENTS, DEPT_WEIGHTS)[0]
            t_in = _clamp_gauss(rng, 9.5, 0.9, 7.5, 12.5)
            t_out = _clamp_gauss(rng, 18.3, 1.0, 15.75, 21.5)
            stays[k].append((emp, t_in, t_out, dept))
            add(t_in, "CHECK_IN", emp, dept, k)
            add(t_out, "CHECK_OUT", emp, dept, k)

    # ---- double check-in (same person badged at 2 sites while still inside the first) -----------
    n_double = 8
    added_b = defaultdict(int)
    doubles = 0
    src_pools = [k for k in sorted(peak) if cat[k] == "normal" and peak[k] >= 20]
    dst_pools = [k for k in positive if cat[k] == "normal" and pools[k]["capacity"] >= 15]
    while doubles < n_double:
        a = rng.choice(src_pools)
        emp, t_in, t_out, dept = rng.choice(stays[a])
        b = rng.choice(dst_pools)
        if pools[b]["city"] == pools[a]["city"]:
            continue
        # keep B below the 90% bracket even with the extra phantom person
        if peak[b] + added_b[b] + 1 > near_threshold(pools[b]["capacity"]) - 1:
            continue
        added_b[b] += 1
        add(rng.uniform(12.75, 15.5) * 3600, "CHECK_IN", emp, dept, b)   # inside A's plateau window
        add(rng.uniform(16.0, 19.0) * 3600, "CHECK_OUT", emp, dept, b)
        doubles += 1

    events.sort(key=lambda e: e["_ts"])
    for i, e in enumerate(events, 1):
        e["event_id"] = f"OCC-{i:06d}"

    plan = {
        "ZERO_SEAT_CHECKIN": sum(peak[k] for k in zero_hit),
        "OVER_CAPACITY": sum(over_k.values()),
        "NEAR_CAPACITY": len(hot) + len(breach),   # one upward crossing per hot/breach pool
        "DOUBLE_CHECKIN": n_double,
    }
    return events, plan


def ref_occupancy(events, m):
    """Reference stateful engine for topic 2 (keyed by pool + keyed by employee)."""
    pools = m["pools"]
    occ = defaultdict(int)
    open_at = defaultdict(set)
    alerts = []

    def emit(e, rule, exp, act, detail):
        alerts.append({"_ts": e["_ts"], "stream": "OCCUPANCY", "event_id": e["event_id"], "rule_id": rule,
                       "severity": SEVERITY[rule], "location_code": e["location_code"], "entity": e["entity"],
                       "office_type": e["office_type"], "expected": str(exp), "actual": str(act), "detail": detail})

    for e in events:
        k = (e["location_code"], e["entity"], e["office_type"])
        cap = pools[k]["capacity"]
        emp = e["employee_id"]
        if e["event_type"] == "CHECK_IN":
            before = occ[k]
            occ[k] += 1
            after = occ[k]
            others = sorted(o[0] for o in open_at[emp] if o != k)
            if others:
                emit(e, "DOUBLE_CHECKIN", "no open check-in elsewhere", f"still inside {others[0]}",
                     f"{emp} checked in at {k[0]} while still checked in at {others[0]}")
            open_at[emp].add(k)
            if cap == 0:
                emit(e, "ZERO_SEAT_CHECKIN", "0 seats in master", f"occupancy {after}", "check-in at a pool with 0 master seats")
            elif after > cap:
                emit(e, "OVER_CAPACITY", f"<= {cap}", after, f"occupancy {after}/{cap} ({after / cap:.0%})")
            else:
                thr = near_threshold(cap)
                if before < thr <= after:
                    emit(e, "NEAR_CAPACITY", f"< {thr}", after, f"occupancy {after}/{cap} ({after / cap:.0%}) crossed the 90% bracket")
        else:
            occ[k] = max(0, occ[k] - 1)
            open_at[emp].discard(k)
    return alerts


# ===========================================================================================
def strip_private(rows):
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="Seat_Count_PAN_India_29052026.xlsx")
    ap.add_argument("--out", default=".")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--date", default="2026-06-01", help="business day to simulate (IST)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rng = random.Random(a.seed)
    day = datetime.strptime(a.date, "%Y-%m-%d").replace(tzinfo=IST)

    m = load_master(a.master)
    sync, sync_plan = gen_site_sync(rng, m, day)
    occ, occ_plan = gen_occupancy(rng, m, day)

    alerts = ref_site_sync(sync, m) + ref_occupancy(occ, m)

    # Answer key must equal the injection plan, otherwise the data is not what we think it is.
    got = defaultdict(int)
    for al in alerts:
        got[al["rule_id"]] += 1
    for rule, n in {**sync_plan, **occ_plan}.items():
        assert got[rule] == n, f"{rule}: planned {n}, engine produced {got[rule]}"
    assert set(got) == set(SEVERITY), set(SEVERITY) ^ set(got)

    alerts.sort(key=lambda r: (r["_ts"], r["event_id"]))
    for i, al in enumerate(alerts, 1):
        al["alert_id"] = f"ALR-{i:04d}"
        al["event_ts"] = ts_iso(al["_ts"])

    def dump(path, rows, cols):
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                r = dict(r, event_ts=ts_iso(r["_ts"]))
                f.write(json.dumps({c: r[c] for c in cols}, ensure_ascii=False) + "\n")

    dump(os.path.join(a.out, "site_sync_events.jsonl"), sync,
         ["event_id", "event_ts", "event_type", "source_system", "location_code", "entity", "office_type", "city", "erp_seats_total"])
    dump(os.path.join(a.out, "occupancy_events.jsonl"), occ,
         ["event_id", "event_ts", "event_type", "source_system", "employee_id", "department",
          "location_code", "entity", "office_type", "city"])
    mcols = ["location_code", "entity", "office_type", "city", "seats_total", "first_start_date", "master_rows"]
    with open(os.path.join(a.out, "seat_master.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mcols)
        w.writeheader()
        w.writerows(m["master_rows"])
    cols = ["alert_id", "stream", "event_id", "rule_id", "severity", "event_ts", "location_code", "entity",
            "office_type", "expected", "actual", "detail"]
    with open(os.path.join(a.out, "expected_alerts.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows({c: r[c] for c in cols} for r in alerts)

    print(f"pools in master      : {len(m['pools'])}  (seats {sum(p['capacity'] for p in m['pools'].values())})")
    print(f"site_sync_events     : {len(sync)}")
    print(f"occupancy_events     : {len(occ)}")
    print(f"expected alerts      : {len(alerts)}")
    for rule in SEVERITY:
        print(f"   {rule:24s} {SEVERITY[rule]:9s} {got[rule]}")


if __name__ == "__main__":
    main()
