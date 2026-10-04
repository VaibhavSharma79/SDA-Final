# CLAUDE.md — Seat Integrity Guard

Project context for Claude Code. Read this first, then `IMPLEMENTATION_PLAN.md`, then `README.md`.

## What this is

A course final project (40 marks, team of 3, presented in Lecture 20): an end-to-end streaming pipeline
**Kafka → processing → dashboard**. It checks every ERP seat record and every check-in/out event against a
**master seat table** (the user's `Seat_Count_PAN_India_29052026.xlsx`). A record that matches the master is
**silent**. A mismatch, or a seat pool filling past its approved seats, raises an alert.

**Stack decision (from the user):** everything runs in **Docker** — Kafka, Zookeeper, MySQL, Grafana, *and* the Python
services (engine, producers, alert sink). **Grafana does the dashboarding and the alerting.** The flow is:
Python code runs in containers → writes to MySQL → Grafana reads MySQL for live dashboards and alert rules.

Grading rubric (the work must be defensible against it):

| Component | Weight | What it needs |
|---|---|---|
| Problem Statement | 25% | Clear business problem, why streaming matters |
| Charts & Visualisations | 30% | Quality, relevance, insight of the dashboards |
| Identification of Data | 20% | Sources, schema, volume justification |
| Technical Working | 15% | End-to-end pipeline **live demonstration** |
| Discussion | 10% | Q&A, business implications, future roadmap |

## Current status (be precise about this)

| Verified already (offline, in a sandbox without Docker or Grafana) | NOT yet verified (your job) |
|---|---|
| `alert_rules.py` reproduces the 118-alert answer key exactly (pytest, 7 tests) | `docker compose up --build` actually working (image build, healthchecks, bind mounts) |
| Engine logic against a real MariaDB, with a stand-in for the Kafka client | Kafka's **two listeners** (`kafka:9093` for containers, `localhost:9092` for the host) and the `wurstmeister` images |
| All 15 SQL dashboard queries, as the read-only `grafana` user, MySQL-8 strict mode | `kafka-python` 3.0.11 real `poll`/`commit` behaviour |
| The 5 Grafana alert-rule queries, in a quiet and a provoked database state | **Grafana loading** the provisioned dashboard, datasource and **alerting files**; rules evaluating; instances firing; the webhook delivering |
| `alert_sink.py` receiving Grafana-shaped webhook payloads | **Grafana rendering** the 16 panels (bar gauge labels, colours, alert list) |
| Replay idempotency; live master-table change (118 → 115) | Running on the team's real machines: Linux (Zorin OS 18) and Windows |
| `docker-compose.yml` and all provisioning YAML parse | |

Do not claim something in the right-hand column works until you have run it. Report failures plainly.

## Architecture

```
generate_seat_data.py -> site_sync_events.jsonl -> [site-sync-producer] -> Kafka seat-site-sync --+
                         occupancy_events.jsonl -> [occupancy-producer] -> Kafka seat-occupancy --+--> [engine] seat_guard_consumer.py
                         seat_master.csv (loaded once into MySQL seat_master)                       |    (uses alert_rules.py)
                         expected_alerts.csv (answer key)                  matched -> silent        |
                                                                           mismatch/breach -> alert v
                      [mysql] seat_guard: seat_master, site_sync_log, occupancy_log, pool_state, alerts      + Kafka seat-alerts
                          |
                      [grafana]  dashboard "Seat Integrity Guard" (16 panels, refresh 5 s)
                          |       + 5 provisioned alert rules (evaluated every 10 s) -> webhook contact point
                          v
                      [alert-sink]  prints each notification            [x] = a container
```

Division of labour: **the engine detects** (stateful, per event, exact), **Grafana routes** (who is notified, grouping, repeat). Keep it that way.

## File map

| File | Role |
|---|---|
| `SETUP.md` | Prerequisites for Linux (incl. Zorin OS 18), macOS and Windows. |
| `docker-compose.yml`, `Dockerfile`, `requirements-docker.txt` | The stack. One image (`python:3.12-slim` + deps) serves every Python service; the project folder is **bind-mounted at `/app`**, so code/data edits need a container restart, not a rebuild. |
| `alert_rules.py` | Pure rule logic: `MasterIndex`, `check_site_sync`, `OccupancyTracker`. No Kafka/MySQL imports. |
| `seat_guard_consumer.py` | The engine. Creates schema, loads master, polls both topics in batches, writes MySQL, publishes alerts. |
| `site_sync_producer.py`, `occupancy_producer.py` | Replay the JSONL files, paced by 5-minute bucket (`SLEEP_PER_BUCKET`, default 0.4). Compose profile `replay`. |
| `alert_sink.py` | Webhook receiver; prints Grafana's notifications. |
| `alerts_notifier.py` | Optional: prints alerts from the `seat-alerts` Kafka topic. Not part of the default demo. |
| `verify_alerts.py` | Compares MySQL `alerts` with `expected_alerts.csv`; prints PASS/FAIL, exit code 0/1. |
| `reset_demo.py` | Truncates result tables (`--master` also clears `seat_master`). |
| `generate_seat_data.py` | Deterministic data + reference engine + answer key (seed 42). Needs pandas + openpyxl, which are **not** in the Docker image; run on the host. |
| `grafana/build_dashboard.py` | **Generates** `grafana/provisioning/dashboards/seat_guard.json`. |
| `grafana/build_alerting.py` | **Generates** `grafana/provisioning/alerting/{rules,contactpoints,policies}.yaml`. |
| `tests/test_alert_rules.py` | Offline pytest suite (runs in the engine container). |

## Fixed identifiers (do not rename; the dashboard, alert rules and scripts depend on them)

- Topics: `seat-site-sync`, `seat-occupancy`, `seat-alerts` — **exactly 1 partition each** (created by `KAFKA_CREATE_TOPICS` in compose).
- Kafka addresses: containers `kafka:9093`; host tools `localhost:9092`.
- MySQL: database `seat_guard`; `root`/`root` for the engine; read-only `grafana`/`grafana` (created by the engine) for Grafana. Host name inside Docker: `mysql`.
- Tables: `seat_master`, `site_sync_log`, `occupancy_log`, `pool_state`, `alerts`.
- Grafana: datasource uid `mysql-seatguard`, dashboard uid `seat-integrity-guard`, alert folder `Seat Integrity Guard`, group `seat-integrity-guard`,
  contact point `seat-guard-webhook`, login `admin`/`admin`, port 3000. Alert rule uids: `seat-pool-over-limit`, `seat-pool-near-limit`,
  `seat-zero-seat-checkin`, `seat-double-checkin`, `seat-erp-master-mismatch`.
- Alert sink: container `alert-sink`, port 5001. Consumer groups: `seat-guard-engine`, `seat-guard-notifier`.

## Invariants — do not break these

1. **The answer key is the contract.** With default brackets the engine must raise exactly the 118
   (event_id, rule_id) pairs in `expected_alerts.csv`: CITY_MISMATCH 6, DOUBLE_CHECKIN 8, ENTITY_MISMATCH 8,
   MISSING_LOCATION_CODE 6, NEAR_CAPACITY 17, OFFICE_TYPE_MISMATCH 6, OVER_CAPACITY 33, SEAT_COUNT_MISMATCH 8,
   SEAT_COUNT_VARIANCE 6, UNKNOWN_LOCATION_CODE 8, ZERO_SEAT_CHECKIN 12. Severity split: 33 CRITICAL, 42 HIGH, 20 MEDIUM, 23 LOW.
   Data: 369 site-sync + 27,310 occupancy = 27,679 events; 321 site-sync records match the master silently.
2. **Determinism.** `python generate_seat_data.py` must reproduce the four data files byte-for-byte. Never reorder or add
   RNG calls in the generator. If you must change the data, regenerate all four files together, then update the README
   numbers, the dashboard's two story pools, and this file.
3. **Match key = (location_code, entity, office_type), seats summed per key.** 123 pools, 22,076 seats. Two master rows
   have no location code and are intentionally excluded.
4. **Never `.strip()` / normalise location codes.** `PB_Tripura_Agartala_Chowmuhani ` has a trailing space on purpose.
5. **One partition per topic.** The running head-count is keyed state; DOUBLE_CHECKIN spans pools (it is per employee),
   so a single ordered stream is required.
6. **Writes are idempotent.** Log tables use `ON DUPLICATE KEY UPDATE`; `alerts` is unique on `(trigger_event_id, rule_id)`.
   A replay must not change the alert count.
7. **`alert_rules.py` stays pure** (no Kafka, MySQL, or file I/O). Tests depend on that.
8. **Generated files are never hand-edited.** Dashboard JSON and alerting YAML are produced by `grafana/build_*.py`.
9. **Panel and alert SQL must run under MySQL 8 default `sql_mode`** (`ONLY_FULL_GROUP_BY` etc.) and use no Grafana macros.
10. **Event-based alert rules use `alerts.raised_at` (real insertion time), never `event_ts`** (the simulated 2026-06-01 time), or they would
    never fire. State-based rules read `pool_state`.
11. **Inside Docker, always use service names** (`kafka:9093`, `mysql`, `alert-sink`), never `localhost`.
12. **The ₹10,000 per seat per month is a labelled placeholder assumption**, not a fact. Keep it labelled everywhere it appears.

## Conventions (match the course's earlier projects)

- Plain top-level scripts, configuration via environment variables with defaults (`KAFKA_BROKER`, `MYSQL_HOST`, `MYSQL_PORT`,
  `MYSQL_USER`, `MYSQL_PASSWORD`); compose sets the in-network values. No framework.
- Engine commits to MySQL, then commits Kafka offsets (at-least-once + idempotent writes).
- **ASCII-only console output** in `.py` files and open every file with `encoding="utf-8"` — the team uses both Windows and Linux, and Windows consoles choke on non-ASCII.
- Python 3.12, `kafka-python` 3.0.11, `mysql-connector-python` 26.7.0.
- Docstring at the top of each script explaining what it does.
- Credentials `root/root` and `admin/admin` are course-demo values only; say so, don't "fix" them.

## Environment (platform-independent)

The project must work on **Linux, macOS and Windows**. The team currently uses **Linux (Zorin OS 18: Ubuntu 24.04 base, x86-64)** and **Windows**.
**Docker with the Compose v2 plugin is the only hard requirement** (see `SETUP.md`). Everything for the demo, including the tests, runs in containers;
host Python is optional (only to regenerate data or the generated Grafana files, ideally in a virtual environment).

- Never assume the shell or the Python executable name: it is `python3` on Linux/macOS and `python` on Windows. Give both forms when they differ.
- Repo files must not contain shell-specific tricks. Compose paths are relative (`./`), scripts open files as UTF-8, console output is ASCII.
- The containers never write into the bind-mounted project folder (bytecode and the pytest cache are disabled), so no root-owned files appear on Linux.
- Linux notes: the user must be in the `docker` group (otherwise `permission denied` on the Docker socket); Grafana runs as a non-root user inside its container, so
  `grafana/provisioning` must be world-readable (`chmod -R a+rX grafana/provisioning` if it is not).
- The `wurstmeister/*` images are amd64-only: fine on x86-64 Linux and Windows, but ARM machines (Apple Silicon, ARM Linux) need emulation or a different broker.

| Task | Linux / macOS (bash) | Windows (PowerShell) |
|---|---|---|
| Override a setting for one `up` | `NEAR_CAPACITY_PCT=80 docker compose up -d engine` | `$env:NEAR_CAPACITY_PCT="80"; docker compose up -d engine` (then `Remove-Item Env:NEAR_CAPACITY_PCT`) |
| Slower replay | `SLEEP_PER_BUCKET=1.0 docker compose --profile replay up site-sync-producer occupancy-producer` | `$env:SLEEP_PER_BUCKET="1.0"; docker compose --profile replay up site-sync-producer occupancy-producer` |
| Check a port is free | `ss -ltn \| grep ':9092 '` | `netstat -ano \| findstr :9092` |
| Host Python | `python3` | `python` |

## Commands

```
docker compose up -d --build                                                  # whole stack (wait ~30-60 s)
docker compose logs -f engine                                                 # expect "Seat Integrity Guard started"
docker compose --profile replay up site-sync-producer occupancy-producer      # replay the day (~1 min)
docker compose logs -f alert-sink                                             # Grafana's notifications
docker compose run --rm engine python verify_alerts.py                        # must print PASS
docker compose run --rm engine python -m pytest tests -q                      # offline tests (7)
docker compose run --rm engine python reset_demo.py && docker compose restart engine     # between runs
python3 grafana/build_dashboard.py && python3 grafana/build_alerting.py       # after editing panels / rules (host Python + PyYAML; `python` on Windows)
```

## Known limitations (intentional; Discussion talking points, not bugs to silently fix)

- Running head-counts live in engine memory. A mid-day crash loses them. Observe and document; only implement recovery if the user asks.
- The two **state** alert rules (pool over/near its limit *now*) hold for only a few real seconds at the default replay speed, because the
  replay compresses hours of simulated time. They are best demonstrated with `SLEEP_PER_BUCKET=1.0`. The **event** rules work at any speed.
- The near-limit Grafana rule hard-codes 90%; the engine's bracket is `NEAR_CAPACITY_PCT`. They only agree at the default.
- The synthetic data is deliberately noisy (~1 in 8 site records faulty). Say so; do not "clean" it.
- In the simulation all arrivals precede all departures, so the all-sites peak equals the number of check-ins (13,655, 61.9% of seats).
- `wurstmeister/*` images are old. If they fail on the user's machine, swapping the broker is allowed (see plan, Phase 1), keeping the
  service name `kafka`, the two addresses above, and one partition per topic.

## Definition of done

`IMPLEMENTATION_PLAN.md` Phases 0–7 pass with evidence recorded in `VERIFICATION_REPORT.md`; `docker compose run --rm engine python -m pytest tests -q`
and `docker compose run --rm engine python verify_alerts.py` both pass; every one of the 16 dashboard panels has been seen rendering (screenshots saved);
the five Grafana alert rules have been seen firing and resolving with their notifications arriving at the alert sink; and the README is corrected for
anything that turned out differently on real infrastructure.
