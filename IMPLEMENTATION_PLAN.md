# IMPLEMENTATION_PLAN.md — Seat Integrity Guard

A runbook for Claude Code. The code already exists and was verified offline (see `CLAUDE.md`, "Current status").
Your job is to **bring the whole Docker stack up, prove the pipeline works on real Kafka, MySQL and Grafana, prove Grafana's dashboards and
alerting work, fix what doesn't, and produce the presentation assets** the grading rubric needs.

Architecture in one line: Python containers (engine + producers) → Kafka → engine → **MySQL** → **Grafana** (dashboards *and* alerting) → webhook (`alert-sink`).
Everything is Docker. Use service names inside the network (`kafka:9093`, `mysql`), `localhost` only from the host. The project must work on Linux, macOS and Windows; read `SETUP.md` and the Environment section of `CLAUDE.md`, and give shell commands in the form that matches the machine you are on.

## How to run this plan

Open this folder in Claude Code and say:

> Read CLAUDE.md, then execute IMPLEMENTATION_PLAN.md phase by phase. After each phase, stop and report PASS or FAIL with
> the evidence (command output, counts, screenshots). Do not continue past a failing phase without telling me. Never change an
> expected value just to make a check pass: if a check fails, find out why.

Rules while executing:

- Record every phase's outcome in `VERIFICATION_REPORT.md` (template at the end).
- If you change any file, say which and why, and re-run the offline tests and (after a full replay) `verify_alerts.py`.
- Anything in the "NOT yet verified" column of `CLAUDE.md` is a hypothesis until you have run it.
- Stay within the invariants in `CLAUDE.md`. If an invariant blocks you, stop and ask.
- Generated files (`seat_guard.json`, `alerting/*.yaml`) are changed only by editing `grafana/build_*.py` and re-running them.

---

## Phase 0 — Orientation and offline checks

1. Read `CLAUDE.md`, `README.md`, `docker-compose.yml`, `alert_rules.py`, `seat_guard_consumer.py`, `grafana/build_alerting.py`.
2. `docker --version`, `docker compose version`, `docker run --rm hello-world` (on Linux this must work **without sudo**; if not, see `SETUP.md`), then validate the compose file: `docker compose config -q` (no output = valid).
3. Check ports 2181, 9092, 3306, 3000, 5001 are free (Linux: `ss -ltn | grep -E ':(2181|9092|3306|3000|5001) '`; macOS: `lsof -nP -iTCP -sTCP:LISTEN`; Windows: `netstat -ano | findstr :9092`).
4. Build the image and run the offline tests inside it (no Kafka/MySQL involved):
   ```
   docker compose build
   docker compose run --rm --no-deps engine python -m pytest tests -q
   ```
5. Optional (needs host Python with pandas + openpyxl): confirm determinism by regenerating the data into a temp folder and comparing with the
   shipped files (`cmp` on Linux/macOS, `fc /b` on Windows). They must be identical.

**Pass when:** the compose file validates, the image builds, 7 tests pass, ports are free.

---

## Phase 1 — Bring the whole stack up

```
docker compose up -d --build
docker compose ps
```

Expected: `zookeeper`, `kafka` (healthy), `mysql` (healthy), `grafana`, `engine`, `alert-sink` all running. The `site-sync-producer` and `occupancy-producer`
services are **not** started (they have the `replay` profile). Then:

```
docker compose logs engine                 # expect: "Seat Integrity Guard started", "master pools: 123 (loaded 123 from seat_master.csv)"
docker compose logs alert-sink             # expect: "Alert sink listening on :5001"
docker compose exec mysql mysqladmin ping -uroot -proot            # mysqld is alive
curl -s http://localhost:3000/api/health                            # "database": "ok"
```

The engine must be **running, not restart-looping** (`docker compose ps` should show a stable uptime). It has `restart: on-failure` and waits for healthy Kafka and MySQL.

**If the `wurstmeister/*` images fail** (they are old; they may not start on some machines): replace the `zookeeper` + `kafka` services with a single-node KRaft
broker (for example `apache/kafka` or `bitnami/kafka`). Constraints: the service must stay named `kafka`; keep **both** listeners (`kafka:9093` inside Docker,
`localhost:9092` for the host); replication factors 1; keep the three topics at one partition (Phase 2). Note the change in `VERIFICATION_REPORT.md` and the README.

**Pass when:** all services are up, the engine is stable, MySQL and Grafana answer.

---

## Phase 2 — Kafka listeners and topics

The engine runs in a container and reaches Kafka at `kafka:9093`; tools on the user's machine use `localhost:9092`. Both must work.

```
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9093 --list           # the INSIDE listener
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list       # the OUTSIDE listener (metadata advertises localhost:9092)
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9093 --describe       # partitions and replication
```

Also confirm port 9092 is reachable from the host (PowerShell: `Test-NetConnection localhost -Port 9092`; bash: `nc -z localhost 9092`).

Topics come from `KAFKA_CREATE_TOPICS` in the compose file. If they do not exist, or show more than one partition, fix the compose file (or add a small
`create_topics.py` using `kafka.admin.KafkaAdminClient` and `NewTopic(..., num_partitions=1, replication_factor=1)`) — invariant 5 depends on it.

**Pass when:** both listeners answer, `seat-site-sync`, `seat-occupancy`, `seat-alerts` exist, each with `PartitionCount: 1`.

---

## Phase 3 — End-to-end run on real Kafka

1. Start from clean tables (skip on a brand-new stack where the database was just created and is empty):
   `docker compose run --rm engine python reset_demo.py` then `docker compose restart engine`.
2. In one terminal follow `docker compose logs -f engine`; in another `docker compose logs -f alert-sink`.
3. Start the replay: `docker compose --profile replay up site-sync-producer occupancy-producer` (about a minute at the default pace; use
   `SLEEP_PER_BUCKET=0.1` for a quick check).
4. Wait until both producers have exited **and** the engine has caught up. Confirm zero consumer lag:
   ```
   docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9093 --describe --group seat-guard-engine
   ```
5. `docker compose run --rm engine python verify_alerts.py`

**Pass when (all of these):**

| Check | Expected |
|---|---|
| `verify_alerts.py` | `PASS`, exit code 0, 118 alerts |
| `site_sync_log` rows | 369 (321 with verdict `OK`) |
| `occupancy_log` rows | 27,310 |
| `alerts` rows | 118 (CRITICAL 33, HIGH 42, MEDIUM 20, LOW 23) |
| `seat_master` rows | 123 |
| `pool_state` rows | 122 (one pool has no events) |
| consumer lag for `seat-guard-engine` | 0 |
| messages on `seat-alerts` | 118 (`docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:9093 --topic seat-alerts --time -1`) |
| every pool back to zero at end of day | `SELECT COUNT(*) FROM pool_state WHERE occupancy > 0;` → 0 |

Query MySQL with, for example:
`docker compose exec mysql mysql -uroot -proot seat_guard -e "SELECT severity, COUNT(*) FROM alerts GROUP BY severity;"`

**Real-Kafka behaviours to check and record** (only tested against a stand-in so far):

- `consumer.poll(timeout_ms=1000, max_records=500)` returns batches and `records.values()` iterates correctly; note typical batch sizes.
- Offsets are committed after each DB commit (lag returns to 0).
- Starting the producers before or after the engine both work (`auto_offset_reset="earliest"`).
- Both topics are consumed concurrently while the producers run in parallel (ordering across topics is not guaranteed and must not matter).
- The engine container's `producer.send` to `seat-alerts` works through the INSIDE listener.

If the alert count is not 118: first check no bracket override is set (`docker compose exec engine env | grep -E "NEAR|VARIANCE"`), `seat_master` was not edited, and tables were
reset; then look at which (event, rule) pairs differ in the `verify_alerts.py` output before changing any code.

---

## Phase 4 — Grafana dashboard, verified for real

### 4.1 Provisioning

```
curl -s -u admin:admin http://localhost:3000/api/datasources/uid/mysql-seatguard/health      # status OK
curl -s -u admin:admin "http://localhost:3000/api/search?query=Seat"                          # dashboard uid seat-integrity-guard
curl -s -u admin:admin http://localhost:3000/api/dashboards/uid/seat-integrity-guard          # 16 panels
```

### 4.2 Every SQL panel query runs through Grafana's own query API

Extract each panel's `rawSql` from `grafana/provisioning/dashboards/seat_guard.json` (15 panels have SQL; the alert list does not), replace `${seat_cost}`
with `10000`, and POST it:

```
POST http://localhost:3000/api/ds/query   (basic auth admin:admin)
{"queries":[{"refId":"A","datasource":{"type":"mysql","uid":"mysql-seatguard"},"rawSql":"<SQL>","format":"table"}],
 "from":"1780272000000","to":"1780358400000"}
```

(`from`/`to` = 2026-06-01T00:00Z and 2026-06-02T00:00Z in epoch ms, the dashboard's fixed window. Use `"format":"time_series"` for the time-series panels and the
alerts-over-time panel.)

**Pass when:** all 15 return no error and non-empty data.

### 4.3 Look at the rendered dashboard

Install Playwright on the host (in a virtual environment: `pip install playwright`, `playwright install chromium`; on Linux also `playwright install-deps chromium`, which needs sudo), log in at `http://localhost:3000` (`admin`/`admin`; skip any password-change
prompt), open `http://localhost:3000/d/seat-integrity-guard`, wait for panels to load, and save screenshots to `docs/screenshots/`: one full-page shot plus one per
panel (`?viewPanel=<id>`). **Look at the images** and check each against this list (after a completed replay):

| # | Panel | What you must see |
|---|---|---|
| 1 | Events Processed | 27,679 |
| 2 | Alerts Raised | 118 |
| 3 | Critical: Pool Over Its Seat Limit | 33, red |
| 4 | ERP vs Master Match Rate | 87.0%, amber |
| 5 | Pools That Went Over Their Limit | 7, red |
| 6 | Cost Exposure (₹/month) | about ₹6.24 million (624 seats x ₹10,000) |
| 7 | Alerts by Rule | 11 horizontal bars; OVER_CAPACITY longest (33), then NEAR_CAPACITY (17), ZERO_SEAT_CHECKIN (12) |
| 8 | Alerts Over Time by Severity | stacked bars between about 06:30 and 22:00 with severity colours; busiest 15-minute bucket 10:30 with 14 alerts |
| 9 | People In Office vs Approved Seats | bell-shaped blue curve peaking at 13,655 under a dashed grey line at 22,076 |
| 10 | Peak Utilisation by Pool | **15 separately labelled bars**, descending; top `PB_UP_VAR_Maldhaiya \| PB Partner` 135.7%, then 125.0% for `PB_MH_NAS_Mumbai_Naka`, `PB_TN_CHE_T.Nagar`, `PB_UP_NOI_Sector 59`; green below 90, amber 90–100, red above 100 |
| 11 | Two Pools, Two Stories | red P116 Gurgaon line climbs to 303 and crosses its dashed 298 line; orange Diamond District Bengaluru line peaks at 366 and stays under its dashed 377 line |
| 12 | Latest Alerts | 50 rows, severity words coloured (row order may differ from an offline run; do not assert it) |
| 13 | ERP vs Master mismatches | 14 rows; largest gap `PB_KL_KOC_Ernakulam` / Policybazaar, master 658, ERP 872, +214 (32.5%); verdict words coloured |
| 14 | Alerts in the Last Minute (live) | **moves**: non-zero during the replay and for up to 60 s after it, then returns to 0 (green) |
| 15 | Events per Second (live, last 10 s) | **moves**: well above 0 during the replay (record the peak), 0 when idle |
| 16 | Grafana Alert Rules (live state) | lists the 5 rules; Normal at rest, Firing during the replay (see Phase 5) |

Also confirm: the time axis reads in UTC but shows the simulated IST wall-clock times as stored (06:00–22:00); changing the "Assumed monthly cost per seat" box to `5000`
halves tile 6; the dashboard refreshes by itself every 5 seconds (watch panels 14–15 during a replay).

### 4.4 Fix what doesn't render

Edit `grafana/build_dashboard.py`, run `python3 grafana/build_dashboard.py` (`python` on Windows; host Python + PyYAML), wait up to 30 seconds (provisioning poll) or `docker compose restart grafana`,
and re-check. Known risks and the likely fix:

| Symptom | Likely fix |
|---|---|
| Datasource health error about the database | move `database` under `jsonData: {database: seat_guard}` in `grafana/provisioning/datasources/mysql.yaml` |
| `Access denied for user 'grafana'` | the engine creates this user; check the engine started; or create it by hand (`CREATE USER 'grafana'@'%' IDENTIFIED BY 'grafana'; GRANT SELECT ON seat_guard.* TO 'grafana'@'%';`) |
| Auth plugin / `caching_sha2_password` error | `ALTER USER 'grafana'@'%' IDENTIFIED WITH mysql_native_password BY 'grafana';` |
| Panel 10 shows one bar, or no labels | keep `reduceOptions.values = true`; keep the string column `pool` first; try `"fields": ""` |
| Severity / verdict words not coloured | switch `custom.cellOptions` to legacy `custom.displayMode: "color-text"` or vice versa |
| Panel 16 empty / wrong | check the `alertInstanceLabelFilter` (`{app="seat-integrity-guard"}`) matches the rule labels; check the state filter |
| "No data" everywhere | the dashboard window is fixed to 2026-06-01; engine not finished; or timezone offset (dashboard timezone is `utc` on purpose) |
| `${seat_cost}` SQL error | the textbox variable must keep a non-empty default (`10000`) |
| MySQL 8 `ONLY_FULL_GROUP_BY` error | rewrite the query to be strictly compliant; do not change the server's `sql_mode` |

**Pass when:** all 16 panels render as described, with screenshots saved in `docs/screenshots/`.

---

## Phase 5 — Grafana alerting, verified for real

This is the part of the project nobody has run yet. Treat every assumption as a hypothesis.

### 5.1 Provisioning loaded

```
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules          # 5 rules, folder "Seat Integrity Guard"
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/contact-points       # seat-guard-webhook -> http://alert-sink:5001/
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/policies             # root receiver seat-guard-webhook
docker compose logs grafana | grep -iE "provision|alert" | tail -30                    # no errors
```

### 5.2 Rules healthy at rest

```
curl -s -u admin:admin http://localhost:3000/api/prometheus/grafana/api/v1/rules
```

Every one of the 5 rules must show `"health": "ok"` and `"state": "inactive"` (Normal) when the stream is idle. **No rule may be in `error` or `nodata`.** The state rules return
no rows when nothing is wrong (this is intended; `noDataState` is `OK`).

### 5.3 Rules fire during a replay, and the notifications arrive

Reset (Phase 3, step 1), then run the replay at the default pace while watching `docker compose logs -f alert-sink` and Alerting > Alert rules in the UI. Expected:

| Rule | Behaviour during a default-speed replay |
|---|---|
| ERP record disagrees with the master | fires within about 15–45 s (10 s evaluation + 5 s group wait); resolves roughly 1.5 minutes after the last site-sync alert |
| Check-in at a site with zero approved seats | fires when the engine raises its first ZERO_SEAT_CHECKIN, then resolves after the 60 s window empties |
| Badge used at two sites at once | same, from the first DOUBLE_CHECKIN |
| Pool over / near its limit (live) | **may not be caught** at the default speed (a pool is over its limit for only a few real seconds); see 5.4 |

In `alert-sink` you must see lines like `[hh:mm:ss] FIRING    [high] <rule title>: <summary text>` and, later, `RESOLVED`. Check that the summary text contains **real numbers**
(for event rules) and **the pool name** (for state rules).

### 5.4 State rules, with a slow replay

Reset, then replay slowly: `SLEEP_PER_BUCKET=1.0 docker compose --profile replay up site-sync-producer occupancy-producer` (Windows PowerShell form in `CLAUDE.md` and `SETUP.md`).
Expected: `Pool over its seat limit (live)` fires per pool (for example `PB_HR_GUR_P116-Sector44 | Policybazaar | Corp`) while its head-count is above its seats, and resolves once people
check out; `Pool near its seat limit (live)` fires for pools at 90% or more (for example Diamond District Bengaluru). Record which pools fired and for how long.

If instances fire but the summary shows an empty pool name, the SQL's `metric` column is not becoming a label. Likely fix: keep `format: time_series` but make sure the column is named
`metric`; otherwise change the annotation to use `{{ $labels }}` and inspect which label keys Grafana actually produced, then update `grafana/build_alerting.py`.

### 5.5 Screenshots and clean-up

Save to `docs/screenshots/`: Alerting > Alert rules showing the five rules (one firing), one firing instance expanded, the contact point, and an excerpt of the `alert-sink` log (copy it into
`docs/alert_sink_log.txt`). Use the contact point's "Test" button once and confirm `alert-sink` receives it.

### 5.6 Fix what doesn't work

Edit `grafana/build_alerting.py`, run `python3 grafana/build_alerting.py` (`python` on Windows; host Python + PyYAML), then `docker compose restart grafana`. Known risks:

| Symptom | Likely fix |
|---|---|
| Provisioning error "invalid" for a rule | compare with an alert rule exported from the Grafana UI (Alerting > Alert rules > Export) and align the field names; Grafana versions differ slightly |
| Rule health `error`: duplicate label set / multiple series without labels | state-rule queries must return a distinct `metric` per row |
| Rule health `error`: datasource | uid must be `mysql-seatguard`; the `grafana` MySQL user must exist (engine ran) |
| Event rules never fire | check `raised_at` is recent: `SELECT NOW(), MAX(raised_at) FROM alerts;`. If MySQL and Grafana disagree on time zone, adjust the SQL (never use `event_ts`) |
| Fires but nothing reaches `alert-sink` | `docker compose exec grafana wget -qO- http://alert-sink:5001/` should answer; check the notification policy and contact point; check `docker compose logs grafana` for notifier errors |
| Rules stuck in `Pending` | `for` must be `0s` |
| Provisioned contact point cannot be edited in the UI | expected: provisioned objects are read-only; edit the generator instead |

**Pass when:** 5.1–5.3 pass, 5.4 is recorded, and every Grafana-side failure found has been fixed or documented.

---

## Phase 6 — Scripted demo scenarios (each rehearsed and recorded)

Each scenario starts from `docker compose run --rm engine python reset_demo.py` and `docker compose restart engine` unless stated.

**A. Live master change before the replay.** With the engine running and no events yet:

```
docker compose exec mysql mysql -uroot -proot seat_guard -e "UPDATE seat_master SET seats_total = 400 WHERE location_code = 'PB_HR_GUR_P116-Sector44' AND entity = 'Policybazaar' AND office_type = 'Corp';"
```

Wait for `[master] reference table changed` in `docker compose logs engine` (within 10 s), then replay. **Expected:** 115 alerts total. For that pool: no OVER_CAPACITY, no NEAR_CAPACITY,
and 3 SEAT_COUNT_MISMATCH (ERP still says 298). `verify_alerts.py` will report differences by design. Restore with `docker compose run --rm engine python reset_demo.py --master`
and restart the engine.

**B. Live master change during the replay.** Same SQL, executed while the occupancy producer is running. Record what changes and when. The point is that the engine reacts without a restart.

**C. Policy challenge.** Predict first: lowering the near-capacity bracket from 90% to 80% adds NEAR_CAPACITY alerts only. Then `NEAR_CAPACITY_PCT=80 docker compose up -d engine`
(Windows PowerShell form in `CLAUDE.md` and `SETUP.md`), reset, replay. **Expected:** NEAR_CAPACITY 17 → 30, total 118 → 131, OVER_CAPACITY unchanged at 33. Afterwards recreate the engine without the override.

**D. Replay idempotency.** Run both producers a second time without resetting. **Expected:** `alerts` stays 118, `site_sync_log` 369, `occupancy_log` 27,310.

**E. Engine restart mid-run (limitation, for the Discussion).** `docker compose stop engine` at roughly half way, `docker compose start engine`, let the replay finish. Running head-counts live in memory and are
lost, so alerts may differ from the answer key. **Do not fix this.** Record what happens (which rules are affected, how many alerts differ).

**F. A fresh machine.** `docker compose down -v`, then `docker compose up -d --build` and a replay, timing it. This is the "can the markers reproduce it" test; record the time and any manual step you needed.

Update the README with any figure that differs from what it currently says.

**Pass when:** A, C, D and F match the expected numbers exactly; B and E are recorded.

---

## Phase 7 — Presentation assets (rubric-driven)

Create these in `docs/`, using the **numbers from your own real run**, not the numbers in this file:

1. `DEMO_SCRIPT.md` — a timed run-of-show, assuming 10 minutes (state the assumption; make it easy to rescale). Suggested split by rubric weight: Problem Statement about 2.5 min,
   Data about 2 min, live pipeline about 1.5 min, dashboard and alerting walk-through about 3 min, Discussion/Q&A the remainder. For every step: what to click or run, what to say, and the
   fallback if it fails live (use the screenshots and `docs/alert_sink_log.txt`, or a recorded video if the user provides one). The team has three members: propose a speaking split.
2. `SLIDES_OUTLINE.md` — one slide per rubric section, each with specific numbers and the screenshot to put on it. The business-case slide must carry the quantified findings, with the ₹ figure
   labelled as an assumption. Include a slide on the two-layer design (the engine detects, Grafana routes).
3. `QA_PREP.md` — short, honest answers to likely questions. Seed list: Why streaming and not a nightly batch? Why Kafka? Why is alerting in Grafana and not in the Python code? What happens if the engine
   crashes? How would it scale (partitions, keys, state stores)? What about false positives and alert fatigue? Why is the data synthetic and how do you know the pipeline is correct? What are the
   privacy implications of check-in events? What is the ₹ figure based on? What would you build next?
4. A one-page cheat-sheet of the exact commands and order for the live demo (PowerShell and bash forms), including the slow-replay variant for the state alert rules.

**Pass when:** the four files exist, quote only numbers you verified, and the screenshots they reference exist.

---

## Phase 8 — Optional hardening (only after Phases 0–7 pass, and ask the user before starting)

In priority order. Each must keep the tests passing and either keep `verify_alerts.py` at PASS or change it consistently and say so.

1. **Alert suppression.** `ALERT_SUPPRESS_MINUTES` (default `0` = off, so the answer key stays valid): at most one alert per (pool, rule) per window. Report the reduction on the real data and add a test.
2. **Engine state recovery.** On startup, rebuild the `OccupancyTracker` from `occupancy_log` (ordered by `event_ts, event_id`) without emitting alerts, and skip any incoming event whose `event_id` is already
   in the log (otherwise an event written but not yet committed to Kafka would be counted twice). Prove it by repeating Scenario E and getting `verify_alerts.py` PASS.
3. **A real notification channel.** Add an email or Slack receiver to `grafana/build_alerting.py` (the user supplies credentials via environment variables; never commit secrets).
4. **Unit tests for edge cases** in `tests/`: blank-string location code, trailing-space code, capacity 1 and 0 pools, check-out before check-in, unknown pool in the occupancy stream.

---

## Final checklist

- [ ] Phases 0–7 passed, with evidence in `VERIFICATION_REPORT.md`
- [ ] `docker compose run --rm engine python -m pytest tests -q` passes; `docker compose run --rm engine python verify_alerts.py` prints PASS after a clean full replay
- [ ] Screenshots of all 16 panels and of the alerting screens saved; nothing looked wrong in them
- [ ] All five Grafana alert rules seen firing and resolving, with notifications in the alert sink log
- [ ] README corrected for every difference found on real infrastructure (commands, compose changes, Windows notes)
- [ ] `docs/` presentation assets created from verified numbers
- [ ] No file other than those listed in your report was changed; generated files were changed only through their generators

---

## Appendix A — Spec reference (so any file can be rebuilt from this document)

**Engine rules** (`alert_rules.py`; first failing check wins within a stream). Brackets: `NEAR_CAPACITY_PCT=90`, `SEAT_VARIANCE_TOLERANCE=0.05`.

| Rule | Stream | Severity | Fires when |
|---|---|---|---|
| MISSING_LOCATION_CODE | site sync (also checked on occupancy) | HIGH | code is null or blank |
| UNKNOWN_LOCATION_CODE | site sync | HIGH | code not in master (exact match, case and space sensitive) |
| ENTITY_MISMATCH | site sync | MEDIUM | (code, entity) not in master |
| OFFICE_TYPE_MISMATCH | site sync | MEDIUM | (code, entity, office type) not in master |
| CITY_MISMATCH | site sync | MEDIUM | city differs from the master city for that code |
| SEAT_COUNT_MISMATCH | site sync | HIGH | ERP seats differ from summed master seats by more than 5%, or master is 0 and ERP > 0 |
| SEAT_COUNT_VARIANCE | site sync | LOW | 0 < difference <= 5%. An exact match is silent |
| ZERO_SEAT_CHECKIN | occupancy | HIGH | check-in at a pool whose master seats = 0 |
| NEAR_CAPACITY | occupancy | LOW | running occupancy first reaches ceil(90% of seats); once per upward crossing |
| OVER_CAPACITY | occupancy | CRITICAL | running occupancy exceeds seats; one alert per offending check-in |
| DOUBLE_CHECKIN | occupancy | HIGH | employee checks in at a second pool while still checked in at the first |

The near-capacity threshold uses integer ceiling (`-(-cap * pct // 100)`); no floating point.

**Grafana alert rules** (`grafana/build_alerting.py`; group `seat-integrity-guard`, folder `Seat Integrity Guard`, evaluated every 10 s, `for: 0s`, `noDataState: OK`; query → Reduce(last) → Threshold):

| uid | Kind | Source | Threshold | Severity label |
|---|---|---|---|---|
| `seat-pool-over-limit` | state, one instance per pool | `pool_state` where `occupancy > capacity` (value = ratio) | > 1 | critical |
| `seat-pool-near-limit` | state, one instance per pool | `pool_state` where ratio is 0.9–1.0 | > 0 | warning |
| `seat-zero-seat-checkin` | event | count of `alerts` with `rule_id='ZERO_SEAT_CHECKIN'` and `raised_at` in the last 60 s | > 0 | high |
| `seat-double-checkin` | event | same, `DOUBLE_CHECKIN` | > 0 | high |
| `seat-erp-master-mismatch` | event | same, `stream='SITE_SYNC'` and `rule_id <> 'SEAT_COUNT_VARIANCE'` | > 0 | high |

Contact point `seat-guard-webhook` (POST to `http://alert-sink:5001/`); notification policy: root receiver `seat-guard-webhook`, group by `grafana_folder, alertname`, group wait 5 s,
group interval 30 s, repeat 1 h. Every rule carries labels `severity` and `app=seat-integrity-guard`.

**Topics and events**

```
seat-site-sync   event_id, event_ts (ISO-8601 +05:30), event_type=SITE_SYNC, source_system=ERP, location_code (nullable),
                 entity, office_type, city, erp_seats_total                               [+ produced_at added by the producer]
seat-occupancy   event_id, event_ts, event_type (CHECK_IN|CHECK_OUT), source_system=ERP-WPM, employee_id, department,
                 location_code, entity, office_type, city                                  [key = location_code|entity|office_type]
seat-alerts      trigger_event_id, stream, rule_id, severity, event_ts, location_code, entity, office_type, expected, actual, detail
```

**MySQL tables** (created by the engine; DDL is in `seat_guard_consumer.py`): `seat_master` (PK location_code, entity, office_type; seats_total), `site_sync_log` (event_id PK; master_seats; verdict;
received_at), `occupancy_log` (event_id PK; occupancy_after; capacity; received_at), `pool_state` (PK pool; capacity, occupancy, peak_occupancy), `alerts` (alert_id auto; UNIQUE trigger_event_id + rule_id;
expected_value, actual_value, detail; raised_at). Event timestamps are `DATETIME(3)`, naive IST wall-clock; `received_at` / `raised_at` are real insertion times.

**Dashboard panels** (generated by `grafana/build_dashboard.py`; 24-column grid; refresh 5 s; window fixed to 2026-06-01):

| Row | Panels |
|---|---|
| 1 (stat tiles, w=4 each) | Events Processed · Alerts Raised · Critical: Pool Over Its Seat Limit · ERP vs Master Match Rate · Pools That Went Over Their Limit · Cost Exposure (`seat_cost` textbox variable, default 10000, labelled as an assumption) |
| 2 | Alerts by Rule (bar chart, w=10) · Alerts Over Time by Severity (stacked bars, w=14) |
| 3 | People In Office vs Approved Seats (w=12) · Peak Utilisation by Pool, top 15 (bar gauge, w=12) |
| 4 | Two Pools, Two Stories: P116 Gurgaon (over limit) vs Diamond District Bengaluru (near limit) (w=24) |
| 5 | Latest Alerts audit table (w=14) · ERP vs Master seat-count mismatches table (w=10) |
| 6 | Alerts in the Last Minute (live) · Events per Second (live) · Grafana Alert Rules (live state, alert list) |

Colours: severity red `#d03b3b` / orange `#ec835a` / amber `#fab219` / grey `#898781`; status green `#0ca30c`; neutral blue `#2a78d6`. Severity colours are reserved for alert state.

## Appendix B — Environment variables

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` (compose sets `kafka:9093`) | all Kafka scripts |
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD` | `localhost` (compose sets `mysql`), `3306`, `root`, `root` | engine, verify, reset |
| `SLEEP_PER_BUCKET` | `0.4` | producers (seconds per 5-minute bucket) |
| `NEAR_CAPACITY_PCT` | `90` | `alert_rules.py` |
| `SEAT_VARIANCE_TOLERANCE` | `0.05` | `alert_rules.py` |
| `MASTER_REFRESH_SECONDS` | `10` | engine |
| `BATCH_SIZE` | `500` | engine |
| `MASTER_CSV` | `seat_master.csv` | engine |
| `MIN_SEVERITY` | `LOW` | `alerts_notifier.py` |
| `PORT` | `5001` | `alert_sink.py` |

## Appendix C — `VERIFICATION_REPORT.md` template

```
# Verification report
Machine: <OS, Docker version>      Date: <date>
Compose changes: <none | what and why>

| Phase | Result | Evidence |
|---|---|---|
| 0 Orientation / offline | PASS/FAIL | compose config, pytest output |
| 1 Stack up | PASS/FAIL | docker compose ps, health outputs |
| 2 Listeners and topics | PASS/FAIL | list/describe output, both listeners |
| 3 End-to-end run | PASS/FAIL | verify_alerts output, row counts, lag, seat-alerts offset |
| 4 Grafana dashboard | PASS/FAIL | API results, screenshots/<files>, panel-by-panel notes |
| 5 Grafana alerting | PASS/FAIL | rule health, firing/resolved evidence, alert_sink_log.txt |
| 6 Scenarios A-F | PASS/FAIL each | numbers observed |
| 7 Presentation assets | PASS/FAIL | files created |

Differences from the README / CLAUDE.md: <list>
Files changed: <list with reasons>
Open problems: <list>
```
