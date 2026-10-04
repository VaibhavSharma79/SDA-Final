# Seat Integrity Guard
### Final Project — End-to-End Streaming Analytics

A real-time guard that checks every ERP seat record and every check-in against the
company's **master seat table**. A record that matches the master is **silent**. A mismatch,
or a seat pool that fills past its approved seat count, raises an alert the moment it happens.

**Everything runs in Docker** (Kafka, MySQL, the Python engine, the replay producers, Grafana). **Grafana does both the
dashboarding and the alerting**: the engine decides *what* is an anomaly and writes it to MySQL; Grafana shows it live and
decides *who is notified*.

This section-by-section README maps directly onto the grading rubric.

---

## 1. Problem Statement (25%)

**Business problem.** A multi-entity company with offices across India keeps one master table of
approved seats per location (location code, entity, office type, seat count). Two things go wrong
quietly:

1. **The ERP drifts away from the master.** A record arrives with a wrong entity, a wrong office
   type, a wrong city, a seat count that does not match, or a location code that does not exist at
   all. Reports built on that record are wrong, and nobody knows.
2. **Seat pools fill past their approved seats.** Someone checks in at a site whose approved seats
   are already taken, at a site with **zero** approved seats (a server room), or the same badge is
   used at two sites at once.

**Why streaming, specifically.** Today both checks happen after the fact: a nightly reconciliation
report, or a facilities audit. By then the over-subscribed day is over and the bad record has been
consumed by downstream reports. The value of the information decays in minutes. A pool that goes over
its limit at 10:40 is only actionable at 10:40. Only a streaming pipeline can compare each event with
the master *as it arrives* and stay silent when everything agrees.

**The less obvious problem.** In aggregate the estate looks healthy. In this dataset the whole
network peaks at about 62% of approved seats (13,655 of 22,076, by construction of the simulation),
yet **7 individual pools breach their limit**. A citywide or company-wide average would hide every
one of them. The guard works per seat pool for that reason.

**Quantified impact (from this dataset).**

| Finding | Number |
|---|---|
| Alerts raised from 27,679 events | 118 (33 critical, 42 high, 20 medium, 23 low) |
| Seat pools that went over their approved seats | 7 (33 check-ins above the limit); worst: 19 people in a 14-seat pool (135.7%) |
| Check-ins at pools with **zero** approved seats | 12, across 5 sites |
| Badges used at two sites at once | 8 |
| Seats the ERP and master disagree on | 624 seats across 13 pools (584 across 7 pools above the 5% bracket) |
| Cost exposure of that disagreement | 624 seats x **assumed** ₹10,000 per seat per month = ₹62.4 lakh per month |

> The ₹10,000 per seat per month is a **placeholder assumption, not a real figure**. Replace it with
> your finance team's number: it is the editable box at the top of the Grafana dashboard.

## 2. Identification of Data (20%)

Three sources, owned by different systems in a real company:

| Source | Topic | What it represents | Volume (one simulated day) |
|---|---|---|---|
| Master seat table | `seat_master` (MySQL reference table) | Source of truth: approved seats per pool | 142 rows in the supplied file, 123 seat pools, 22,076 seats |
| ERP site sync | `seat-site-sync` | The ERP's own copy of each site's entity, office type, city and seat total | 369 events |
| Check-in / check-out | `seat-occupancy` | Who entered or left which site | 27,310 events |

**27,679 events** over a simulated business day (2026-06-01, IST). Averaged over 24 hours that is only
0.32 events per second, so the live demo replays the day compressed: about a minute at the default
pace (0.4 real seconds per 5-minute bucket).

**The master.** `Seat_Count_PAN_India_29052026.xlsx`, sheet `Active`, used as supplied. The master has
several rows per location code, so the match key is **location code + entity + office type**, with seats
**summed** per key (123 keys). Two rows have no location code and cannot be joined, so the streams
exclude them. One location code (`PB_Tripura_Agartala_Chowmuhani `) has a trailing space, which would break
a join on a trimmed code, so the streams carry it verbatim.

**Why synthetic.** Real ERP and badge data is not available for a course project. The streams are
generated (`generate_seat_data.py`, seed 42) from the real master so they reference real locations. Faults are
**injected on purpose, one per record**, and a separate reference engine produces `expected_alerts.csv`, the
answer key. That is what lets `verify_alerts.py` prove the pipeline is right, not just busy. The fault rate is
deliberately high (about 1 in 8 site records) so alerts are visible; a real ERP would be well under 1%.

**Schema.**

```
seat-site-sync   event_id, event_ts, event_type, source_system, location_code, entity, office_type, city, erp_seats_total
seat-occupancy   event_id, event_ts, event_type (CHECK_IN | CHECK_OUT), source_system, employee_id, department,
                 location_code, entity, office_type, city
seat-alerts      trigger_event_id, stream, rule_id, severity, event_ts, location_code, entity, office_type,
                 expected, actual, detail
```

MySQL (`seat_guard`): `seat_master`, `site_sync_log` (every ERP record + its verdict), `occupancy_log` (every
check-in/out + the running occupancy after it), `pool_state` (current and peak occupancy per pool), `alerts`
(the audit trail: expected value, actual value, reason).

## 3. Technical Working (15%) — running the live demo

Requires **Docker** (Engine on Linux, Desktop on Windows/macOS) with the Compose plugin; see `SETUP.md` (it covers Zorin OS / Ubuntu). Nothing needs to be installed in Python on your machine.

```
docker compose up -d --build            # zookeeper, kafka, mysql, grafana, engine, alert-sink (first build takes a few minutes)
docker compose logs -f engine           # wait for "Seat Integrity Guard started" (the engine retries until Kafka and MySQL are ready)
```

Open **http://localhost:3000** (`admin` / `admin`): the "Seat Integrity Guard" dashboard (refreshes every 5 seconds) and the five alert
rules (Alerting > Alert rules) are provisioned automatically. Then start the replay and watch:

```
docker compose --profile replay up site-sync-producer occupancy-producer     # replays the simulated day, about a minute
docker compose logs -f alert-sink                                            # in another terminal: the notifications Grafana sends
```

**Verify it worked.** After the replay finishes:

```
docker compose run --rm engine python verify_alerts.py
```

It compares the `alerts` table with `expected_alerts.csv` and prints `PASS: the pipeline raised exactly the expected alerts.`
(118 expected). To run again cleanly: `docker compose run --rm engine python reset_demo.py`, then `docker compose restart engine`,
then the replay command again.

**Live rule change (worth doing in the presentation).** The engine re-reads `seat_master` every 10 seconds, so you can change the
source of truth while the stream runs:

```
docker compose exec mysql mysql -uroot -proot seat_guard -e "UPDATE seat_master SET seats_total = 400 WHERE location_code = 'PB_HR_GUR_P116-Sector44' AND entity = 'Policybazaar' AND office_type = 'Corp';"
```

The engine logs `[master] reference table changed` and later verdicts use the new seat count. Done before the replay starts, this turns
118 alerts into 115: the pool's 5 over-capacity and 1 near-capacity alerts disappear, and its three ERP records (still saying 298 seats)
become seat-count mismatches.

**Other commands.**

| Task | Command |
|---|---|
| Offline rule tests (no Kafka/MySQL involved) | `docker compose run --rm engine python -m pytest tests -q` |
| Slower replay (to watch the live-state alert rules fire and resolve) | `SLEEP_PER_BUCKET=1.0 docker compose --profile replay up site-sync-producer occupancy-producer` |
| Change a bracket (policy challenge) | `NEAR_CAPACITY_PCT=80 docker compose up -d engine` (Windows PowerShell: `$env:NEAR_CAPACITY_PCT="80"; docker compose up -d engine`) |
| Print alerts from the Kafka alerts topic (optional) | `docker compose run --rm engine python alerts_notifier.py` |
| Look inside MySQL | `docker compose exec mysql mysql -uroot -proot seat_guard` |
| Regenerate the data (needs Python with pandas + openpyxl on your machine) | `pip install -r requirement.txt` then `python generate_seat_data.py` |
| Regenerate the dashboard / alert rules after editing them | `python grafana/build_dashboard.py` / `python grafana/build_alerting.py` |

**Kafka listeners.** Containers reach Kafka at `kafka:9093`; tools on your own machine at `localhost:9092`.

**Testing strategy.** `alert_rules.py` is a pure module (no Kafka, no MySQL), so the rules can be tested on their own. The engine's
output is checked against an answer key produced by a *separate* implementation of the same rules in the generator, so a bug would have
to occur identically in both to go unnoticed.

## 4. Charts & Visualisations (30%)

Sixteen panels, each answering a different business question:

| Panel | Type | Why this form |
|---|---|---|
| Events Processed / Alerts Raised | Stat tiles | Headline volume, a KPI row rather than a one-bar chart |
| Critical: Pool Over Its Seat Limit / Pools That Went Over Their Limit | Stat tiles, red at 1 | The two numbers a facilities manager acts on |
| ERP vs Master Match Rate | Stat, amber/green thresholds | The "silent" side: how much of the stream agrees with the master |
| Cost Exposure: Seat-Count Mismatch (₹/month) | Stat, editable assumption | Turns seats into money for the business case |
| Alerts by Rule | Horizontal bar chart | Eleven categories, a ranking, so bars beat a pie |
| Alerts Over Time by Severity | Stacked bars, severity colours | Shows *when* trouble happens (the morning ramp) and how bad |
| People In Office vs Approved Seats (all sites) | Timeseries + dashed capacity line | The aggregate looks safe: that is the point of the next panels |
| Peak Utilisation by Pool (top 15) | Bar gauge, green / amber at 90% / red at 100% | A ratio against a limit; thresholds match the alert brackets exactly |
| Two Pools, Two Stories | Timeseries, 4 series | Deliberately not 123 lines. One pool crosses its seat limit, another reaches 90% and stops |
| Latest Alerts (audit log) | Table, severity colour | Every alert with expected vs actual and the reason |
| ERP vs Master: Seat-Count Mismatches | Table | Words and numbers side by side, which a chart cannot do |
| Alerts in the Last Minute / Events per Second (live) | Stat tiles on real time | The "streaming" feel: they move while the replay runs |
| Grafana Alert Rules (live state) | Alert list | Which of the five rules are firing right now |

Colour follows the data's job: severity colours (red / orange / amber / grey) are reserved for alert state and
never reused to identify a site; the capacity lines are grey and dashed so they read as a limit, not a series.

### Alerting (Grafana)

Grafana evaluates five provisioned alert rules every 10 seconds against the MySQL tables and sends notifications through a webhook
contact point (the `alert-sink` container prints them: `docker compose logs -f alert-sink`; change the URL in
`grafana/provisioning/alerting/contactpoints.yaml` to reach Slack or Teams, or add an email receiver).

| Rule | Severity | Kind | Fires when |
|---|---|---|---|
| Pool over its seat limit (live) | critical | state, one alert per pool | a pool's **current** head-count is above its master seats; resolves when people leave |
| Pool near its seat limit (live) | warning | state, one alert per pool | a pool's current head-count is at 90% or more of its seats |
| Check-in at a site with zero approved seats | high | event | the engine raised ZERO_SEAT_CHECKIN in the last 60 seconds |
| Badge used at two sites at once | high | event | the engine raised DOUBLE_CHECKIN in the last 60 seconds |
| ERP record disagrees with the master | high | event | the engine raised a site-sync alert (other than a small variance) in the last 60 seconds |

Event rules use the alert's real insertion time (`raised_at`), not the simulated 2026-06-01 timestamp, so they work whatever today's date is.
The two state rules only hold for a few real seconds at the default replay speed (a pool is over its limit for hours of *simulated* time but
the replay compresses that); use `SLEEP_PER_BUCKET=1.0` to watch them fire and resolve.

## 5. Discussion (10%) — talking points

- **Two layers on purpose.** The engine detects (stateful, per event, exact: it needs running head-counts and the master join). Grafana
  routes (who is told, how often, grouped how, silenced when). Changing a contact point or a policy needs no code change and no redeploy of
  the pipeline.
- **Alert fatigue.** 118 alerts in a day is a lot. The engine already suppresses the noisiest case
  (near-capacity fires once per upward crossing, not on every check-in). Next step: one alert per pool per window.
  The brackets are policy knobs, not code (below).
- **Policy challenge (built in).** Re-run with a different bracket and no code change:
  ```
  NEAR_CAPACITY_PCT=80 docker compose up -d engine      # PowerShell: $env:NEAR_CAPACITY_PCT="80"; docker compose up -d engine
  (reset the tables and replay again; Grafana's near-limit alert rule stays at 90%)
  ```
  Predict, then compare: lowering the bracket from 90% to 80% raises near-capacity alerts from **17 to 30** (total
  118 to 131) and leaves the 33 over-capacity alerts unchanged. `SEAT_VARIANCE_TOLERANCE=0.10` moves the
  LOW/HIGH line for seat-count gaps. (`verify_alerts.py` will report differences, as expected, when brackets change.)
- **State recovery.** The running head-counts live in the engine's memory. If it crashes mid-day, the counts are
  lost and must be rebuilt by replaying the topic. A production build would keep that state in a changelog topic or a
  stream processor with managed state (Kafka Streams, Flink).
- **Ordering and keys.** Occupancy is keyed by seat pool so each pool's head-count is one ordered stream. The
  double-check-in rule needs the opposite key (by employee), so a production build would re-partition by employee.
- **Privacy.** Check-in events carry an employee id: this is personal attendance data. A real deployment needs a stated
  purpose, retention limits, and probably pseudonymised ids.
- **What the master cannot say.** The master's seat count is *approved seats*, not a legal safety limit, and it has
  its own quality problems (two rows with no code, a trailing-space code). The guard is only as good as its reference table.
- **Roadmap.** Floor-level capacity (the supplied file's `All` sheet has seats per floor); utilisation forecasting to
  predict a breach before it happens; auto-raising a ticket per critical alert; per-entity cost allocation.

---

## Architecture

```
                                      Docker Compose network
generate_seat_data.py -> site_sync_events.jsonl -> [site-sync-producer] --> Kafka: seat-site-sync --+
  (reads the master xlsx)  occupancy_events.jsonl -> [occupancy-producer] --> Kafka: seat-occupancy --+--> [engine]  seat_guard_consumer.py
                           seat_master.csv (reference table, loaded once into MySQL)                       |   uses alert_rules.py
                           expected_alerts.csv (answer key)                                    matched -> silent
                                                                                               mismatch / breach -> alert
                                                          +--------------------------------------------+------------------+
                                                          v                                                                v
                                           [mysql] seat_guard: seat_master, site_sync_log,                    Kafka: seat-alerts
                                                   occupancy_log, pool_state, alerts                          (optional notifier)
                                                          |
                                                          v
                                           [grafana]  dashboard (16 panels, 5 s refresh)  +  5 alert rules (every 10 s)
                                                          |
                                                          v   webhook
                                                   [alert-sink]  prints each notification

verify_alerts.py: alerts table vs expected_alerts.csv          [bracketed] = a container
```

## Files

| File | Purpose |
|---|---|
| `SETUP.md` | Prerequisites and troubleshooting for Linux (incl. Zorin OS 18), macOS and Windows |
| `docker-compose.yml`, `Dockerfile`, `requirements-docker.txt` | The whole stack; one Python image used by every Python service |
| `alert_rules.py` | Pure rule logic (reference check, running occupancy, brackets) |
| `seat_guard_consumer.py` | The engine: consumes both topics, writes MySQL, publishes alerts |
| `site_sync_producer.py`, `occupancy_producer.py` | Replay the data files into Kafka, paced by 5-minute bucket |
| `alert_sink.py` | Webhook receiver that prints Grafana's notifications |
| `alerts_notifier.py` | Optional: prints alerts from the `seat-alerts` Kafka topic |
| `verify_alerts.py` | Proves the alerts table matches the answer key |
| `reset_demo.py` | Clears results between runs (`--master` also clears the master) |
| `generate_seat_data.py` | Regenerates every data file from the master xlsx |
| `seat_master.csv`, `site_sync_events.jsonl`, `occupancy_events.jsonl`, `expected_alerts.csv` | Generated data |
| `Seat_Count_PAN_India_29052026.xlsx` | The supplied master table |
| `grafana/provisioning/` | Data source, dashboard and alerting (rules, contact point, policy), auto-provisioned |
| `grafana/build_dashboard.py`, `grafana/build_alerting.py` | Generate the dashboard JSON and the alerting YAML: edit there, then re-run |
| `tests/test_alert_rules.py` | Offline tests of the rules against the answer key |
| `CLAUDE.md`, `IMPLEMENTATION_PLAN.md` | Context and a phased runbook for Claude Code to verify and finish the project on real infrastructure |
