# Verification report
Machine: Linux (Zorin OS 18), Docker 29.8.1, Compose v5.5.1, Grafana 13.2.3      Date: 2026-10-04
Compose changes: none so far

| Phase | Result | Evidence |
|---|---|---|
| 0 Orientation / offline | PASS | `docker compose config -q` exit 0; image built by `docker compose up -d --build`; `docker compose run --rm --no-deps engine python -m pytest tests -q` -> `7 passed in 0.25s`; ports 2181, 9092, 3306, 3000, 5001 were listening, held by this stack |
| 1 Stack up | PASS | `docker compose ps`: all 6 services Up 34 min, kafka and mysql `(healthy)`, RestartCount 0 for every container; engine log: `Seat Integrity Guard started`, `master pools: 123 (loaded 123 from seat_master.csv)`; alert-sink: `Alert sink listening on :5001`; `mysqld is alive`; Grafana `/api/health` -> `"database": "ok"` |
| 2 Listeners and topics | PASS | `kafka-topics.sh --list` answers on both `kafka:9093` and `localhost:9092`; `seat-site-sync`, `seat-occupancy`, `seat-alerts` each `PartitionCount: 1 ReplicationFactor: 1`; `nc -z localhost 9092` succeeds from the host |
| 3 End-to-end run | PASS | Fresh stack, tables empty, default brackets (90 / 0.05). Documented command `docker compose --profile replay up site-sync-producer occupancy-producer` ran 69 s and **exited by itself, code 0** (it did not hang, and the engine kept running). `verify_alerts.py`: `PASS: the pipeline raised exactly the expected alerts.`, exit 0. `site_sync_log` 369 (321 `OK`); `occupancy_log` 27,310; `alerts` 118 (CRITICAL 33, HIGH 42, MEDIUM 20, LOW 23); `seat_master` 123; `pool_state` 122; `pool_state WHERE occupancy > 0` = 0; consumer lag 0 on both topics (27,310 / 369 offsets committed); `seat-alerts` end offset 118; no Traceback or Error in the engine log |

Real-Kafka behaviours (Phase 3 list): `poll(timeout_ms=1000, max_records=500)` returned 401 non-empty batches for 27,679 events (average 69, maximum 492, so the 500 cap was not hit); offsets committed after each DB commit (lag 0 at the end); the engine consumed both topics concurrently while both producers ran in parallel; `producer.send` to `seat-alerts` through the INSIDE listener delivered all 118. Not yet tried: producers started before the engine.

Notes:
- Engine prints a `DeprecationWarning: value_deserializer does not implement kafka.serializer.Deserializer` at start (kafka-python 3.0.11). Harmless so far; watch it in Phase 3.
- Grafana startup (seen in `docker compose logs grafana`): 4 `Failed to evaluate rule` errors at 13:21:00-04 for `seat-erp-master-mismatch` and `seat-pool-near-limit`, text `plugin.notRegistered plugin not registered` (retryable; no further errors after). Grafana and the engine both started at 13:20:15, so this is Grafana evaluating before its MySQL datasource plugin registered, not a missing `grafana` user or tables. Transient on first boot; re-check rule health in Phase 5.
- The replay command did not hang (the code review suspected it might because of `depends_on: engine`).
- Phase 0 step 5 (determinism) was checked by the offline code review in a scratch folder: all four data files and the generated Grafana files were byte-identical to the shipped ones.

## Run 2: full teardown and rebuild from scratch (2026-10-04)

`docker compose --profile replay down -v --rmi local --remove-orphans`, then `docker compose build --no-cache`, then `docker compose up -d`. (A first attempt without `--no-cache` finished `up --build` in 13 s because BuildKit's layer cache survives `--rmi local`; it was discarded and redone with `--no-cache`.)

| Step | Time (IST) | Duration |
|---|---|---|
| `down -v --rmi local` (volumes, containers, 4 images removed) | 20:05:21 - 20:05:46 | 25 s |
| `build --no-cache` (pip installed kafka-python 3.0.11, mysql-connector-python 26.7.0, pytest 9.1.1) | 20:05:46 - 20:05:56 | 10 s |
| `up -d` | 20:06:00 - 20:06:11 | 11 s |
| engine prints `Seat Integrity Guard started` | 20:06:20 | 9 s after `up` returned |
| Grafana `/api/health` answers | 20:06:27 | 16 s after `up` returned |
| replay (`--profile replay up site-sync-producer occupancy-producer`) | 20:06:37 - 20:07:46 | 69 s, exit 0 |
| `verify_alerts.py` | right after | PASS, exit 0 |

Everything from `down` to PASS took under 3 minutes. No manual step was needed.

| Phase | Result | Evidence (fresh stack) |
|---|---|---|
| 0 | PASS | `docker compose config -q` exit 0; `7 passed in 0.22s` |
| 1 | PASS | all 6 services Up, kafka and mysql `(healthy)`, RestartCount 0 everywhere; engine `master pools: 123 (loaded 123 from seat_master.csv)`; alert-sink listening; `mysqld is alive`; Grafana 13.2.3 `"database":"ok"` |
| 2 | PASS | `--list` answers on `kafka:9093` and `localhost:9092`; all three topics `PartitionCount 1`; `nc -z localhost 9092` open |
| 3 | PASS | tables empty before; `verify_alerts.py` PASS exit 0; site_sync_log 369 (321 OK); occupancy_log 27,310; alerts 118 (33/42/20/23); seat_master 123; pool_state 122; occupancy>0 pools 0; lag 0/0; `seat-alerts` end offset 118; engine restarts 0, no Traceback or Error in the engine log |

Early look at Phase 5 (full verification still to do): 5 of 5 rules `health: ok` after the first minute; the three event rules fired and their notifications reached `alert-sink` with real numbers (e.g. `12 check-in(s) at zero-seat pools in the last minute`). See issue 1 for the junk notifications that came with them.

## Issues found (rated)

Rating: **Medium** = visible wrong behaviour in the demo or a plan check that can fail; **Low** = noise or latent risk, nothing breaks today; **Info** = process note. Nothing found so far is High (nothing blocks the pipeline or the answer key). Not fixed yet; this is the log.

| # | Sev | Issue | Where it breaks | Fix needed? |
|---|---|---|---|---|
| 1 | Medium | **Junk Grafana notifications at first boot.** Reproduced on both boots (14:20-14:21 in run 1, 14:36:40 in run 2): the rules `seat-erp-master-mismatch` and `seat-pool-near-limit` fail their first evaluation with `[plugin.notRegistered] plugin not registered`, and `alert-sink` then prints `FIRING [high] DatasourceError: [no value] ERP record(s) disagreed...` plus two `RESOLVED ... DatasourceError: [no value] ... = [no value]` lines | The Alert-rules panel and the alert-sink log show error instances in the first minute; the Phase 5.2 check "no rule in `error`" fails if run in the first ~60 s; a live audience would see "[no value]" notifications. The pipeline and the answer key are unaffected, and all rules are `health: ok` afterwards | Yes, before the live demo (cheap) |
| 2 | Low | `Failed to read plugin provisioning files from directory ... /etc/grafana/provisioning/plugins: no such file` logged at `level=error` on every Grafana start | Nothing breaks; it only looks alarming in the log | Optional |
| 3 | Low | Engine start prints `DeprecationWarning: value_deserializer does not implement kafka.serializer.Deserializer` (kafka-python 3.0.11) | Nothing today; would only matter if a later kafka-python removes plain-callable deserializers. The version is pinned in `requirements-docker.txt` | No (revisit only on a kafka-python upgrade) |
| 4 | Low | Image build prints pip's `Running pip as the 'root' user` warning | Nothing | No |
| 5 | Low | `build: .` appears on four services, so the same image is built and tagged four times (`alert-sink`, `engine`, both producers) | Nothing; layers are shared, the whole no-cache build took 10 s | No |
| 6 | Info | `down --rmi local` does not clear BuildKit's cache, so a rebuild without `--no-cache` is not truly from scratch | Only affects how we describe a "fresh machine" test (Scenario F) | Doc note: use `build --no-cache` for a true scratch build |

### Issue 1 in detail
- **Root cause (evidence-based):** the five rules use `"execErrState": "Error"` (`grafana/build_alerting.py:48`). Grafana's alert scheduler runs the first evaluation about 30 s after the container starts (14:36:11 start, 14:36:40 first evaluation) and the MySQL datasource plugin is not yet registered at that moment. Each failed evaluation is retried (`max_attempts=3`, about 1 s apart, so about 4 s in total), still fails, and with `execErrState: Error` Grafana turns it into a `DatasourceError` alert instance. That instance goes through the notification policy to the webhook. The instance carries no pool or summary labels, which is why the annotations render as `[no value]`. It clears by itself on the next successful evaluation. Why the plugin registers late is Grafana-internal and not investigated further.
- **Possible fixes, best first:**
  1. Set `execErrState` to `KeepLast` in `build_alerting.py` and regenerate `rules.yaml`: a failed evaluation keeps the previous state (Normal at boot), so no error instance and no notification. To verify first: that Grafana 13.2.3 accepts `KeepLast` in provisioned rules. Downside: a real, lasting datasource failure no longer raises an alert, so the dashboard's rule-health would be the only place to see it.
  2. Set `execErrState` to `OK`: same effect, simpler, same downside.
  3. Keep `Error` but add a mute timing or a longer `for:` on the notification path: more config for one start-up blip.
  4. No code change: after `docker compose up`, wait about 90 s before opening Grafana or starting the replay, and record it in the demo script.
- **Recommendation:** option 4 for the immediate demo, option 1 as the real fix (small change in one generator file, then re-check Phase 5.2 right after a cold start).

### Issue 2 in detail
- **Root cause:** compose mounts `./grafana/provisioning` as the whole provisioning folder and Grafana looks for `plugins/` inside it; the folder does not exist.
- **Fix:** add an empty `grafana/provisioning/plugins/.gitkeep` (git then keeps the folder), or ignore the line.

Differences from the README / CLAUDE.md: none yet
Files changed: VERIFICATION_REPORT.md (new)
Open problems: Phases 4-7 not yet run; issue 1 (junk DatasourceError notifications at first boot) needs a fix or a demo workaround
