# Phase 2 verification report

Date: 2026-10-31 (Asia/Seoul).

## Result

Phase 2 application functionality is implemented and the local SUMO-to-verified-DT
workflow is validated. Live Ditto/MQTT/Kafka acceptance remains pending: none was
listening on its default localhost port during verification. The local SQLite result
is not evidence of a successful external-service deployment. No GNN, RL, optimization,
or OpenWhisk/cold-start measurements were added.

## Inspection and scope

Read the supplied 34-page research draft using pdftotext and inspected its synchronization
workflow figure (page 6). Its entity/PV/association/ownership/operation-specific commit
semantics informed this implementation. The task prompt's prediction-only requirement
takes precedence over the older RL statements in the draft's abstract/introduction.

Initial git index and application directories were empty; the workspace contained the
Phase 1 folder skeleton, an installed virtual environment and untracked upstream Ditto /
OpenWhisk checkouts. There were no application configurations, adapters or working
modules to reuse or remove. The installed SUMO/netconvert and dependencies were reused;
third-party sources and environment were preserved. No external maps were downloaded.

## Implemented deliverables

- Generated signalized four-arm, two-lane intersection with deterministic seeded arrivals,
  configurable durations/observation intervals and two logical RSU regions.
- TraCI vehicle insertion/observation/arrival, signal observation and road aggregate extraction.
- Pydantic event schema, JSON serialization, schema version, event identity/dedup validation,
  explicit simulation vs Unix timestamp fields.
- Broker-confirmed durable MQTT/Kafka publication and manual-ack/manual-offset consumers.
- Stable DT identity and PV, version, association, subscription, owner and receipt models.
- Conditional-write Eclipse Ditto adapter; explicit separate SQLite local test adapter.
- Deterministic Follow/Attach/Detach/Transfer/Update identification and handlers, including
  explicit renewal and opt-in due subscription renewal.
- Durable dependency plans, FIFO processing, stale/duplicate handling, optimistic version
  checks, per-operation commit predicates, idempotent receipts and restart recovery.
- Lifecycle transitions, error categories, versions, dependencies and clock-separated
  provenance; unsuccessful operations receive no successful latency labels.
- Executable CLI, setup/consistency documentation, unit and opt-in integration tests.

## Commands and observed results

```bash
.venv/bin/python -m pytest -q -rs
```

**24 passed, 3 skipped in 20.63 seconds.** The suite selects SQLite WAL/NORMAL to test
process-restart recovery without repeated FULL-mode fsync; it does not simulate power loss.
The three explicit skips were:

| Check | Missing configuration |
|---|---|
| Live Ditto five-operation lifecycle | DITTO_TEST_URL |
| Live MQTT publication/consumption | MQTT_TEST_HOST |
| Live Kafka publication/consumption | KAFKA_TEST_BOOTSTRAP |

Unit checks cover all five triggers/predicates, renewal authorization, bad schema,
duplicates/stale sequence/time, ownership/version validation, state machine terminals,
failed/cancelled labels, CAS conflicts, concurrent duplicate workers, dependency failure,
pending Update before Detach, broker outage/outbox retry and consumption dedup, HTTP
Ditto ETag/conditional headers, failed commit verification, and restart after a durable
write with lost response. The real SUMO integration test runs the same seeded 100-second
scenario twice and compares every event except emitted_at_unix_ms. It verifies multiple
vehicles, all three observed entity types, and actual Transfer and Detach operations. It also replays both event streams through
independent DT/journal databases and checks identical operation IDs, types, dependencies,
versions, owners and outcomes.

An earlier 65-second test produced no actual arrival before its cutoff and failed the
Detach assertion. The scenario was extended to 100 seconds; the passing test uses real
arrivals and does not fabricate a departure to satisfy the assertion.

```bash
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 local \
  --backend sqlite --follow --output results/phase2-final
```

The default seeded 90-second scenario completed with:

| Operation | Verified COMMITTED |
|---|---:|
| Attach | 26 |
| Follow | 17 |
| Update | 1,714 |
| Transfer | 9 |
| Detach | 7 |
| **Total** | **1,773** |

There were 17 observed vehicles, 1,738 input events (including explicit Follow requests),
and 10,638 lifecycle transition records. The 26 attachments comprise 17 vehicles,
8 road segments and 1 signal. All 1,773 final operations had nonnegative, same-session
monotonic request-to-verified-commit durations. These are local pipeline durations,
not measured serverless synchronization latency. Vehicles still present at cutoff remain
attached; cutoff does not emit a false departure.

Outputs are in `results/phase2-final/events.jsonl`, `lifecycle.jsonl`, `journal.sqlite`,
`twins.sqlite` and `scenario/`. Results are gitignored runtime artifacts. An earlier
interrupted development run is separately under results/phase2; use phase2-final for
the completed evidence. A second completed run under phase2-verified produced the
same operation counts.

```bash
.venv/bin/python -m compileall -q simulation synchronization dt monitoring perfsyncgnn
```

Passed. A separate temporary-database smoke check verified that both adapters' SQLite
connections default to synchronous=FULL (PRAGMA value 2), with Attach and Update
committing successfully in that mode. TCP probes returned connection refused for local
ports 8080, 1883 and 9092. Docker reported no running containers.

## Remaining acceptance and next steps

1. Start/provision Ditto with a writable policy and persistent MQTT/Kafka services; use
   the environment variables and commands in phase2.md to run the three live checks.
2. Run producer -> selected broker -> consumer -> Ditto using separate output directories
   and preserve lifecycle/journal evidence. Test outage/redelivery against the actual broker
   deployment before treating the distributed path as validated.
3. Keep production journals on local storage with FULL sync, stable run IDs and consumer
   IDs. Coordinate backups of journal and Ditto state. Independent hosts require a shared
   ownership/fencing strategy; this phase uses a single-host serialized worker.

Read-after-write is not a distributed atomic transaction. Receipt history is retained
without compaction, and the serial worker favors deterministic correctness over throughput.
TLS/credentials and broker replication/retention are deployment configuration; CLI broker
examples assume local unauthenticated test services. Recovery verifies receipts and
records acknowledgement time, but leaves cross-session duration labels null. Refer to
phase2.md for the full consistency and timing contract. Phase 3 remains unimplemented.
