> Update 2026-11-01: this is the earlier manual-workflow report. See [launcher verification](PHASE2_LAUNCHER_VERIFICATION.md) for connected GUI and unified startup checks.

# Phase 2 verification: user-guide execution

2026-10-31, Asia/Seoul. Guide: [PHASE2_USER_GUIDE.md](PHASE2_USER_GUIDE.md).
This supersedes the initial implementation report in `phase2-verification.md`.

## Capability status

“Fully working” below means the stated local test conditions were exercised; it is not
an unqualified production/deployment guarantee.

| Capability | Status | Executed evidence |
|---|---|---|
| Signalized intersection, seeded arrivals, TraCI vehicle/signal/road observations | Fully working | Fresh 90-second run; real SUMO reproducibility test |
| Standalone visual SUMO-GUI | Fully working as replay | sumo-gui executed to completion under Xvfb: 450 steps, 17 inserted, 7 arrived, no collisions/teleports |
| GUI process connected to the synchronization pipeline | Not implemented | TraCIClient hardcodes `sumo`; no --gui option; replay is an independent process |
| Live Eclipse Ditto entity creation/read/update | Fully working with local test identity/policy | Live five-operation test passed; 26 Things independently retrieved after actual MQTT traffic |
| Follow, Attach, Detach, Transfer, Update | Fully working under tested rules | All five verified in real traffic MQTT-to-Ditto run; unit tests cover renewal and failures |
| State versions, conditional writes, commit verification | Fully working under single-host writer protocol | 1,773 remote receipts checked; version lineages and latest PVs checked; stale/CAS/recovery tests passed |
| Lifecycle and error records | Fully working | Journal + JSONL transitions, per-operation clocks/version/dependencies; unsuccessful labels tested |
| MQTT publish/consume/manager/Ditto | Fully working with corrected runtime broker settings | 1,738 exact event identities sent and processed; live duplicate replay did not add operations |
| Kafka publish/consume/manager/Ditto | Fully working in bounded smoke | Live round trip passed; real 3-second SUMO CLI path: 32 events / 42 verified remote commits |
| Bundled service deployment | Partially working | Upstream Ditto Compose exists; bootstrap restart needed. No project MQTT/Kafka Compose; test brokers launched separately |
| CLI/config/tests | Fully working for existing interfaces | local/publish/consume/recover inspected; all 27 existing tests passed against available services |
| REST service / custom DT dashboard / visual RSU overlays | Not implemented | No application API, dashboard or overlay modules in repository |
| Phase 3 / OpenWhisk latency | Not implemented, intentionally | No graph learning, serverless dispatch or cold-start claims |

All application Python modules, configs, tests, CLI and existing deployment files were
inspected. `scripts/` and `infrastructure/docker/` are empty; upstream source checkouts
were preserved. No application code or tests changed for this task. Only documentation
and gitignored runtime configuration/evidence were created or updated.

## Commands actually executed and results

| Command / invocation | Result | Actual observation / dependency |
|---|---|---|
| `.venv/bin/python -m pytest -q -rs` before services | PASS with skips | 24 passed, 3 skipped in 22.03s; missing opt-in env variables clearly reported |
| `.venv/bin/python -m pytest --collect-only -q` | PASS | 27 tests collected; actual node names used in guide |
| `PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 local --backend sqlite --follow --output results/phase2-guide/local` | PASS | 1,773 verified local commits; SQLite is explicitly not Ditto |
| `timeout 25s xvfb-run -a sumo-gui -c results/phase2-final/scenario/intersection.sumocfg --seed 27 --start --quit-on-end --delay 0 --no-step-log true --summary-output /tmp/perfsync-guide-gui-summary.xml` | PASS (exit 0) | Offscreen GUI executable check, not a live visual-to-Ditto connection or interactive desktop inspection |
| `docker compose -p perfsync-phase2-guide -f third_party/ditto/deployment/docker/docker-compose.yml config --services` | PASS | All 9 upstream services identified; no MQTT/Kafka service |
| `docker compose --progress plain -p perfsync-phase2-guide -f third_party/ditto/deployment/docker/docker-compose.yml up -d` | Eventually started; readiness initially FAIL | First bounded pull attempt timed out; unbounded retry completed. Initial HTTP 502 / bootstrap stall resolved by restarting policies once other nodes were present |
| `MQTT_TEST_HOST=localhost .venv/bin/python -m pytest -q -rs tests/test_services.py::test_live_mqtt_roundtrip` | PASS | 1 passed in 1.47s against actual Mosquitto |
| `DITTO_TEST_URL` + configured credentials/policy, `python -m pytest -q -rs tests/test_services.py::test_live_ditto_complete_lifecycle` | PASS after readiness | 1 passed in 15.99s against actual Ditto. Earlier startup-time attempt failed HTTP 502, not a mock success |
| `KAFKA_TEST_BOOTSTRAP=localhost:9092 .venv/bin/python -m pytest -q -rs tests/test_services.py::test_live_kafka_roundtrip` | PASS after readiness | 1 passed in 13.47s. Earlier attempt before Kafka Server started failed message timeout |
| Full `.venv/bin/python -m pytest -q -rs` with all service env variables | **PASS** | **27 passed, no skips, in 57.14s**, output in `results/phase2-guide/tests-live.log` |

The service-enabled suite was launched from a Python subprocess that loaded the generated
private credential JSON into its environment and set DITTO_TEST_URL, MQTT_TEST_HOST and
KAFKA_TEST_BOOTSTRAP. No credentials were printed or put on command arguments. The
same command can be reproduced after the guide's secure environment-loading step.
Tests use SQLite WAL/NORMAL for process-restart verification; no power-loss durability
or production broker outage test is claimed. Unit HTTP/failure tests still use fakes;
they are distinct from the additional real-service tests.

## Full MQTT traffic demonstration

After correcting queue/persistence settings and confirming subscriber SUBACK:

```bash
# Consumer, with private Ditto environment loaded as shown in the guide:
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 consume \
  --backend ditto --broker mqtt --output results/phase2-guide/mqtt-verified-consumer
# Separate producer:
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 publish \
  --broker mqtt --follow --config results/phase2-guide/mqtt-verified.json \
  --output results/phase2-guide/mqtt-verified-producer
```

Actual run ID: `intersection-guide-mqtt-verified`; seed 27, 90 seconds, observation interval
1 second, arrival rate 0.3/s. The consumer invocation loaded credentials inside its Python
process; the displayed CLI is equivalent once that environment is loaded.

| Audit | Result |
|---|---:|
| Producer acknowledged outbox rows | 1,738 |
| Consumer completed inbox rows | 1,738 |
| Identity sets and serialized bodies | Exactly equal |
| Ditto Things retrieved directly | 26 |
| Attach | 26 |
| Follow | 17 |
| Update | 1,714 |
| Transfer | 9 |
| Detach | 7 |
| Total verified COMMITTED operations | 1,773 |
| Same-session nonnegative duration records | 1,773 |
| Atomic remote operation receipts checked | 1,773 |

For each Thing, the independent HTTP audit checked contiguous application versions,
expected operation receipt fingerprints/versions, latest PVs against its last Update,
and detached status/no owner/no subscriptions when its final operation was Detach.
A captured actual insertion event was redelivered through MQTT; consumer inbox remained
1,738 and operations remained 1,773, demonstrating live duplicate handling.

Actual vehicle selected by requiring all five committed types: vehicle-0000. It attached
at 200 simulation ms, initial Update then Follow committed, Transfer occurred at
49,000 ms (x=308.2252574271, west -> east), and actual arrival caused Detach at 77,800 ms.
Final app version 82, sequence 79, ownership_version 2, owner null, subscriptions empty.
The final retained position x=591.8166964216 matches its last real Update. Detach's source
timestamp marks arrival; it does not fabricate an arrival-position observation.

Remote observations were also sampled while processing: version/sequence/source time
advanced, independently confirming live Ditto changes. GUI replay has the same seeded
traffic, but its Play/Pause controls do not control this headless process.

## Kafka CLI smoke

A separate official `apache/kafka:3.9.1` single-node container was started on loopback 9092,
as documented in the [Apache Kafka Docker instructions](https://kafka.apache.org/39/getting-started/docker/).
There is no repository Kafka Compose file. Topic `perfsync.events` must exist before the
current consumer starts; an initial attempt exited UNKNOWN_TOPIC_OR_PART. After explicit
topic provisioning and consumer restart, Kafka retained and delivered the records.

```bash
# Consumer with Ditto environment:
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 consume \
  --backend ditto --broker kafka --output results/phase2-guide/kafka-smoke-consumer
# Producer:
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL .venv/bin/python -m perfsyncgnn.phase2 publish \
  --broker kafka --follow --config results/phase2-guide/kafka-smoke.json \
  --output results/phase2-guide/kafka-smoke-producer
```

Run ID `intersection-guide-kafka-smoke`, duration 3 seconds. Exactly 32 acknowledged event
identities matched 32 completed consumer identities; 42 operations (Attach, Follow, Update)
were COMMITTED with all corresponding receipts retrieved from actual Ditto. The short
cutoff naturally contains no Transfer/Detach; this smoke does not claim the five-operation
physical lifecycle on Kafka. That lifecycle was verified through MQTT and local SUMO.

## Deployment corrections and failures preserved honestly

1. **MQTT queue overflow:** The unmodified upstream test config's default per-client queue
   dropped messages when the unpaced publisher outran Ditto. Logs explicitly reported
   outgoing messages being dropped. The initial directories `mqtt-producer/` and
   `mqtt-consumer/` are incomplete diagnostic evidence, not a passed demo. A runtime copy
   adds unlimited message/byte queues for this small test, a persistent volume/location,
   and autosave. A fresh run verified exact identity equality. Larger production workloads
   still need capacity limits/backpressure; PUBACK alone is not subscriber delivery proof.
2. **Ditto bootstrap:** Policies initially discovered only one contact point; other nodes
   waited for the lowest-address seed. Restarting the policies test container after peers
   were present allowed cluster formation. Healthy HTTP authentication/policy creation
   and live tests then passed. No source/Compose file was patched.
3. **Authentication:** A generated isolated local identity, private credential file and
   runtime nginx volume override were used. The repository's nginx.htpasswd was untouched.
   Keep this override when recreating these test containers; otherwise credentials differ.
4. **Kafka readiness/topic:** Mapped ports existed before Kafka was ready. The initial
   live test failed message timeout; it passed after the broker STARTED log. The CLI
   consumer also exits on unknown topic, so guide startup explicitly creates the topic.

## Runtime evidence and safety

Gitignored outputs under `results/phase2-guide/`:

- `local/`: fresh SUMO-to-SQLite events, journal, twins and lifecycle JSONL.
- `mqtt-verified-producer/`, `mqtt-verified-consumer/`: complete live distributed run.
- `mqtt-digital-twin-verification.json`: exact event/operation/remote-state audit.
- `ditto-observations.jsonl`: live remote samples without credentials.
- `kafka-smoke-producer/`, `kafka-smoke-consumer/`, `kafka-digital-twin-verification.json`.
- `tests-live.log`: final all-services test result.
- Runtime config copies, startup/shutdown logs, and private auth material. Never publish
  the private credential file. Runtime configs are not new application functionality.

The test consumers were gracefully interrupted after their queues drained, exporting
JSONL. Test services are stopped with their containers and broker volume retained for
resuming; no `down -v` or volume deletion is used. MongoDB's upstream Compose definition
has no explicit named persistent data volume: do not promise data survival across
container recreation. Guide commands show startup, inspection, shutdown and recovery.

## Remaining limits before Phase 3

The visual demo remains an independent SUMO-GUI replay. There is no connected GUI mode,
visual RSU overlay, custom dashboard or API. Only one topology is implemented. Remote
writes are serialized by a single-host journal and protected by conditional writes;
read-after-write is not a distributed transaction. Receipt storage is unbounded. Broker
security/replication, power-loss tests and distributed-writer fencing are not validated.
Local Python request-to-verification durations do not measure OpenWhisk cold starts or
serverless queueing. Phase 3 remains untouched.

Stabilize service version pinning, readiness checks, and production queue capacity before
larger experiments. For the current Phase 2 demonstration, follow the verified local
service/runtime settings and compare producer/consumer identities after every run.
