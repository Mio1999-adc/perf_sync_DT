> For the current terminal-by-terminal guide, corrected broker settings and live-service evidence, see [PHASE2_USER_GUIDE.md](PHASE2_USER_GUIDE.md) and [PHASE2_VERIFICATION.md](PHASE2_VERIFICATION.md).

# Phase 2 setup and operation semantics

PerfSyncGNN is prediction-only. This phase implements deterministic synchronization,
not GNN inference, reinforcement learning, scheduling, or serverless latency measurement.
The supplied paper informed entity/PV/version/operation semantics; its older abstract's
RL scope is not adopted. No application modules or adapters existed in the initial
Phase 1 directories; existing third-party checkouts and virtual environment were preserved.

## Install and run

Python >=3.9, SUMO and netconvert must be on PATH. No external map download is needed.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest -q -rs
.venv/bin/python -m perfsyncgnn.phase2 local --backend sqlite --follow
```

The last command explicitly uses a local SQLite twin backend. It is not a Ditto test.
The default backend is Ditto. Run from the repository root. The JSON config controls
seed, duration, arrival rate, observation interval, step and RSU regions. Run IDs scope
entity identities and sequence lineages: reuse one only to replay the same simulation;
use a new ID for a different scenario/configuration. The generated two-lane intersection
uses actual TraCI vehicle positions/speeds/roads/lanes, departures/arrivals, signal state,
and road aggregates. SUMO normalizes the center to x=300; the configured two-region
policy assigns x<300 to rsu-west and x>=300 to rsu-east. Lane changes do not cause Transfer.
Vehicle arrival times are seeded exponential arrivals, with four route directions; they
are SUMO inputs, not invented observations. Simulation cutoff does not fabricate departures.

Generated net/route/config XML and the exact scenario JSON are saved in the output's
`scenario/` directory. Events and lifecycle transitions are JSONL; SQLite stores durable
inbox/outbox and operations. Use separate output directories for producer and consumer.
Do not share a journal with different simulations or manually edit journal contents.

## Ditto

Use an existing Eclipse Ditto deployment, including its persistence and authentication.
The preserved checkout at `third_party/ditto/deployment/docker` contains upstream
Docker deployment files. This implementation does not independently write Ditto's MongoDB.

```bash
export DITTO_URL=http://localhost:8080
export DITTO_USERNAME=your-user
export DITTO_PASSWORD=your-password
export DITTO_POLICY_ID=your-namespace:your-policy
.venv/bin/python -m perfsyncgnn.phase2 local --backend ditto --follow --output results/ditto
DITTO_TEST_URL="$DITTO_URL" .venv/bin/python -m pytest -q tests/test_services.py -rs
```

Provision a policy granting the authenticated principal READ/WRITE on the test Things.
Each dedicated Thing stores the complete authoritative Twin in `attributes.perfsync`.
GET requires ETag; create uses If-None-Match:* and replacement uses If-Match.
The application owns these Things; do not put unrelated attributes/features in them,
because a full Thing replacement is used. Stable IDs include run, entity type and physical ID.
Live tests create uniquely named test Things and retain them for inspection.

## MQTT and Kafka

Use a persistent Mosquitto/Kafka deployment. MQTT requires persistent broker storage
and a stable consumer client ID, QoS 1, and persistent sessions. Establish the MQTT
consumer before the producer: an absent, never-established subscription cannot receive
historical MQTT messages. Kafka retains events independently of consumer availability.

```bash
# Terminal 1; defaults to Ditto, or explicitly add --backend sqlite for local DT tests
.venv/bin/python -m perfsyncgnn.phase2 consume --broker mqtt --output results/consumer
# Terminal 2
.venv/bin/python -m perfsyncgnn.phase2 publish --broker mqtt --follow --output results/producer

# Kafka alternatives (default topic perfsync.events)
.venv/bin/python -m perfsyncgnn.phase2 consume --broker kafka --bootstrap localhost:9092 --output results/kafka-consumer
.venv/bin/python -m perfsyncgnn.phase2 publish --broker kafka --bootstrap localhost:9092 --follow --output results/kafka-producer

MQTT_TEST_HOST=localhost .venv/bin/python -m pytest -q tests/test_services.py -k mqtt
KAFKA_TEST_BOOTSTRAP=localhost:9092 .venv/bin/python -m pytest -q tests/test_services.py -k kafka
```

Producer writes its outbox before sending, marks sent only on broker acknowledgement,
and flushes pending rows on restart. Delivery is at least once. Consumers acknowledge
MQTT or commit Kafka offsets only after durable ingestion and processing. The inbox
handles redelivery. Outages or invalid payloads stop processing visibly and leave records
pending; no poison-message skipping or fabricated acknowledgement is used. Resume by
restarting the consumer after correcting the cause. Broker authentication/TLS, topic ACLs,
replication and retention must be configured in the service deployment; CLI examples
use unauthenticated local brokers. This is an initial testbed, not a hardened deployment.

## Rules and dependencies

| Operation | Trigger/preconditions | Verified committed transition |
|---|---|---|
| Attach | Insertion or observation without association; reattach requires insertion | Association attached, expected physical ID and owner |
| Follow | Explicit request for known PVs on attached target; valid subscriber and lifetime | Exact subscription and simulation-time expiry |
| Follow renewal | Observation at/after expiry of an explicitly `auto_renew: true` subscription | Extended subscription expiry |
| Transfer | New admissible observation in a different configured ownership region | Expected owner and ownership-version increment |
| Update | Attached entity; increasing entity sequence, nondecreasing source time, matching owner | Exact PV patch, sequence, timestamp and state version |
| Detach | New departure/removal from current owner | Association detached, no owner/subscriptions, departure sequence |

Subscriptions default to no automatic renewal. An explicit renewal must extend expiry.
Automatic renewal is evaluated on new admissible observations, not by a background timer.
Follow on an unknown/unattached target fails. The CLI's --follow is an explicit demo
tracking request, emitted once on each actual vehicle insertion. Ordinary observations
do not subscribe themselves. Lifetimes and expiry use simulation time.

An event may produce Attach -> Update or Transfer -> Update; requested tracking follows
initial association/update. An authorized due renewal follows Update. Operations in one
plan form a dependency chain. A failed prerequisite cancels dependants without latency
labels. Earlier durable inbox rows drain before Detach; later observations are ignored
while detached until a new insertion. Stale/duplicate observations cannot transfer ownership.
Higher sequence with older timestamp is also stale. Sequence numbers are global per
physical entity within a run, continuing across logical RSU changes, not independent
per-RSU counters. Producers with different counter semantics need an explicit mapping.

Ignored events remain in the inbox with an outcome; conflicting content for one dedup
identity raises an error. Event IDs are human-readable; dedup uses run/type/entity/kind/sequence.
Physical identity includes entity type, avoiding collisions between road and vehicle IDs.

## Persistence and concurrency guarantees

The journal has a process lock. Workers sharing one journal serialize planning and
execution in durable inbox order (stronger than per-entity ordering; throughput scaling
is deferred). The complete operation plan is saved transactionally before external writes.
Lifecycle and operation rows are saved together. This lock is for one host/filesystem;
independent hosts must not use unrelated journals for the same entity lineage. Ditto's
ETag or SQLite CAS protects versioned writes from outside writers; conflicting stale
plans fail or retry visibly instead of overwriting accepted versions.

Every operation adds one application version. Attach/Transfer increment ownership version.
The intended transition and an operation receipt are written in one conditional Thing
replacement. The predicate is checked against the intended state before writing and the
committed state after writing. If a later valid version has already advanced the entity,
the atomic receipt proves that the validated transition was included in that version
lineage. This relies on all writers respecting this protocol. It is not a globally atomic
transaction across the journal, Ditto and broker. Read-after-write alone is not an atomicity
claim. Ditto durability/availability depends on its configured deployment.

Ambiguous responses, CAS retry exhaustion and verification failures remain nonterminal
with an error record and propagate to the caller. Retry/recovery checks the durable
operation receipt before writing again. Deterministic precondition failures are FAILED.
The state model additionally supports CANCELLED and SUPERSEDED; supersession is not
used by the FIFO policy. Receipts are currently retained indefinitely; archival/compaction
and distributed high-throughput ownership fencing are outside Phase 2.

SQLite uses WAL and synchronous=FULL by default. Files must be on local storage with
working locks. For quicker local verification, PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL
is process-crash safe but can lose recent transactions on OS/power failure. Tests select
NORMAL and validate process restart recovery, not power-loss durability.

```bash
.venv/bin/python -m perfsyncgnn.phase2 recover --backend sqlite --output results/phase2
```

Recovery also runs before consumer processing. Pending plans and receipts survive process
restart. A simulation crash is recovered by replaying the same seeded run; active SUMO
process state is not checkpointed. An ignored event interrupted before its done marker
is harmlessly reevaluated. Journals and DT state require coordinated backup/retention.

## Clocks and future learning data

Event source and simulation timestamps are simulation milliseconds. emitted_at_unix_ms
is a separate host wall clock. No subtraction between simulation and Unix clocks is used.
Operation request/start/verified-commit timestamps use Unix ns. Runtime duration uses
monotonic ns only within the same executor clock session. After recovery, verification
time is recorded, but duration_ns stays null for requests from an earlier session: actual
remote commit time may have preceded acknowledgement. Do not use recovered durations
as ordinary latency labels. Failed/cancelled/superseded operations have no commit timestamp
or successful latency label. COMMITTED alone is insufficient for selecting a training
sample: require a non-null, same-session duration and valid provenance.

The lifecycle JSONL includes IDs, physical/DT identities, event and simulation provenance,
source RSU, owner, dependencies, versions, status transitions, errors and request/start/
commit clocks. Local executor durations measure this Python pipeline and storage, not
OpenWhisk queueing/cold starts. P/D/F/R/C/O graph construction and all prediction/model
work remain later phases.
