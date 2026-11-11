> Update 2026-11-01: connected SUMO-GUI and automatic supervision are now available through [perfsyncctl](PHASE2_ONE_COMMAND_GUIDE.md). This document preserves the earlier manual workflow; its former GUI limitation is superseded.

# Phase 2 user guide: run, observe, and verify

Verified on Ubuntu, 2026-10-31 (Asia/Seoul). Repository:
`/home/hpclab-lananh/PerfSyncGNN`. Current results and service limitations are in
[PHASE2_VERIFICATION.md](PHASE2_VERIFICATION.md).

## 1. Choose the path that currently works

| Path | What it does | Verification |
|---|---|---|
| Headless SUMO -> TraCI -> manager -> SQLite | Real traffic, all five operations, local persisted DTs | Passed |
| Generated scenario -> standalone SUMO-GUI | Visual replay of the same traffic configuration | GUI executable completed under Xvfb |
| Headless SUMO -> manager -> Ditto | Direct live Ditto synchronization, no broker | Live five-operation Ditto test and MQTT traffic commits passed |
| Headless SUMO -> MQTT/Kafka -> consumer -> Ditto | Distributed event path | MQTT full traffic path passed; Kafka live round trip and CLI smoke verified |
| One SUMO-GUI process feeding the manager | Interactive GUI connected to TraCI pipeline | Not implemented: adapter hardcodes `sumo` |

**Do not mistake the separate GUI replay for a live DT connection.** There is no `--gui`
flag, standalone event-generator script, synchronization HTTP API, or custom DT dashboard.
The `local` CLI mode embeds TraCI, the generator, and the manager in one process.
The `publish` mode embeds TraCI and publication; `consume` runs the manager.
The name `local` describes Python execution and can use either SQLite or live Ditto.

## 2. Prerequisites and environment

Every terminal starts here:

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
python -m perfsyncgnn.phase2 --help
```

The existing `.venv` works. Python >=3.9 is required. Dependencies are declared in
`pyproject.toml`. Only if setting up a fresh Ubuntu machine:

```bash
sudo apt-get update
sudo apt-get install python3-venv sumo sumo-tools jq apache2-utils
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

Docker Engine and Compose v2 are additionally required for the bundled Ditto deployment.
They are already available on this host. SUMO-GUI needs a working desktop `DISPLAY`;
Xvfb is only for offscreen executable checks. Ubuntu SUMO supplies `sumo-gui`.

```bash
command -v python sumo sumo-gui netconvert jq
sumo --version
sumo-gui --version
docker compose version
docker ps --format '{{.Names}} {{.Ports}}'
```

No Docker service is required for the verified SQLite path. A Ditto deployment requires
at least 2 Docker CPUs / 4 GB RAM according to the bundled upstream README. Do not run
OpenWhisk, Kubernetes, MongoDB clients, graph databases, or training jobs for this demo.

## 3. Shortest verified demo (two terminals)

### Terminal 1: run actual traffic and local DT synchronization

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
python -m perfsyncgnn.phase2 local --backend sqlite --follow \
  --config configs/intersection.json --output results/phase2-guide/local
```

`--follow` explicitly requests tracking of speed/position for every actual inserted vehicle.
The simulator itself starts SUMO; do not start another TraCI server for this command.
The same output directory and unchanged run are retry-safe; use a new run ID and output
directory for a changed scenario. A repeat run retains the original operations/durations.
For a fresh timing measurement, use a fresh journal and fresh DT run ID.

Default SQLite writes use WAL/FULL. The execution verified in this session used:

```bash
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL python -m perfsyncgnn.phase2 local \
  --backend sqlite --follow --output results/phase2-guide/local
```

NORMAL is an optional faster local-demo setting: process-restart recovery is supported,
but recent transactions can be lost after OS/power failure. It is not the production
setting or evidence of power-loss durability. Simulation seconds are not wall seconds.
The run is unpaced and may finish before you start a monitoring command.

Actual verified summary: 26 Attach, 17 Follow, 1,714 Update, 9 Transfer, 7 Detach;
1,773 operations, all COMMITTED against the explicit SQLite backend.

### Terminal 2: see the same scenario in SUMO-GUI

After Terminal 1 has generated its `scenario/` files:

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
sumo-gui -c results/phase2-guide/local/scenario/intersection.sumocfg \
  --seed 27 --delay 100
```

This opens paused. Use Play to run, Stop to pause, Play to resume; Single Step advances
one tick while paused. Close the window to end the replay. Increasing Delay slows the
animation; `--delay 100` waits 100 ms between steps. Page Up/Page Down change delay.
Right-click a vehicle -> Show Parameter to inspect its ID, speed and lane; Start Tracking
follows it. Mouse wheel zooms and dragging pans. Inspect signal colors at the center;
right-click signal/junction objects for their parameters. These controls follow the
[official SUMO-GUI documentation](https://sumo.dlr.de/docs/sumo-gui.html).

Vehicles are generated on four routes with two lanes per arm. They enter, move, stop
at signals and eventually leave. At 90 simulation seconds the default run ends; vehicles
still present are not falsely detached. Reload the configuration to replay it.

There are no drawn RSU polygons. `rsu_boundary_x=300` is a logical vertical dividing
line at the normalized intersection center: x<300 is rsu-west; x>=300 is rsu-east.
Inspect `position_x`, event `source_id`, Transfer records and DT `owner` to see crossings.
Changing lanes or road IDs alone does not imply an ownership change.

A standalone GUI process generates no PerfSyncGNN JSONL or synchronization operations.
Those are produced by Terminal 1's separate headless TraCI run. A shared seed/configuration
permits comparison by simulation time, but the two processes do not share a clock or state.
Do not alter vehicle/signal states interactively and then assume they match the headless run.

## 4. Actual service files, ports, and startup order

| Component | Existing file / implementation | Address |
|---|---|---|
| Ditto stack | `third_party/ditto/deployment/docker/docker-compose.yml` | nginx HTTP 8080; gateway 8081; MongoDB 27017 |
| Ditto auth | adjacent `nginx.conf`, `nginx.htpasswd` | Basic auth through nginx; subject `nginx:<username>` |
| MQTT local test configuration | `third_party/ditto/connectivity/service/src/test/resources/mosquitto.conf` | 1883 |
| MQTT transport | `simulation/event_generator/publisher.py`, `consumer.py` | topic `perfsync/events`, QoS 1 |
| Kafka transport | same Python modules | bootstrap 9092; topic `perfsync.events`; group `perfsync-sync` |
| Kafka deployment | **No bundled config**; official single-node image tested during guide verification | 9092 |
| Sync manager | `python -m perfsyncgnn.phase2 consume` | No HTTP listener |

`infrastructure/docker/` and `scripts/` contain no deployment/start scripts. Do not run
fictional `start_simulation.sh`, `docker-compose.phase2.yml` or a `--gui` CLI option.
MQTT and Kafka are alternative paths; both need not run. Ditto is authoritative when
selected; do not independently write its MongoDB.

### Terminal A: start the bundled Ditto stack

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
docker compose -p perfsync-phase2-guide \
  -f third_party/ditto/deployment/docker/docker-compose.yml config --services
# This host already has the verification auth override; retain it on recreation:
if test -f results/phase2-guide/ditto-auth.override.yml; then
  docker compose --progress plain -p perfsync-phase2-guide \
    -f third_party/ditto/deployment/docker/docker-compose.yml \
    -f results/phase2-guide/ditto-auth.override.yml up -d
else
  docker compose --progress plain -p perfsync-phase2-guide \
    -f third_party/ditto/deployment/docker/docker-compose.yml up -d
fi
docker compose -p perfsync-phase2-guide \
  -f third_party/ditto/deployment/docker/docker-compose.yml ps
docker compose -p perfsync-phase2-guide \
  -f third_party/ditto/deployment/docker/docker-compose.yml logs --tail 80 gateway things policies nginx
```

The actual services are mongodb, policies, things, things-search, connectivity, gateway,
ditto-ui, swagger-ui and nginx. The file defaults to mutable `latest` Ditto images unless
you supply `DITTO_VERSION`; this session observed Ditto 3.9.8 in the container logs.
No release tag is pinned by repository configuration.
The Compose obsolete `version` warning does not itself prevent startup.
Allow time for image downloads, cluster formation and health checks (120-second start
period). This session's startup attempt is reported separately; a parsed Compose file
is not evidence of a healthy Ditto service.

Use `http://localhost:8080` through nginx, not direct gateway port 8081. The latter uses
a different preauthentication path. Upstream nginx also exposes `/ui/`; it is a generic
Ditto UI, not a verified PerfSyncGNN dashboard. API queries below are the supported
inspection path.

### Terminal B: start MQTT with queue capacity for the unpaced publisher

The raw upstream test config was executable but its default per-client queue overflowed
in the first full run: broker logs explicitly reported outgoing messages being dropped.
That preliminary run is **not** a successful end-to-end demo. A runtime config copy
corrects queue/persistence settings for this small local workload without editing source:

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
python - <<'PYMQTT'
from pathlib import Path
p=Path('results/phase2-guide')
p.mkdir(parents=True,exist_ok=True)
base=Path('third_party/ditto/connectivity/service/src/test/resources/mosquitto.conf').read_text()
(p/'mosquitto.conf').write_text(base+'\nmax_queued_messages 0\nmax_queued_bytes 0\npersistence_location /mosquitto/data/\nautosave_interval 5\n')
PYMQTT
docker run -d --name perfsync-phase2-guide-mqtt \
  -p 127.0.0.1:1883:1883 \
  --mount type=bind,src=/home/hpclab-lananh/PerfSyncGNN/results/phase2-guide/mosquitto.conf,dst=/mosquitto/config/mosquitto.conf,readonly \
  --mount type=volume,src=perfsync-phase2-guide-mqtt-data,dst=/mosquitto/data \
  eclipse-mosquitto:2
docker logs --tail 40 perfsync-phase2-guide-mqtt
```

Use `docker start perfsync-phase2-guide-mqtt` if the corrected named container already
exists and is stopped. Do not run two brokers on port 1883. The unlimited local queue
is suitable for this bounded ~1,738-event test; it is not production flow control.
Do not substitute the unmodified upstream test config for this full workload.
Broker PUBACK alone does not prove delivery to a subscriber: compare outbox and inbox
identity sets as well as successful DT operations. Configure bounded capacity and
backpressure appropriately for larger workloads.

The config is anonymous on loopback. Python CLI has no broker TLS or username/password
flags. The named volume retains broker data; do not prune it as routine shutdown.

There is no bundled Kafka deployment configuration. For this verification, the existing
Kafka adapter was tested against a separately launched official single-node local broker:

```bash
docker run -d --name perfsync-phase2-guide-kafka \
  -p 127.0.0.1:9092:9092 apache/kafka:3.9.1
docker logs --tail 15 perfsync-phase2-guide-kafka
# After Kafka Server started, provision the application topic BEFORE the consumer:
docker exec perfsync-phase2-guide-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic perfsync.events --partitions 1 --replication-factor 1
```

Wait for “Kafka Server started” before testing; an open mapped port during startup is
insufficient. This executable command follows the [Apache Kafka 3.9 Docker instructions](https://kafka.apache.org/39/getting-started/docker/).
It is a local single-node test broker with no explicitly mounted data volume, not a
repository Compose service or a production deployment. Use `docker start` to resume an
existing stopped named container. `docker compose up kafka` against Ditto's Compose file
cannot work because there is no such service.

## 5. Ditto authentication, policy, and live direct run

These commands require healthy services and configured authentication. The live Ditto
and MQTT/Kafka round-trip tests passed during this guide verification. Use your configured Basic-auth identity;
do not print or commit passwords. Environment variables are the adapter's existing
mechanism. Run the following in every terminal that talks to Ditto:

For the **already prepared verification setup on this host**, a generated identity is
stored in `results/phase2-guide/private/credentials.json` (private directory 0700, file
0600). Its runtime auth override is `results/phase2-guide/ditto-auth.override.yml`.
No repository auth file was changed. Do not print, paste or commit that JSON. Load it
securely into your terminal without echoing its values:

```bash
source <(python - <<'PYAUTH'
import json,shlex
from pathlib import Path
c=json.loads(Path('results/phase2-guide/private/credentials.json').read_text())
for key,value in c.items():
    print('export '+key+'='+shlex.quote(value))
PYAUTH
)
```

When recreating this task's nginx container, include its actual runtime override:

```bash
docker compose -p perfsync-phase2-guide \
  -f third_party/ditto/deployment/docker/docker-compose.yml \
  -f results/phase2-guide/ditto-auth.override.yml up -d
```

An initial cluster-bootstrap stall produced HTTP 502. After the other cluster containers
were running, `docker restart perfsync-phase2-guide-policies-1` resolved it and the policy
creation/live test passed. Use that diagnostic action only if logs show the same
one-contact-point bootstrap stall; do not repeatedly restart a healthy cluster.

For **a different setup with your own existing auth file**, use the manual prompts below
instead of loading this host's private verification identity:

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
export DITTO_URL=http://localhost:8080
read -r -p 'Configured Ditto username: ' DITTO_USERNAME
read -r -s -p 'Ditto password: ' DITTO_PASSWORD
printf '\n'
export DITTO_USERNAME DITTO_PASSWORD
export DITTO_POLICY_ID=perfsync:phase2-guide
```

Your username must already be present in the bundled `nginx.htpasswd` or equivalent
service configuration. Do not assume a password. If you need to add your own local user,
`htpasswd third_party/ditto/deployment/docker/nginx.htpasswd "$DITTO_USERNAME"`
prompts for a password; this deliberately changes that auth file. Restart only nginx
after such a change. No auth files were changed by this guide task.

Create or review the dedicated policy, using authentication in the Python process rather
than a password in command arguments. This writes the named local test policy; choose
a different `DITTO_POLICY_ID` if that policy already belongs to another workload:

```bash
python - <<'PY'
import os, requests
from urllib.parse import quote
base = os.environ['DITTO_URL'].rstrip('/')
s = requests.Session()
s.auth = (os.environ['DITTO_USERNAME'], os.environ['DITTO_PASSWORD'])
policy = {'entries': {'phase2': {
    'subjects': {'nginx:' + os.environ['DITTO_USERNAME']: {'type': 'Phase 2 local test user'}},
    'resources': {
        'thing:/': {'grant': ['READ', 'WRITE'], 'revoke': []},
        'policy:/': {'grant': ['READ', 'WRITE'], 'revoke': []}
    }
}}}
r = s.put(base + '/api/2/policies/' + quote(os.environ['DITTO_POLICY_ID'], safe=''),
          json=policy, timeout=15)
r.raise_for_status()
print('policy HTTP', r.status_code)
PY
DITTO_TEST_URL="$DITTO_URL" python -m pytest -q -rs tests/test_services.py::test_live_ditto_complete_lifecycle
```

A successful authenticated response plus the live lifecycle test establishes more than
a TCP connection. 401 means authentication failed; 403 usually means policy/subject
permissions; 5xx or connection refusal means service readiness/connectivity.

For the shortest actual SUMO -> Ditto path (no broker required):

```bash
python -m perfsyncgnn.phase2 local --backend ditto --follow \
  --config configs/intersection.json --output results/phase2-guide/ditto
```

It runs headlessly. Observe the actual remote Things as described in section 7. Reusing
a simulation run ID against existing Things reuses that state, so later observations may
be ignored as stale after the first completed run. For a new independent Ditto demo,
create a copied config with a fresh run ID as in section 11; do not erase real Things.

## 6. Broker workflow: separate terminals

After healthy Ditto and the chosen broker are available, use a fresh run ID for new
visible changes. The recorded verified run config is `results/phase2-guide/mqtt-verified.json`
(run ID `intersection-guide-mqtt-verified`); replaying it against its already committed
Things demonstrates idempotence rather than new state transitions. For a fresh demo,
prepare a config copy with existing schema fields before starting the producer:

```bash
python - <<'PYLIVE'
import json,uuid
from pathlib import Path
from simulation.scenarios.intersection import Scenario
c=json.loads(Path('configs/intersection.json').read_text())
c['run_id']='intersection-live-'+uuid.uuid4().hex[:12]
p=Path('results/phase2-guide/live.json')
p.parent.mkdir(parents=True,exist_ok=True)
p.write_text(Scenario.model_validate(c).model_dump_json(indent=2))
print('Run ID:',c['run_id'])
PYLIVE
```

Run this preparation once per new demonstration, not while a producer is running.
The commands below use the original verified output directory names; journals retain
old records, so set FILTER_RUN to the new config's run_id when inspecting a repeat.
The newest producer events.jsonl replaces its prior text export; SQLite outbox/history
remains. Choose fresh --output directories in both terminals to keep distinct exports.

### Terminal C: consumer / synchronization manager FIRST

Activate `.venv`, set Ditto environment as in section 5, then:

```bash
python -m perfsyncgnn.phase2 consume --backend ditto --broker mqtt \
  --host localhost --port 1883 --output results/phase2-guide/mqtt-verified-consumer
```

MQTT client ID is fixed to `perfsync-sync`, persistent session, manual QoS1 ack.
Run only one consumer with this client ID. Establish its subscription before publishing;
a never-established MQTT subscriber has no historical messages. Confirm subscription
in `docker logs perfsync-phase2-guide-mqtt` before Terminal D. The CLI has no readiness
banner; merely seeing a running Python process is insufficient proof of a subscription.

### Terminal D: TraCI generator / publisher

```bash
cd /home/hpclab-lananh/PerfSyncGNN
source .venv/bin/activate
python -m perfsyncgnn.phase2 publish --broker mqtt --host localhost --port 1883 \
  --follow --config results/phase2-guide/live.json --output results/phase2-guide/mqtt-verified-producer
```

This launches its own headless SUMO, emits actual observations and explicit Follow
requests, persists its outbox, and publishes on broker acknowledgement. Producer output
may show zero operations: operation execution belongs to Terminal C. Do not start
`local` concurrently for the same run/Things through a different journal.

### Terminal E: inspect events and consumer journal

```bash
tail -f results/phase2-guide/mqtt-verified-producer/events.jsonl
```

Use `RUN_DIR=results/phase2-guide/mqtt-verified-consumer` with the journal queries in section 9.
Use the **producer** directory for physical `events.jsonl`: consumers do not create it.
DT changes are seen through Ditto GETs, not the producer journal.

### Kafka alternative

With the separately launched local test broker (or a real external bootstrap service) already running:

```bash
# Consumer terminal, with Ditto environment configured
python -m perfsyncgnn.phase2 consume --backend ditto --broker kafka \
  --bootstrap localhost:9092 --output results/phase2-guide/kafka-consumer
# Separate producer terminal
python -m perfsyncgnn.phase2 publish --broker kafka --bootstrap localhost:9092 \
  --follow --output results/phase2-guide/kafka-producer
```

Topic is `perfsync.events`; CLI has no topic override. Create it before subscribing.
The existing consumer exits on UNKNOWN_TOPIC_OR_PART if started before topic creation;
this occurred during verification and was resolved by topic provisioning and restart.
Broker auto-creation after a producer starts is too late for that first consumer attempt. Kafka producer uses idempotence/acks=all; consumer group `perfsync-sync`
commits offsets after processing. Kafka retains historical records independently of
consumer subscription. Existing committed group offsets affect replay. Do not reset
shared offsets to demonstrate a fresh run; use an isolated broker/test context.

## 7. Inspect remote digital twins and compare with physical events

### List Things using the actual HTTP API

With Ditto environment set:

```bash
python - <<'PY'
import os, requests
s = requests.Session()
s.auth = (os.environ['DITTO_USERNAME'], os.environ['DITTO_PASSWORD'])
r = s.get(os.environ['DITTO_URL'].rstrip('/') + '/api/2/search/things',
          params={'filter': 'eq(attributes/perfsync/run_id,"intersection-seed27")',
                  'fields': 'thingId,attributes/perfsync/physical_id,attributes/perfsync/version'},
          timeout=15)
r.raise_for_status()
print(r.text)
PY
```

Search is indexed and eventually consistent; an immediate missing search result does
not prove a write failed. Direct GET by a known Thing ID is used for commit verification.
Search results can be paginated. There is no FastAPI endpoint in Phase 2.

### Pick an actual vehicle and retrieve its Thing

For direct Ditto mode, set the physical event source to `results/phase2-guide/ditto`;
for MQTT use `results/phase2-guide/mqtt-verified-producer`:

```bash
export EVENT_DIR=results/phase2-guide/mqtt-verified-producer
python - <<'PY'
import json, os
from pathlib import Path
from urllib.parse import quote
import requests
from simulation.event_generator.schemas import TrafficEvent
from dt.models import digital_id
p = Path(os.environ['EVENT_DIR']) / 'events.jsonl'
event = next(TrafficEvent.model_validate_json(line) for line in p.open()
             if json.loads(line)['entity_type'] == 'vehicle')
s = requests.Session()
s.auth = (os.environ['DITTO_USERNAME'], os.environ['DITTO_PASSWORD'])
r = s.get(os.environ['DITTO_URL'].rstrip('/') + '/api/2/things/' +
          quote(digital_id(event), safe=''), timeout=15)
r.raise_for_status()
print('physical ID:', event.entity_id, 'Thing ID:', digital_id(event), 'ETag:', r.headers.get('ETag'))
print(json.dumps(r.json()['attributes']['perfsync'], indent=2))
PY
```

Do not manually remove percent escaping: `digital_id()` encodes slashes within the
Thing name, and the HTTP path must quote that entire ID again, just as `DittoAdapter` does.
State is under `attributes.perfsync`, not Ditto `features`. Inspect:

| Field | Meaning |
|---|---|
| physical_id, dt_id, entity_type, run_id | Association identity / sequence lineage |
| process_variables | Vehicle x/y, speed, lane_id, road_segment; type-specific signal/road values |
| association_status | attached or detached (unlinked during initialization) |
| owner, ownership_version | Current logical RSU and ownership revision |
| version | Monotonic application version, distinct from HTTP ETag / Ditto revision |
| source_timestamp_ms, sequence_number | Latest committed source ordering metadata |
| subscriptions | Follow subscriber, variables, lifetime and simulation expiry |
| last_operation, applied_operations | Last operation and atomic commit receipts |

Repeat this GET while the **headless** simulation is running to see changing positions,
speeds and versions. Because there is no pacing, the run may finish rapidly or the request
may catch an older/newer observation. Compare sequence and simulation timestamp, not
wall-clock equality with a separate GUI.

### Compare an observation with its committed Update

Run after/while the consumer is processing; select the consumer journal and corresponding
producer event stream (or the same direct Ditto directory for both):

```bash
export RUN_DIR=results/phase2-guide/mqtt-verified-consumer
python - <<'PY'
import json, os, sqlite3
from pathlib import Path
from urllib.parse import quote
import requests
j = sqlite3.connect('file:' + str(Path(os.environ['RUN_DIR'])/'journal.sqlite') + '?mode=ro', uri=True)
row = j.execute("""SELECT body FROM operations
    WHERE json_extract(body,'$.operation_type')='Update'
      AND json_extract(body,'$.status')='COMMITTED'
      AND json_extract(body,'$.event.entity_type')='vehicle'
    ORDER BY rowid DESC LIMIT 1""").fetchone()
assert row, 'No verified vehicle Update yet'
op = json.loads(row[0])
s = requests.Session()
s.auth = (os.environ['DITTO_USERNAME'], os.environ['DITTO_PASSWORD'])
r = s.get(os.environ['DITTO_URL'].rstrip('/') + '/api/2/things/' + quote(op['dt_id'], safe=''), timeout=15)
r.raise_for_status()
twin = r.json()['attributes']['perfsync']
receipt = twin['applied_operations'].get(op['operation_id'])
assert receipt and receipt['version_after'] == op['version_after']
print('vehicle/event/sequence:', op['event']['entity_id'], op['event']['event_id'], op['event']['sequence_number'])
print('physical PVs:', op['event']['process_variables'])
print('committed receipt version:', receipt['version_after'], 'current version:', twin['version'])
if twin['version'] == op['version_after']:
    assert all(twin['process_variables'].get(k) == v for k,v in op['event']['process_variables'].items())
    assert twin['sequence_number'] == op['event']['sequence_number']
    print('Current DT exactly matches this Update')
else:
    print('DT has advanced; receipt proves historical application, not equality of current PVs')
PY
```

The operation embeds the same validated physical event published by TraCI. To match
it to the original JSONL, filter by event_id and run_id. An operation receipt is written
in the same conditional Thing replacement as the validated transition. A receipt is
historical proof under this writer protocol, not a distributed transaction or an
independent audit of other writers. If current version advanced, do not demand that old
PVs equal the latest state. Ditto must be the only authoritative DT writer.

## 8. Demonstrate all five operations

A single **headless** CLI run with `--follow` demonstrates the five operations locally.
The same commands with `--backend ditto` require the live service checks to pass first.
Handlers live in `synchronization/operations/<lowercase-operation>.py`.

For each successful operation the lifecycle is:
`CREATED -> VALIDATED -> QUEUED -> RUNNING -> COMMITTING -> COMMITTED`.
Request acceptance or RUNNING/COMMITTING is not a successful commit. `apply()` mutates
an in-memory copy; the executor must perform a conditional write and verify its receipt
and operation predicate before recording COMMITTED.

| Operation | Real trigger and observable state | Reasons it may not trigger / commit | Exact existing reproduction test |
|---|---|---|---|
| Attach (`attach.py`) | Actual insertion/first admissible observation without association; attached physical ID, owner; version +1 | Already attached; duplicate; detached target needs explicit insertion | `python -m pytest -q tests/test_phase2.py::test_attach_update_verified` |
| Follow (`follow.py`) | `--follow` creates explicit request after insertion/update; `subscriptions.phase2-demo` with speed/position_x and expiry | No --follow; unknown/unattached target; unknown PV; renewal fails to extend expiry | `python -m pytest -q tests/test_phase2.py::test_lifecycle_all_five_and_follow_renewal` |
| Update (`update.py`) | New entity sequence and nondecreasing source time; correct attached owner; exact PV patch/seq/time; version +1 | Duplicate/stale observation; owner/association mismatch; service or CAS error | `python -m pytest -q tests/test_phase2.py::test_each_commit_predicate_rejects_wrong_state` |
| Transfer (`transfer.py`) | New observation crosses x=300 policy; owner west/east changes, ownership_version +1, app version +1, then dependent Update | No crossing; stale sequence/time; mismatched previous owner/version | `python -m pytest -q tests/test_sumo.py::test_real_sumo_reproducible_and_transfer` |
| Detach (`detach.py`) | Actual SUMO arrival (event_type departure); association detached, owner null, subscriptions empty, source sequence/time advanced | Vehicle still active at cutoff; earlier pending operation; stale departure; wrong owner | `python -m pytest -q tests/test_phase2.py::test_detach_waits_for_pending_update_then_blocks_late_observation` |

All tests in `test_phase2.py` use explicit constructed test events and SQLite/fakes; they
are not measured physical traffic or real Ditto. The SUMO test uses actual traffic.

Renewal demonstrations use these existing tests:

```bash
python -m pytest -q tests/test_phase2.py::test_lifecycle_all_five_and_follow_renewal
python -m pytest -q tests/test_phase2.py::test_follow_due_only_for_explicit_auto_renew_subscription
```

Explicit Follow lifetime must be 1..86,400,000 simulation milliseconds and requested
PVs must exist. CLI requests expire after 30,000 simulation ms and have no auto-renew.
Opt-in `auto_renew: true` is implemented in the event/handler interface and tested;
its renewal is evaluated on admissible observations at/after expiry, not a timer.
Follow stores durable subscription metadata; it does not establish a new per-vehicle
MQTT subscription or change TraCI polling behavior. Expired metadata is not automatically
removed from the dictionary; consult `expires_simulation_ms` for validity.

### Complete actual vehicle example from the verified run

The ID was selected from records after requiring all five committed operation types;
it was not assumed in advance. On this run it was **vehicle-0000**:

| Simulation time (ms) | Observed event / commit | Version after | Relevant result |
|---:|---|---:|---|
| 200 | Insertion -> Attach | 1 | rsu-west, attached, x=5.1 |
| 200 | Initial Update, then explicit Follow | 2, then 3 | Initial PVs, then subscription |
| 1,000 | Observation -> Update | 4 | x=6.0798326495; speed=1.9774262188; WC_0 |
| 49,000 | Observation -> Transfer | 52 | x=308.2252574271, new rsu-east owner |
| 49,000 | Dependent Update | 53 | Same observation's PVs committed |
| 77,800 | Actual arrival -> Detach | 82 | detached, owner=null, subscriptions={} |

The implemented order is **Attach -> initial Update -> Follow -> later Updates ->
Transfer -> Update -> ... -> Detach**, not Attach -> Follow -> first Update. This is
because Follow validates the requested PVs already exist. Final sequence is 79,
ownership_version=2; source timestamp=77,800. Final retained PVs are the last Update,
not an invented observation at departure. Detach updates ordering metadata without
clearing historical PVs. Ten vehicles remained active at the 90-second cutoff.

To select a qualifying ID dynamically on your own completed run:

```bash
export RUN_DIR=results/phase2-guide/local
export ENTITY_ID="$(python - <<'PY'
import collections,json,os,sqlite3
from pathlib import Path
j=sqlite3.connect('file:'+str(Path(os.environ['RUN_DIR'])/'journal.sqlite')+'?mode=ro',uri=True)
by=collections.defaultdict(set)
for (body,) in j.execute('SELECT body FROM operations ORDER BY rowid'):
    o=json.loads(body)
    if o['event']['entity_type']=='vehicle' and o['status']=='COMMITTED':
        by[o['event']['entity_id']].add(o['operation_type'])
print(next((entity for entity,kinds in by.items() if kinds >= {'Attach','Follow','Update','Transfer','Detach'}),''))
PY
)"
printf '%s\n' "$ENTITY_ID"
```

If empty, the run may lack --follow, crossing or arrival; extend duration or examine the
separate tests, rather than fabricating operations. `vehicle-example.json` under
`results/phase2-guide/` preserves this task's selected local example and final state.

## 9. Lifecycle logs: live view, filter, export

| File/table | Contents / availability |
|---|---|
| `<output>/events.jsonl` | Physical and explicit Follow input events; local/publish modes, flushed per physical event |
| `<output>/journal.sqlite` `inbox` | Durable validated inputs, dedup fingerprints, done/outcome |
| `operations` | Latest status/model per operation |
| `lifecycle` | Every recorded transition/attempt error, in ordinal order |
| `outbox` | Publication body and sent flag; producer only |
| `<output>/twins.sqlite` | Authoritative state only when --backend sqlite is explicitly selected |
| `<output>/lifecycle.jsonl` | Export at successful local/recover completion; consumer exports when leaving its try/finally |

For live monitoring read SQLite; do not wait for JSONL export. Local/publish exceptions
can leave no final export; use Journal.export below. A process killed with SIGKILL cannot
run finalizers. Queries use read-only connections so they do not create missing databases.

```bash
export RUN_DIR=results/phase2-guide/local
python - <<'PY'
import json,os,sqlite3
from pathlib import Path
j=sqlite3.connect('file:'+str(Path(os.environ['RUN_DIR'])/'journal.sqlite')+'?mode=ro',uri=True)
for (body,) in j.execute('SELECT body FROM lifecycle ORDER BY ordinal DESC LIMIT 15'):
    o=json.loads(body)
    print(o['operation_id'],o['operation_type'],o['event']['entity_id'],o['status'],
          o['version_before'],o['version_after'],o['error_category'],o['error'])
print('inbox pending:',j.execute('SELECT count(*) FROM inbox WHERE done=0').fetchone()[0])
PY
```

Filter any combination of entity, operation, status and simulation run. An empty variable
means no filter. Use `lifecycle` for transitions; use `operations` for one final row per op:

```bash
# Set ENTITY_ID using section 8, or enter an ID you actually observed
export FILTER_ENTITY="$ENTITY_ID"
export FILTER_TYPE=Transfer
export FILTER_STATUS=COMMITTED
export FILTER_RUN=intersection-seed27
python - <<'PY'
import json,os,sqlite3
from pathlib import Path
j=sqlite3.connect('file:'+str(Path(os.environ['RUN_DIR'])/'journal.sqlite')+'?mode=ro',uri=True)
for (body,) in j.execute('SELECT body FROM lifecycle ORDER BY ordinal'):
    o=json.loads(body)
    fields={'FILTER_ENTITY':o['event']['entity_id'],'FILTER_TYPE':o['operation_type'],
            'FILTER_STATUS':o['status'],'FILTER_RUN':o['event']['run_id']}
    if all(not os.getenv(k) or os.environ[k]==v for k,v in fields.items()):
        print(json.dumps(o))
PY
```

For all failed operations: unset FILTER_ENTITY and FILTER_TYPE, set
`FILTER_STATUS=FAILED`. For all operations of the selected vehicle: unset FILTER_TYPE
and FILTER_STATUS. For a whole run: set FILTER_RUN only. A successful demo can legitimately
have no FAILED rows; failure tests deliberately exercise error paths.

Export the durable history after stopping the worker, without claiming pending work is done:

```bash
python - <<'PY'
import os
from pathlib import Path
from monitoring.store import Journal
p=Path(os.environ['RUN_DIR'])
assert (p/'journal.sqlite').exists()
Journal(p/'journal.sqlite').export(p/'lifecycle.jsonl')
print(p/'lifecycle.jsonl')
PY
```

After export, concise jq views are available:

```bash
jq -c 'select(.status=="COMMITTED") | {operation_id,operation_type,entity:.event.entity_id,request_unix_ns,start_unix_ns,commit_unix_ns,version_before,version_after,duration_ns}' "$RUN_DIR/lifecycle.jsonl"
jq -c 'select(.status=="FAILED" or .status=="CANCELLED" or .error_category!=null) | {operation_id,status,error_category,error}' "$RUN_DIR/lifecycle.jsonl"
jq -c --arg id "$ENTITY_ID" 'select(.event.entity_id==$id)' "$RUN_DIR/lifecycle.jsonl"
```

Unix-ns timestamps and large monotonic values should be analyzed with Python integers;
jq numeric formatting can lose precision for large integer timestamps.

### Clock and status interpretation

- `event.simulation_time_ms`, `source_timestamp_ms`, Follow expiry: SUMO simulation ms.
- `emitted_at_unix_ms`: separate wall-clock emission time; **not** a mapping of SUMO time.
- `request_unix_ns`, `start_unix_ns`, `commit_unix_ns`: host Unix ns. Commit means the
  transition/receipt was verified, not the unknown exact instant remote storage persisted it.
- `request_monotonic_ns`, `commit_monotonic_ns`: duration clock, usable within one executor
  `clock_session`. `duration_ns / 1_000_000` gives local request-to-verification ms.
- A restarted executor leaves cross-session `duration_ns=null`, even if it verifies a
  receipt and records COMMITTED. Do not synthesize a label from unrelated clocks.
- CREATED is requested; VALIDATED/QUEUED precede execution; RUNNING is processing;
  COMMITTING means a write is being attempted; only COMMITTED indicates verified success.
  FAILED/CANCELLED/SUPERSEDED are terminal without successful commit labels. SUPERSEDED
  exists in the state machine but the FIFO policy does not emit it automatically.
- There is no separate durable “function returned successfully” state or serverless
  activation record. A handler return alone is not proof of a committed DT transition.

## 10. Versions, ordering, rejection and recovery

| Case | Actual implemented behavior | Existing test selector |
|---|---|---|
| New observation | Attached owner; strictly increasing entity seq, nondecreasing source time; patch PVs | `test_attach_update_verified` |
| Duplicate event | Stable dedup identity; no new write; changed body for same identity raises error | `test_duplicate_stale_and_wrong_identity` |
| Stale / out of order | Ignore lower/equal sequence or older source time; retain inbox outcome; no reorder buffer | same test |
| Retry after lost response | Deterministic operation ID + atomic receipt avoids a second application | `test_commit_failure_and_restart_idempotency` |
| Concurrent duplicates | Workers sharing journal serialize with file lock; SQLite/Ditto conditional version checks | `test_concurrent_duplicate_workers`, `test_cas_conflict` |
| Update with owner change | Transfer first, dependent Update next; previous owner/version validated | `test_lifecycle_all_five_and_follow_renewal`, `test_transfer_checks_previous_owner_version` |
| Detach while earlier work pending | Drain earlier durable inbox entries; failure blocks progress; no silent successful Detach | `test_detach_waits_for_pending_update_then_blocks_late_observation` |
| Invalid predicate / missing durable write | Never COMMITTED; verification exception remains pending | `test_failed_commit_verification_is_pending`, `test_each_commit_predicate_rejects_wrong_state` |
| Deterministic precondition failure | FAILED; dependent op CANCELLED; no successful latency | `test_failed_attach_cancels_dependent_update`, `test_failed_follow_no_success_label` |

Run a selected case by its actual node name, for example:

```bash
python -m pytest -q tests/test_phase2.py::test_duplicate_stale_and_wrong_identity
python -m pytest -q tests/test_phase2.py::test_commit_failure_and_restart_idempotency
python -m pytest -q tests/test_phase2.py::test_detach_waits_for_pending_update_then_blocks_late_observation
python -m pytest -q tests/test_phase2.py::test_each_commit_predicate_rejects_wrong_state
```

Every successful operation increments app version once, including Follow. Attach and
Transfer also increment ownership_version. The journal lock is global/FIFO, stronger
than per-entity ordering. A plan is persisted before external writes. Ditto uses ETag
If-Match, create uses If-None-Match:*. This is not a transaction spanning Ditto, journal
and broker. Do not run unrelated independent journals against the same DT lineage and
assume cross-host ordering. Source sequence is per entity per run, continuing across RSUs.

An Update arriving late during a Transfer is processed in inbox order; one that is stale
by processing time is ignored. Later observations cannot reattach a detached entity;
a new insertion is required. Ignored stale events are not FAILED operations because
no operation was generated. Their inbox outcome is `ignored_stale_or_unassociated`.

Transport/verification errors are recorded on nonterminal operations and propagate;
fix the cause and recover. Deterministic FAILED is terminal and is not automatically retried.

```bash
python -m perfsyncgnn.phase2 recover --backend sqlite --output results/phase2-guide/local
# With Ditto environment and the same Ditto consumer journal:
python -m perfsyncgnn.phase2 recover --backend ditto --output results/phase2-guide/mqtt-verified-consumer
```

Do not point recover at a different backend from the one used to produce the journal.
A consumer also recovers before subscribing. Preserve journals and DT storage together.
An interrupted SUMO process is not checkpointed; replay the same seeded run. Already
acknowledged MQTT history is not automatically republished by an outbox replay marked sent.

## 11. Configure the implemented scenario

There is **one** implemented topology: `simulation/scenarios/intersection.py`, configured
by `configs/intersection.json`. No multi-intersection or external-map scenario is available.

| Parameter | Default | Effect |
|---|---:|---|
| seed | 27 | Python arrival RNG and SUMO random seed |
| duration_s | 90 | Simulation cutoff; does not fabricate entity departures |
| step_s | 0.2 | TraCI step size |
| observation_interval_s | 1 | Vehicle/signal/road observation interval; integer multiple of step_s |
| arrival_rate_per_s | 0.3 | Seeded exponential arrival rate; higher -> denser traffic |
| run_id | intersection-seed27 | Identity/ordering lineage; change for a new independent scenario |
| rsu_boundary_x | 300 | Logical normalized x ownership boundary |
| west_owner, east_owner | rsu-west, rsu-east | Distinct owner IDs |

The generator emits vehicle routes until 65% of duration_s, leaving a tail for traffic
to drain. It has no CLI “exact vehicle count” parameter; arrival rate and duration control
expected count (approximately rate * 0.65 * duration, with seeded variation). Do not use
SUMO-GUI replay-only scaling and assume the headless synchronized run changed with it.

Create a runtime config copy with existing schema fields, not a new scenario implementation:

```bash
python - <<'PY'
import json
from pathlib import Path
from simulation.scenarios.intersection import Scenario
p=Path('results/phase2-guide/dense.json')
p.parent.mkdir(parents=True,exist_ok=True)
c=json.loads(Path('configs/intersection.json').read_text())
c.update(run_id='intersection-dense-guide',arrival_rate_per_s=0.6,
         duration_s=120,observation_interval_s=0.4,seed=27)
p.write_text(Scenario.model_validate(c).model_dump_json(indent=2))
print(p)
PY
python -m perfsyncgnn.phase2 local --backend sqlite --follow \
  --config results/phase2-guide/dense.json --output results/phase2-guide/dense
```

This customization example is not a claimed executed workload in the verification report.
Changing only SUMO's `--seed` on an existing generated route file does not regenerate
its Python-seeded arrival schedule. For complete seed changes use the JSON + generator.
Do not manually edit generated network/routes between seeded reproducibility tests.

To generate files for a GUI-only inspection without running the sync pipeline:

```bash
python - <<'PY'
from pathlib import Path
from simulation.scenarios.intersection import Scenario,generate
c=Scenario.model_validate_json(Path('configs/intersection.json').read_text())
print(generate('results/phase2-guide/gui-only',c))
PY
sumo-gui -c results/phase2-guide/gui-only/intersection.sumocfg --seed 27 --delay 100
```

Again this path has no event publication or DT synchronization.

## 12. Tests and troubleshooting

```bash
python -m pytest -q -rs
python -m pytest -q tests/test_phase2.py
python -m pytest -q tests/test_sumo.py
python -m pytest -q -rs tests/test_services.py
# Opt in only when the actual services and credentials are ready:
DITTO_TEST_URL="$DITTO_URL" python -m pytest -q tests/test_services.py::test_live_ditto_complete_lifecycle
MQTT_TEST_HOST=localhost MQTT_TEST_PORT=1883 python -m pytest -q tests/test_services.py::test_live_mqtt_roundtrip
KAFKA_TEST_BOOTSTRAP=localhost:9092 python -m pytest -q tests/test_services.py::test_live_kafka_roundtrip
```

The three live tests skip explicitly when their opt-in variables are unset; once set,
an unavailable/broken service is a failure, not a simulated pass. The Kafka live test
uses topic `perfsync.test` by default (override `KAFKA_TEST_TOPIC`), distinct from the
application topic. Broker tests use test payloads; the live Ditto test uses constructed
events. None alone proves the entire physical-to-broker-to-Ditto pipeline.

| Symptom | Inspect / resolve |
|---|---|
| SUMO/TraCI import or executable missing | Activate .venv; check command -v; install declared dependencies/SUMO |
| GUI cannot open display | Run from desktop terminal with DISPLAY; Xvfb checks rendering process offscreen only |
| No vehicles visible | Play, increase Delay, zoom in; check generated routes; default delay 0 may finish instantly |
| No Follow | Include --follow; ordinary observations never subscribe themselves |
| No Transfer | Inspect x and source_id; crossing must be observed; stale data cannot transfer |
| No Detach | Entity may still be active at cutoff; extend duration; no false cutoff departure |
| Position differs from selected observation | Match seq/time/version; DT may have advanced or GUI is an independent replay |
| MQTT consumer sees nothing | Start/confirm subscription before producer; inspect broker logs, topic and outbox |
| Kafka sees nothing | Bootstrap/advertised listeners/topic creation/group offsets; no bundled broker config |
| Producer summary has no operations | Expected in publish mode; inspect consumer journal and Ditto instead |
| Repeated run has no new operations | Same run/entity ordering lineage already committed; use fresh run ID for fresh workload |
| HTTP 401 / 403 | Basic auth / nginx subject policy; never print credentials to debug |
| Conditional-write error | ETag/version conflict; independent writers or stale plan; inspect nonterminal errors |
| JSONL missing during run | Read SQLite; export after worker stops |
| COMMITTED but duration null | Cross-executor recovery; valid verified receipt, unavailable same-clock duration |
| “database is locked” / duplicate consumers | Use local filesystem, one worker context, correct journal/client ownership |
| Compose image pull still running | Inspect startup log and registry access; do not label services ready based on command launch |

## 13. Complete shutdown and known limits

1. Let the finite publisher/local run finish when possible. To interrupt, Ctrl+C in its
   terminal closes the TraCI connection via `finally`; it does not invent departure events.
2. Leave the consumer running until producer outbox has sent=1 and consumer inbox done=1
   and operations have terminal outcomes. Inspect SQLite, not just an empty terminal.
3. Ctrl+C in consumer; its finally exports lifecycle JSONL. Export manually if interrupted
   elsewhere. Preserve the database plus WAL files consistently; do not copy active DBs
   without SQLite-aware backup.
4. Close standalone SUMO-GUI. Ctrl+C ends tail/log watchers.
5. Stop only the test services you started:

```bash
docker stop perfsync-phase2-guide-mqtt perfsync-phase2-guide-kafka
docker compose -p perfsync-phase2-guide \
  -f third_party/ditto/deployment/docker/docker-compose.yml stop
unset DITTO_PASSWORD DITTO_USERNAME
```

Use `stop` to retain containers for resuming with `docker start` / `docker compose start`.
The upstream Ditto MongoDB service has no explicit persistent named data volume in this
Compose file; do not promise data retention across `down`/container recreation. Do not
run `down -v`, volume prune, or remove journal/DT data as routine shutdown. An absent
MQTT container after a failed image pull needs no stop. Do not stop unrelated services.

Known limits: no connected GUI mode, one topology, logical RSUs only, unpaced headless
execution, no integrated live DT dashboard, no bundled Kafka deployment config, no Phase 2 REST API,
no cross-host distributed ordering guarantee, unbounded operation receipts, deployment-
dependent remote durability and authentication, no real OpenWhisk measurements. Follow
is subscription metadata, not a broker subscription manager. The local SQLite
path remains explicitly separate from the verified local-deployment Ditto integration. Phase 3 is not implemented.

## 14. End-to-end evidence and no-loss check

The corrected recorded MQTT run has producer and consumer event identity sets equal:
1,738 sent outbox rows, 1,738 completed inbox rows; 1,773 COMMITTED operations and 26
Ditto Things with all expected receipts. `results/phase2-guide/mqtt-digital-twin-verification.json`
records the independent API audit. Final current PVs match each entity's last committed
physical observation. The separate Kafka 3-second smoke uses `kafka-smoke.json`,
`kafka-smoke-producer/` and `kafka-smoke-consumer/`; its short cutoff does not test Transfer
or Detach. The full MQTT run supplies those real traffic transitions.

For a completed MQTT run, compare identities rather than trusting producer PUBACKs:

```bash
python - <<'PYLOSS'
import sqlite3
from pathlib import Path
p=Path('results/phase2-guide')
a=sqlite3.connect('file:'+str(p/'mqtt-verified-producer/journal.sqlite')+'?mode=ro',uri=True)
b=sqlite3.connect('file:'+str(p/'mqtt-verified-consumer/journal.sqlite')+'?mode=ro',uri=True)
sent={r[0] for r in a.execute('SELECT key FROM outbox WHERE sent=1')}
done={r[0] for r in b.execute('SELECT key FROM inbox WHERE done=1')}
print('sent:',len(sent),'done:',len(done),'missing:',len(sent-done))
assert sent==done,'Consumer still behind, processing failed, or broker delivery is incomplete'
print(b.execute("SELECT json_extract(body,'$.status'),count(*) FROM operations GROUP BY 1").fetchall())
PYLOSS
```

Do not run that assertion while intentionally expecting a backlog to still be draining.
The first raw-config run under `mqtt-producer/` / `mqtt-consumer/` was aborted after
queue drops and is retained only for diagnosis. Use the `mqtt-verified-*` directories
for successful evidence. No source code changed during this guide task; runtime service
configuration and readiness/topic ordering were corrected as described above.
