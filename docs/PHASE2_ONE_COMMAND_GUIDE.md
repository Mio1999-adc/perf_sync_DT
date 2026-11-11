# Phase 2 one-command launcher

The launcher reuses `python -m perfsyncgnn.phase2`, the existing five handlers, and the
retained Phase 2 Docker containers. It adds process supervision and a connected GUI;
it does not add model training, OpenWhisk or another Ditto/MongoDB deployment.

## Prerequisites and fastest start

```bash
cd /home/hpclab-lananh/PerfSyncGNN
./scripts/perfsyncctl start --follow
```

No environment activation is necessary: the executable wrapper uses this repository's
`.venv/bin/python`. Install `.venv` and declared dependencies first if absent, following
[the existing setup guide](PHASE2_USER_GUIDE.md). SUMO and netconvert must be on PATH.
GUI additionally requires sumo-gui, xdpyinfo (`x11-utils` on Ubuntu), and a usable X11
DISPLAY. Docker is unnecessary for the default SQLite run.

Default behavior: one finite, headless SQLite intersection run; seed 27; no Follow
unless `--follow` is explicit; a unique run ID and output directory; stdout reports the
location, completion/failure and operation counts. The default output is
`results/phase2-runs/<UTC timestamp>-<random suffix>/`. Existing nonempty outputs are
rejected. Stop never deletes results or volumes.

SQLite WAL/FULL is unchanged. Optional faster local verification (process-crash recovery,
not power-loss durability):

```bash
PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL ./scripts/perfsyncctl run --follow
```

## Commands and options

| Command | Behavior |
|---|---|
| `start` | Start required existing infrastructure, supervise simulation, drain broker path, clean up |
| `run` | Alias for start |
| `gui` | Start the same controlled simulation using SUMO-GUI |
| `stop` | Interrupt supervisor/owned child groups; stop only containers it started |
| `restart` | Stop then launch a fresh run with prior settings plus supplied overrides |
| `status` | Process identities, run/output, last observed TraCI state, journals and service probes |
| `logs` | Launcher/worker or selected Docker logs; secrets redacted |
| `test` | Existing and launcher tests; `--live` starts all retained services and opts into live checks |

```bash
./scripts/perfsyncctl --help
./scripts/perfsyncctl start --help
./scripts/perfsyncctl start --backend sqlite --follow --scenario intersection --seed 27
./scripts/perfsyncctl start --follow --detach
./scripts/perfsyncctl status
./scripts/perfsyncctl logs --service sumo --tail 20
./scripts/perfsyncctl stop
```

Supported run options: `--backend sqlite|ditto`, `--live` (Ditto alias), `--follow`, `--gui`,
`--scenario intersection`, `--config <existing Scenario JSON>`, `--seed <int>`,
`--output <new-or-empty-directory>`, `--detach`, `--delay <GUI ms/step>`,
`--timeout <seconds>`, `--broker mqtt|kafka`, `--services ditto,mqtt,kafka|all`, and
`--infra-only`. Only one scenario topology exists. If --seed is omitted, the selected
config's seed is used. Every new run receives a new entity/run lineage, even at the same
seed. Traffic and logical rules remain deterministic; run-scoped IDs and clocks differ.

`--timeout` defaults to 300 seconds for readiness and separately for post-producer broker
drain. It is not a maximum simulation duration or a GUI pause timeout. A finite GUI run
can take longer if paused. There is no fixed TraCI port: TraCI selects an available port.

The default controller serializes launcher runs on this host. Duplicate launches are
rejected, and explicit output directories are atomically reserved. Detached submission
means the supervisor was spawned, not that services are ready; inspect status/logs. The advanced
PERFSYNCCTL_STATE_DIR environment variable isolates controller state for tests; do not
use it to run competing live consumers against the fixed broker subscription/group.

## Live Ditto and broker paths

Direct SUMO -> manager -> Ditto, with automatic retained-service startup:

```bash
./scripts/perfsyncctl start --live --follow
```

Actual SUMO -> MQTT -> consumer/manager -> Ditto:

```bash
./scripts/perfsyncctl start --live --broker mqtt --follow
```

Kafka alternative:

```bash
./scripts/perfsyncctl start --live --broker kafka --follow
```

A broker can also be used with `--backend sqlite` to test transport without Ditto. The
consumer is started first; producer launch waits for MQTT SUBACK or Kafka partition
assignment. Kafka's existing `perfsync.events` topic is created idempotently through the
retained Kafka container. When the producer exits, exact acknowledged outbox and completed
inbox identity sets must match before shutdown; equal row counts alone are insufficient.
Unsuccessful/nonterminal operations make the run fail rather than report success.

The launcher uses these **existing** resources:

- Compose project `perfsync-phase2-guide` and
  `third_party/ditto/deployment/docker/docker-compose.yml`.
- Existing `results/phase2-guide/ditto-auth.override.yml`, when present, is always retained.
- Containers `perfsync-phase2-guide-mqtt` and `perfsync-phase2-guide-kafka`.
- Loopback ports: Ditto nginx 8080, gateway 8081, MongoDB 27017, MQTT 1883, Kafka 9092.

The implementation checks Docker availability, validates Compose, discovers containers
by project/service labels, checks stopped-container port bindings for conflicts, and
uses **compose start**, not **up**. Missing containers cause an actionable failure;
creation is an explicit setup step in the earlier guide. No container recreation,
volume deletion, pruning, or alternative infrastructure is hidden in the launcher.
The private nginx override is not discarded.

Readiness means authenticated Ditto policy GET, MongoDB ping, MQTT CONNACK and Kafka
metadata response, rather than a running PID or open socket alone. A known policies
bootstrap stall may be recovered **once**, only if the policies container was started by
this launcher, after the precise one-contact/required-two diagnostic is observed. It
never restarts a policies container that was already running before launch. Other
readiness failures time out with a non-success result.

Credentials are loaded in Python from existing DITTO_* environment variables, with
fallback to `results/phase2-guide/private/credentials.json` (mode 0600 required).
Values are neither printed nor written to public run metadata nor passed as command
arguments. There is no shell process substitution. The launcher requires the existing
policy to be provisioned; it does not overwrite policies or auth files. Reuse the guide's
secure local setup if provisioning is needed.

To start/check all retained services without running traffic:

```bash
./scripts/perfsyncctl start --services all --infra-only --detach
./scripts/perfsyncctl status
./scripts/perfsyncctl stop
```

Only stopped containers actually started by the current supervisor are owned for cleanup.
Services already running are left running. Successful cleanup releases ownership so a
later stop does not stop services someone subsequently starts manually.

## Connected SUMO-GUI: actual traffic, not replay

```bash
./scripts/perfsyncctl gui --follow
# Actual live MQTT/DT path with the same GUI-controlled SUMO state:
./scripts/perfsyncctl start --live --broker mqtt --gui --follow
```

The runner selects `sumo-gui` instead of `sumo` in its **same traci.start call** and uses
that connection for all observations. The GUI starts with --start and default delay
100 ms/step. Pause/Stop and Play operate on the server serving this TraCI connection;
while paused, simulation stepping waits and lifecycle progression stops. Resume with
Play. See the [SUMO TraCI documentation](https://sumo.dlr.de/docs/TraCI/) for GUI server
start/step behavior. Adjust the Delay control, or use `--delay 0` for unpaced tests.

No separate replay window is launched. GUI close or an interrupted TraCI session returns
a cancelled run (exit 130) and preserves partial lifecycle records. It does not invent
vehicle departures or label the whole run successfully complete. Unexpected connection
loss in headless mode fails. In GUI mode a connection loss cannot be reliably distinguished
from closing the GUI; both are reported as cancellation rather than fabricated success.

The launcher validates DISPLAY with xdpyinfo. A terminal without graphics gets an early,
clear error. Offscreen CI check, not an interactive desktop demo:

```bash
xvfb-run -a ./scripts/perfsyncctl gui --follow --delay 0
```

`status` reports TraCI state and its last observation timestamp, not a second concurrent
TraCI probe. A paused, alive GUI may have an old observation timestamp. GUI pause/resume
controls are supported by SUMO; automated checks validate event equivalence and closure,
not mouse interaction. RSU regions remain logical; there are no new map overlays.

## Logs, status and operation verification

```bash
./scripts/perfsyncctl status
./scripts/perfsyncctl logs --tail 30
./scripts/perfsyncctl logs --service sumo --tail 30
./scripts/perfsyncctl logs --service sync --tail 30
./scripts/perfsyncctl logs --service ditto --tail 20
./scripts/perfsyncctl logs --service mongodb --tail 20
./scripts/perfsyncctl logs --service mqtt --tail 20
./scripts/perfsyncctl logs --service kafka --tail 20
./scripts/perfsyncctl logs --service sumo --follow
```

Allowed log names correspond to actual processes/services. `launcher` selects the
supervisor log; `sumo` is the TraCI worker, not only SUMO stdout. In direct mode `sync`
selects the same co-located worker log. Broker mode has its own consumer log. `tests`
is available for test runs. Ctrl+C ends log following without stopping the run.

The printed output directory contains:

```text
scenario-input.json          exact seed, options and new run ID
launcher/config.json         public launcher configuration, no credentials
launcher/state.json          supervisor/child identities, exit codes and outcomes
launcher/events.jsonl        launcher startup/error/shutdown events
launcher/supervisor.log      supervisor messages
launcher/sumo.log            simulation/generator stdout and stderr
launcher/sumo-runtime.json   observed TraCI handshake/ticks/closure and SUMO PID
launcher/sync.log            broker consumer stdout/stderr, when selected
launcher/sync-runtime.json   acknowledged subscription/assignment observations
simulation/                 direct mode: events, scenario, journal, lifecycle, optional twins
producer/                   broker mode: physical events, scenario and durable outbox
consumer/                   broker mode: operations, lifecycle and optional SQLite twins
```

Launcher logs are separate from synchronization lifecycle/latency records. `status` also
lists actual journal/export paths. GUI/worker PID observations never substitute for
operation commit verification. Application version/receipt validation remains in the
existing executor and Ditto adapter.

With --follow, the default seed-27 run has previously and newly produced 26 Attach,
17 Follow, 1,714 Update, 9 Transfer, 7 Detach: 1,773 COMMITTED operations. The expected
logical order begins Attach -> initial Update -> explicit Follow. Follow is requested
tracking metadata, not a new physical event or a per-vehicle MQTT subscription. A shorter
scenario may end before Transfer/Detach; a cutoff does not fabricate departures.

Set RUN_DIR to the printed run output, then inspect successful operation types:

```bash
export RUN_DIR=/absolute/path/printed/by/launcher
# Direct mode:
jq -r 'select(.status=="COMMITTED") | .operation_type' \
  "$RUN_DIR/simulation/lifecycle.jsonl" | sort | uniq -c
# Broker mode instead:
jq -r 'select(.status=="COMMITTED") | .operation_type' \
  "$RUN_DIR/consumer/lifecycle.jsonl" | sort | uniq -c
```

[The detailed Phase 2 guide](PHASE2_USER_GUIDE.md) contains vehicle filtering, source-vs-
commit comparison, Ditto API inspection and operation-specific predicates. Its former
statement that connected GUI is unavailable is superseded by this launcher guide.

## Tests

```bash
./scripts/perfsyncctl test
./scripts/perfsyncctl test --live
./scripts/perfsyncctl logs --service tests --tail 30
```

Default tests do not start Docker; unconfigured live tests explicitly skip. --live starts
all retained services, loads credentials securely, opts into Ditto/MQTT/Kafka tests and
cleans up only infrastructure it started. A test process failure propagates a nonzero
exit. Test summary is printed on foreground completion. The GUI tests require an
accessible DISPLAY and otherwise explicitly skip; xvfb-run supplies an offscreen display.
There are no phase-3 or serverless performance tests.

## Shutdown, restart and failure recovery

```bash
./scripts/perfsyncctl stop
./scripts/perfsyncctl restart --follow
```

Ctrl+C in a foreground run asks the supervisor to stop. It interrupts the producer's
owned session/process group (including its SUMO engine), then the consumer, allowing
finally blocks to export lifecycle records and close transports. If graceful termination
fails, the owned group escalates to TERM/KILL; partial records can remain pending and
must not be called successfully completed. Normal broker completion drains first.
Containers started by the launcher stop after children; MongoDB stops after its clients.
No data or volumes are removed. Completed and interrupted runs remain on disk.

Restart preserves the previous backend/seed/GUI/follow/broker/config settings unless
overridden, but makes a fresh output and run lineage. It is **not** continuation of an
interrupted SUMO process. Use the existing Phase 2 `recover --backend ... --output ...`
command for a pending old journal with the same backend and credentials; do not start
a new SQLite run against a Ditto journal.

After supervisor SIGKILL or host/process failure, `status` shows stale process observations;
`stop` reconciles identity-verified orphan child groups and recorded immutable container
IDs. A new run is blocked while unresolved active ownership remains. Linux PID start
identity and boot ID prevent killing unrelated processes after PID reuse. No Python
supervisor can execute finalizers after SIGKILL; use stop/recovery and inspect journals.

## Troubleshooting and limits

- Missing executable/venv: install the existing dependencies; wrapper will not install
  or replace them automatically. A bad graphical session fails before native launch.
- Missing containers/auth config: follow existing provisioning guide. Launcher deliberately
  refuses to recreate infrastructure. Policy GET 401/403/404 is not “ready”.
- Port occupied while selected container is stopped: resolve that conflict; launcher
  does not kill a port owner or select another unverified deployment.
- Readiness timeout: inspect selected service logs; only the specific owned policies
  stall has a bounded one-time recovery. Start/stop report actual failure.
- Broker drain timeout: inspect consumer errors and broker queue capacity; sent PUBACKs
  are not delivery proof. Existing corrected Mosquitto runtime configuration must remain.
- Duplicate launch or reserved output: use status/stop, then a fresh directory.
- Only one default controller/live consumer context is supported. Do not run competing manual producers/consumers alongside the launcher.
- No distributed coordinator, production TLS/queue hardening, receipt compaction,
  native Wayland-only GUI support, automatic infrastructure creation, or new scenario
  topology is introduced. Local durations are not OpenWhisk latency labels.

See [launcher verification](PHASE2_LAUNCHER_VERIFICATION.md) for actual commands/results.

## GUI end-of-run and interrupted restarts (2026-11-01 correction)

The default scenario ends at **90 simulated seconds**. The connected adapter uses
`--quit-on-end` and closes TraCI after the configured duration, so the GUI closes
normally at that point. This is not an infinite demonstration. Display delay, durable
journaling and service work can stretch the run over several wall-clock minutes.
Increase `duration_s` in a copied Scenario JSON and pass `--config` to run longer;
do not interpret closing at 90000 ms as a crash.

After the GUI closes, the supervisor may remain DRAINING while the consumer catches up.
Wait for COMPLETED if a full dataset is required. Ctrl+C/stop during draining cancels
the workload and can leave acknowledged broker messages awaiting consumer processing.

A corrected journal transaction handler now checks whether a transaction remains active
before rollback. An interrupt arriving just after COMMIT no longer becomes a misleading
“cannot rollback - no transaction is active” error. The original interruption is preserved,
so the runner can export lifecycle records and return cancellation cleanly.

Launcher consumers now use stable **run-scoped** MQTT session IDs and Kafka groups,
rather than sharing the manual runner's default `perfsync-sync` identity. New Kafka
groups snapshot their initial end offset before announcing readiness; existing group
offsets resume normally. This prevents a fresh run from ingesting the previous cancelled
run's backlog and then failing the exact-identity drain check. The manual runner's
existing default identities and earliest-offset behavior remain unchanged.

A fully drained successful MQTT run releases its own completed broker session. Cancelled
sessions, pending messages, original journals and DT state are preserved for explicit
recovery; no old run is relabeled successful. To recover a run-scoped session, use its
original journal and the --consumer-id / --consumer-group values derived deterministically from that run's saved run_id, with the matching backend.
Legacy interrupted sessions are retained and may need explicit operator recovery/cleanup;
the launcher does not delete their pending queues automatically.
