# Unified Phase 2 launcher verification

Verified 2026-11-01 (Asia/Seoul). This report covers the new launcher, not the historical
manual guide. Entry point: `./scripts/perfsyncctl`. Instructions:
[PHASE2_ONE_COMMAND_GUIDE.md](PHASE2_ONE_COMMAND_GUIDE.md).

## Implemented scope

Python controller modules under `perfsyncgnn/launcher/` manage the existing Phase 2
module and retained Docker containers. The executable wrapper selects `.venv` without
activation. All eight commands are implemented: start, stop, restart, status, logs,
test, run and gui. Backends remain SQLite and Ditto. Optional MQTT/Kafka pipelines run
producer and consumer under the same supervisor. Unique outputs/run IDs prevent
accidental overwrite or reuse of remote entity lineages.

The minimal existing-runtime changes are:

- TraCIClient chooses `sumo-gui` when requested, through the same traci.start connection.
  --start, --quit-on-end and GUI delay are supplied; headless behavior is retained.
- Separate runtime observations record an actual TraCI handshake/ticks/closure and SUMO
  PID, rather than inferring a connection from a running process.
- Consumer readiness callbacks distinguish MQTT SUBACK and Kafka assignment from process
  startup. Producer launch waits for this readiness.
- Phase 2 always exports lifecycle history in a finalizer, including interruption/error.
  Connected GUI close returns cancellation rather than a fabricated complete run.

No synchronization rules, commit predicates, GNN code, training or serverless execution
were replaced. Existing result datasets, upstream files, credentials and volumes were
preserved. Docker creation/recreation is deliberately unavailable in the launcher.

## Actual executable verification

| Invocation | Actual result |
|---|---|
| `.venv/bin/python -m pytest -q -rs tests/test_phase2.py tests/test_sumo.py` before edits | 24 passed in 17.58s |
| `PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL ./scripts/perfsyncctl run --follow` | COMPLETED; all 1,773 operations COMMITTED |
| `./scripts/perfsyncctl status` and `logs --service sumo` | Correct finished/active process information, output paths and real operation summary |
| `PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL ./scripts/perfsyncctl start --live --follow --broker mqtt --timeout 180` | COMPLETED; 1,738 exact sent/completed identities, 1,773 verified commits; infrastructure started/cleaned up by launcher |
| `PERFSYNCCTL_STATE_DIR=results/launcher-verification/gui-state PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL xvfb-run -a ./scripts/perfsyncctl gui --config results/launcher-verification/short.json --follow --delay 0 --output results/launcher-verification/gui-run` | COMPLETED; 3-second same-process GUI, 42 COMMITTED operations |
| Detached connected GUI `start --gui --follow --detach`, separate status / stop | Live report showed traci=connected, gui=true and simulation_time_ms=52400; stop preserved 5,616 lifecycle rows in the interrupted run |
| Second start during that detached run | Rejected: launcher already active; no duplicate run launched |
| `restart --config results/launcher-verification/short.json --delay 0 --detach` under the isolated GUI test controller | Fresh output/lineage; COMPLETED; inherited GUI/follow settings; 42 COMMITTED operations |
| `env -u DISPLAY ... ./scripts/perfsyncctl gui` | Correct nonzero failure before launch; no usable graphical session claimed |
| `logs --service ditto` / `logs --service kafka` | Retrieved actual retained service logs; known credential values redacted |
| `PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL ./scripts/perfsyncctl test --live --timeout 300` | **45 passed, no skips, in 38.57s**; supervisor COMPLETED, exit 0 |

Additional completed checks:

- `start --live --broker kafka --follow --config results/launcher-verification/short.json
  --output results/launcher-verification/kafka-run --timeout 300` completed with all
  42 operations COMMITTED, exercising launcher assignment readiness and broker drain.
- A manually pre-started MQTT container was used by detached `start --services mqtt
  --infra-only`; its ownership ledger stayed empty. After launcher stop, Docker inspect
  still reported the broker running. It was then stopped explicitly by the verification
  session, demonstrating preservation of an already-running service.
- Final status reported COMPLETED, no alive owned children, and all selected test
  containers stopped. Kafka's normal consumer exit 130 is its deliberate post-drain
  interrupt, not a failed workload; the producer exited 0 and run completed.

The live test command automatically started the nine retained Ditto/MongoDB stack
containers plus existing MQTT/Kafka containers, loaded private credentials in Python,
ran the complete suite with all live opt-ins enabled, then stopped all eleven containers
it had started. In that successful run no bootstrap restart was needed. An earlier
readiness attempt reproduced the known stall and was deliberately cancelled; it was
not reported as a successful test run.

### Workload details

The default seed-27 full run has 26 Attach, 17 Follow, 1,714 Update, 9 Transfer, 7 Detach.
SQLite and actual MQTT-to-Ditto launcher runs both retained all 1,773 COMMITTED operations.
The MQTT producer's acknowledged identity set exactly equaled the consumer's completed
set (1,738 each). This explicitly checks more than a producer return or PUBACK. Operation
commits still use the existing backend conditional-write and receipt-verification path.

The 3-second smoke config was constructed with the existing Scenario schema in
`results/launcher-verification/short.json`; it does not fabricate departures and therefore
contains Attach/Follow/Update only. The full 90-second workloads establish all five
operations. Runtime/launcher observations are not successful-commit training labels.

## Automated launcher tests

18 new test cases exercise:

- CLI parsing, unsupported scenario/options, timeout bounds and dependencies.
- Source config loading, fresh run IDs and nonempty-output protection.
- Health deadline and early child-failure behavior.
- Real subprocess group startup/termination and finalizer preservation.
- PID identity mismatch without signaling a reused/unrelated PID.
- Duplicate controller rejection before output mutation.
- Existing Docker project/service discovery and missing-container failure, with mocks.
- Start-only-stopped ownership and preserving already-running services, with mocks.
- Port conflict prevention and immutable container identity checks.
- Credential redaction.
- Real SQLite launch through the executable wrapper and lifecycle preservation.
- Real connected GUI vs headless event equivalence (excluding emission wall time).
- Real GUI-engine closure preserving partial lifecycle, exit 130, cancelled outcome.
- Orphan child cleanup after simulated supervisor death.
- Exact identity-set drain comparison, including unequal sets with equal counts.
- One-time, owned-only policies bootstrap recovery based on its precise diagnostic,
  tested with controlled Docker responses (not claimed as a live restart in the final run).

The 27 original tests also pass, including real SUMO and actual Ditto/MQTT/Kafka service
tests. DISPLAY was accessible; GUI tests ran rather than skipping. Offscreen launcher GUI
execution was additionally checked under Xvfb. Mouse-driven pause/resume interaction was
not automated; GUI server stepping follows SUMO's documented controls.

## State, ownership and recovery guarantees

Default controller state is `results/.perfsyncctl/active.json`; run-local state and logs
are separate from synchronization lifecycle data. PID plus Linux start identity/boot ID
are checked before signaling owned groups. A process PID alone is not readiness.
Service probes use policy HTTP, MongoDB ping, MQTT protocol acknowledgement and Kafka
metadata. Last TraCI observations are timestamped; status does not open an independent
TraCI connection or claim that a paused GUI is making progress.

Only stopped containers selected and started by a run are acquired for cleanup. Already-
running services are not acquired. Owned IDs are persisted before mutation; successful
cleanup releases ownership. MongoDB is stopped after its clients. Missing containers or
port conflicts fail; compose up/recreate and volume deletion are never issued.

A bounded policies restart is permitted only for a container started by that launcher,
after the observed one-contact/required-two stall. Other timeouts fail with logs. The
300-second readiness and drain defaults are bounded; a simulation's deliberate GUI pause
has no automatic workload deadline.

After SIGKILL, finalizers cannot run. A new launch is blocked until stop reconciles stale
ownership; it cleans only identity-verified groups and recorded Docker IDs. Interrupted
journals and data remain available to the existing backend-specific recovery command.

## Evidence locations

- `results/phase2-runs/20261101T082107Z-6c2b90d6/`: first full SQLite launcher run.
- `results/phase2-runs/20261101T082255Z-4bfb7b19/`: full MQTT -> Ditto launcher run.
- `results/phase2-runs/20261101T082954Z-181607bc/`: final 45-test live launcher verification,
  including `launcher/tests.log`, public config, state and released-container history.
- `results/launcher-verification/kafka-run/`: completed live Kafka launcher smoke.
- `results/launcher-verification/infra-active.json` and `infra-run/`: pre-existing broker ownership check.
- `results/launcher-verification/gui-run/`: offscreen connected GUI smoke.
- `results/launcher-verification/stopped-run/`: interrupted connected GUI lifecycle.
- `results/launcher-verification/connected-gui-active.json`: observed connected GUI status.

All runtime evidence remains gitignored. Private auth JSON is never included in public
run config or logs. The task preserves the existing auth override and infrastructure.

## Limits

One single-host default controller is supported. Independent custom controller directories
must not compete for the fixed MQTT client/Kafka consumer group. Infrastructure must
already be provisioned by the existing guide; no automatic Docker installation or service
creation is implemented. Broker security/capacity, distributed ownership fencing and
power-loss durability are not newly claimed. Receipt storage remains unbounded.
Closing a GUI and an unexpected GUI connection loss both produce cancellation because
the current TraCI interface cannot reliably distinguish them. Local Python durations
are not OpenWhisk cold-start/queueing latency. Phase 3 remains untouched.


## GUI closure and restart recheck — 2026-11-01

The reported full GUI run reached simulation_time_ms=90000 with producer exit 0:
its window closed at the configured 90-second end, as requested by --quit-on-end.
The overall run was later interrupted during broker drain: 1,738 published events,
519 completed consumer events and 527 committed operations. That historical run is
partial and remains CANCELLED; it is not relabeled as a successful full dataset.

A KeyboardInterrupt delivered just after SQLite COMMIT was masked by unconditional
ROLLBACK (“no transaction is active”). The journal now rolls back only an active
transaction and preserves the original interruption. Tests cover both interruption
before commit (rollback) and immediately after commit (durable data retained).

Fresh launcher runs now have run-scoped stable MQTT session IDs / Kafka groups, so
queued messages from cancelled earlier runs do not contaminate a fresh run's identity
comparison. A new Kafka group's initial end offset is resolved before producer readiness;
existing committed group offsets are respected. Only completed, fully drained MQTT
sessions are released. Interrupted queues/journals remain available for operator recovery.
The underlying manual runner keeps its previous default client/group behavior.

Recheck evidence:

- `results/gui-recheck-mqtt/`: real connected GUI -> MQTT -> Ditto 3-second run,
  32/32 matching event identities, 42 COMMITTED operations, only its new run_id in
  the consumer inbox despite the legacy cancelled session retaining old backlog.
- `PERFSYNCCTL_STATE_DIR=results/gui-recheck-tests-controller
  PERFSYNC_SQLITE_SYNCHRONOUS=NORMAL ./scripts/perfsyncctl test --live
  --output results/gui-recheck-tests`: **50 passed, no skips, in 35.75 seconds**.
  Source tests and actual Ditto/MQTT/Kafka checks passed. This includes MQTT completed-
  session release and deterministic Kafka offset-readiness tests.

No change was made to the default scenario duration or GUI auto-close policy. Wait for
COMPLETED after the window closes if a fully drained synchronization dataset is required.
