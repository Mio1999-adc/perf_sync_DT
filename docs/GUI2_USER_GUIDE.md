# GUI 2: Digital Twin State Viewer

GUI 1 is the same TraCI-controlled SUMO-GUI process that generates observations. GUI 2 is a local React/TypeScript viewer served by FastAPI. Its digital map reads **committed Eclipse Ditto state**, never a copy of the SUMO observation stream.

Complete [PHASE3_DEPLOYMENT.md](PHASE3_DEPLOYMENT.md) and the private Grafana-password setup in [PHASE3_ONE_COMMAND_GUIDE.md](PHASE3_ONE_COMMAND_GUIDE.md), then run:

```bash
cd ~/PerfSyncGNN
export PATH="$PWD/results/phase3-tools:$PWD/results/phase3-tools/linux-amd64:$PWD/results/phase3-tools/node-v22.16.0-linux-x64/bin:$PATH"
export PERFSYNC_GRAFANA_PASSWORD="$(cat results/phase3/private/grafana-admin-password)"
./scripts/perfsyncctl run --live --executor openwhisk --broker mqtt \
  --gui --dashboard --monitoring --keep-dashboard --follow \
  --delay 200 --config configs/phase3-demo.json --timeout 900
```

Open http://127.0.0.1:8090 after the dashboard readiness message. GUI 1 is the SUMO-GUI launched by the same command. Both GUIs use the same run and vehicle IDs, such as `vehicle-0000`. `--dashboard-port 8091` changes the port. `--dashboard-refresh 2` changes the minimum delay between snapshot polls to two seconds; the minimum allowed interval is 0.25 seconds. `--executor local` is supported for comparison and must not be described as an OpenWhisk run. At simulation end, `--keep-dashboard` leaves GUI 2 and the monitoring stack available until `./scripts/perfsyncctl stop`.

## What the panels mean

- **Overview:** counts from committed run-specific twins, grouped into attached/detached entities, active subscriptions, owners, and versions. Subscription expiry is evaluated against the latest actual observed simulation time, not wall time. Aggregates refresh every five seconds; operation counts and latest successful commit come from actual journal records.
- **Map:** actual SUMO lane geometry transformed consistently with committed positions. Green/purple vehicles identify RSU ownership, gray vehicles retain their last committed detached position. Signal markers use committed traffic-light state. There is no motion interpolation or prediction.
- **Entity inspector:** click a map marker or entity button. Inspect physical/digital ID, owner, position/speed, lane/road, association, subscription, sequence/version, last operation, latest locally verified commit, and pending operations. The JSON view is the committed twin projection; the potentially large historical receipt dictionary is omitted.
- **Physical comparison:** opt in to separately labelled latest TraCI observations. These values can lead committed state and do not drive the digital map. An arrival/departure observation may omit position/speed, in which case those physical fields display unavailable.
- **Timeline:** choose operation type/status filters. Entries show actual request/commit timestamps and dependencies. Click an operation ID to open its real lifecycle record endpoint. The table displays the latest 100 matching records; API callers can request further bounded pages.

The map/entity list uses pages of up to 100 entities. API page limits are capped at 200. Complete snapshots replace the visible page on each update, so reconnecting does not append duplicate entities. Detached entities remain inspectable; absence from SUMO does not independently remove a twin.

## Freshness and consistency

- **Sequence gap:** `max(0, latest physical observation sequence - committed sequence)` for the same entity. Zero is current relative to available observations; positive is lagging. No physical observation means unavailable.
- **Simulation observation gap:** the nonnegative difference between physical and committed source timestamps, in simulation milliseconds.
- **Wall-clock commit age:** current server wall time minus the manager's latest verified commit time. If Ditto's latest operation does not match the recorded committed operation, this age is unavailable until verified.
- **Synchronization duration:** a separate execution metric described in [PHASE3_MONITORING_GUIDE.md](PHASE3_MONITORING_GUIDE.md). It is not either freshness age.
- **Ownership consistency:** checks the current twin version/owner against its latest atomic operation receipt and the configured owner/association rules. A physical/digital owner difference while Transfer is pending can represent lag; it is not silently overwritten from SUMO.

Source timestamps are simulation time, not dates. A stopped simulation can remain current relative to its final observations while its wall-clock commit age increases.

## Disconnects and stopping

The header changes to disconnected/stale if SSE or Ditto polling fails. Existing values remain visible with that warning. EventSource reconnects automatically; fresh snapshots replace stale data. API failures do not fall back to physical observations.

The viewer binds to 127.0.0.1 and rejects unexpected Host/Origin headers. Ditto/OpenWhisk credentials stay on the backend. This is a local research viewer, not an externally authenticated multi-user deployment.

```bash
./scripts/perfsyncctl status
./scripts/perfsyncctl logs --service dashboard
./scripts/perfsyncctl stop
```

SUMO-GUI closes at the configured simulation end. The launcher drains the synchronization pipeline and holds GUI 2 open when `--keep-dashboard` was supplied. `stop` closes the managed viewer, monitoring, API forwarding process, and only the containers owned by that run; run evidence and named Prometheus/Grafana volumes remain. Inspect saved lifecycle/trace evidence afterward. A GUI window closing at the finite scenario end is distinct from a failed synchronization run; check launcher status and operation outcomes.

Actual browser captures from the accepted run are in [`results/phase3-acceptance-verified/qa/`](../results/phase3-acceptance-verified/qa/). See [PHASE3_VERIFICATION.md](PHASE3_VERIFICATION.md) for the selected-vehicle and final-state screenshots.
