# Phase 3 final verification

Audit date: 2026-11-02 (Asia/Seoul).

**Decision: FULLY VERIFIED for the MQTT, Eclipse Ditto, Apache OpenWhisk, SUMO-GUI, GUI 2, and Prometheus/Grafana acceptance path described here.** The final acceptance run was launched and shut down through `perfsyncctl`; independent verification compared every committed operation with authoritative Ditto receipts. All evidence below is retained under `results/phase3-acceptance-verified/`.

## Implementation and verification status

| Component | Status and evidence |
| --- | --- |
| Existing Phase 2 services and regression coverage | Pass. The default repository suite includes Phase 2 tests. Ditto, MongoDB, and MQTT were actually used in the acceptance run. |
| SUMO-GUI and TraCI | Pass. The launcher recorded `gui: true`, TraCI connected, and the 85-second simulation ended normally at 85,000 ms. SUMO and the producer used the same run ID as the consumer and GUI 2. |
| OpenWhisk synchronization | Pass. All five operation kinds completed using real Apache OpenWhisk activations. The live integration suite passed. |
| Ditto state and receipts | Pass. The independent run verifier fetched 12 twins and checked each operation's event fingerprint and resulting version against its Ditto receipt. |
| GUI 2 | Pass. Playwright loaded the running application, observed successful API responses and SSE reconnect, selected vehicles, compared physical observations with committed state, and filtered the timeline across all five operation kinds. Screenshots were visually inspected. |
| OpenTelemetry and Prometheus | Pass. The Prometheus container ran, loaded its configuration, and scraped `127.0.0.1:8090` with target health `up`. Actual operation and resource series were queryable. |
| Grafana | Pass with a nonblocking browser-console issue. Basic authentication, Prometheus datasource health, and the provisioned eight-panel dashboard were checked. All eight panels rendered real time-series data in the captured browser screenshot. Grafana also logged a 404 while loading its optional built-in Loki Explore plugin; it did not prevent the dashboard from rendering. |
| Unified launcher and lifecycle | Pass. One `perfsyncctl run` command started GUI 2, monitoring, the sync consumer, and the connected SUMO-GUI in readiness order. `status`, service logs, `stop`, and retained outputs were verified. |

## Accepted end-to-end run

Run ID: `launcher-phase3-acceptance-verified-46f73816`

Configuration: MQTT broker, OpenWhisk executor, Eclipse Ditto, SUMO-GUI enabled, GUI 2 enabled, Prometheus/Grafana enabled, 200 ms event-generation delay, finite 85-second scenario. The launcher completed with exit code 0 after an explicit graceful stop. The consumer journal had no pending inbox entries.

| Operation | Committed |
| --- | ---: |
| Attach | 12 |
| Update | 195 |
| Follow | 3 |
| Transfer | 2 |
| Detach | 2 |
| **Total** | **214** |

Every operation is `COMMITTED`, has an OpenWhisk activation ID, and has ordered request/validation/queue/start/verified-commit timestamps. Ditto returned matching event fingerprints and resulting versions for all 214 operations. The verifier confirmed 12 entities, captured movement for `vehicle-0000`, `vehicle-0001`, and `vehicle-0002`, ownership transfers for `vehicle-0000` and `vehicle-0001`, and detach for those two vehicles.

The verification output is [`verification.json`](../results/phase3-acceptance-verified/verification.json). Live Ditto snapshots are in [`dashboard-observations.jsonl`](../results/phase3-acceptance-verified/dashboard-observations.jsonl). Trace artifacts are under `consumer/` and `producer/`; the independent verifier correlated **197 trace IDs** across the source, manager, and remote activation traces and checked the required synchronization, invocation, function, conditional-write, and commit-validation spans.

One measured Transfer example is operation `op-aa0ad9d2a43a56b880916544353d1614ccae34b277667553f8609416c44c0650`: it changed the owner to `rsu-east`, committed version 13 after version 12, and correlated with OpenWhisk activation `37943dcd77c14edf943dcd77c16edfc9`. Its manager monotonic request-to-verified-commit duration was 14.126969255 seconds. The OpenWhisk function duration is separately recorded and is not substituted for this end-to-end latency.

## GUI 2 and monitoring evidence

The real browser screenshots are retained here:

- [`gui2-selected-vehicle.png`](../results/phase3-acceptance-verified/qa/gui2-selected-vehicle.png) shows `vehicle-0000` selected while attached, committed at DT version 4, the separate physical-versus-digital comparison, and actual Attach/Update/Follow lifecycle rows with activation IDs.
- [`gui2-selected-final.png`](../results/phase3-acceptance-verified/qa/gui2-selected-final.png) shows the final committed state for attached `vehicle-0002`, freshness, owner, version, sequence, and comparison values.
- [`gui2-attach-timeline.png`](../results/phase3-acceptance-verified/qa/gui2-attach-timeline.png) captures the actual filtered Attach timeline; the browser report records committed rows for Attach, Follow, Update, Transfer, and Detach.
- [`grafana-dashboard.png`](../results/phase3-acceptance-verified/qa/grafana-dashboard.png) shows the provisioned dashboard's eight panels and measured time series.

GUI 2 displayed Ditto as the committed-state source, kept physical TraCI observations in their own labelled comparison, and returned HTTP 200 for entity, geometry, and operation APIs. The SSE reconnect probe observed two connection attempts and recovered a fresh snapshot. Browser reports have no GUI 2 JavaScript exceptions or failed API requests.

The Prometheus target was `up` at `http://127.0.0.1:8090/metrics`. The run's metric snapshot and Prometheus query results showed 214 invocation, synchronization-duration, and function-duration observations split into the same operation counts listed above; host CPU and memory were measured (one observed scrape was 6.5% CPU and 9,898,672,128 bytes memory). Grafana connected successfully to its provisioned Prometheus datasource and loaded all eight panels. Failure and retry series were absent in the final run's metrics snapshot, consistent with zero failed business operations and a sum of zero operation retry counts; historical points may remain in the retained Prometheus volume. Cold-start and queue-delay values were unavailable because the platform did not provide a reliable measurement. They were not estimated.

## Commands and test results

The verified one-command workflow is documented in [PHASE3_ONE_COMMAND_GUIDE.md](PHASE3_ONE_COMMAND_GUIDE.md). For a fresh output directory, set the private Grafana password and run:

```bash
export PERFSYNC_GRAFANA_PASSWORD="$(cat results/phase3/private/grafana-admin-password)"
./scripts/perfsyncctl run --live --broker mqtt --executor openwhisk \
  --monitoring --dashboard --keep-dashboard --gui --follow --delay 200 \
  --timeout 900 --config configs/phase3-demo.json --output results/<new-run-name>
```

After the finite simulation drains, the viewer and monitoring stack remain available until `./scripts/perfsyncctl stop`. Check `./scripts/perfsyncctl status` and `./scripts/perfsyncctl logs --service launcher` before stopping. The stop retained both named monitoring volumes and the run artifacts; it stopped only the containers/processes owned by this run. Ditto was started briefly after shutdown for the read-only receipt audit and then returned to its stopped state. Pre-existing Ditto UI and Swagger UI containers remained running.

| Verification | Result |
| --- | --- |
| Full default suite: `.venv/bin/python -m pytest -q -rs` | **58 passed, 7 skipped** in 48.93 s |
| Full suite with live OpenWhisk, Ditto, and MQTT enabled | **64 passed, 1 skipped** in 63.67 s |
| Frontend production build (`tsc` and Vite) | Pass |
| Monitoring Compose configuration validation | Pass |
| Live Prometheus target, Grafana health, datasource, dashboard | Pass |
| GUI 2 Playwright browser and timeline checks | Pass; screenshots and JSON reports retained |
| Independent Ditto receipt, lifecycle, snapshot, and trace audit | Pass: 214 operations and 197 correlated trace IDs |

The seven default-run skips were three opt-in real OpenWhisk tests (`PERFSYNC_TEST_OPENWHISK=1`) and four live service checks requiring Ditto URL, local MQTT, Kafka bootstrap, or MQTT session cleanup configuration. With those live OpenWhisk/Ditto/MQTT variables provided, all except the Kafka check passed. The single remaining skip was `tests/test_services.py:72`, `KAFKA_TEST_BOOTSTRAP unset`; Kafka is not required for this explicitly MQTT-backed acceptance run. Skips are not counted as passes.

## Known limits

- The Grafana image logs a nonblocking 404 while its optional Loki Explore plugin is loaded. The provisioned dashboard, Prometheus datasource, and all eight panels still rendered successfully.
- OpenWhisk did not provide initialization timing for every invocation, so cold-start classification remains unavailable where the native annotation is missing.
- Queue delay, remote Ditto storage latency, and a reliable platform cold-start duration are not measured by this setup.
- Prometheus/Grafana require the Linux host-network Compose configuration and the default dashboard port 8090 unless the scrape configuration is changed too.
