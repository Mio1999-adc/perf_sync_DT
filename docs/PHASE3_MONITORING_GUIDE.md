# Phase 3 monitoring

## Data and clock definitions

SUMO `simulation_time_ms` and `source_timestamp_ms` belong to the simulation clock. They are not Unix timestamps. Host/function Unix timestamps identify wall-clock observations; monotonic timestamps are used for within-session durations.

Successful synchronization duration is `(commit_monotonic_ns - request_monotonic_ns) / 1e9` in the **same manager clock session**, from operation creation to independent Ditto commit verification. `request_unix_ns` and `commit_unix_ns` are also recorded, but cross-host wall-clock subtraction is not used as a substitute. Recovery across sessions leaves duration unavailable. FAILED, CANCELLED, SUPERSEDED, and unresolved operations do not enter the success histogram.

Each operation records request, validated, queued, start, and verified commit timestamps; operation status/type, dependencies, source event/run/entity IDs, retry count, activation ID, and returned function start/end timestamps. Function timestamps stay in the function's wall-clock domain. Actual transition history remains in `journal.sqlite` and exported `lifecycle.jsonl`.

The trace path is `sumo.event → operation_manager.submit → synchronization → openwhisk.invoke → function.execute → ditto.conditional_write / ditto.read_or_validate`, followed by caller `ditto.commit_validation`. W3C trace context travels in the durable event and action payload. The remote function returns its real OTel spans to the caller for export.

## Run artifacts

For MQTT/Kafka runs, look under `RUN/consumer/`; for direct runs, `RUN/simulation/`. The producer's physical observations are under `RUN/producer/` for broker runs.

| Artifact | Meaning |
| --- | --- |
| journal.sqlite | Durable inbox, operations, transition lifecycle |
| lifecycle.jsonl | Exported lifecycle records, including unsuccessful/pending operations |
| traces.jsonl | Manager/producer OTel spans |
| remote-traces.jsonl | Function and Ditto spans returned by actual activations |
| invocations.jsonl | Platform responses correlated with operation/activation IDs |
| activations.jsonl | Function timing/platform annotations for returned executions |
| metrics.prom | Atomic Prometheus snapshot, sampled every two seconds and at shutdown |
| resources.jsonl | Host CPU/memory/network and Linux pressure-stall samples |

Files are append-only evidence except atomic current metrics snapshots. They are not an authoritative substitute for Ditto state. Concurrent snapshots are read through the dashboard's `/metrics` and `/api/resources` endpoints.

## Metrics

- `perfsync_sync_seconds`: measured successful request-to-verified-commit duration, grouped by operation.
- `perfsync_function_seconds`: measured OpenWhisk activation duration in milliseconds converted to seconds.
- `perfsync_invocations_total`, `perfsync_invocation_failures_total`, `perfsync_retries_total`: caller observations, including retry attempts. These count attempts, not distinct business operations.
- `perfsync_invocations_inflight`: current requests outstanding in one manager process; this is not cluster-wide function concurrency.
- `perfsync_host_cpu_percent`, `perfsync_host_memory_used_bytes`, `perfsync_host_network_bytes`: host measurements, not pod usage. CPU is interval-sampled; the first sample may be zero because no interval exists yet.

Cold-start metadata is derived only when the platform supplies an `initTime` activation annotation: positive means initialization observed. Missing annotation means unavailable, not warm. Platform duration/annotation clocks are not mixed with the manager monotonic clock. Queue delay is unavailable without a reliable platform measurement; no function-start minus caller timestamp approximation is made.

## Container/pod and contention evidence

```bash
./scripts/collect-phase3-resources > results/phase3-resource-snapshot.json
```

This collects real Docker statistics and pod/container status. Kubernetes `top pods` is attempted only against this project's namespace; a missing metrics-server is explicitly reported as unavailable. Nested Docker statistics measure OpenWhisk containers on the Minikube node without requiring metrics-server; records are filtered to this project namespace. Host Linux CPU/memory/I/O pressure-stall data identifies measured contention symptoms. `perfsync_journal_save_seconds` separately measures local SQLite WAL save transactions including commit, not remote Ditto disk latency. Broker backlog and storage-operation latency are explicitly unavailable in the current collector; do not label host network or I/O pressure as either of those metrics.

## Optional Prometheus/Grafana

The Linux development Compose file uses host networking and loopback listeners to reach the loopback-only dashboard. It uses its own Compose project and ports 9095 and 3005; it does not replace the unrelated existing Grafana stack.

```bash
# Set PERFSYNC_GRAFANA_PASSWORD privately in your shell before starting.
docker compose -p perfsync-phase3-monitoring -f infrastructure/monitoring/compose.yml up -d
```

Prometheus: http://127.0.0.1:9095. Grafana: http://127.0.0.1:3005 (admin plus your configured password). Provisioning includes invocation, retry, failure, caller concurrency, host resources, function duration, and synchronization duration panels. The scrape target is GUI 2 port 8090; update the Prometheus configuration if using another dashboard port. Panels remain empty when no measured series exists.

```bash
docker compose -p perfsync-phase3-monitoring -f infrastructure/monitoring/compose.yml down
```

The launcher integrates the same Compose project using `--monitoring`; `--keep-dashboard` leaves its scrape target and Grafana available after the finite simulation until explicit `perfsyncctl stop`. It waits for Prometheus readiness, the healthy GUI metrics scrape, Grafana API authentication, the provisioned datasource's live health check, and the provisioned dashboard before starting SUMO. It reuses a running Compose project and shuts it down only when this run started it. The named data volumes remain after shutdown.

This integration was exercised in run `launcher-phase3-acceptance-verified-46f73816`. Prometheus reported its `perfsync` target `up` at `127.0.0.1:8090`; the actual scrape exposed 214 invocation, synchronization-duration, and function-duration samples with counts matching the five committed operation types. One scrape measured host CPU at 6.5% and memory at 9,898,672,128 bytes. Grafana authenticated, returned `OK` for its provisioned Prometheus datasource, loaded the `PerfSync Phase 3 — measured execution` dashboard, and rendered all eight panels. The actual screenshot is retained at [`results/phase3-acceptance-verified/qa/grafana-dashboard.png`](../results/phase3-acceptance-verified/qa/grafana-dashboard.png).

In that acceptance run, the final operation journal contained no failed business operations and zero operation retry counts, so failure/retry metric series were absent from the final metrics snapshot. Prometheus storage is persistent and can retain history across runs; do not attribute older failure/retry samples to a particular run without matching timestamps and run evidence.

The dashboard panels include verified synchronization latency, invocations, failures, retries, in-flight manager calls, host CPU/memory, and function duration. A cross-host queue-delay metric is unavailable because no reliable queue clock is present. Cold-start metadata is unavailable for activations without OpenWhisk's native `initTime` annotation; those activations are not counted as warm. Remote Ditto storage latency is not measured. See the verification report for observed metric samples and any unavailable series.
