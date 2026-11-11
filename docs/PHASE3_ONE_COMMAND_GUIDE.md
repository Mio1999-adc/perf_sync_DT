# Phase 3 one-command run

This launcher reuses the existing Ditto/MongoDB and broker containers, checks the retained OpenWhisk cluster, starts GUI 2 and the isolated Phase 3 Prometheus/Grafana Compose project, then starts the connected SUMO-GUI and synchronization consumer. GUI 2 and monitoring become ready before SUMO starts. At normal simulation completion the launcher drains synchronization and holds the viewer and monitoring services until `stop`.

## One-time preparation

Complete [PHASE3_DEPLOYMENT.md](PHASE3_DEPLOYMENT.md), build the frontend, and provision the OpenWhisk actions. Set a private Grafana password in the current shell. For example, generate a password without displaying it:

```bash
install -d -m 700 results/phase3/private
umask 077
openssl rand -base64 32 > results/phase3/private/grafana-admin-password
export PERFSYNC_GRAFANA_PASSWORD="$(cat results/phase3/private/grafana-admin-password)"
```

## Start the synchronized system

From the repository root:

```bash
cd ~/PerfSyncGNN
# Needed when the project's Minikube/kubectl/Node tools are not already on PATH.
export PATH="$PWD/results/phase3-tools:$PWD/results/phase3-tools/linux-amd64:$PWD/results/phase3-tools/node-v22.16.0-linux-x64/bin:$PATH"
export PERFSYNC_GRAFANA_PASSWORD="$(cat results/phase3/private/grafana-admin-password)"
./scripts/perfsyncctl run \
  --live --broker mqtt --executor openwhisk \
  --gui --dashboard --monitoring --keep-dashboard --follow \
  --delay 200 --timeout 900 --config configs/phase3-demo.json
```

The launcher prints readiness and URLs. SUMO-GUI is GUI 1. GUI 2 is at `http://127.0.0.1:8090`, Prometheus at `http://127.0.0.1:9095`, and Grafana at `http://127.0.0.1:3005` (login `admin` and the password generated above). Grafana dashboard `PerfSync Phase 3 — measured execution` is provisioned from the repository files. The default run is finite; after it drains, the launcher keeps the dashboard and scrape target available until explicit shutdown.

The launcher creates a unique output directory by default. You can specify `--output results/my-run` only when that directory is new or empty. The command does not start a separate SUMO replay. `--delay 200` slows event generation to make a visual demonstration easier to follow.

## Inspect and stop

```bash
./scripts/perfsyncctl status
./scripts/perfsyncctl logs --service launcher --follow
./scripts/perfsyncctl logs --service sumo
./scripts/perfsyncctl logs --service dashboard
./scripts/perfsyncctl stop
```

Stop signals the supervisor, which closes SUMO and the consumer, then GUI 2, OpenWhisk forwarding, and only monitoring/containers owned by this run. Compose named volumes for Prometheus and Grafana remain intact. Ditto data and the Kubernetes cluster are retained. The run directory contains journals, lifecycle records, trace exports, metrics samples, resource samples, and `launcher/state.json`.

## Verify a completed run

Keep Ditto available while checking real receipts:

```bash
./scripts/verify-phase3-run results/<run-name>
```

## Expected data and limits

GUI 2 reads committed vehicle twins from Ditto and displays physical TraCI records in a separately labeled comparison. The timeline uses actual durable lifecycle records. `perfsync_sync_seconds` measures request-to-verified-commit duration in the manager's monotonic clock; `perfsync_function_seconds` measures platform activation duration. Host CPU, memory, and network are sampled separately. Broker queue delay and remote Ditto storage latency are unavailable; see [PHASE3_MONITORING_GUIDE.md](PHASE3_MONITORING_GUIDE.md). A missing OpenWhisk initialization annotation is shown as unavailable rather than treated as a warm start.

If using a nondefault dashboard port, update `infrastructure/monitoring/prometheus/prometheus.yml` to scrape that port before starting monitoring.
