# Phase 3 deployment

The OpenWhisk launcher starts the five core Ditto containers (MongoDB, policies, things, gateway, nginx), plus the two lightweight UI containers required by the existing nginx configuration. It does not start the search or connectivity service; already running services are preserved. MQTT/Kafka delivery uses the existing application consumer rather than Ditto connectivity. The local executor retains the full Phase 2 service behavior.

Phase 3 uses the existing SUMO/TraCI producer, MQTT or Kafka transport, operation journal, and Eclipse Ditto backend. OpenWhisk invokes the same five handlers in a Python 3.11 action image. SQLite and the local executor remain available for regression work.

Read [PHASE3_VERIFICATION.md](PHASE3_VERIFICATION.md) for the current verified status. Source implementation or Kubernetes readiness alone does not establish end-to-end acceptance.

## Prerequisites

Use the existing Docker and Minikube cluster. Required tools: Docker, Helm 3, a compatible kubectl, SUMO/netconvert (and an X11 display for SUMO-GUI), Python >=3.9, and Node >=22 for building the viewer. The development machine has project-local tools in `results/phase3-tools`; they are not tracked installation dependencies.

```bash
cd ~/PerfSyncGNN
export PATH="$PWD/results/phase3-tools:$PWD/results/phase3-tools/linux-amd64:$PWD/results/phase3-tools/node-v22.16.0-linux-x64/bin:$PATH"
.venv/bin/python -m pip install -e '.[test]'
npm --prefix dashboard/frontend ci
npm --prefix dashboard/frontend run build
minikube start -p minikube
./scripts/openwhisk-infra deploy
./scripts/build-openwhisk-runtime
./scripts/configure-openwhisk-local
```

The infrastructure script prepares a local chart copy with PVC retention annotations and a controller startup probe (up to ten minutes on slow cold storage); the upstream checkout is preserved. The infrastructure script uses the official Apache OpenWhisk Helm chart already checked out under `third_party/openwhisk-deploy-kube`, chart version 1.0.1. Platform images are pinned by that chart to `ef725a6`. The action runtime base is pinned by digest in `perfsyncgnn/serverless/runtime.Dockerfile` and uses Python 3.11. No Python 3.7 project runtime is used.

If nested Docker image pulls fail with TLS timeouts, pull the **reported exact image** on the host and load it into the existing Minikube Docker runtime:

```bash
docker pull IMAGE:TAG
docker save IMAGE:TAG | docker exec -i minikube docker load
```

This is a real platform deployment, not an OpenWhisk simulator. The current configuration enables persistent OpenWhisk platform storage (`k8s.persistence.enabled: true`) using the cluster storage class. The earlier ephemeral development deployment could lose CouchDB databases after a node restart; it was replaced before acceptance. Cleanup does not delete PVCs or Docker volumes. Ditto volumes and run journals are separate and preserved. Production deployments also need backups and a supported maintained platform/runtime configuration.

## Private configuration and action deployment

`results/phase3/private/helm-secrets.json` holds generated guest/system API keys and database credentials. The directory is mode 0700 and files are mode 0600. `configure-openwhisk-local` writes `connection.json` with the private guest API key, loopback API URL, and Ditto address reachable from action pods. Never commit or publish these files.

The existing private Phase 2 Ditto configuration is reused. It must contain DITTO_URL, DITTO_USERNAME, DITTO_PASSWORD, and DITTO_POLICY_ID, or those variables must be supplied in the environment. Action defaults store these credentials in the private OpenWhisk action definition. A holder of the namespace API key can administer/read that definition; treat the key as a credential. No credentials enter frontend JavaScript, launcher state, or normal invocation payloads.

For initial provisioning, keep this port-forward running in one terminal:

```bash
kubectl -n perfsync-openwhisk port-forward --address 127.0.0.1 service/perfsync-ow-nginx 3233:80
```

Then deploy and verify:

```bash
./scripts/openwhisk-actions
./scripts/openwhisk-check
```

The check must return a genuine activation ID and Python version. A 503 or timeout is a failed readiness check. The development API is HTTP only on the loopback port-forward; remote APIs should use HTTPS with a trusted certificate or OPENWHISK_CA_BUNDLE. Certificate verification is enabled in the client.

After provisioning, stop the manual port-forward. The unified launcher creates and supervises its own when port 3233 is free. An already running reachable API is reused and preserved. For another cluster, supply OPENWHISK_API_URL, OPENWHISK_AUTH, an optional OPENWHISK_CA_BUNDLE, and a Ditto URL reachable from action pods (`--ditto-url` when deploying actions).

## Unified run

```bash
./scripts/perfsyncctl run --live --executor openwhisk --broker mqtt \
  --gui --dashboard --follow --config configs/phase3-demo.json --timeout 900
```

GUI 2: http://127.0.0.1:8090. Use `--dashboard-port 8091` for a conflicting port and `--dashboard-refresh 2` for a two-second poll interval. Use `--executor local` explicitly to run handlers in the worker; this is not serverless verification. The launcher validates the real Python probe before running a serverless simulation.

```bash
./scripts/perfsyncctl status
./scripts/perfsyncctl logs --service sync --follow
./scripts/perfsyncctl logs --service dashboard
./scripts/perfsyncctl logs --service openwhisk-api
./scripts/perfsyncctl stop
```

The simulation is finite. SUMO-GUI closes when its simulation ends; the consumer drains outstanding work, then the launcher closes its dashboard/API children and stops only containers it started. The existing Kubernetes cluster and Helm deployment are retained because they may share infrastructure with unrelated workloads. No independent SUMO replay is launched.

## Failure recovery and cleanup

A transport failure, missing receipt, or failed commit predicate leaves work unresolved for durable recovery; it does not manufacture a COMMITTED label. Action retries keep the same operation ID, and Ditto receipts prevent duplicate transitions. Failed handler preconditions produce FAILED operations without successful latency labels.

For a stale launcher, inspect status and run stop to reconcile recorded, identity-checked resources before restarting. Never remove its SQLite journal to hide pending work. For interrupted Helm operations, inspect `helm status`/`helm history`; do not change release metadata to pretend deployment succeeded.

To remove this project's OpenWhisk release and its labelled action pods:

```bash
./scripts/openwhisk-infra cleanup
```

The script does not delete the cluster, other namespaces, Ditto volumes, or unrelated resources. Export any required activation evidence first, and inspect retained PVCs before reinstalling a release against existing data. Never rerun database initialization over a populated store merely to clear a readiness error.

An observed failure on the development HDD was `Failed to initialize reactive invoker: Futures timed out after [30 seconds]` while deleting old runtime pods. The chart now allows 180 seconds for runtime cleanup. High host I/O pressure can still exceed Kubernetes/Helm deadlines; collect `/proc/pressure/io`, pod events, and previous invoker logs and report the failure instead of declaring the platform ready.
