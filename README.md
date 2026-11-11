# PerfSyncGNN

Performance-aware graph learning for synchronization-time prediction in serverless
smart-traffic digital twins. Phase 2 implements a deterministic synchronization pipeline;
Phase 3 adds OpenWhisk execution and a committed-Ditto state viewer. Graph learning is outside the current scope. See the verification report for live acceptance status.

See [Phase 2 practical user guide](docs/PHASE2_USER_GUIDE.md) and
[launcher verification report](docs/PHASE2_LAUNCHER_VERIFICATION.md).

```bash
./scripts/perfsyncctl start --follow
# Watch the actual TraCI-controlled traffic and synchronization:
./scripts/perfsyncctl gui --follow
```

The wrapper uses the existing `.venv` automatically. See the
[one-command launcher guide](docs/PHASE2_ONE_COMMAND_GUIDE.md) for live Ditto/MQTT/Kafka,
status, logs, tests and safe shutdown. Install dependencies first with
`.venv/bin/pip install -e '.[test]'` if needed.


SUMO and netconvert are required for the scenario. The SQLite command is an explicit
local validation backend. The launcher defaults to SQLite; the underlying Phase 2 runner defaults to Ditto; MQTT/Kafka and Ditto
live checks require running services and opt-in environment variables documented above.

Phase 3: [deployment](docs/PHASE3_DEPLOYMENT.md), [one-command guide](docs/PHASE3_ONE_COMMAND_GUIDE.md), [monitoring](docs/PHASE3_MONITORING_GUIDE.md), [GUI 2 guide](docs/GUI2_USER_GUIDE.md), and [verification status](docs/PHASE3_VERIFICATION.md).

```bash
# After one-time provisioning and private Grafana password setup:
./scripts/perfsyncctl run --live --executor openwhisk --broker mqtt --gui --dashboard --monitoring --keep-dashboard --follow --config configs/phase3-demo.json --timeout 900
```
