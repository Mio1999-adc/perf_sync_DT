# PerfSyncGNN

Performance-aware graph learning for synchronization-time prediction in serverless
smart-traffic digital twins. Phase 2 implements a deterministic synchronization pipeline;
graph learning and OpenWhisk execution are later phases.

See [Phase 2 setup and semantics](docs/phase2.md) and
[verification report](docs/phase2-verification.md).

```bash
.venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest -q -rs
.venv/bin/python -m perfsyncgnn.phase2 local --backend sqlite --follow
```

SUMO and netconvert are required for the scenario. The SQLite command is an explicit
local validation backend. Eclipse Ditto is the default backend; MQTT/Kafka and Ditto
live checks require running services and opt-in environment variables documented above.
