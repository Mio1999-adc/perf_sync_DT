"""Run with python -m perfsyncgnn.phase2 --help."""
import argparse
import json
import os
from pathlib import Path
from dt.adapter import DittoAdapter, SQLiteTwinAdapter
from monitoring.store import Journal
from simulation.scenarios.intersection import Scenario, generate
from simulation.traci_adapter.client import TraCIClient
from simulation.event_generator.schemas import TrafficEvent
from simulation.event_generator.publisher import DurablePublisher, MQTTTransport, KafkaTransport
from simulation.event_generator.consumer import consume_kafka, consume_mqtt
from synchronization.operation_manager.manager import OperationManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['local', 'publish', 'consume', 'recover'])
    parser.add_argument('--config', default='configs/intersection.json')
    parser.add_argument('--output', default='results/phase2')
    parser.add_argument('--backend', choices=['sqlite', 'ditto'], default='ditto')
    parser.add_argument('--broker', choices=['mqtt', 'kafka'], default='mqtt')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=1883)
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--follow', action='store_true', help='Explicitly request tracking for each newly inserted vehicle')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    config = Scenario.model_validate_json(Path(args.config).read_text())
    journal = Journal(output/'journal.sqlite')
    adapter = SQLiteTwinAdapter(output/'twins.sqlite') if args.backend == 'sqlite' else DittoAdapter(os.getenv('DITTO_URL', 'http://localhost:8080'), os.getenv('DITTO_USERNAME'), os.getenv('DITTO_PASSWORD'), os.getenv('DITTO_POLICY_ID'))
    manager = OperationManager(adapter, journal)
    if args.mode == 'consume':
        try:
            manager.recover()
            if args.broker == 'mqtt':
                consume_mqtt(manager, args.host, args.port)
            else:
                consume_kafka(manager, args.bootstrap)
        finally:
            journal.export(output/'lifecycle.jsonl')
        return
    if args.mode == 'recover':
        manager.recover()
    else:
        publisher = None
        if args.mode == 'publish':
            transport = MQTTTransport(args.host, args.port) if args.broker == 'mqtt' else KafkaTransport(args.bootstrap)
            publisher = DurablePublisher(journal, transport)
            publisher.flush()
        def deliver(event):
            if publisher:
                publisher.publish(event)
            else:
                manager.submit(event)
        try:
            scenario_path = generate(output/'scenario', config)
            with (output/'events.jsonl').open('w') as stream:
                for event in TraCIClient(config, scenario_path).events():
                    stream.write(event.model_dump_json()+'\n')
                    stream.flush()
                    deliver(event)
                    if args.follow and event.entity_type == 'vehicle' and event.event_type == 'insertion':
                        follow = TrafficEvent.model_validate(dict(event.model_dump(), event_id=event.event_id+'-follow', event_type='follow_request', process_variables={}, subscription={'subscriber_id': 'phase2-demo', 'variables': ['speed', 'position_x'], 'lifetime_ms': 30000}))
                        stream.write(follow.model_dump_json()+'\n')
                        deliver(follow)
        finally:
            if publisher:
                transport.close()
    journal.export(output/'lifecycle.jsonl')
    operations = journal.operations()
    print(json.dumps({'backend':args.backend, 'operations':len(operations), 'statuses':{status:sum(o.status == status for o in operations) for status in sorted({o.status.value for o in operations})}, 'types':{kind:sum(o.operation_type == kind for o in operations) for kind in sorted({o.operation_type for o in operations})}}, indent=2))


if __name__ == '__main__':
    main()
