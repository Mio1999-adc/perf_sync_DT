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
from monitoring.runtime import RuntimeReporter
import traci
from opentelemetry import trace, propagate
from monitoring.telemetry import configure, Sampler, TracedAdapter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['local', 'publish', 'consume', 'recover'])
    parser.add_argument('--config', default='configs/intersection.json')
    parser.add_argument('--output', default='results/phase2')
    parser.add_argument('--executor', choices=['local','openwhisk'], default='local')
    parser.add_argument('--backend', choices=['sqlite', 'ditto'], default='ditto')
    parser.add_argument('--broker', choices=['mqtt', 'kafka'], default='mqtt')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=1883)
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--follow', action='store_true', help='Explicitly request tracking for each newly inserted vehicle')
    parser.add_argument('--gui', action='store_true', help='Use the same TraCI-controlled SUMO-GUI process (requires DISPLAY)')
    parser.add_argument('--delay', type=float, default=100, help='Connected GUI delay in milliseconds per step')
    parser.add_argument('--runtime-status', help='Optional process/TraCI observation JSON path')
    parser.add_argument('--consumer-id', default='perfsync-sync', help='Stable MQTT session ID for this consumer journal')
    parser.add_argument('--consumer-group', default='perfsync-sync', help='Kafka consumer group for this journal')
    parser.add_argument('--offset-reset', choices=['earliest','latest'], default='earliest', help='Initial Kafka group offset when no committed offset exists')
    args = parser.parse_args()
    if args.gui and not os.getenv('DISPLAY'):
        parser.error('--gui requires DISPLAY')
    if args.delay < 0:
        parser.error('--delay must be nonnegative')
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    config = Scenario.model_validate_json(Path(args.config).read_text())
    journal = Journal(output/'journal.sqlite')
    adapter = SQLiteTwinAdapter(output/'twins.sqlite') if args.backend == 'sqlite' else DittoAdapter(os.getenv('DITTO_URL', 'http://localhost:8080'), os.getenv('DITTO_USERNAME'), os.getenv('DITTO_PASSWORD'), os.getenv('DITTO_POLICY_ID'), timeout=float(os.getenv('PERFSYNC_DITTO_TIMEOUT','30')))
    if args.executor == "openwhisk" and args.backend != "ditto":
        parser.error("OpenWhisk requires the Ditto backend")
    provider = configure(output/"traces.jsonl")
    sampler = Sampler(output)
    sampler.start()
    adapter = TracedAdapter(adapter)
    executor = None
    if args.executor == "openwhisk" and args.mode != "publish":
        from perfsyncgnn.serverless.openwhisk_executor import OpenWhiskExecutor
        executor = OpenWhiskExecutor(adapter, journal)
    manager = OperationManager(adapter, journal, executor=executor)
    reporter = RuntimeReporter(args.runtime_status)
    reporter.update(phase='running', mode=args.mode, synchronization='initializing', traci='not_started')
    outcome = 'completed'
    try:
        return execute(args, config, output, journal, manager, reporter)
    except KeyboardInterrupt:
        outcome = 'cancelled'
        return 130
    except traci.FatalTraCIError:
        outcome = 'cancelled' if args.gui else 'failed'
        if args.gui:
            print('Connected GUI/TraCI session closed; partial lifecycle records preserved.')
            return 130
        raise
    except BaseException:
        outcome = 'failed'
        raise
    finally:
        journal.export(output/'lifecycle.jsonl')
        reporter.update(phase=outcome, traci='closed', synchronization='stopped')
        sampler.close()
        provider.shutdown()


def execute(args, config, output, journal, manager, reporter):
    reporter.update(synchronization='ready' if args.mode != 'consume' else 'recovering')
    if args.mode == 'consume':
        try:
            manager.recover()
            if args.broker == 'mqtt':
                consume_mqtt(manager, args.host, args.port, client_id=args.consumer_id, ready=lambda: reporter.update(synchronization='ready', subscription='acknowledged'))
            else:
                consume_kafka(manager, args.bootstrap, group=args.consumer_group, offset_reset=args.offset_reset, ready=lambda: reporter.update(synchronization='ready', subscription='assigned'))
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
                for event in TraCIClient(config, scenario_path, gui=args.gui, delay=args.delay, reporter=reporter).events():
                    stream.write(event.model_dump_json()+'\n')
                    stream.flush()
                    with trace.get_tracer("perfsyncgnn").start_as_current_span("sumo.event", attributes={"source.event":event.event_id,"simulation.time_ms":event.simulation_time_ms}):
                        carrier = {}
                        propagate.inject(carrier)
                        deliver(event.model_copy(update={"trace_context":carrier}))
                    if args.follow and event.entity_type == 'vehicle' and event.event_type == 'insertion':
                        follow = TrafficEvent.model_validate(dict(event.model_dump(), event_id=event.event_id+'-follow', event_type='follow_request', trace_context=carrier, process_variables={}, subscription={'subscriber_id': 'phase2-demo', 'variables': ['speed', 'position_x'], 'lifetime_ms': 30000}))
                        stream.write(follow.model_dump_json()+'\n')
                        deliver(follow)
        finally:
            if publisher:
                transport.close()
    journal.export(output/'lifecycle.jsonl')
    operations = journal.operations()
    print(json.dumps({'backend':args.backend, 'operations':len(operations), 'statuses':{status:sum(o.status == status for o in operations) for status in sorted({o.status.value for o in operations})}, 'types':{kind:sum(o.operation_type == kind for o in operations) for kind in sorted({o.operation_type for o in operations})}}, indent=2))


if __name__ == '__main__':
    raise SystemExit(main())
