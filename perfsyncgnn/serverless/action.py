"""One shared OpenWhisk entry point; action defaults hold private Ditto configuration."""
import json
import sys
sys.path.insert(0,"/opt/perfsync")
import time
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from dt.adapter import DittoAdapter, Conflict
from synchronization.executor import LocalExecutor, HANDLERS
from synchronization.models import Operation, Status
from simulation.event_generator.schemas import TrafficEvent
from synchronization.operation_manager.preconditions import PreconditionError
from dt.models import digital_id

class ActionJournal:
    def save(self, operation):
        pass  # The manager persists the caller-side lifecycle.

class Exporter(SpanExporter):
    def __init__(self): self.spans=[]
    def export(self,spans):
        self.spans.extend(json.loads(span.to_json(indent=None)) for span in spans)
        return SpanExportResult.SUCCESS

exporter=Exporter()
provider=TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))
tracer=provider.get_tracer('perfsync.action')

def main(args):
    started=time.time_ns()
    exporter.spans.clear()
    def failure(category,message):
        return {'error_category':category,'message':message,'function_start_unix_ns':started,'function_end_unix_ns':time.time_ns(),'spans':exporter.spans}
    op=Operation.model_validate(args['operation'])
    config=args['__perfsync_config']  # Protected OpenWhisk action defaults, not frontend data.
    event=TrafficEvent.model_validate(op.event)
    if op.operation_type not in HANDLERS or op.operation_type != config['operation_type']:
        return failure('ValueError','action type mismatch')
    if op.dt_id != digital_id(event) or op.payload['event_fingerprint'] != event.fingerprint:
        return failure('ValueError','event identity mismatch')
    context=propagate.extract(args.get('trace_context',{}))
    with tracer.start_as_current_span('function.execute',context=context,attributes={'operation.id':op.operation_id,'operation.type':op.operation_type,'run.id':event.run_id}):
        adapter=DittoAdapter(config['url'],config['username'],config['password'],config['policy_id'],timeout=config.get('timeout',30))
        # Instrument actual remote adapter reads/writes, without recording credentials.
        class TracedAdapter:
            def get(self,identity):
                with tracer.start_as_current_span('ditto.read_or_validate'):
                    return adapter.get(identity)
            def commit(self,twin,expected):
                with tracer.start_as_current_span('ditto.conditional_write'):
                    return adapter.commit(twin,expected)
        current=adapter.get(op.dt_id)
        receipt=current.applied_operations.get(op.operation_id) if current else None
        if not receipt and ((current.version if current else None) != args['expected_version'] or (current.owner if current else None) != args['current_owner']):
            return failure('Conflict','dispatch state changed')
        if not receipt and any(dependency not in (current.applied_operations if current else {}) for dependency in op.dependencies):
            return failure('PreconditionError','dependency receipt missing')
        op.status=Status.CREATED
        op.clock_session=''  # Never subtract monotonic timestamps across machines.
        try:
            LocalExecutor(TracedAdapter(),ActionJournal()).execute(op)
        except (PreconditionError,ValueError) as exc:
            return failure(type(exc).__name__,str(exc))
    provider.force_flush()
    return {'operation_id':op.operation_id,'dt_id':op.dt_id,'version_after':op.version_after,
            'function_start_unix_ns':started,'function_end_unix_ns':time.time_ns(),
            'trace_context':args.get('trace_context',{}),'receipt_verified':True,'spans':exporter.spans}
