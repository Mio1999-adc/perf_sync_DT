from opentelemetry import trace, propagate
import sqlite3
import requests
from dt.models import digital_id
from dt.adapter import Conflict
from simulation.event_generator.schemas import TrafficEvent
from synchronization.models import Operation, Status, TERMINAL
from synchronization.executor import LocalExecutor
from .triggers import identify, due_subscriptions
from .deduplication import operation_id
from .ordering import dependencies_committed
from .preconditions import PreconditionError


class OperationManager:
    def __init__(self, adapter, journal, executor=None):
        self.adapter = adapter
        self.journal = journal
        self.executor = executor or LocalExecutor(adapter, journal)

    def submit(self, event):
        parent = propagate.extract(event.trace_context) if event.trace_context else None
        with trace.get_tracer("perfsyncgnn").start_as_current_span("operation_manager.submit", context=parent, attributes={"source.event":event.event_id,"run.id":event.run_id}):
            return self._submit(event)

    def _submit(self, event):
        with self.journal.transaction():
            self.journal.ingest(event)
        self.recover()
        return self.journal.operations(event.deduplication_id)

    def recover(self):
        # A process lock serializes workers sharing this journal, including
        # planning and read/conditional-write/verification. Ditto CAS also protects
        # against independent journals. No distributed atomic transaction is implied.
        with self.journal.worker_lock():
            rows = self.journal.db.execute('SELECT key,body FROM inbox WHERE done=0 ORDER BY rowid').fetchall()
            for key, body in rows:
                event = TrafficEvent.model_validate_json(body)
                ops = self.journal.operations(event.deduplication_id)
                known = {o.operation_id: o for o in ops}
                if not ops:
                    with self.journal.transaction():
                        twin = self.adapter.get(digital_id(event))
                        previous = None
                        plans = []
                        for kind in identify(event, twin):
                            if kind == 'Follow' and event.event_type != 'follow_request':
                                plans.extend((kind, subscription) for subscription in due_subscriptions(event, twin))
                            else:
                                plans.append((kind, None))
                        for kind, renewal in plans:
                            payload = {'event_fingerprint': event.fingerprint, 'event_key': event.deduplication_id}
                            if renewal:
                                payload['renewal_subscription'] = renewal
                            if kind == 'Transfer':
                                payload.update(previous_owner=twin.owner, ownership_version=twin.ownership_version)
                            suffix = kind + (':' + renewal['subscriber_id'] if renewal else '')
                            op = Operation(operation_id=operation_id(event, suffix), operation_type=kind, event=event.model_dump(), dt_id=digital_id(event), dependencies=[previous] if previous else [], payload=payload, clock_session=self.executor.clock_session)
                            self.journal.save(op)
                            known[op.operation_id] = op
                            ops.append(op)
                            previous = op.operation_id
                for op in ops:
                    if op.status in TERMINAL:
                        continue
                    if not dependencies_committed(op, known):
                        op.error_category = 'dependency_failed'
                        self.executor.move(op, Status.CANCELLED)
                        continue
                    try:
                        self.executor.execute(op)
                    except (PreconditionError, ValueError) as exc:
                        op.error_category = type(exc).__name__
                        op.error = str(exc)
                        self.executor.move(op, Status.FAILED)
                    except Exception as exc:
                        op.error_category = type(exc).__name__
                        op.error = str(exc)
                        self.journal.save(op)
                        raise
                    # Transport, CAS exhaustion, or failed verification propagate:
                    # inbox remains pending for retry; never synthesize success.
                outcome = 'processed' if ops else 'ignored_stale_or_unassociated'
                self.journal.db.execute('UPDATE inbox SET done=1,outcome=? WHERE key=?', (outcome, key))
