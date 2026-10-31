"""Same structured operation interface can be dispatched by a future FaaS executor."""
import time
import uuid
from dt.models import Twin
from dt.adapter import Conflict
from synchronization.models import Status
from synchronization.operations.attach import Attach
from synchronization.operations.update import Update
from synchronization.operations.follow import Follow
from synchronization.operations.transfer import Transfer
from synchronization.operations.detach import Detach

HANDLERS = {h.kind: h() for h in (Attach, Update, Follow, Transfer, Detach)}


class LocalExecutor:
    def __init__(self, adapter, journal):
        self.adapter = adapter
        self.journal = journal
        self.clock_session = str(uuid.uuid4())

    def move(self, op, status):
        op.transition(status)
        self.journal.save(op)

    def execute(self, op):
        handler = HANDLERS[op.operation_type]
        if op.status == Status.CREATED:
            self.move(op, Status.VALIDATED)
        if op.status == Status.VALIDATED:
            self.move(op, Status.QUEUED)
        if op.status == Status.QUEUED:
            op.start_unix_ns = time.time_ns()
            self.move(op, Status.RUNNING)
        # Recovery resumes RUNNING/COMMITTING through the same durable receipt check.
        for attempt in range(4):
            twin = self.adapter.get(op.dt_id)
            if twin and op.operation_id in twin.applied_operations:
                receipt = twin.applied_operations[op.operation_id]
                # Receipt was written atomically with the checked postcondition. Later
                # operations may legitimately supersede the current postcondition.
                if receipt['operation_type'] != op.operation_type or receipt['event_fingerprint'] != op.payload['event_fingerprint']:
                    raise RuntimeError('commit receipt identity mismatch')
                op.version_before = receipt['version_before']
                op.version_after = receipt['version_after']
                op.owner = receipt['owner']
                break
            if twin is None:
                if op.operation_type != 'Attach':
                    raise ValueError('target does not exist')
                twin = Twin(dt_id=op.dt_id, physical_id=op.event['entity_id'], entity_type=op.event['entity_type'], run_id=op.event['run_id'])
                expected = None
            else:
                expected = twin.version
            op.version_before = twin.version
            handler.apply(twin, op)
            twin.version += 1
            twin.last_operation = op.operation_id
            twin.synchronization_status = twin.association_status
            if not handler.verify(twin, op):
                raise RuntimeError('transition fails commit predicate')
            op.owner = twin.owner
            twin.applied_operations[op.operation_id] = dict(operation_type=op.operation_type, event_fingerprint=op.payload['event_fingerprint'], version_before=op.version_before, version_after=twin.version, owner=twin.owner)
            if op.status == Status.RUNNING:
                self.move(op, Status.COMMITTING)
            try:
                self.adapter.commit(twin, expected)
            except Conflict:
                if attempt == 3:
                    raise
                continue
            verified = self.adapter.get(op.dt_id)
            if verified is None or verified.applied_operations.get(op.operation_id) != twin.applied_operations[op.operation_id]:
                raise RuntimeError('durable commit receipt missing')
            if verified.version == twin.version and not handler.verify(verified, op):
                raise RuntimeError('durable commit predicate failed')
            op.version_after = twin.version
            break
        if op.status == Status.RUNNING:
            self.move(op, Status.COMMITTING)
        op.error_category = None
        op.error = None
        op.commit_unix_ns = time.time_ns()
        op.commit_monotonic_ns = time.monotonic_ns()
        # Monotonic values must never be subtracted across executor sessions/reboots.
        if op.clock_session == self.clock_session:
            op.duration_ns = op.commit_monotonic_ns - op.request_monotonic_ns
        self.move(op, Status.COMMITTED)
        return op
