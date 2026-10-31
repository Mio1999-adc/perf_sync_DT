import json
from concurrent.futures import ThreadPoolExecutor
import pytest
from pydantic import ValidationError
from dt.adapter import SQLiteTwinAdapter, Conflict
from dt.models import digital_id
from monitoring.store import Journal
from simulation.event_generator.schemas import TrafficEvent
from simulation.event_generator.publisher import DurablePublisher
from simulation.event_generator.consumer import accept
from synchronization.models import Operation, Status
from synchronization.operation_manager.manager import OperationManager
from synchronization.executor import HANDLERS


def event(seq=1, kind='observation', owner='rsu-west', time_ms=None, **kwargs):
    return TrafficEvent(event_id=f'event-{seq}-{kind}', run_id='run', entity_id='v1', entity_type='vehicle', event_type=kind, simulation_time_ms=time_ms if time_ms is not None else seq*1000, source_timestamp_ms=time_ms if time_ms is not None else seq*1000, sequence_number=seq, source_id=owner, process_variables={'speed':float(seq), 'position_x':10.0} if kind in {'insertion','observation'} else {}, **kwargs)


@pytest.fixture
def system(tmp_path):
    adapter = SQLiteTwinAdapter(tmp_path/'twins.db')
    journal = Journal(tmp_path/'journal.db')
    return adapter, journal, OperationManager(adapter, journal)


def test_attach_update_verified(system):
    adapter, journal, manager = system
    e = event()
    ops = manager.submit(e)
    assert [o.operation_type for o in ops] == ['Attach', 'Update']
    assert ops[1].dependencies == [ops[0].operation_id]
    assert all(o.status == Status.COMMITTED for o in ops)
    assert [o.version_after for o in ops] == [1,2]
    assert all(o.duration_ns >= 0 and o.commit_unix_ns >= o.request_unix_ns for o in ops)
    twin = adapter.get(digital_id(e))
    assert twin.version == 2 and twin.process_variables == e.process_variables
    for op in ops:
        assert op.operation_id in twin.applied_operations
    statuses = [json.loads(r[0])['status'] for r in journal.db.execute('SELECT body FROM lifecycle')]
    assert statuses[:6] == ['CREATED','CREATED','VALIDATED','QUEUED','RUNNING','COMMITTING']


def test_lifecycle_all_five_and_follow_renewal(system):
    adapter, journal, manager = system
    manager.submit(event(kind='insertion'))
    follow = event(2, 'follow_request', subscription={'subscriber_id':'viewer','variables':['speed'], 'lifetime_ms':5000})
    assert manager.submit(follow)[0].status == Status.COMMITTED
    renewal = event(3, 'follow_request', subscription={'subscriber_id':'viewer','variables':['speed'], 'lifetime_ms':8000})
    manager.submit(renewal)
    assert adapter.get(digital_id(follow)).subscriptions['viewer']['expires_simulation_ms'] == 11000
    ops = manager.submit(event(4, owner='rsu-east'))
    assert [o.operation_type for o in ops] == ['Transfer','Update']
    assert ops[1].dependencies == [ops[0].operation_id]
    twin = adapter.get(digital_id(follow))
    assert twin.owner == 'rsu-east' and twin.ownership_version == 2
    manager.submit(event(5, 'departure', owner='rsu-east'))
    twin = adapter.get(digital_id(follow))
    assert twin.association_status == 'detached' and not twin.subscriptions and twin.owner is None
    assert manager.submit(event(6)) == []
    assert [o.operation_type for o in manager.submit(event(7, 'insertion'))] == ['Attach','Update']


def test_duplicate_stale_and_wrong_identity(system):
    adapter, journal, manager = system
    e = event(4)
    manager.submit(e)
    manager.submit(e)
    assert len(journal.operations()) == 2
    assert manager.submit(event(3, owner='rsu-east')) == []
    assert manager.submit(event(5, time_ms=1000)) == []
    assert adapter.get(digital_id(e)).owner == 'rsu-west'
    with pytest.raises(ValueError, match='conflicting event identity'):
        manager.submit(e.model_copy(update={'process_variables':{'speed':999}}))


@pytest.mark.parametrize('subscription', [None, {}, {'subscriber_id':'a','variables':[],'lifetime_ms':1}, {'subscriber_id':'a','variables':['speed'],'lifetime_ms':0}])
def test_subscription_schema(subscription):
    with pytest.raises(ValidationError):
        event(kind='follow_request', subscription=subscription)


def test_schema_serialization_and_time():
    e = event()
    assert TrafficEvent.model_validate_json(e.model_dump_json()) == e
    for changed in [{'sequence_number':-1}, {'source_timestamp_ms':2000}, {'schema_version':'2'}, {'entity_type':'alien'}, {'process_variables':{'speed':float('nan')}}]:
        with pytest.raises((ValidationError, ValueError)):
            TrafficEvent.model_validate(dict(e.model_dump(), **changed))


def test_failed_follow_no_success_label(system):
    adapter, journal, manager = system
    manager.submit(event())
    bad = event(2, 'follow_request', subscription={'subscriber_id':'a','variables':['unknown'],'lifetime_ms':1000})
    operation = manager.submit(bad)[0]
    assert operation.status == Status.FAILED
    assert operation.duration_ns is None and operation.commit_unix_ns is None
    assert adapter.get(digital_id(bad)).version == 2


def test_follow_missing_target_and_nonextension(system):
    adapter, journal, manager = system
    missing = event(2,'follow_request', subscription={'subscriber_id':'a','variables':['speed'],'lifetime_ms':5000})
    assert manager.submit(missing)[0].status == Status.FAILED
    manager.submit(event(3))
    valid = event(4,'follow_request', subscription={'subscriber_id':'a','variables':['speed'],'lifetime_ms':10000})
    manager.submit(valid)
    short = event(5,'follow_request', subscription={'subscriber_id':'a','variables':['speed'],'lifetime_ms':1000})
    assert manager.submit(short)[0].status == Status.FAILED


def test_transfer_checks_previous_owner_version(system):
    adapter, journal, manager = system
    manager.submit(event())
    twin = adapter.get(digital_id(event()))
    e = event(2, owner='rsu-east')
    op = Operation(operation_id='x', operation_type='Transfer', event=e.model_dump(), dt_id=twin.dt_id, payload={'previous_owner':'wrong','ownership_version':1})
    with pytest.raises(ValueError, match='previous owner'):
        HANDLERS['Transfer'].apply(twin,op)
    op.payload['previous_owner'] = twin.owner
    op.payload['ownership_version'] = 0
    with pytest.raises(ValueError, match='ownership version'):
        HANDLERS['Transfer'].apply(twin,op)


def test_wrong_owner_departure_failed(system):
    adapter, journal, manager = system
    manager.submit(event())
    assert manager.submit(event(2,'departure',owner='rsu-east'))[0].status == Status.FAILED
    assert adapter.get(digital_id(event())).association_status == 'attached'


def test_state_machine():
    op = Operation(operation_id='x',operation_type='Update',event={},dt_id='x')
    with pytest.raises(ValueError):
        op.transition(Status.COMMITTED)
    for status in [Status.VALIDATED,Status.QUEUED,Status.RUNNING,Status.COMMITTING,Status.COMMITTED]:
        op.transition(status)
    with pytest.raises(ValueError):
        op.transition(Status.FAILED)
    for status in [Status.FAILED,Status.CANCELLED,Status.SUPERSEDED]:
        other = Operation(operation_id='x',operation_type='Update',event={},dt_id='x')
        other.transition(status)
        assert other.commit_unix_ns is None


def test_cas_conflict(system):
    adapter, journal, manager = system
    manager.submit(event())
    a = adapter.get(digital_id(event()))
    b = a.model_copy(deep=True)
    a.version += 1
    adapter.commit(a,2)
    b.version += 1
    with pytest.raises(Conflict):
        adapter.commit(b,2)


def test_commit_failure_and_restart_idempotency(system,tmp_path):
    adapter, journal, manager = system
    original = adapter.commit
    failed = False
    def crash_after_write(twin, expected):
        nonlocal failed
        original(twin, expected)
        if twin.last_operation and not failed:
            failed = True
            raise ConnectionError('lost response after durable commit')
    adapter.commit = crash_after_write
    with pytest.raises(ConnectionError):
        manager.submit(event())
    pending = journal.operations()
    assert pending[0].status == Status.COMMITTING
    assert pending[0].commit_unix_ns is None
    adapter.commit = original
    recovered_journal = Journal(tmp_path/'journal.db')
    recovered = OperationManager(SQLiteTwinAdapter(tmp_path/'twins.db'), recovered_journal)
    recovered.recover()
    ops = recovered_journal.operations()
    assert len(ops) == 2 and all(o.status == Status.COMMITTED for o in ops)
    assert adapter.get(digital_id(event())).version == 2
    assert all(o.duration_ns is None for o in ops)
    assert all(o.error is None for o in ops)


def test_failed_commit_verification_is_pending(system):
    adapter,journal,manager = system
    adapter.commit = lambda twin, expected: None
    with pytest.raises(RuntimeError, match='receipt missing'):
        manager.submit(event())
    assert journal.operations()[0].commit_unix_ns is None
    assert journal.db.execute('SELECT done FROM inbox').fetchone()[0] == 0


def test_outbox_retry_and_consume(system):
    adapter,journal,manager = system
    class Broker:
        failed = True
        payloads = []
        def send(self,key,body):
            if self.failed:
                raise ConnectionError('offline')
            self.payloads.append(body)
    broker = Broker()
    publisher = DurablePublisher(journal,broker)
    with pytest.raises(ConnectionError):
        publisher.publish(event())
    assert journal.db.execute('SELECT sent FROM outbox').fetchone()[0] == 0
    broker.failed = False
    DurablePublisher(journal,broker).flush()
    publisher.publish(event())
    assert len(broker.payloads) == 1
    accept(broker.payloads[0], manager)
    accept(broker.payloads[0], manager)
    assert len(journal.operations()) == 2


def test_concurrent_duplicate_workers(tmp_path):
    def run(_):
        adapter = SQLiteTwinAdapter(tmp_path/'twins.db')
        manager = OperationManager(adapter,Journal(tmp_path/'journal.db'))
        manager.submit(event())
    # Establish schemas before workers compete.
    SQLiteTwinAdapter(tmp_path/'twins.db')
    Journal(tmp_path/'journal.db')
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run,range(8)))
    assert SQLiteTwinAdapter(tmp_path/'twins.db').get(digital_id(event())).version == 2
    assert len(Journal(tmp_path/'journal.db').operations()) == 2


def test_failed_attach_cancels_dependent_update(system):
    adapter,journal,manager = system
    handler = HANDLERS['Attach']
    original = handler.apply
    def reject(twin,op):
        raise ValueError('association denied')
    handler.apply = reject
    try:
        ops = manager.submit(event())
    finally:
        handler.apply = original
    assert [op.status for op in ops] == [Status.FAILED,Status.CANCELLED]
    assert all(op.commit_unix_ns is None and op.duration_ns is None for op in ops)


def test_detach_waits_for_pending_update_then_blocks_late_observation(system):
    adapter,journal,manager = system
    manager.submit(event())
    original = adapter.commit
    def outage(twin,expected):
        raise ConnectionError('service down')
    adapter.commit = outage
    with pytest.raises(ConnectionError):
        manager.submit(event(2))
    with pytest.raises(ConnectionError):
        manager.submit(event(3,'departure'))
    assert adapter.get(digital_id(event())).association_status == 'attached'
    adapter.commit = original
    manager.recover()
    ops = journal.operations()
    assert [o.operation_type for o in ops] == ['Attach','Update','Update','Detach']
    assert all(o.status == Status.COMMITTED for o in ops)
    assert manager.submit(event(4)) == []


def test_each_commit_predicate_rejects_wrong_state(system):
    adapter,journal,manager = system
    ops = manager.submit(event())
    twin = adapter.get(digital_id(event()))
    assert HANDLERS['Update'].verify(twin,ops[-1])
    invalid = twin.model_copy(deep=True)
    invalid.sequence_number = 0
    assert not HANDLERS['Update'].verify(invalid,ops[-1])
    invalid = twin.model_copy(deep=True)
    invalid.association_status = 'detached'
    assert not HANDLERS['Attach'].verify(invalid,ops[0])
    follow = manager.submit(event(2,'follow_request',subscription={'subscriber_id':'a','variables':['speed'],'lifetime_ms':1000}))[0]
    invalid = adapter.get(twin.dt_id)
    invalid.subscriptions = {}
    assert not HANDLERS['Follow'].verify(invalid,follow)
    transfer = manager.submit(event(3,owner='rsu-east'))[0]
    invalid = adapter.get(twin.dt_id)
    invalid.ownership_version -= 1
    assert not HANDLERS['Transfer'].verify(invalid,transfer)
    detach = manager.submit(event(4,'departure',owner='rsu-east'))[0]
    invalid = adapter.get(twin.dt_id)
    invalid.association_status = 'attached'
    assert not HANDLERS['Detach'].verify(invalid,detach)


def test_follow_due_only_for_explicit_auto_renew_subscription(system):
    adapter,journal,manager = system
    manager.submit(event())
    manager.submit(event(2,'follow_request',subscription={'subscriber_id':'a','variables':['speed'],'lifetime_ms':2000,'auto_renew':True}))
    assert [o.operation_type for o in manager.submit(event(3))] == ['Update']
    ops = manager.submit(event(4))
    assert [o.operation_type for o in ops] == ['Update','Follow']
    assert all(o.status == Status.COMMITTED for o in ops)
    assert adapter.get(digital_id(event())).subscriptions['a']['expires_simulation_ms'] == 6000


def test_ditto_conditional_http_contract():
    from dt.adapter import DittoAdapter
    from dt.models import Twin
    class Response:
        status_code = 200
        headers = {'ETag':'"revision-1"'}
        def json(self):
            return {'attributes':{'perfsync':twin.model_dump()}}
        def raise_for_status(self):
            pass
    class Session:
        calls = []
        response = Response()
        def get(self,url,**kwargs):
            return self.response
        def put(self,url,**kwargs):
            self.calls.append((url,kwargs))
            return self.response
    twin = Twin(dt_id='perfsync:test',physical_id='v',entity_type='vehicle',run_id='test')
    adapter = DittoAdapter('http://ditto',policy_id='perfsync:policy')
    adapter.session = Session()
    adapter.commit(twin,None)
    assert adapter.session.calls[-1][1]['headers'] == {'If-None-Match':'*'}
    read = adapter.get(twin.dt_id)
    read.version = 1
    adapter.commit(read,0)
    call = adapter.session.calls[-1][1]
    assert call['headers'] == {'If-Match':'"revision-1"'}
    assert call['json']['policyId'] == 'perfsync:policy'
    adapter.session.response.status_code = 412
    with pytest.raises(Conflict):
        adapter.commit(read,0)
    adapter.session.response.status_code = 200
    adapter.session.response.headers = {}
    with pytest.raises(RuntimeError,match='ETag required'):
        adapter.get(twin.dt_id)
