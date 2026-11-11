"""Unit safety checks plus explicitly opted-in real OpenWhisk/Ditto verification."""
import os
import time
import uuid
import pytest
from dt.adapter import SQLiteTwinAdapter, DittoAdapter
from dt.models import digital_id
from monitoring.store import Journal
from simulation.event_generator.schemas import TrafficEvent
from synchronization.models import Operation, Status
from synchronization.operation_manager.manager import OperationManager
from perfsyncgnn.serverless.openwhisk_executor import OpenWhiskExecutor, OpenWhiskClient

def event(seq=0,kind='insertion',owner='rsu-west',run='phase3-unit',subscription=None):
    return TrafficEvent(event_id=str(seq),run_id=run,entity_id='vehicle-0000',entity_type='vehicle',event_type=kind,
        simulation_time_ms=seq*1000,source_timestamp_ms=seq*1000,sequence_number=seq,source_id=owner,
        process_variables={'speed':10,'position_x':100+seq,'position_y':300} if kind in {'insertion','observation'} else {}, subscription=subscription)

class LyingClient:
    def invoke(self,name,payload):
        op=payload['operation']
        return {'activationId':'unit-not-a-real-activation','response':{'success':True,'result':{'operation_id':op['operation_id'],'dt_id':op['dt_id'],'version_after':1}}}

def test_action_success_without_ditto_receipt_never_commits(tmp_path,monkeypatch):
    monkeypatch.setattr('perfsyncgnn.serverless.openwhisk_executor.time.sleep',lambda _:None)
    adapter=SQLiteTwinAdapter(tmp_path/'twins.db');journal=Journal(tmp_path/'journal.db')
    manager=OperationManager(adapter,journal,OpenWhiskExecutor(adapter,journal,LyingClient()))
    with pytest.raises(RuntimeError,match='receipt missing'):
        manager.submit(event())
    op=journal.operations()[0]
    assert op.status == Status.COMMITTING and op.commit_unix_ns is None and op.duration_ns is None
    assert op.retry_count == 2
    assert journal.db.execute('SELECT done FROM inbox').fetchone()[0] == 0

def test_trace_context_does_not_change_event_identity():
    original=event(); traced=original.model_copy(update={'trace_context':{'traceparent':'00-'+'1'*32+'-'+'2'*16+'-01'}})
    assert original.fingerprint == traced.fingerprint
    assert original.deduplication_id == traced.deduplication_id

@pytest.mark.integration
def test_real_openwhisk_five_operations_and_idempotency(tmp_path):
    if os.getenv('PERFSYNC_TEST_OPENWHISK') != '1': pytest.skip('PERFSYNC_TEST_OPENWHISK=1 required for real serverless test')
    from perfsyncgnn.launcher.config import credentials
    env=credentials()
    adapter=DittoAdapter(env['DITTO_URL'],env['DITTO_USERNAME'],env['DITTO_PASSWORD'],env['DITTO_POLICY_ID'])
    from monitoring.telemetry import configure
    configure(tmp_path/'traces.jsonl')
    journal=Journal(tmp_path/'journal.db');executor=OpenWhiskExecutor(adapter,journal)
    manager=OperationManager(adapter,journal,executor)
    run='phase3-integration-'+uuid.uuid4().hex
    for seq,kind,owner,subscription in [(0,'insertion','rsu-west',None),(1,'follow_request','rsu-west',{'subscriber_id':'test','variables':['speed'],'lifetime_ms':10000}),(2,'observation','rsu-west',None),(3,'observation','rsu-east',None),(4,'departure','rsu-east',None)]:
        ops=manager.submit(event(seq,kind,owner,run,subscription))
        assert ops and all(op.status == Status.COMMITTED and op.activation_id for op in ops)
        for op in ops:
            assert op.request_unix_ns <= op.validated_unix_ns <= op.queued_unix_ns <= op.start_unix_ns <= op.commit_unix_ns
            assert op.duration_ns is not None and op.duration_ns >= 0
            assert op.trace_context.get('traceparent')
            assert op.function_metadata['function_start_unix_ns'] <= op.function_metadata['function_end_unix_ns']
    operations=journal.operations()
    assert {op.operation_type for op in operations} == {'Attach','Follow','Update','Transfer','Detach'}
    last=operations[-1]; before=adapter.get(last.dt_id)
    activation=executor.client.invoke('perfsync/detach',{'operation':last.model_dump(mode='json'),'expected_version':before.version,'current_owner':before.owner,'trace_context':last.trace_context})
    assert activation['response']['success']
    assert adapter.get(last.dt_id).version == before.version
    assert before.association_status == 'detached' and before.owner is None
    assert (tmp_path/'activations.jsonl').exists() and (tmp_path/'remote-traces.jsonl').exists()
    import json
    remote=[json.loads(line) for line in (tmp_path/'remote-traces.jsonl').read_text().splitlines()]
    trace_ids={op.trace_context['traceparent'].split('-')[1] for op in operations}
    assert any(span['context']['trace_id'].removeprefix('0x') in trace_ids for span in remote)
    assert any(span['name']=='ditto.conditional_write' for span in remote)

@pytest.mark.integration
def test_real_openwhisk_failed_precondition_has_no_success_label(tmp_path):
    if os.getenv('PERFSYNC_TEST_OPENWHISK') != '1': pytest.skip('real serverless test not opted in')
    from perfsyncgnn.launcher.config import credentials
    env=credentials();adapter=DittoAdapter(env['DITTO_URL'],env['DITTO_USERNAME'],env['DITTO_PASSWORD'],env['DITTO_POLICY_ID'])
    journal=Journal(tmp_path/'journal.db');manager=OperationManager(adapter,journal,OpenWhiskExecutor(adapter,journal))
    run='phase3-failure-'+uuid.uuid4().hex
    manager.submit(event(run=run))
    ops=manager.submit(event(1,'follow_request',run=run,subscription={'subscriber_id':'bad','variables':['unavailable_field'],'lifetime_ms':1000}))
    assert len(ops)==1 and ops[0].status==Status.FAILED
    assert ops[0].commit_unix_ns is None and ops[0].duration_ns is None

def test_launcher_serverless_dashboard_configuration():
    from perfsyncgnn.launcher.cli import parser, child_args
    from perfsyncgnn.launcher.config import LaunchConfig
    config=LaunchConfig.from_args(parser().parse_args(['run','--live','--executor','openwhisk','--dashboard','--dashboard-port','8091']))
    assert config.executor=='openwhisk' and config.dashboard and config.dashboard_port==8091
    for flags in (['run','--executor','openwhisk'],['run','--dashboard'],['run','--live','--dashboard-port','80'],['run','--live','--dashboard-refresh','0.1']):
        with pytest.raises(ValueError): LaunchConfig.from_args(parser().parse_args(flags))

@pytest.mark.integration
def test_real_openwhisk_response_loss_retries_idempotently(tmp_path):
    if os.getenv('PERFSYNC_TEST_OPENWHISK') != '1': pytest.skip('real serverless test not opted in')
    import requests
    from perfsyncgnn.launcher.config import credentials
    env=credentials();adapter=DittoAdapter(env['DITTO_URL'],env['DITTO_USERNAME'],env['DITTO_PASSWORD'],env['DITTO_POLICY_ID'])
    class LoseFirstResponse:
        def __init__(self):self.real=OpenWhiskClient();self.lost=False
        def invoke(self,name,payload):
            activation=self.real.invoke(name,payload)
            if not self.lost:
                self.lost=True
                raise requests.ConnectionError('test: discard real committed activation response')
            return activation
    journal=Journal(tmp_path/'journal.db');client=LoseFirstResponse()
    manager=OperationManager(adapter,journal,OpenWhiskExecutor(adapter,journal,client))
    run='phase3-retry-'+uuid.uuid4().hex
    ops=manager.submit(event(run=run))
    assert all(op.status==Status.COMMITTED for op in ops)
    assert sum(op.retry_count for op in ops)==1
    twin=adapter.get(ops[-1].dt_id)
    assert twin.version==2 and len(twin.applied_operations)==2

def test_failed_blocking_activation_keeps_its_id():
    import json,requests
    from unittest.mock import Mock
    client=OpenWhiskClient(url='http://unit.invalid',auth='unit:private')
    response=requests.Response();response.status_code=502
    body={'activationId':'unit-failed-activation','response':{'success':False,'status':'action developer error'}}
    response._content=json.dumps(body).encode()
    client.session.post=Mock(return_value=response)
    assert client.invoke('perfsync/attach',{})==body

def test_dispatch_read_timeout_is_retried(tmp_path):
    import requests
    class TimeoutAdapter:
        def get(self,identity):raise requests.ReadTimeout('unit: dispatch read timeout')
    journal=Journal(tmp_path/'journal.db')
    executor=OpenWhiskExecutor(TimeoutAdapter(),journal,LyingClient())
    e=event();op=Operation(operation_id='dispatch-read',operation_type='Attach',event=e.model_dump(),dt_id=digital_id(e),payload={'event_fingerprint':e.fingerprint},clock_session=executor.clock_session)
    with pytest.raises(requests.ReadTimeout):executor.execute(op)
    assert op.retry_count==2 and op.status==Status.RUNNING and op.commit_unix_ns is None
