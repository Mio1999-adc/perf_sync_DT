import json
import time
from fastapi.testclient import TestClient
from dashboard.backend.main import create_app, Reader, freshness
from dt.adapter import SQLiteTwinAdapter
from dt.models import digital_id
from monitoring.store import Journal
from synchronization.operation_manager.manager import OperationManager
from test_phase3 import event

class BatchAdapter:
    def __init__(self,adapter): self.adapter=adapter
    def get_many(self,ids): return [t for identity in ids if (t:=self.adapter.get(identity)) is not None]


def setup(tmp_path,monkeypatch):
    (tmp_path/'simulation').mkdir()
    (tmp_path/'scenario-input.json').write_text(json.dumps({'run_id':'phase3-unit','rsu_boundary_x':300,'west_owner':'rsu-west','east_owner':'rsu-east'}))
    adapter=SQLiteTwinAdapter(tmp_path/'twins.db')
    journal=Journal(tmp_path/'simulation/journal.sqlite')
    manager=OperationManager(adapter,journal)
    manager.submit(event())
    monkeypatch.setattr(Reader,'adapter',lambda self:BatchAdapter(SQLiteTwinAdapter(tmp_path/'twins.db')))
    return manager,adapter,TestClient(create_app(tmp_path),base_url='http://localhost')

def test_committed_state_not_physical_and_timeline(tmp_path,monkeypatch):
    manager,adapter,client=setup(tmp_path,monkeypatch)
    # A later observed event is durable, but deliberately not executed.
    with manager.journal.transaction(): manager.journal.ingest(event(1))
    data=client.get('/api/entities?comparison=true').json()
    entity=data['entities'][0]
    assert entity['twin']['process_variables']['position_x']==100
    assert entity['physical']['process_variables']['position_x']==101
    assert entity['freshness']['sequence_gap']==1 and entity['freshness']['simulation_gap_ms']==1000
    assert entity['twin']['version']==2 and entity['state_source']=='Eclipse Ditto'
    operation=client.get('/api/operations?kind=Attach&status=COMMITTED').json()[0]
    records=client.get('/api/operations/'+operation['operation_id']+'/lifecycle').json()
    assert records[-1]['record']['status']=='COMMITTED'
    assert client.get('/api/entities?limit=201').status_code==422
    assert client.get('/api/entities',headers={'host':'evil.example'}).status_code==403
    assert client.get('/api/entities',headers={'origin':'https://evil.example'}).status_code==403

def test_transfer_detach_and_missing_state(tmp_path,monkeypatch):
    manager,adapter,client=setup(tmp_path,monkeypatch)
    manager.submit(event(1,owner='rsu-east',kind='observation'))
    data=client.get('/api/entities').json()['entities'][0]
    assert data['twin']['owner']=='rsu-east' and data['twin']['ownership_version']==2
    assert data['ownership_consistency']=='consistent'
    manager.submit(event(2,'departure','rsu-east'))
    data=client.get('/api/entities').json()['entities'][0]
    assert data['twin']['association_status']=='detached' and data['twin']['owner'] is None
    adapter.db.execute('DELETE FROM twins')
    assert client.get('/api/entities').json()['entities']==[]

def test_ditto_unavailable_is_503_not_physical_fallback(tmp_path,monkeypatch):
    _,_,client=setup(tmp_path,monkeypatch)
    monkeypatch.setattr(Reader,'adapter',lambda self: (_ for _ in ()).throw(ConnectionError()))
    response=client.get('/api/entities?comparison=true')
    assert response.status_code==503
    assert 'position_x' not in response.text

def test_timeline_handles_journal_file_before_consumer_schema(tmp_path):
    import sqlite3
    (tmp_path/'scenario-input.json').write_text(json.dumps({'run_id':'startup','rsu_boundary_x':300,'west_owner':'rsu-west','east_owner':'rsu-east'}))
    sync=tmp_path/'simulation';sync.mkdir()
    sqlite3.connect(sync/'journal.sqlite').close()
    reader=Reader(tmp_path,False)
    assert reader.operations()==[]
    assert reader.query('SELECT ordinal,body FROM lifecycle')==[]

def test_sse_reconnect_replaces_bounded_page(tmp_path,monkeypatch):
    import asyncio
    manager,adapter,_=setup(tmp_path,monkeypatch)
    app=create_app(tmp_path)
    endpoint=next(route.endpoint for route in app.routes if getattr(route,'path',None)=='/api/stream')
    class Connected:
        async def is_disconnected(self): return False
    async def first_snapshot():
        response=await endpoint(Connected(),offset=0,limit=100,comparison=True)
        iterator=response.body_iterator
        first=await iterator.__anext__()
        await iterator.aclose()
        return json.loads(first.split('data: ',1)[1])
    first=asyncio.run(first_snapshot())
    second=asyncio.run(first_snapshot())
    assert [e['twin']['dt_id'] for e in first['entities']]==[e['twin']['dt_id'] for e in second['entities']]
    assert len(second['entities'])==1
    monkeypatch.setattr(Reader,'adapter',lambda self: (_ for _ in ()).throw(ConnectionError()))
    async def unavailable():
        response=await endpoint(Connected(),offset=0,limit=100,comparison=True)
        iterator=response.body_iterator
        first=await iterator.__anext__()
        await iterator.aclose()
        return first
    assert 'event: unavailable' in asyncio.run(unavailable())
