"""Loopback-only viewer. Twin properties are fetched from Ditto, never the inbox."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import time
import threading
from urllib.parse import quote
import xml.etree.ElementTree as ET
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from dt.adapter import DittoAdapter
from monitoring.telemetry import resources

TERMINAL = {'COMMITTED','FAILED','CANCELLED','SUPERSEDED'}

def freshness(twin, physical, commit_ns):
    gap = max(0, physical['sequence_number']-twin.sequence_number) if physical else None
    return {'sequence_gap':gap, 'simulation_gap_ms':max(0, physical['source_timestamp_ms']-twin.source_timestamp_ms) if physical else None,
            'commit_age_seconds':max(0,(time.time_ns()-commit_ns)/1e9) if commit_ns else None,
            'state':'unavailable' if gap is None else 'current' if gap == 0 else 'lagging'}

class Reader:
    def __init__(self, output, broker):
        self.output = Path(output)
        self.sync = self.output/('consumer' if broker else 'simulation')
        self.producer = self.output/('producer' if broker else 'simulation')
        self.run = json.loads((self.output/'scenario-input.json').read_text())
        self.summary = {}
        self.summary_at = 0
        self.summary_lock = threading.Lock()
    def connection(self):
        path = self.sync/'journal.sqlite'
        if not path.exists(): return None
        return sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True, timeout=3)
    def query(self, sql, params=()):
        db = self.connection()
        if db is None: return []
        try:
            try:return db.execute(sql, params).fetchall()
            except sqlite3.OperationalError as exc:
                # The launcher starts GUI 2 before the consumer. SQLite creates
                # the journal file before its schema; empty startup reads are a
                # valid disconnected/initial state, not an API failure.
                if 'no such table:' in str(exc):return []
                raise
        finally: db.close()
    def operations(self, entity=None, kind=None, status=None, offset=0, limit=100):
        where, args = [], []
        for field, value in [('event.entity_id',entity),('operation_type',kind),('status',status)]:
            if value:
                where.append("json_extract(body, '$."+field+"')=?")
                args.append(value)
        sql = 'SELECT body FROM operations'+ (' WHERE '+' AND '.join(where) if where else '')+' ORDER BY rowid DESC LIMIT ? OFFSET ?'
        return [json.loads(r[0]) for r in self.query(sql, tuple(args+[limit,offset]))]
    def physical(self, entity):
        # Indexed SQL bounded result; only separately labelled comparison data.
        dbpath = self.producer/'journal.sqlite'
        if not dbpath.exists(): return None
        with sqlite3.connect('file:'+str(dbpath)+'?mode=ro',uri=True) as db:
            table = 'outbox' if self.producer != self.sync else 'inbox'
            row = db.execute(f"SELECT body FROM {table} WHERE json_extract(body,'$.entity_id')=? AND json_extract(body,'$.event_type') IN ('observation','insertion','departure','removal') ORDER BY rowid DESC LIMIT 1",(entity,)).fetchone()
        return json.loads(row[0]) if row else None
    def simulation_now(self):
        path=self.producer/'journal.sqlite'
        if not path.exists(): return None
        table='outbox' if self.producer != self.sync else 'inbox'
        with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True,timeout=3) as db:
            row=db.execute(f"SELECT MAX(json_extract(body,'$.simulation_time_ms')) FROM {table}").fetchone()
        return row[0] if row else None
    def adapter(self):
        return DittoAdapter(os.environ['DITTO_URL'],os.environ['DITTO_USERNAME'],os.environ['DITTO_PASSWORD'],os.environ['DITTO_POLICY_ID'])
    def entities(self, offset=0, limit=100, comparison=False):
        # Journal supplies identities only. Every rendered state is a current Ditto GET.
        rows = self.query("SELECT DISTINCT json_extract(body,'$.dt_id') FROM operations ORDER BY 1 LIMIT ? OFFSET ?", (limit,offset))
        adapter = self.adapter()
        result = []
        for twin in adapter.get_many([row[0] for row in rows]):
            dt_id = twin.dt_id
            latest = self.query("SELECT body FROM operations WHERE json_extract(body,'$.dt_id')=? AND json_extract(body,'$.status')='COMMITTED' ORDER BY rowid DESC LIMIT 1",(dt_id,))
            operation = json.loads(latest[0][0]) if latest else None
            commit = operation.get('commit_unix_ns') if operation and twin.last_operation == operation['operation_id'] else None
            physical = self.physical(twin.physical_id) if comparison else None
            pending = self.query("SELECT COUNT(*) FROM operations WHERE json_extract(body,'$.dt_id')=? AND json_extract(body,'$.status') NOT IN ('COMMITTED','FAILED','CANCELLED','SUPERSEDED')",(dt_id,))[0][0]
            result.append({'twin':twin.model_dump(exclude={'applied_operations'}), 'physical':physical, 'last_commit_unix_ns':commit,
                           'pending_operations':pending, 'ownership_consistency': ('consistent' if twin.applied_operations.get(twin.last_operation,{}).get('version_after') == twin.version and twin.applied_operations.get(twin.last_operation,{}).get('owner') == twin.owner and ((twin.association_status == 'attached' and twin.owner in {self.run['west_owner'],self.run['east_owner']}) or (twin.association_status == 'detached' and twin.owner is None)) else 'unverified_or_inconsistent'), 'freshness':freshness(twin,physical,commit), 'state_source':'Eclipse Ditto'})
        return result
    def overview(self):
        with self.summary_lock:
            if time.monotonic()-self.summary_at > 5:
                counts = {'total_entities':0,'attached':0,'detached':0,'active_subscriptions':0,'owners':{},'latest_version':0}
                adapter = self.adapter()
                offset = 0
                simulation_now = self.simulation_now()
                counts['subscription_clock_simulation_ms'] = simulation_now
                while True:
                    ids = self.query("SELECT DISTINCT json_extract(body,'$.dt_id') FROM operations ORDER BY 1 LIMIT 200 OFFSET ?",(offset,))
                    if not ids: break
                    for twin in adapter.get_many([row[0] for row in ids]):
                        counts['total_entities'] += 1
                        counts['attached'] += twin.association_status == 'attached'
                        counts['detached'] += twin.association_status == 'detached'
                        counts['latest_version'] = max(counts['latest_version'],twin.version)
                        counts['active_subscriptions'] += sum(sub.get('expires_simulation_ms',0) > (simulation_now if simulation_now is not None else twin.source_timestamp_ms) for sub in twin.subscriptions.values())
                        if twin.owner: counts['owners'][twin.owner] = counts['owners'].get(twin.owner,0)+1
                    offset += 200
                self.summary = counts
                self.summary_at = time.monotonic()
        rows = self.query("SELECT json_extract(body,'$.status'),count(*) FROM operations GROUP BY 1")
        # Clearly labelled operation summary; entity totals are page-scoped in client.
        latest = self.query("SELECT MAX(json_extract(body,'$.commit_unix_ns')) FROM operations WHERE json_extract(body,'$.status')='COMMITTED'")
        return dict(self.summary, operation_status_counts=dict(rows),run_id=self.run['run_id'],state_source='Eclipse Ditto',entity_count_scope='current run, refreshed every five seconds', latest_commit_unix_ns=latest[0][0] if latest else None)
    def geometry(self):
        path = self.producer/'scenario/intersection.net.xml'
        if not path.exists():
            files = list((self.producer/'scenario').glob('*.net.xml'))
            if not files: return {'lanes':[], 'boundary_x':self.run['rsu_boundary_x'], 'junctions': []}
            path = files[0]
        net = ET.parse(path).getroot()
        return {'lanes':[{'id':lane.attrib['id'],'shape':[[float(v) for v in point.split(',')] for point in lane.attrib['shape'].split()]} for lane in net.findall('.//lane')], 'boundary_x':self.run['rsu_boundary_x'], 'junctions':[{'id':j.attrib['id'],'x':float(j.attrib['x']),'y':float(j.attrib['y'])} for j in net.findall('junction') if j.attrib.get('type') == 'traffic_light']}

def create_app(output, broker=False, refresh=1.0):
    app = FastAPI(title='PerfSyncGNN committed digital twins', docs_url=None, redoc_url=None)
    reader = Reader(output,broker)
    @app.middleware('http')
    async def protect(request: Request, call_next):
        host = request.headers.get('host','').split(':')[0]
        origin = request.headers.get('origin')
        if host not in {'127.0.0.1','localhost'} or (origin and origin not in {'http://'+request.headers.get('host','')}):
            return Response('Local viewer: invalid host or origin',status_code=403)
        response = await call_next(request)
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Cache-Control']='no-store'
        return response
    @app.get('/api/health')
    def health():
        try:
            adapter=reader.adapter()
            r=adapter.session.get(os.environ['DITTO_URL'].rstrip('/')+'/api/2/policies/'+quote(os.environ['DITTO_POLICY_ID'],safe=''),timeout=3)
            r.raise_for_status()
            return {'status':'ready','state_source':'Eclipse Ditto'}
        except Exception:
            raise HTTPException(503,'Ditto unavailable')
    @app.get('/api/entities')
    def entities(offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=200),comparison:bool=False):
        try: return {'entities':reader.entities(offset,limit,comparison),'offset':offset,'limit':limit,'overview':reader.overview()}
        except Exception: raise HTTPException(503,'Committed Ditto state unavailable')
    @app.get('/api/geometry')
    def geometry(): return reader.geometry()
    @app.get('/api/operations')
    def operations(entity:str=None,kind:str=None,status:str=None,offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=200)):
        return reader.operations(entity,kind,status,offset,limit)
    @app.get('/api/operations/{operation_id}/lifecycle')
    def lifecycle(operation_id:str,offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=200)):
        return [dict(ordinal=r[0],record=json.loads(r[1])) for r in reader.query("SELECT ordinal,body FROM lifecycle WHERE json_extract(body,'$.operation_id')=? ORDER BY ordinal LIMIT ? OFFSET ?",(operation_id,limit,offset))]
    @app.get('/api/resources')
    def resource_metrics(): return resources()
    @app.get('/metrics')
    def metrics():
        path=reader.sync/'metrics.prom'
        return Response(path.read_text() if path.exists() else '# No executor metrics observed yet\n',media_type='text/plain')
    @app.get('/api/stream')
    async def stream(request:Request,offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=200),comparison:bool=False):
        async def events():
            # Each tick is a complete bounded page snapshot, so reconnect cannot duplicate.
            while not await request.is_disconnected():
                try:
                    data = await asyncio.to_thread(reader.entities,offset,limit,comparison)
                    summary = await asyncio.to_thread(reader.overview)
                    yield 'event: snapshot\ndata: '+json.dumps({'entities':data,'overview':summary,'observed_unix_ns':time.time_ns()})+'\n\n'
                except Exception:
                    yield 'event: unavailable\ndata: {"message":"Ditto disconnected; last state is stale"}\n\n'
                await asyncio.sleep(refresh)
        return StreamingResponse(events(),media_type='text/event-stream',headers={'X-Accel-Buffering':'no'})
    dist=Path(__file__).resolve().parents[1]/'frontend/dist'
    if dist.exists(): app.mount('/',StaticFiles(directory=dist,html=True),name='frontend')
    return app

if __name__ == '__main__':
    import uvicorn
    p=argparse.ArgumentParser()
    p.add_argument('--output',required=True)
    p.add_argument('--broker',action='store_true')
    p.add_argument('--port',type=int,default=8090)
    p.add_argument('--refresh',type=float,default=1)
    args=p.parse_args()
    if args.refresh < .25: p.error('refresh must be at least .25 seconds')
    uvicorn.run(create_app(args.output,args.broker,args.refresh),host='127.0.0.1',port=args.port,access_log=False,timeout_graceful_shutdown=2)
