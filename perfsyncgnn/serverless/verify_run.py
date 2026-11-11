"""Verify real saved run evidence; never generates synthetic observations or success."""
import argparse
import json
from pathlib import Path
import sqlite3
import time
import requests
from dt.adapter import DittoAdapter
from perfsyncgnn.launcher.config import credentials

KINDS={'Attach','Update','Follow','Transfer','Detach'}
def watch(output,url,seconds):
    seen={};deadline=time.monotonic()+seconds
    with (output/'dashboard-observations.jsonl').open('a') as stream:
        while time.monotonic()<deadline:
            try:
                response=requests.get(url+'/api/entities',params={'comparison':'true'},timeout=15)
                response.raise_for_status()
                payload=response.json()
                for entity in payload['entities']:
                    twin=entity['twin'];key=(twin['version'],twin['last_operation'])
                    if seen.get(twin['dt_id'])==key:continue
                    seen[twin['dt_id']]=key
                    stream.write(json.dumps({'observed_unix_ns':time.time_ns(),'entity':entity})+'\n')
                stream.flush()
            except requests.RequestException:
                if (output/'launcher/state.json').exists() and json.loads((output/'launcher/state.json').read_text()).get('status') in {'COMPLETED','FAILED','CANCELLED'}:break
            time.sleep(1)

def verify(output):
    state=json.loads((output/'launcher/state.json').read_text())
    assert state['status']=='COMPLETED', 'launcher did not complete'
    assert state['config']['executor']=='openwhisk' and state['config']['backend']=='ditto'
    sync=output/('consumer' if state['config']['broker'] else 'simulation')
    with sqlite3.connect(sync/'journal.sqlite') as db:
        operations=[json.loads(row[0]) for row in db.execute('SELECT body FROM operations ORDER BY rowid')]
        assert db.execute('SELECT COUNT(*) FROM inbox WHERE done=0').fetchone()[0]==0
    assert operations and {o['operation_type'] for o in operations}==KINDS
    assert all(o['status']=='COMMITTED' and o['activation_id'] and o['duration_ns'] is not None for o in operations)
    assert all(o['request_unix_ns']<=o['validated_unix_ns']<=o['queued_unix_ns']<=o['start_unix_ns']<=o['commit_unix_ns'] for o in operations)
    env=credentials();adapter=DittoAdapter(env['DITTO_URL'],env['DITTO_USERNAME'],env['DITTO_PASSWORD'],env['DITTO_POLICY_ID'])
    identities=sorted({o['dt_id'] for o in operations});twins={}
    for start in range(0,len(identities),200):twins.update((t.dt_id,t) for t in adapter.get_many(identities[start:start+200]))
    for op in operations:
        receipt=twins[op['dt_id']].applied_operations[op['operation_id']]
        assert receipt['event_fingerprint']==op['payload']['event_fingerprint'] and receipt['version_after']==op['version_after']
    local_ids=set();remote_ids=set();names=set()
    for filename,ids in [('traces.jsonl',local_ids),('remote-traces.jsonl',remote_ids)]:
        with (sync/filename).open() as stream:
            for line in stream:
                span=json.loads(line);ids.add(span['context']['trace_id']);names.add(span['name'])
    source_ids=set()
    source_dir=output/('producer' if state['config']['broker'] else 'simulation')
    with (source_dir/'traces.jsonl').open() as source:
        for line in source:
            span=json.loads(line)
            if span['name']=='sumo.event':source_ids.add(span['context']['trace_id'])
    assert source_ids & local_ids & remote_ids, 'no trace links actual SUMO emission to manager and action'
    assert local_ids & remote_ids and {'synchronization','openwhisk.invoke','function.execute','ditto.conditional_write','ditto.commit_validation'}<=names
    metrics=(sync/'metrics.prom').read_text()
    assert 'perfsync_sync_seconds_count' in metrics and 'perfsync_function_seconds_count' in metrics
    assert (sync/'resources.jsonl').stat().st_size>0
    observations=output/'dashboard-observations.jsonl';observed={}
    if observations.exists():
        for line in observations.open():
            e=json.loads(line)['entity'];t=e['twin']
            assert e['state_source']=='Eclipse Ditto'
            if t['entity_type']=='vehicle':observed.setdefault(t['physical_id'],[]).append(t)
    moved=[identity for identity,states in observed.items() if len({(t['process_variables'].get('position_x'),t['process_variables'].get('position_y')) for t in states})>1]
    transferred=[identity for identity,states in observed.items() if len({t['owner'] for t in states if t['owner']})>=2]
    detached=[identity for identity,states in observed.items() if any(t['association_status']=='detached' for t in states)]
    assert moved and transferred and detached, 'missing live dashboard transition evidence'
    report={'verified':True,'run_id':state['run_id'],'operations':len(operations),'types':{k:sum(o['operation_type']==k for o in operations) for k in sorted(KINDS)},'entities':len(twins),'dashboard_vehicles_moved':moved,'dashboard_vehicles_transferred':transferred,'dashboard_vehicles_detached':detached,'correlated_trace_ids':len(local_ids & remote_ids)}
    (output/'verification.json').write_text(json.dumps(report,indent=2))
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--watch',action='store_true');p.add_argument('--url',default='http://127.0.0.1:8090');p.add_argument('--seconds',type=float,default=900)
    args=p.parse_args()
    if args.watch:watch(args.output,args.url,args.seconds)
    else:print(json.dumps(verify(args.output),indent=2))
