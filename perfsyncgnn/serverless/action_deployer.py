"""Deploy five private actions using official OpenWhisk REST API."""
import argparse
import json
from pathlib import Path
from perfsyncgnn.launcher.config import credentials
from .config import environment, ROOT
from .openwhisk_executor import OpenWhiskClient

KINDS=('Attach','Update','Follow','Transfer','Detach')
def deploy(image='perfsync-action:phase3-v2',ditto_url=None):
    env=environment(credentials())
    client=OpenWhiskClient(env['OPENWHISK_API_URL'],env['OPENWHISK_AUTH'],env.get('OPENWHISK_CA_BUNDLE'))
    client.health()
    response=client.session.put(client.endpoint('packages','perfsync'),params={'overwrite':'true'},json={'publish':False},timeout=15)
    response.raise_for_status()
    probe={'exec':{'kind':'blackbox','image':image,'code':'def main(args):\n import sys\n sys.path.insert(0,"/opt/perfsync")\n import pydantic\n from dt.models import Twin\n from synchronization.executor import LocalExecutor\n from simulation.event_generator.schemas import TrafficEvent\n from opentelemetry.sdk.trace import TracerProvider\n return {"python":sys.version.split()[0],"pydantic":pydantic.__version__,"runtime":"perfsync","project_imports":True}\n','main':'main'},'limits':{'timeout':60000,'memory':256,'logs':1}}
    response=client.session.put(client.endpoint('actions','perfsync-python-probe'),params={'overwrite':'true'},json=probe,timeout=30)
    response.raise_for_status()
    source=(ROOT/'perfsyncgnn/serverless/action.py').read_text()
    for kind in KINDS:
        config={'operation_type':kind,'url':ditto_url or env.get('DITTO_ACTION_URL',env['DITTO_URL']),
                'username':env['DITTO_USERNAME'],'password':env['DITTO_PASSWORD'],'policy_id':env['DITTO_POLICY_ID'],'timeout':30}
        body={'namespace':'_','name':kind.lower(),'publish':False,
              'exec':{'kind':'blackbox','image':image,'code':source,'main':'main'},
              'parameters':[{'key':'__perfsync_config','value':config}],
              'limits':{'timeout':120000,'memory':256,'logs':1}}
        response=client.session.put(client.endpoint('actions','perfsync/'+kind.lower()),params={'overwrite':'true'},json=body,timeout=30)
        response.raise_for_status()
        print('Deployed perfsync/'+kind.lower())

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--image',default='perfsync-action:phase3-v2')
    p.add_argument('--ditto-url',help='Ditto address reachable from action pods; no embedded credentials')
    args=p.parse_args()
    deploy(args.image,args.ditto_url)
