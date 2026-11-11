import json
import subprocess
import time
from .config import ROOT, COMPOSE, OVERRIDE, PROJECT, BROKERS
from .health_checks import port_open, wait_ready, ditto, mqtt, kafka


class DockerManager:
    def __init__(self, env, timeout=180, stop=lambda:False, minimal=False):
        self.env=env
        self.minimal=minimal
        self.timeout=timeout
        self.stop_requested=stop
        self.compose=['docker','compose','-p',PROJECT,'-f',str(COMPOSE)]
        if OVERRIDE.exists():
            self.compose+=['-f',str(OVERRIDE)]

    def execute(self, args):
        starting = 'start' in args or 'restart' in args
        stopping = len(args) > 1 and args[1] == 'stop'
        timeout = min(self.timeout, 180 if starting else 90 if stopping else 30)
        process = subprocess.Popen(args,cwd=ROOT,env=self.env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        deadline = time.monotonic()+timeout
        try:
            while True:
                try:
                    stdout,stderr = process.communicate(timeout=.2)
                    break
                except subprocess.TimeoutExpired:
                    if (starting and self.stop_requested()) or time.monotonic()>deadline:
                        from .process_manager import identity, terminate
                        info=identity(process.pid)
                        terminate(info,.5)
                        process.communicate(timeout=3)
                        if starting and self.stop_requested(): raise InterruptedError('Docker startup interrupted')
                        raise TimeoutError('Docker command deadline exceeded; inspect retained service state')
            if process.returncode:
                raise RuntimeError('Docker command failed; check Docker daemon, retained containers, and existing compose/auth configuration')
            return stdout
        finally:
            if process.poll() is None:
                from .process_manager import identity, terminate
                terminate(identity(process.pid),.5)
                process.wait(timeout=3)

    def inspect(self, name):
        result=subprocess.run(['docker','inspect',name],capture_output=True,text=True,timeout=5)
        if result.returncode:
            return None
        return json.loads(result.stdout)[0]

    def discover(self, services):
        self.execute(['docker','info','--format','{{.ServerVersion}}'])
        names=[]
        if 'ditto' in services:
            service_names=self.execute(self.compose+['config','--services']).split()
            if self.minimal:
                service_names=[name for name in service_names if name in {'mongodb','policies','things','gateway','nginx','swagger-ui','ditto-ui'}]
            ids=self.execute(['docker','ps','-aq','--filter',f'label=com.docker.compose.project={PROJECT}']).split()
            found={}
            for container_id in ids:
                container=self.inspect(container_id)
                label=(container['Config'].get('Labels') or {}).get('com.docker.compose.service')
                if label in service_names:
                    if label in found:
                        raise RuntimeError('ambiguous retained Compose service; refusing to recreate or select a replica')
                    found[label]=container
            if set(service_names)-set(found):
                raise RuntimeError('Ditto containers missing; use the existing setup guide to create them explicitly. Launcher never calls up or recreates services.')
            names += [found[n] for n in service_names]
        for service in services:
            if service in BROKERS:
                container=self.inspect(BROKERS[service])
                if not container:
                    raise RuntimeError(f'existing {service} container missing; provision it with the Phase 2 guide')
                names.append(container)
        return names

    def start(self, services, owned, save):
        containers=self.discover(services)
        stopped=[c for c in containers if not c['State']['Running']]
        for container in stopped:
            for bindings in (container.get('HostConfig',{}).get('PortBindings') or {}).values():
                for binding in bindings or []:
                    port=binding.get('HostPort')
                    if port and port_open(int(port)):
                        raise RuntimeError(f'port {port} is occupied while selected container is stopped')
        # Persist intent/immutable IDs before mutation for failure cleanup.
        owned.extend({'id':c['Id'],'name':c['Name'].lstrip('/')} for c in stopped)
        save()
        compose_stopped=[c for c in stopped if (c['Config'].get('Labels') or {}).get('com.docker.compose.project')==PROJECT]
        if compose_stopped:
            self.execute(self.compose+['start']+[c['Config']['Labels']['com.docker.compose.service'] for c in compose_stopped])
        for container in stopped:
            if container not in compose_stopped:
                self.execute(['docker','start',container['Id']])
        probe_start=time.monotonic()
        policies=next((item for item in owned if item['name']==PROJECT+'-policies-1'),None)
        def check_ditto():
            if ditto(self.env):
                return True
            # Existing upstream DNS-bootstrap failure: recover only our newly started
            # policies container, once, after observing the specific stalled diagnostic.
            if policies and not policies.get('bootstrap_restarted') and time.monotonic()-probe_start>20:
                diagnostic=self.execute(['docker','logs','--tail','30',policies['id']])
                if 'Discovered [1] contact points' in diagnostic and 'required [2]' in diagnostic:
                    policies['bootstrap_restarted']=True
                    save()
                    self.execute(['docker','restart','--time','10',policies['id']])
            return False
        for service in services:
            check={'ditto':check_ditto,'mqtt':mqtt,'kafka':kafka}[service]
            wait_ready(check,self.timeout,self.stop_requested)
        # TCP alone is not MongoDB readiness; issue the actual database ping.
        if 'ditto' in services:
            mongo=next(c for c in containers if (c['Config'].get('Labels') or {}).get('com.docker.compose.service')=='mongodb')
            from pymongo import MongoClient
            bindings = mongo.get('HostConfig',{}).get('PortBindings',{}).get('27017/tcp') or []
            if not bindings: raise RuntimeError('MongoDB readiness requires its published loopback port')
            mongo_port = int(bindings[0]['HostPort'])
            def mongo_ready():
                with MongoClient('127.0.0.1',mongo_port,serverSelectionTimeoutMS=2000,connectTimeoutMS=2000,socketTimeoutMS=2000) as client:
                    return client.admin.command('ping').get('ok') == 1
            wait_ready(mongo_ready,self.timeout,self.stop_requested)
        if 'kafka' in services:
            self.execute(['docker','exec',BROKERS['kafka'],'/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092','--create','--if-not-exists','--topic','perfsync.events','--partitions','1','--replication-factor','1'])

    def shutdown(self, owned):
        errors=[]
        selected=[]
        for item in owned:
            try:
                container=self.inspect(item['id'])
                if container and container['Name'].lstrip('/')==item['name'] and container['State']['Running']:
                    selected.append(item)
            except Exception:
                errors.append(item['name'])
        # Stop app/broker containers concurrently; keep Mongo until its clients stop.
        groups=[[item for item in selected if item['name'] != PROJECT+'-mongodb-1'],
                [item for item in selected if item['name'] == PROJECT+'-mongodb-1']]
        for group in groups:
            if not group:
                continue
            try:
                self.execute(['docker','stop','--time','10']+[item['id'] for item in group])
            except Exception:
                errors.extend(item['name'] for item in group)
        return errors
