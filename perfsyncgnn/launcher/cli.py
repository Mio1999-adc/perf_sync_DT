"""Owns a single supervisor and its child groups; all mutations are lock-protected."""
import argparse
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from . import config as settings
from .config import LaunchConfig, ROOT, credentials, require_dependencies
from .docker_manager import DockerManager
from .health_checks import wait_ready, pipeline_drained, operation_summary, ditto, mqtt, kafka
from .log_manager import write_json, read_json, record, redact
from .process_manager import identity, alive, lock, spawn, terminate


def parser():
    p=argparse.ArgumentParser(description='Phase 2 process controller; existing containers are reused, never recreated.')
    subs=p.add_subparsers(dest='command',required=True,metavar='{start,stop,restart,status,logs,test,run,gui}')
    for command in ['start','run','gui','restart','test']:
        sub=subs.add_parser(command,help={'start':'start and supervise a finite simulation','run':'alias for start','gui':'same connected simulation, using SUMO-GUI','restart':'stop then run with prior settings and a fresh output','test':'run regression tests; --live starts existing services for live tests'}[command])
        sub.add_argument('--backend',choices=['sqlite','ditto'])
        sub.add_argument('--executor',choices=['local','openwhisk'])
        sub.add_argument('--dashboard',action='store_true',default=None,help='serve committed Ditto GUI 2 on loopback')
        sub.add_argument('--dashboard-port',type=int)
        sub.add_argument('--dashboard-refresh',type=float,help='Ditto snapshot interval, minimum .25 seconds')
        sub.add_argument('--monitoring',action='store_true',default=None,help='start/reuse this project’s Prometheus and Grafana services')
        sub.add_argument('--keep-dashboard',action='store_true',default=None,help='keep GUI 2 and monitoring available after the finite simulation until stop')
        sub.add_argument('--live',action='store_true',help='use Ditto; test also checks MQTT and Kafka')
        sub.add_argument('--follow',action='store_true',default=None)
        sub.add_argument('--gui',action='store_true',default=None)
        sub.add_argument('--scenario',choices=['intersection'])
        sub.add_argument('--config',dest='config_file',help='existing Scenario JSON; defaults to configs/intersection.json')
        sub.add_argument('--seed',type=int)
        sub.add_argument('--output',help='new/empty run directory; default is unique per run')
        sub.add_argument('--timeout',type=float,help='readiness / pipeline drain deadline in seconds (default 300)')
        sub.add_argument('--delay',type=float,help='connected GUI step delay in ms (default 100)')
        sub.add_argument('--broker',choices=['mqtt','kafka'],help='optional producer/consumer pipeline; otherwise direct synchronization')
        sub.add_argument('--services',help='existing infra to start: ditto,mqtt,kafka or all')
        sub.add_argument('--infra-only',action='store_true',default=None,help='hold requested services until stop; no simulation')
        sub.add_argument('--detach',action='store_true',help='background the supervisor, not an independent GUI replay')
    subs.add_parser('stop',help='stop owned processes and containers started by this supervisor')
    subs.add_parser('status',help='show identity-checked processes, last TraCI observation, and service probes')
    log=subs.add_parser('logs',help='show launcher/child or selected existing Docker service logs')
    log.add_argument('--service',choices=['launcher','sumo','sync','tests','ditto','mongodb','mqtt','kafka','dashboard','monitoring','openwhisk-api','openwhisk-cluster'],default='launcher')
    log.add_argument('--tail',type=int,default=50)
    log.add_argument('--follow',action='store_true')
    internal=subs.add_parser('_supervise',help=argparse.SUPPRESS)
    internal.add_argument('config_path')
    subs._choices_actions = [choice for choice in subs._choices_actions if choice.dest != '_supervise']
    return p


def state_path():
    return settings.STATE_DIR/'active.json'


def get_state():
    return read_json(state_path(),{})


def save_state(state):
    write_json(state_path(),state)
    if state.get('output'):
        write_json(Path(state['output'])/'launcher/state.json',state)


def broker_session(config):
    run_id = json.loads((Path(config.output)/'scenario-input.json').read_text())['run_id']
    return hashlib.sha256(run_id.encode()).hexdigest()[:20]


def child_args(config, mode, output, runtime):
    args=[sys.executable,'-u','-m','perfsyncgnn.phase2',mode,'--backend',config.backend,'--config',str(Path(config.output)/'scenario-input.json'),'--output',str(output),'--runtime-status',str(runtime)]
    args += ['--executor',config.executor]
    if mode == 'consume':
        session = broker_session(config)
        # Stable for recovery of this run, distinct from interrupted older runs.
        args += ['--consumer-id', 'ps-'+session, '--consumer-group', 'perfsync-'+session, '--offset-reset', 'latest']
    if config.follow:
        args+=['--follow']
    if config.broker:
        args+=['--broker',config.broker]
    if config.gui and mode!='consume':
        args+=['--gui','--delay',str(config.delay)]
    return args


def supervise(config):
    stop_requested=False
    def requested(signum,frame):
        nonlocal stop_requested
        stop_requested=True
    signal.signal(signal.SIGINT,requested)
    signal.signal(signal.SIGTERM,requested)
    output=Path(config.output)
    state={'supervisor':identity(os.getpid()),'config':config.public(),'output':str(output),
           'run_id':json.loads((output/'scenario-input.json').read_text())['run_id'],
           'status':'STARTING','children':{},'containers_started':[],'monitoring_owned':False,'exit_code':None}
    env=os.environ.copy()
    manager=None
    processes={}
    result=1
    failure=None
    def save():
        save_state(state)
    def launch(name, argv):
        child, info=spawn(argv,output/'launcher'/f'{name}.log',env,ROOT)
        processes[name]=child
        state['children'][name]={'identity':info,'exit_code':None}
        save()
        record(output,'process_started',service=name,pid=info['pid'])
        return child
    try:
        with lock(settings.STATE_DIR,'supervisor',blocking=False):
            with lock(settings.STATE_DIR):
                save()
            require_dependencies(config)
            services=config.required_services()
            if 'ditto' in services:
                env=credentials()
            if services:
                record(output,'infrastructure_starting',services=services)
                manager=DockerManager(env,config.timeout,lambda:stop_requested,minimal=config.executor=='openwhisk')
                manager.start(services,state['containers_started'],save)
                record(output,'infrastructure_ready',services=services)
            if config.executor == 'openwhisk':
                from perfsyncgnn.serverless.config import environment
                from perfsyncgnn.serverless.openwhisk_executor import OpenWhiskClient
                from .health_checks import port_open
                from urllib.parse import urlsplit
                import shutil
                env = environment(env)
                if not all(env.get(key) for key in ('OPENWHISK_API_URL','OPENWHISK_AUTH')):
                    raise RuntimeError('OpenWhisk is not provisioned; follow docs/PHASE3_DEPLOYMENT.md')
                address = urlsplit(env['OPENWHISK_API_URL'])
                if address.hostname in {'localhost','127.0.0.1'} and address.port == 3233 and not port_open(3233):
                    kubectl = shutil.which('kubectl') or str(ROOT/'results/phase3-tools/kubectl')
                    # Restart only the existing configured development cluster, not a new one.
                    cluster = manager.inspect('minikube') if manager else None
                    if cluster and not cluster['State']['Running']:
                        minikube = shutil.which('minikube') or str(ROOT/'results/phase3-tools/minikube')
                        setup = launch('openwhisk-cluster',[minikube,'start','-p','minikube'])
                        def cluster_started():
                            code = setup.poll()
                            if code is not None and code != 0: raise ChildProcessError('existing Minikube cluster failed to start')
                            return code == 0
                        wait_ready(cluster_started,config.timeout,lambda:stop_requested)
                    def ingress_ready():
                        probe = subprocess.run([kubectl,'--request-timeout=5s','get','--raw','/api/v1/namespaces/perfsync-openwhisk/endpoints/perfsync-ow-nginx'],capture_output=True,text=True,timeout=8)
                        return probe.returncode == 0 and any(s.get('addresses') for s in json.loads(probe.stdout).get('subsets',[]))
                    wait_ready(ingress_ready,config.timeout,lambda:stop_requested,interval=2)
                    launch('openwhisk-api',[kubectl,'-n','perfsync-openwhisk','port-forward','--address','127.0.0.1','service/perfsync-ow-nginx','3233:80'])
                client = OpenWhiskClient(env['OPENWHISK_API_URL'],env['OPENWHISK_AUTH'],env.get('OPENWHISK_CA_BUNDLE'))
                wait_ready(client.health,config.timeout,lambda:stop_requested)
                for kind in ('attach','update','follow','transfer','detach'):
                    response = client.session.get(client.endpoint('actions','perfsync/'+kind),timeout=10)
                    if response.status_code != 200:
                        raise RuntimeError('OpenWhisk actions missing; run scripts/openwhisk-actions')
                def action_ready():
                    activation=client.invoke('perfsync-python-probe',{})
                    return activation.get('response',{}).get('success') is True
                wait_ready(action_ready,config.timeout,lambda:stop_requested,interval=2)
                record(output,'openwhisk_ready')
            if config.dashboard:
                from .health_checks import port_open
                if port_open(config.dashboard_port):
                    raise RuntimeError('dashboard port is already in use; choose --dashboard-port')
                if not (ROOT/'dashboard/frontend/dist/index.html').exists():
                    raise RuntimeError('dashboard build missing; run npm --prefix dashboard/frontend ci && npm --prefix dashboard/frontend run build')
                argv=[sys.executable,'-u','-m','dashboard.backend.main','--output',str(output),'--port',str(config.dashboard_port),'--refresh',str(config.dashboard_refresh)]
                if config.broker: argv.append('--broker')
                child=launch('dashboard',argv)
                import requests
                def dashboard_ready():
                    if child.poll() is not None: raise ChildProcessError('dashboard exited before readiness')
                    base=f'http://127.0.0.1:{config.dashboard_port}'
                    if requests.get(base+'/api/health',timeout=3).status_code != 200:return False
                    with requests.get(base+'/api/stream?comparison=true&offset=0',stream=True,timeout=5) as response:
                        if response.status_code != 200:return False
                        content=bytearray()
                        for chunk in response.iter_content(chunk_size=1):
                            content.extend(chunk)
                            if b'event: snapshot' in content:return True
                            if len(content)>8192:return False
                    return False
                wait_ready(dashboard_ready,config.timeout,lambda:stop_requested)
                state['dashboard_url']=f'http://127.0.0.1:{config.dashboard_port}'
                save()
                print('GUI 2: '+state['dashboard_url'],flush=True)
                record(output,'dashboard_ready',url=state['dashboard_url'])
                try:
                    import webbrowser
                    if os.environ.get('DISPLAY'):
                        opened=webbrowser.open(state['dashboard_url'],new=2)
                        record(output,'dashboard_browser_opened' if opened else 'dashboard_browser_open_unavailable')
                except Exception:
                    record(output,'dashboard_browser_open_unavailable')
            if config.monitoring:
                import requests
                compose=['docker','compose','-p','perfsync-phase3-monitoring','-f',str(ROOT/'infrastructure/monitoring/compose.yml')]
                if not env.get('PERFSYNC_GRAFANA_PASSWORD'):
                    raise RuntimeError('set PERFSYNC_GRAFANA_PASSWORD privately before starting --monitoring')
                probe=subprocess.run(compose+['ps','--status','running','-q'],capture_output=True,text=True,timeout=15)
                if probe.returncode:
                    raise RuntimeError('could not inspect the PerfSync monitoring Compose project')
                state['monitoring_owned']=not bool(probe.stdout.strip())
                with (output/'launcher/monitoring-compose.log').open('a') as log:
                    up=subprocess.run(compose+['up','-d'],stdout=log,stderr=subprocess.STDOUT,env=env,timeout=config.timeout)
                if up.returncode:
                    raise RuntimeError('Prometheus/Grafana Compose startup failed; inspect monitoring-compose.log')
                save()
                def monitoring_ready():
                    try:
                        prom=requests.get('http://127.0.0.1:9095/-/ready',timeout=3)
                        grafana=requests.get('http://127.0.0.1:3005/api/health',timeout=3)
                        if prom.status_code != 200 or grafana.status_code != 200:return False
                        auth=('admin',env['PERFSYNC_GRAFANA_PASSWORD'])
                        authenticated=requests.get('http://127.0.0.1:3005/api/datasources',auth=auth,timeout=3)
                        if authenticated.status_code == 401:
                            # Grafana only applies its environment password while creating
                            # the admin account. Reconcile a fresh/reused named volume to
                            # the explicitly configured private password through stdin.
                            reset=subprocess.run(compose+['exec','-T','grafana','grafana','cli','admin','reset-admin-password','--password-from-stdin'],input=env['PERFSYNC_GRAFANA_PASSWORD'],capture_output=True,text=True,timeout=30)
                            if reset.returncode:return False
                            authenticated=requests.get('http://127.0.0.1:3005/api/datasources',auth=auth,timeout=3)
                        if authenticated.status_code != 200:return False
                        datasources=authenticated.json()
                        if not any(d.get('uid')=='perfsync-prometheus' and d.get('type')=='prometheus' for d in datasources):return False
                        source=requests.get('http://127.0.0.1:3005/api/datasources/uid/perfsync-prometheus/health',auth=auth,timeout=5)
                        dashboard=requests.get('http://127.0.0.1:3005/api/dashboards/uid/perfsync-phase3',auth=auth,timeout=5)
                        if source.status_code != 200 or source.json().get('status') != 'OK' or dashboard.status_code != 200:return False
                        targets=requests.get('http://127.0.0.1:9095/api/v1/targets?state=active',timeout=3).json()['data']['activeTargets']
                        return any(t.get('health')=='up' and t.get('scrapeUrl','').endswith(':8090/metrics') for t in targets)
                    except (requests.RequestException,ValueError,KeyError,subprocess.TimeoutExpired):return False
                wait_ready(monitoring_ready,config.timeout,lambda:stop_requested,interval=2)
                record(output,'monitoring_ready',prometheus='http://127.0.0.1:9095',grafana='http://127.0.0.1:3005')
                print('Prometheus: http://127.0.0.1:9095  Grafana: http://127.0.0.1:3005',flush=True)
            if stop_requested:
                raise InterruptedError('startup interrupted')
            state['status']='RUNNING'
            save()
            if config.infra_only:
                while not stop_requested:
                    time.sleep(0.2)
                result=130
            elif config.command=='test':
                if config.backend=='ditto':
                    env.update(DITTO_TEST_URL=env['DITTO_URL'],MQTT_TEST_HOST='localhost',KAFKA_TEST_BOOTSTRAP='localhost:9092')
                child=launch('tests',[sys.executable,'-u','-m','pytest','-q','-rs'])
                while child.poll() is None and not stop_requested:
                    time.sleep(0.2)
                result=130 if stop_requested else child.returncode
            else:
                consumer=None
                if config.broker:
                    consumer=launch('sync',child_args(config,'consume',output/'consumer',output/'launcher/sync-runtime.json'))
                    def subscriber_ready():
                        if consumer.poll() is not None:
                            raise ChildProcessError('consumer exited before subscription readiness')
                        return read_json(output/'launcher/sync-runtime.json',{}).get('synchronization')=='ready'
                    wait_ready(subscriber_ready,config.timeout,lambda:stop_requested)
                producer=launch('sumo',child_args(config,'publish' if config.broker else 'local',output/'producer' if config.broker else output/'simulation',output/'launcher/sumo-runtime.json'))
                deadline=None
                while not stop_requested:
                    for service in ('dashboard','openwhisk-api'):
                        if service in processes and processes[service].poll() is not None:
                            raise RuntimeError(service+' exited; inspect its log')
                    if consumer and consumer.poll() is not None:
                        raise RuntimeError('synchronization consumer exited; inspect sync log')
                    code=producer.poll()
                    if code is not None:
                        if code != 0:
                            result=code
                            break
                        if not config.broker or pipeline_drained(output):
                            state['operations']=operation_summary(output,bool(config.broker))
                            if any(status != 'COMMITTED' for status in state['operations']['statuses']):
                                raise RuntimeError('workload finished with unsuccessful or unresolved operations; lifecycle records retained')
                            result=0
                            if config.dashboard:
                                # Give SSE clients a final committed snapshot before finite-run cleanup.
                                final_view_until=time.monotonic()+max(3,2*config.dashboard_refresh)
                                while time.monotonic()<final_view_until and not stop_requested:
                                    time.sleep(.1)
                            break
                        if deadline is None:
                            deadline=time.monotonic()+config.timeout
                            state['status']='DRAINING'
                            save()
                        if time.monotonic()>deadline:
                            raise TimeoutError('producer finished but subscriber identity sets did not drain; check broker capacity and sync errors')
                    time.sleep(0.1)
                if result == 0 and config.keep_dashboard and config.dashboard:
                    state['status']='HOLDING'
                    save()
                    record(output,'simulation_finished_dashboard_held')
                    while not stop_requested:
                        if processes['dashboard'].poll() is not None:
                            raise RuntimeError('dashboard exited while held open')
                        time.sleep(0.5)
                if stop_requested:
                    result=0 if config.keep_dashboard and config.dashboard and state.get('operations') is not None else 130
    except InterruptedError:
        result=130
    except Exception as exc:
        failure=redact(str(exc),env)
        record(output,'failure',category=type(exc).__name__,message=failure)
    finally:
        state['status']='STOPPING'
        save()
        # Stop the producer before its consumer, allowing worker finally blocks to export.
        for name in ['sumo','sync','tests','dashboard','openwhisk-api','openwhisk-cluster']:
            if name in state['children']:
                try:
                    terminate(state['children'][name]['identity'])
                    state['children'][name]['exit_code']=processes[name].wait(timeout=3)
                except Exception:
                    failure=failure or 'owned child cleanup incomplete; use stop to retry'
        if result == 0 and config.broker == 'mqtt' and not config.infra_only and config.command != 'test':
            # The producer is stopped and exact identity sets drained. Release only
            # this completed session so it cannot queue copies of future runs.
            try:
                from .health_checks import release_mqtt_session
                release_mqtt_session('ps-'+broker_session(config))
                record(output,'completed_mqtt_session_released')
            except Exception:
                record(output,'warning',message='completed MQTT session cleanup failed; committed data is preserved')
        if manager:
            errors=manager.shutdown(state['containers_started'])
            state['containers_released']=[item for item in state['containers_started'] if item['name'] not in errors]
            state['containers_started']=[item for item in state['containers_started'] if item['name'] in errors]
            if errors:
                failure=f'container cleanup incomplete ({len(errors)}); use stop to retry'
        if state.get('monitoring_owned'):
            try:
                compose=['docker','compose','-p','perfsync-phase3-monitoring','-f',str(ROOT/'infrastructure/monitoring/compose.yml')]
                result_compose=subprocess.run(compose+['down'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=90,env=env)
                if result_compose.returncode: failure=failure or 'monitoring cleanup failed; use stop to retry'
                else: state['monitoring_owned']=False
            except Exception:
                failure=failure or 'monitoring cleanup failed; use stop to retry'
        state['status']='FAILED' if failure or result not in (0,130,-signal.SIGINT) else ('CANCELLED' if result else 'COMPLETED')
        state['exit_code']=result
        state['error']=failure
        save()
        record(output,'finished',status=state['status'],exit_code=result)
        print(f"{state['status']}: {output}",flush=True)
        if failure:
            print(failure,file=sys.stderr)
    return result


def stop():
    # Never hold controller lock while waiting for a supervisor to exit/startup.
    with lock(settings.STATE_DIR):
        state=get_state()
        info=state.get('supervisor')
        if alive(info):
            os.kill(info['pid'],signal.SIGINT)
    if alive(info):
        deadline=time.monotonic()+45
        while alive(info) and time.monotonic()<deadline:
            time.sleep(0.2)
        if alive(info):
            raise RuntimeError('supervisor has not completed safe shutdown; retry status/stop (no unverified PID is killed)')
    # Crash recovery: only identity-verified owned child groups / immutable Docker IDs.
    state=get_state()
    for child in state.get('children',{}).values():
        terminate(child['identity'])
    if state.get('monitoring_owned'):
        compose=['docker','compose','-p','perfsync-phase3-monitoring','-f',str(ROOT/'infrastructure/monitoring/compose.yml')]
        stopped=subprocess.run(compose+['down'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=90)
        if stopped.returncode:
            raise RuntimeError('owned Prometheus/Grafana services could not be stopped; retry stop')
        state['monitoring_owned']=False
    if state.get('containers_started'):
        errors=DockerManager(os.environ.copy()).shutdown(state['containers_started'])
        state['containers_started']=[item for item in state['containers_started'] if item['name'] in errors]
        save_state(state)
        if errors:
            raise RuntimeError('some owned containers could not be stopped; Docker must be available')
    if state:
        state['status']='STOPPED' if state.get('status') not in {'COMPLETED','FAILED','CANCELLED'} else state['status']
        save_state(state)
    print('Owned processes stopped; run data and volumes preserved.')


def launch(args):
    if args.command=='restart':
        previous=get_state().get('config',{})
        stop()
    else:
        previous=None
    config=LaunchConfig.from_args(args,previous)
    with lock(settings.STATE_DIR):
        state=get_state()
        if alive(state.get('supervisor')) or any(alive(c['identity']) for c in state.get('children',{}).values()):
            raise RuntimeError('a launcher run is already active; inspect status or stop it first')
        if state.get('status') in {'STARTING','RUNNING','DRAINING','STOPPING','HOLDING'}:
            raise RuntimeError('previous supervisor exited unexpectedly; run stop to reconcile owned resources')
        require_dependencies(config)
        scenario=config.prepare()
        path=Path(config.output)/'launcher/config.json'
        write_json(path,config.public())
        supervisor,info=spawn([sys.executable,'-u','-m','perfsyncgnn.launcher.cli','_supervise',str(path)],Path(config.output)/'launcher/supervisor.log',os.environ.copy(),ROOT)
        save_state({'supervisor':info,'config':config.public(),'output':config.output,'run_id':scenario.run_id,'status':'STARTING','children':{},'containers_started':[]})
    print(f'Run {scenario.run_id}\nOutput: {config.output}',flush=True)
    if config.dashboard:
        print(f'GUI 2 URL (available after readiness): http://127.0.0.1:{config.dashboard_port}',flush=True)
    if args.detach:
        print('Supervisor detached; use perfsyncctl status / logs / stop.',flush=True)
        return 0
    try:
        result=supervisor.wait()
    except KeyboardInterrupt:
        stop()
        return 130
    latest=get_state()
    print(f"{latest.get('status')}: {config.output}")
    if config.command == 'test':
        test_log = Path(config.output)/'launcher/tests.log'
        if test_log.exists():
            try:
                log_env = credentials()
            except RuntimeError:
                log_env = os.environ.copy()
            print(redact(''.join(test_log.read_text().splitlines(keepends=True)[-20:]), log_env))
    if latest.get('operations'):
        print(json.dumps(latest['operations'],indent=2))
    if latest.get('error'):
        print(latest['error'],file=sys.stderr)
    return result


def status():
    state=get_state()
    data={'run_id':state.get('run_id'),'output':state.get('output'),'status':state.get('status','NO_RUN'),
          'supervisor_alive':alive(state.get('supervisor')),'processes':{},'operations':state.get('operations'),'dashboard_url':state.get('dashboard_url'),'executor':state.get('config',{}).get('executor','local')}
    for name,child in state.get('children',{}).items():
        data['processes'][name]={'alive':alive(child['identity']),'exit_code':child.get('exit_code')}
    if state.get('output'):
        output=Path(state['output'])
        runtime=read_json(output/'launcher/sumo-runtime.json',{})
        data['traci_last_observation']=runtime
        data['synchronization_manager']=read_json(output/'launcher/sync-runtime.json',{}) if state.get('config',{}).get('broker') else runtime.get('synchronization','not started')
        data['operation_logs']=[str(p) for p in output.glob('*/lifecycle.jsonl')]
        data['operation_journals']=[str(p) for p in output.glob('*/journal.sqlite')]
    services={}
    try:
        dm=DockerManager(os.environ.copy(),timeout=5)
        for name,container in [('ditto',settings.PROJECT+'-gateway-1'),('mongodb',settings.PROJECT+'-mongodb-1'),*settings.BROKERS.items()]:
            obj=dm.inspect(container)
            services[name]={'container':container,'state':obj['State']['Status'] if obj else 'missing','readiness':'not checked (stopped/missing)'}
            if obj and obj['State']['Running']:
                try:
                    if name=='ditto':
                        ok=ditto(credentials())
                    elif name=='mongodb':
                        ok=dm.execute(['docker','exec',container,'mongosh','--quiet','--eval','db.runCommand({ping:1}).ok']).strip()=='1'
                    else:
                        ok={'mqtt':mqtt,'kafka':kafka}[name]()
                    services[name]['readiness']='ready' if ok else 'not ready'
                except Exception:
                    services[name]['readiness']='probe unavailable/failed'
    except Exception:
        services['docker']='unavailable; no service readiness claimed'
    data['services']=services
    print(json.dumps(data,indent=2))
    return 0


def logs(args):
    if args.tail<=0:
        raise ValueError('--tail must be positive')
    state=get_state()
    env=os.environ.copy()
    try:
        env=credentials()
    except RuntimeError:
        pass
    if args.service in {'ditto','mongodb','mqtt','kafka'}:
        name={'ditto':settings.PROJECT+'-gateway-1','mongodb':settings.PROJECT+'-mongodb-1',**settings.BROKERS}[args.service]
        command=['docker','logs','--tail',str(args.tail)]
        if args.follow:
            command+=['--follow']
        command+=[name]
        process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        try:
            for line in process.stdout:
                print(redact(line,env),end='',flush=True)
        except KeyboardInterrupt:
            process.terminate()
        return process.wait()
    if not state.get('output'):
        raise RuntimeError('no run logs available')
    name='supervisor' if args.service=='launcher' else 'monitoring-compose' if args.service=='monitoring' else args.service
    if name == 'sync' and not state.get('config',{}).get('broker'):
        name='sumo'  # direct mode co-locates the manager in the TraCI worker
    path=Path(state['output'])/'launcher'/f'{name}.log'
    if not path.exists():
        raise RuntimeError(f'{args.service} has no log for this run')
    with path.open() as stream:
        lines=stream.readlines()
        for line in lines[-args.tail:]:
            print(redact(line,env),end='')
        if args.follow:
            try:
                while True:
                    line=stream.readline()
                    if line:
                        print(redact(line,env),end='',flush=True)
                    else:
                        time.sleep(0.2)
            except KeyboardInterrupt:
                pass
    return 0


def main(argv=None):
    args=parser().parse_args(argv)
    try:
        if args.command=='_supervise':
            return supervise(LaunchConfig(**read_json(args.config_path)))
        if args.command=='status':
            return status()
        if args.command=='stop':
            stop()
            return 0
        if args.command=='logs':
            return logs(args)
        return launch(args)
    except (RuntimeError, ValueError, OSError, TimeoutError) as exc:
        # Unexpected credential values never appear in error output.
        env=os.environ.copy()
        try:
            env=credentials()
        except RuntimeError:
            pass
        print(redact(str(exc),env),file=sys.stderr)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
