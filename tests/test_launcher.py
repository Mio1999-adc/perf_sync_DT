import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import Mock, MagicMock
import pytest
from perfsyncgnn.launcher import config as cfg
from perfsyncgnn.launcher.cli import parser, launch, status
from perfsyncgnn.launcher.config import LaunchConfig, ROOT, require_dependencies
from perfsyncgnn.launcher.docker_manager import DockerManager
from perfsyncgnn.launcher.health_checks import wait_ready
from perfsyncgnn.launcher.log_manager import write_json, redact
from perfsyncgnn.launcher.process_manager import identity, alive, spawn, terminate
from simulation.scenarios.intersection import Scenario, generate
from simulation.traci_adapter.client import TraCIClient
from monitoring.runtime import RuntimeReporter


def test_cli_and_configuration_validation(tmp_path):
    args=parser().parse_args(['start','--live','--broker','mqtt','--follow','--seed','42'])
    config=LaunchConfig.from_args(args)
    assert config.backend=='ditto' and config.follow and config.seed==42
    assert config.required_services()==['ditto','mqtt']
    assert LaunchConfig.from_args(parser().parse_args(['gui'])).gui
    assert LaunchConfig.from_args(parser().parse_args(['test','--live'])).required_services()==['ditto','kafka','mqtt']
    monitored=LaunchConfig.from_args(parser().parse_args(['start','--live','--dashboard','--monitoring','--keep-dashboard']))
    assert monitored.monitoring and monitored.keep_dashboard and monitored.backend=='ditto'
    for argv in [['start','--live','--backend','sqlite'],['start','--timeout','0'],['start','--timeout','nan'],['start','--services','unknown'],['start','--infra-only'],['test','--gui'],['start','--monitoring'],['start','--keep-dashboard']]:
        with pytest.raises(ValueError):
            LaunchConfig.from_args(parser().parse_args(argv))
    with pytest.raises(SystemExit):
        parser().parse_args(['start','--scenario','unimplemented'])


def test_fresh_run_configuration_and_output_protection(tmp_path):
    config=LaunchConfig(output=str(tmp_path/'run'))
    first=config.prepare()
    assert first.run_id.startswith('launcher-') and first.seed==27
    assert json.loads((tmp_path/'run/scenario-input.json').read_text())['run_id']==first.run_id
    with pytest.raises(ValueError,match='not empty'):
        config.prepare()
    second=LaunchConfig(output=str(tmp_path/'second')).prepare()
    assert first.run_id!=second.run_id


def test_dependency_and_graphical_session_errors(monkeypatch):
    monkeypatch.setattr('shutil.which',lambda name:None)
    with pytest.raises(RuntimeError,match='missing executable'):
        require_dependencies(LaunchConfig())
    monkeypatch.setattr('shutil.which',lambda name:'/bin/'+name)
    monkeypatch.delenv('DISPLAY',raising=False)
    with pytest.raises(RuntimeError,match='DISPLAY'):
        require_dependencies(LaunchConfig(gui=True))


def test_health_timeout_and_child_failure():
    with pytest.raises(TimeoutError):
        wait_ready(lambda:False,0.02,interval=0.002)
    with pytest.raises(InterruptedError):
        wait_ready(lambda:False,1,stop=lambda:True)
    def died():
        raise ChildProcessError('child exited')
    with pytest.raises(ChildProcessError):
        wait_ready(died,1)


def test_process_group_start_stop_preserves_finalizer(tmp_path):
    ready=tmp_path/'ready';final=tmp_path/'final'
    code="import time\nfrom pathlib import Path\nPath(%r).touch()\ntry:\n while True: time.sleep(.1)\nfinally: Path(%r).touch()" % (str(ready),str(final))
    process,info=spawn([sys.executable,'-u','-c',code],tmp_path/'child.log',os.environ.copy(),ROOT)
    try:
        wait_ready(ready.exists,5)
        assert alive(info)
        terminate(info,2)
        process.wait(timeout=3)
        assert not alive(info) and final.exists()
    finally:
        terminate(info,1)


def test_pid_identity_guard_does_not_signal_reused_pid(monkeypatch):
    info=dict(identity(os.getpid()),start_ticks='invalid')
    kill=Mock()
    monkeypatch.setattr(os,'killpg',kill)
    terminate(info,0.01)
    kill.assert_not_called()


def test_duplicate_launch_is_rejected_before_mutation(tmp_path,monkeypatch):
    monkeypatch.setattr(cfg,'STATE_DIR',tmp_path/'control')
    write_json(cfg.STATE_DIR/'active.json',{'supervisor':identity(os.getpid()),'children':{}})
    with pytest.raises(RuntimeError,match='already active'):
        launch(parser().parse_args(['start','--output',str(tmp_path/'unused')]))
    assert not (tmp_path/'unused').exists()


def container(name,service=None,running=False,ports=None):
    labels={'com.docker.compose.project':cfg.PROJECT,'com.docker.compose.service':service} if service else None
    return {'Id':'immutable-'+name,'Name':'/'+name,'Config':{'Labels':labels},'State':{'Running':running,'Status':'running' if running else 'exited'},'HostConfig':{'PortBindings':ports or {}}}


def test_docker_discovery_and_missing_container_do_not_create(monkeypatch):
    dm=DockerManager({})
    mongo=container('mongodb','mongodb');gateway=container('gateway','gateway')
    def execute(args):
        if '--services' in args: return 'mongodb\ngateway\n'
        if '-aq' in args: return mongo['Id']+'\n'+gateway['Id']+'\n'
        return ''
    dm.execute=Mock(side_effect=execute)
    dm.inspect=Mock(side_effect=lambda name:{mongo['Id']:mongo,gateway['Id']:gateway}.get(name))
    assert len(dm.discover(['ditto']))==2
    assert not any('up' in call.args[0] for call in dm.execute.call_args_list)
    dm.inspect=Mock(return_value=None)
    with pytest.raises(RuntimeError,match='container missing'):
        dm.discover(['mqtt'])


def test_docker_starts_only_stopped_and_keeps_running_services(monkeypatch):
    dm=DockerManager({})
    running=container(cfg.BROKERS['mqtt'],running=True)
    stopped=container(cfg.BROKERS['kafka'])
    dm.discover=Mock(return_value=[running,stopped])
    dm.execute=Mock(return_value='')
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.wait_ready',lambda *args:True)
    owned=[]
    dm.start(['mqtt','kafka'],owned,lambda:None)
    assert owned==[{'id':stopped['Id'],'name':stopped['Name'][1:]}]
    starts=[c.args[0] for c in dm.execute.call_args_list if c.args[0][:2]==['docker','start']]
    assert starts==[['docker','start',stopped['Id']]]


def test_port_conflict_prevents_start(monkeypatch):
    dm=DockerManager({})
    item=container(cfg.BROKERS['mqtt'],ports={'1883/tcp':[{'HostPort':'1883'}]})
    dm.discover=Mock(return_value=[item]);dm.execute=Mock()
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.port_open',lambda port:True)
    with pytest.raises(RuntimeError,match='port 1883'):
        dm.start(['mqtt'],[],lambda:None)
    dm.execute.assert_not_called()


def test_shutdown_uses_immutable_container_identity():
    dm=DockerManager({})
    dm.inspect=Mock(return_value={'Name':'/someone-else','State':{'Running':True}})
    dm.execute=Mock()
    assert dm.shutdown([{'id':'old-id','name':'owned'}])==[]
    dm.execute.assert_not_called()


def test_credentials_redacted():
    assert redact('a password identity policy',{'DITTO_PASSWORD':'password','DITTO_USERNAME':'identity','DITTO_POLICY_ID':'policy'})=='a [redacted] [redacted] [redacted]'


@pytest.mark.integration
def test_launcher_sqlite_execution_and_preserved_logs(tmp_path):
    source=tmp_path/'scenario.json'
    source.write_text(Scenario(duration_s=2).model_dump_json())
    env=dict(os.environ,PERFSYNCCTL_STATE_DIR=str(tmp_path/'controller'),PERFSYNC_SQLITE_SYNCHRONOUS='NORMAL')
    result=subprocess.run([str(ROOT/'scripts/perfsyncctl'),'run','--config',str(source),'--follow','--output',str(tmp_path/'run')],env=env,capture_output=True,text=True,timeout=45)
    assert result.returncode==0,result.stdout+result.stderr
    state=json.loads((tmp_path/'controller/active.json').read_text())
    assert state['status']=='COMPLETED' and not alive(state['supervisor'])
    rows=[json.loads(line) for line in (tmp_path/'run/simulation/lifecycle.jsonl').read_text().splitlines()]
    assert {'Attach','Update','Follow'} <= {r['operation_type'] for r in rows}
    assert all(r['commit_unix_ns'] is not None for r in rows if r['status']=='COMMITTED')
    assert (tmp_path/'run/launcher/sumo.log').exists()
    info=json.loads((tmp_path/'run/launcher/sumo-runtime.json').read_text())
    assert info['traci']=='closed' and info['simulation_time_ms']==2000


@pytest.mark.integration
def test_connected_gui_same_traffic_and_connection(tmp_path):
    import shutil
    if not os.getenv('DISPLAY') or not shutil.which('sumo-gui') or not shutil.which('xdpyinfo'):
        pytest.skip('connected GUI requires SUMO-GUI and a graphical DISPLAY; use xvfb-run for offscreen verification')
    if subprocess.run(['xdpyinfo'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
        pytest.skip('DISPLAY cannot be opened')
    scenario=Scenario(duration_s=2)
    path=generate(tmp_path/'scenario',scenario)
    reporter=RuntimeReporter(tmp_path/'runtime.json')
    graphical=list(TraCIClient(scenario,path,gui=True,delay=0,reporter=reporter).events())
    headless=list(TraCIClient(scenario,path).events())
    assert graphical and [e.model_dump(exclude={'emitted_at_unix_ms'}) for e in graphical]==[e.model_dump(exclude={'emitted_at_unix_ms'}) for e in headless]
    assert reporter.state['gui'] is True and reporter.state['sumo_pid']>0
    assert reporter.state['traci']=='closed'


def test_stop_recovers_orphan_child_after_supervisor_crash(tmp_path,monkeypatch,capsys):
    from perfsyncgnn.launcher.cli import stop
    monkeypatch.setattr(cfg,'STATE_DIR',tmp_path/'state')
    ready=tmp_path/'ready';final=tmp_path/'final'
    code="import time\nfrom pathlib import Path\nPath(%r).touch()\ntry:\n while True: time.sleep(.1)\nfinally: Path(%r).touch()" % (str(ready),str(final))
    process,info=spawn([sys.executable,'-c',code],tmp_path/'child.log',os.environ.copy(),ROOT)
    try:
        wait_ready(ready.exists,5)
        write_json(cfg.STATE_DIR/'active.json',{'supervisor':None,'children':{'sumo':{'identity':info}},'status':'RUNNING','containers_started':[]})
        stop()
        process.wait(timeout=3)
        assert final.exists() and not alive(info)
    finally:
        terminate(info,1)


def test_pipeline_compares_identity_sets_not_just_counts(tmp_path):
    import sqlite3
    from perfsyncgnn.launcher.health_checks import pipeline_drained
    for directory in ['producer','consumer']:
        (tmp_path/directory).mkdir()
    a=sqlite3.connect(tmp_path/'producer/journal.sqlite')
    a.executescript("CREATE TABLE outbox(key TEXT,sent INTEGER); INSERT INTO outbox VALUES('a',1);")
    b=sqlite3.connect(tmp_path/'consumer/journal.sqlite')
    b.executescript("CREATE TABLE inbox(key TEXT,done INTEGER); INSERT INTO inbox VALUES('b',1); CREATE TABLE operations(body TEXT);")
    assert not pipeline_drained(tmp_path)
    b.execute("UPDATE inbox SET key='a'");b.commit()
    assert pipeline_drained(tmp_path)


@pytest.mark.integration
def test_connected_gui_close_preserves_partial_lifecycle(tmp_path):
    import shutil
    if not os.getenv('DISPLAY') or not shutil.which('sumo-gui') or not shutil.which('xdpyinfo'):
        pytest.skip('connected GUI close test requires graphical DISPLAY')
    if subprocess.run(['xdpyinfo'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
        pytest.skip('DISPLAY cannot be opened')
    source=tmp_path/'scenario.json';source.write_text(Scenario(duration_s=30).model_dump_json())
    output=tmp_path/'run';runtime=tmp_path/'runtime.json'
    env=dict(os.environ,PERFSYNC_SQLITE_SYNCHRONOUS='NORMAL')
    with (tmp_path/'worker.log').open('w') as log:
        process=subprocess.Popen([sys.executable,'-m','perfsyncgnn.phase2','local','--backend','sqlite','--gui','--delay','100','--config',str(source),'--output',str(output),'--runtime-status',str(runtime)],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            def progressed():
                from perfsyncgnn.launcher.log_manager import read_json
                state=read_json(runtime,{})
                return state if state.get('simulation_time_ms',0)>=1000 else False
            state=wait_ready(progressed,15)
            engine=identity(state['sumo_pid'])
            assert engine and state['gui']
            os.kill(engine['pid'],signal.SIGTERM)  # only this test's TraCI-owned engine
            assert process.wait(timeout=10)==130
            assert (output/'lifecycle.jsonl').read_text()
            assert json.loads(runtime.read_text())['phase']=='cancelled'
        finally:
            if process.poll() is None:
                process.terminate();process.wait(timeout=10)


def test_bootstrap_recovery_is_once_and_only_for_owned_policy(monkeypatch):
    dm=DockerManager({})
    policy=container(cfg.PROJECT+'-policies-1','policies')
    mongo=container(cfg.PROJECT+'-mongodb-1','mongodb')
    mongo['HostConfig']['PortBindings']={'27017/tcp':[{'HostPort':'27017'}]}
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.port_open',lambda port:False)
    client=MagicMock()
    client.__enter__.return_value.admin.command.return_value={'ok':1}
    monkeypatch.setattr('pymongo.MongoClient',lambda *a,**k:client)
    dm.discover=Mock(return_value=[policy,mongo])
    def execute(args):
        if args[:2]==['docker','logs']:
            return 'Discovered [1] contact points, confirmed [1], which is less than the required [2]'
        return '1'
    dm.execute=Mock(side_effect=execute)
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.ditto',lambda env:False)
    ticks=iter([0,21,22,23])
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.time.monotonic',lambda:next(ticks))
    def probe(check,*args):
        check();check()
    monkeypatch.setattr('perfsyncgnn.launcher.docker_manager.wait_ready',probe)
    owned=[]
    dm.start(['ditto'],owned,lambda:None)
    restarts=[call.args[0] for call in dm.execute.call_args_list if call.args[0][:2]==['docker','restart']]
    assert len(restarts)==1 and restarts[0][-1]==policy['Id']
    assert owned[0]['bootstrap_restarted'] is True


def test_broker_identity_is_stable_per_run_and_distinct_across_runs(tmp_path):
    from perfsyncgnn.launcher.cli import child_args
    one=LaunchConfig(output=str(tmp_path/'one'),broker='mqtt');one.prepare()
    two=LaunchConfig(output=str(tmp_path/'two'),broker='mqtt');two.prepare()
    a=child_args(one,'consume',tmp_path/'consumer',tmp_path/'runtime')
    repeated=child_args(one,'consume',tmp_path/'consumer',tmp_path/'runtime')
    b=child_args(two,'consume',tmp_path/'consumer2',tmp_path/'runtime2')
    assert a==repeated
    assert a[a.index('--consumer-id')+1]!=b[b.index('--consumer-id')+1]
    assert a[a.index('--consumer-group')+1]!=b[b.index('--consumer-group')+1]
    assert a[a.index('--offset-reset')+1]=='latest'
    # Existing run resumes its session/group; a new producer starts only after readiness.
    assert len(a[a.index('--consumer-id')+1])==23


def test_kafka_offsets_are_resolved_before_ready_callback(monkeypatch):
    from types import SimpleNamespace
    from simulation.event_generator.consumer import consume_kafka
    import confluent_kafka
    assigned=[]
    ready=[]
    class FakeConsumer:
        def __init__(self, config):
            assert config['auto.offset.reset']=='latest'
        def subscribe(self, topics, on_assign):
            on_assign(self,[SimpleNamespace(offset=-1001)])
        def committed(self, partitions, timeout):
            return [SimpleNamespace(offset=-1001)]
        def get_watermark_offsets(self, partition, timeout):
            assert not ready
            return 0,17
        def assign(self, partitions):
            assigned.append(partitions[0].offset)
        def poll(self, timeout):
            raise KeyboardInterrupt
        def close(self):
            pass
    monkeypatch.setattr(confluent_kafka,'Consumer',FakeConsumer)
    with pytest.raises(KeyboardInterrupt):
        consume_kafka(None,offset_reset='latest',ready=lambda:ready.append(assigned[0]))
    assert ready==[17]
