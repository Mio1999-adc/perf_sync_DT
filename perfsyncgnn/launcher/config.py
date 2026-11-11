import json
import math
import os
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from simulation.scenarios.intersection import Scenario

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = Path(os.environ.get('PERFSYNCCTL_STATE_DIR', str(ROOT/'results/.perfsyncctl'))).resolve()
COMPOSE = ROOT/'third_party/ditto/deployment/docker/docker-compose.yml'
OVERRIDE = ROOT/'results/phase2-guide/ditto-auth.override.yml'
CREDENTIALS = ROOT/'results/phase2-guide/private/credentials.json'
PROJECT = 'perfsync-phase2-guide'
BROKERS = {'mqtt': PROJECT+'-mqtt', 'kafka': PROJECT+'-kafka'}


@dataclass
class LaunchConfig:
    backend: str = 'sqlite'
    executor: str = 'local'
    dashboard: bool = False
    dashboard_port: int = 8090
    dashboard_refresh: float = 1.0
    monitoring: bool = False
    keep_dashboard: bool = False
    follow: bool = False
    gui: bool = False
    scenario: str = 'intersection'
    seed: int = 27
    output: str = ''
    timeout: float = 300
    broker: str = ''
    services: str = ''
    infra_only: bool = False
    delay: float = 100
    config_file: str = str(ROOT/'configs/intersection.json')
    command: str = 'start'

    @classmethod
    def from_args(cls, args, previous=None):
        values = {k:v for k,v in vars(args).items() if k in cls.__dataclass_fields__ and v is not None}
        old = dict(previous or {})
        old.pop('output', None)
        old.update(values)
        if getattr(args, 'live', False):
            if values.get('backend') == 'sqlite':
                raise ValueError('--live conflicts with --backend sqlite')
            old['backend'] = 'ditto'
        if args.command == 'gui':
            old['gui'] = True
        config = cls(**old)
        if config.backend not in {'sqlite','ditto'} or config.scenario != 'intersection':
            raise ValueError('only sqlite/ditto and the intersection scenario are implemented')
        if not math.isfinite(config.timeout) or config.timeout <= 0 or not math.isfinite(config.delay) or config.delay < 0:
            raise ValueError('timeout must be positive; delay must be finite and nonnegative')
        if config.broker not in {'','mqtt','kafka'}:
            raise ValueError('unknown broker')
        config.config_file = str(Path(config.config_file).resolve())
        source = Scenario.model_validate_json(Path(config.config_file).read_text())
        if 'seed' not in old:
            config.seed = source.seed
        if config.executor not in {'local','openwhisk'}:
            raise ValueError('unknown executor')
        if (config.executor == 'openwhisk' or config.dashboard) and config.backend != 'ditto':
            raise ValueError('OpenWhisk and GUI 2 require --backend ditto or --live')
        if config.monitoring and not config.dashboard:
            raise ValueError('--monitoring requires --dashboard so Prometheus can scrape its metrics endpoint')
        if config.keep_dashboard and not config.dashboard:
            raise ValueError('--keep-dashboard requires --dashboard')
        if not 1024 <= config.dashboard_port <= 65535 or not math.isfinite(config.dashboard_refresh) or config.dashboard_refresh < .25:
            raise ValueError('dashboard port must be 1024..65535; refresh must be >= .25 seconds')
        required = config.required_services()
        if config.infra_only and (not required or config.gui or config.dashboard):
            raise ValueError('--infra-only requires services and cannot use --gui')
        if config.command == 'test' and (config.gui or config.follow or config.broker or config.infra_only):
            raise ValueError('test uses --live/--services; GUI/follow/broker/infra-only are simulation options')
        return config

    def required_services(self):
        requested = set(filter(None, self.services.split(',')))
        if 'all' in requested:
            requested = {'ditto','mqtt','kafka'}
        if requested - {'ditto','mqtt','kafka'}:
            raise ValueError('services must be ditto,mqtt,kafka or all')
        if self.backend == 'ditto':
            requested.add('ditto')
        if self.broker:
            requested.add(self.broker)
        if self.command == 'test' and self.backend == 'ditto':
            requested |= {'ditto','mqtt','kafka'}
        return sorted(requested)

    def prepare(self):
        if self.output:
            out = Path(self.output).resolve()
            if out.exists() and any(out.iterdir()):
                raise ValueError('output directory is not empty; choose a new directory to preserve results')
        else:
            stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            out = ROOT/'results/phase2-runs'/f'{stamp}-{uuid.uuid4().hex[:8]}'
        out.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(out/'.perfsync-run', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        except FileExistsError:
            raise ValueError('output is reserved by another run') from None
        self.output = str(out)
        scenario = Scenario.model_validate_json(Path(self.config_file).read_text())
        scenario.seed = self.seed
        scenario.run_id = 'launcher-' + out.name + '-' + uuid.uuid4().hex[:8]
        (out/'scenario-input.json').write_text(scenario.model_dump_json(indent=2))
        (out/'launcher').mkdir()
        return scenario

    def public(self):
        return asdict(self)


def require_dependencies(config):
    for executable in (['sumo-gui' if config.gui else 'sumo', 'netconvert'] if not config.infra_only else []):
        if not shutil.which(executable):
            raise RuntimeError(f'missing executable: {executable}')
    if config.required_services() and not shutil.which('docker'):
        raise RuntimeError('Docker executable unavailable')
    if config.gui:
        display = os.environ.get('DISPLAY')
        if not display:
            raise RuntimeError('connected GUI requires a graphical X11 DISPLAY (or xvfb-run for testing)')
        import subprocess
        if not shutil.which('xdpyinfo'):
            raise RuntimeError('xdpyinfo required to verify DISPLAY; install x11-utils')
        probe = subprocess.run(['xdpyinfo','-display',display],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=5)
        if probe.returncode:
            raise RuntimeError('DISPLAY is set but cannot be opened')


def credentials():
    env = os.environ.copy()
    if CREDENTIALS.exists():
        try:
            if CREDENTIALS.stat().st_mode & 0o077:
                raise RuntimeError('private credential file must have mode 0600')
            data = json.loads(CREDENTIALS.read_text())
            for key in ['DITTO_URL','DITTO_USERNAME','DITTO_PASSWORD','DITTO_POLICY_ID']:
                if key in data and key not in env:
                    env[key] = str(data[key])
        except (ValueError, OSError):
            raise RuntimeError('unable to read private Ditto configuration') from None
    if not all(env.get(k) for k in ['DITTO_URL','DITTO_USERNAME','DITTO_PASSWORD','DITTO_POLICY_ID']):
        raise RuntimeError('Ditto credentials/policy missing; configure DITTO_* or the existing private JSON (values never logged)')
    from urllib.parse import urlsplit
    url = urlsplit(env['DITTO_URL'])
    if url.scheme not in {'http','https'} or not url.hostname or url.username or url.password:
        raise RuntimeError('DITTO_URL must be an HTTP(S) URL without embedded credentials')
    return env
