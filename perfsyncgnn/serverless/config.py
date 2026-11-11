"""Server-side private connection configuration. Never serialize into launcher state."""
import json
import os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
PRIVATE = ROOT/'results/phase3/private'
CONFIG = PRIVATE/'connection.json'

def environment(base=None):
    env = dict(os.environ if base is None else base)
    if CONFIG.exists():
        if CONFIG.stat().st_mode & 0o077:
            raise RuntimeError('OpenWhisk connection file requires mode 0600')
        for key, value in json.loads(CONFIG.read_text()).items():
            env.setdefault(key,str(value))
    return env

def save_connection(data):
    PRIVATE.mkdir(parents=True,exist_ok=True,mode=0o700)
    os.chmod(PRIVATE,0o700)
    fd=os.open(CONFIG,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    os.chmod(CONFIG,0o600)
    with os.fdopen(fd,'w') as out: json.dump(data,out)
