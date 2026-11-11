import json
import os
import time
from pathlib import Path


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name+f'.{os.getpid()}.tmp')
    temporary.write_text(json.dumps(value,indent=2))
    temporary.replace(path)


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, ValueError):
        return default


def record(output, event, **fields):
    with (Path(output)/'launcher/events.jsonl').open('a') as stream:
        stream.write(json.dumps(dict(unix_ns=time.time_ns(),event=event,**fields))+'\n')


def redact(text, env):
    # Credentials are never passed as argv, persisted in run state, or printed.
    for key in ['DITTO_PASSWORD','DITTO_USERNAME','DITTO_POLICY_ID','OPENWHISK_AUTH']:
        if env.get(key):
            text = text.replace(env[key], '[redacted]')
    return text
