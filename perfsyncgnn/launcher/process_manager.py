import fcntl
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path


def identity(pid):
    try:
        data = Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
        if data[0] == 'Z':
            return None
        return {'pid':int(pid),'start_ticks':data[19], 'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
    except (FileNotFoundError, ProcessLookupError):
        return None


def alive(info):
    return bool(info) and identity(info['pid']) == info


@contextmanager
def lock(directory, name='controller', blocking=True):
    Path(directory).mkdir(parents=True,exist_ok=True)
    with (Path(directory)/(name+'.lock')).open('a') as file:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(file,flags)
        except BlockingIOError:
            raise RuntimeError('another launcher controller is active') from None
        try:
            yield
        finally:
            fcntl.flock(file,fcntl.LOCK_UN)


def spawn(argv, output, env, cwd):
    with Path(output).open('ab',buffering=0) as log:
        child = subprocess.Popen(argv,cwd=cwd,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    return child, identity(child.pid)


def terminate(info, grace=10):
    """Only signal an identity-verified group owned by a child session leader."""
    if not alive(info):
        return
    if os.getpgid(info['pid']) != info['pid']:
        raise RuntimeError('refusing to signal a process that is not its owned group leader')
    for sig, seconds in [(signal.SIGINT,grace),(signal.SIGTERM,3),(signal.SIGKILL,1)]:
        if not alive(info):
            return
        os.killpg(info['pid'],sig)
        end=time.monotonic()+seconds
        while alive(info) and time.monotonic()<end:
            time.sleep(0.1)
