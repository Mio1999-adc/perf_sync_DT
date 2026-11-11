import json
import socket
import time
import requests


def wait_ready(check, timeout, stop=lambda:False, interval=0.5):
    deadline=time.monotonic()+timeout
    last='not ready'
    while time.monotonic()<deadline:
        if stop():
            raise InterruptedError('startup interrupted')
        try:
            result=check()
            if result:
                return result
        except ChildProcessError:
            raise
        except Exception as exc:
            # Do not propagate credential-containing exception details.
            last=type(exc).__name__
        time.sleep(min(interval,max(0,deadline-time.monotonic())))
    raise TimeoutError(f'readiness deadline exceeded ({last})')


def port_open(port):
    with socket.socket() as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(('127.0.0.1',port))==0


def ditto(env):
    try:
        from urllib.parse import quote
        r=requests.get(env['DITTO_URL'].rstrip('/')+'/api/2/policies/'+quote(env['DITTO_POLICY_ID'],safe=''),auth=(env['DITTO_USERNAME'],env['DITTO_PASSWORD']),timeout=2)
        return r.status_code==200
    except requests.RequestException:
        return False


def mqtt():
    import threading
    import paho.mqtt.client as mqtt_client
    ready=threading.Event()
    client=mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect=lambda c,u,f,r,p: ready.set() if r==0 else None
    try:
        client.connect('127.0.0.1',1883)
        client.loop_start()
        return ready.wait(1)
    except OSError:
        return False
    finally:
        client.disconnect()
        client.loop_stop()


def kafka():
    from confluent_kafka.admin import AdminClient
    try:
        meta=AdminClient({'bootstrap.servers':'localhost:9092','log_level':0}).list_topics(timeout=2)
        return bool(meta.brokers)
    except Exception:
        return False


def pipeline_drained(output):
    import sqlite3
    from pathlib import Path
    p=Path(output)
    try:
        with sqlite3.connect(f'file:{p / "producer/journal.sqlite"}?mode=ro',uri=True) as a, sqlite3.connect(f'file:{p / "consumer/journal.sqlite"}?mode=ro',uri=True) as b:
            sent={r[0] for r in a.execute('SELECT key FROM outbox WHERE sent=1')}
            pending=a.execute('SELECT count(*) FROM outbox WHERE sent=0').fetchone()[0]
            done={r[0] for r in b.execute('SELECT key FROM inbox WHERE done=1')}
            nonterminal=b.execute("SELECT count(*) FROM operations WHERE json_extract(body,'$.status') NOT IN ('COMMITTED','FAILED','CANCELLED','SUPERSEDED')").fetchone()[0]
            return not pending and not nonterminal and bool(sent) and sent==done
    except sqlite3.Error:
        return False


def operation_summary(output, broker=False):
    import sqlite3
    from pathlib import Path
    path=Path(output)/('consumer' if broker else 'simulation')/'journal.sqlite'
    with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as journal:
        rows=journal.execute("SELECT json_extract(body,'$.status'), json_extract(body,'$.operation_type'),count(*) FROM operations GROUP BY 1,2").fetchall()
    statuses={}
    kinds={}
    for status,kind,count in rows:
        statuses[status]=statuses.get(status,0)+count
        kinds[kind]=kinds.get(kind,0)+count
    return {'statuses':statuses,'types':kinds,'total':sum(statuses.values())}


def release_mqtt_session(client_id):
    import threading
    import paho.mqtt.client as mqtt_client
    acknowledged=threading.Event()
    client=mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2,client_id=client_id,clean_session=True)
    client.on_connect=lambda c,u,f,r,p: acknowledged.set() if r==0 else None
    try:
        client.connect('127.0.0.1',1883)
        client.loop_start()
        if not acknowledged.wait(3):
            raise TimeoutError('MQTT session release not acknowledged')
    finally:
        client.disconnect()
        client.loop_stop()
