"""Opt-in live checks fail once configured; missing opt-in is an explicit skip."""
import os
import time
import uuid
import pytest
from dt.adapter import DittoAdapter
from monitoring.store import Journal
from simulation.event_generator.publisher import MQTTTransport, KafkaTransport
from simulation.event_generator.schemas import TrafficEvent
from synchronization.operation_manager.manager import OperationManager


@pytest.mark.integration
def test_live_ditto_complete_lifecycle(tmp_path):
    if not os.getenv('DITTO_TEST_URL'):
        pytest.skip('DITTO_TEST_URL unset: live Eclipse Ditto unavailable/unconfigured')
    adapter = DittoAdapter(os.environ['DITTO_TEST_URL'],os.getenv('DITTO_USERNAME'),os.getenv('DITTO_PASSWORD'),os.getenv('DITTO_POLICY_ID'))
    manager = OperationManager(adapter, Journal(tmp_path/'journal.db'))
    run = 'integration-'+uuid.uuid4().hex
    def send(seq, kind, owner='rsu-west', subscription=None):
        e = TrafficEvent(event_id=str(seq), run_id=run,entity_id='vehicle',entity_type='vehicle',event_type=kind,simulation_time_ms=seq*1000,source_timestamp_ms=seq*1000,sequence_number=seq,source_id=owner,process_variables={'speed':10} if kind in {'insertion','observation'} else {},subscription=subscription)
        ops = manager.submit(e)
        assert ops and all(o.status == 'COMMITTED' for o in ops)
        return ops
    send(1,'insertion')
    send(2,'follow_request',subscription={'subscriber_id':'test','variables':['speed'],'lifetime_ms':10000})
    send(3,'observation')
    send(4,'observation','rsu-east')
    send(5,'departure','rsu-east')
    assert {o.operation_type for o in manager.journal.operations()} == {'Attach','Follow','Update','Transfer','Detach'}


@pytest.mark.integration
def test_live_mqtt_roundtrip():
    host = os.getenv('MQTT_TEST_HOST')
    if not host:
        pytest.skip('MQTT_TEST_HOST unset: live MQTT broker unavailable/unconfigured')
    import paho.mqtt.client as mqtt
    import threading
    ready = threading.Event()
    received = threading.Event()
    bodies = []
    topic = 'perfsync/test/'+uuid.uuid4().hex
    port = int(os.getenv('MQTT_TEST_PORT','1883'))
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = lambda c,u,f,r,p: c.subscribe(topic,qos=1)
    client.on_subscribe = lambda c,u,m,r,p: ready.set()
    def message(c,u,m):
        bodies.append(m.payload.decode())
        received.set()
    client.on_message = message
    client.connect(host,port)
    client.loop_start()
    publisher = None
    try:
        assert ready.wait(10), 'MQTT subscription acknowledgement missing'
        publisher = MQTTTransport(host,port,topic)
        publisher.send('key','{"test":1}')
        assert received.wait(10)
        assert bodies == ['{"test":1}']
    finally:
        if publisher:
            publisher.close()
        client.disconnect()
        client.loop_stop()


@pytest.mark.integration
def test_live_kafka_roundtrip():
    bootstrap = os.getenv('KAFKA_TEST_BOOTSTRAP')
    if not bootstrap:
        pytest.skip('KAFKA_TEST_BOOTSTRAP unset: live Kafka unavailable/unconfigured')
    from confluent_kafka import Consumer
    topic = os.getenv('KAFKA_TEST_TOPIC','perfsync.test')
    consumer = Consumer({'bootstrap.servers':bootstrap,'group.id':'phase2-test-'+uuid.uuid4().hex,'auto.offset.reset':'earliest','enable.auto.commit':False})
    transport = KafkaTransport(bootstrap,topic)
    unique = uuid.uuid4().hex
    try:
        consumer.subscribe([topic])
        transport.send(unique,unique)
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is not None and not message.error() and message.key().decode() == unique:
                assert message.value().decode() == unique
                return
        pytest.fail('Kafka published record not consumed before deadline')
    finally:
        consumer.close()
        transport.close()
