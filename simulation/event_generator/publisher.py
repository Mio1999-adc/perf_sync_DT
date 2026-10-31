"""Durable at-least-once outbox; mark sent only after broker confirmation."""
import json
from simulation.event_generator.schemas import TrafficEvent


class DurablePublisher:
    def __init__(self, journal, transport):
        self.journal = journal
        self.transport = transport

    def publish(self, event):
        with self.journal.transaction():
            old = self.journal.db.execute('SELECT body FROM outbox WHERE key=?', (event.deduplication_id,)).fetchone()
            if old and TrafficEvent.model_validate_json(old[0]).fingerprint != event.fingerprint:
                raise ValueError('conflicting outbox identity')
            self.journal.db.execute('INSERT OR IGNORE INTO outbox(key,body) VALUES (?,?)', (event.deduplication_id, event.model_dump_json()))
        self.flush()

    def flush(self):
        with self.journal.worker_lock():
            for key, body in self.journal.db.execute('SELECT key,body FROM outbox WHERE sent=0 ORDER BY rowid').fetchall():
                self.transport.send(key, body)
                self.journal.db.execute('UPDATE outbox SET sent=1 WHERE key=?', (key,))


class MQTTTransport:
    def __init__(self, host='localhost', port=1883, topic='perfsync/events'):
        import paho.mqtt.client as mqtt
        import threading
        self.connected = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        def on_connect(client, userdata, flags, reason, properties):
            if reason == 0:
                self.connected.set()
        self.client.on_connect = on_connect
        self.client.connect(host, port)
        self.client.loop_start()
        self.topic = topic
        if not self.connected.wait(10):
            self.close()
            raise TimeoutError('MQTT connection acknowledgement missing')

    def send(self, key, body):
        info = self.client.publish(self.topic, body, qos=1)
        info.wait_for_publish(timeout=10)
        if info.rc != 0 or not info.is_published():
            raise TimeoutError('MQTT publication not acknowledged')

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


class KafkaTransport:
    def __init__(self, bootstrap='localhost:9092', topic='perfsync.events'):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True, 'acks': 'all', 'message.timeout.ms': 10000})
        self.topic = topic

    def send(self, key, body):
        result = []
        self.producer.produce(self.topic, key=key, value=body, on_delivery=lambda error, message: result.append(error))
        remaining = self.producer.flush(12)
        if remaining or not result or result[0]:
            raise RuntimeError(f'Kafka delivery failed: {result}')

    def close(self):
        self.producer.flush(12)
