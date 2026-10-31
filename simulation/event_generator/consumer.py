from simulation.event_generator.schemas import TrafficEvent


def accept(payload, manager):
    event = TrafficEvent.model_validate_json(payload)
    manager.submit(event)


def consume_kafka(manager, bootstrap='localhost:9092', topic='perfsync.events', group='perfsync-sync'):
    from confluent_kafka import Consumer, KafkaException
    client = Consumer({'bootstrap.servers': bootstrap, 'group.id': group, 'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    client.subscribe([topic])
    try:
        while True:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            accept(message.value(), manager)
            client.commit(message=message, asynchronous=False)
    finally:
        client.close()


def consume_mqtt(manager, host='localhost', port=1883, topic='perfsync/events', client_id='perfsync-sync'):
    import paho.mqtt.client as mqtt
    errors = []
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=False, manual_ack=True)
    def connected(c, userdata, flags, reason, properties):
        if reason != 0:
            errors.append(RuntimeError(f'MQTT connection rejected: {reason}'))
            c.disconnect()
            return
        c.subscribe(topic, qos=1)
    def received(c, userdata, message):
        try:
            accept(message.payload, manager)
            c.ack(message.mid, message.qos)
        except Exception as exc:
            errors.append(exc)
            c.disconnect()  # unacknowledged QoS1 retained by broker persistent session
    client.on_connect = connected
    client.on_message = received
    client.connect(host, port)
    try:
        client.loop_forever()
    finally:
        client.disconnect()
    if errors:
        raise errors[0]
