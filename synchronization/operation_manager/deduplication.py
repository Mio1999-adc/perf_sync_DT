import hashlib


def operation_id(event, kind):
    return 'op-' + hashlib.sha256(f'{event.deduplication_id}:{kind}'.encode()).hexdigest()
