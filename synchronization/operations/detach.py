from .base import Handler, require


class Detach(Handler):
    kind = 'Detach'

    def apply(self, twin, operation):
        event = self.event(operation)
        require(twin.association_status == 'attached', 'association required')
        require(event.event_type in {'departure', 'removal'}, 'departure or removal required')
        require(event.sequence_number > twin.sequence_number and event.source_timestamp_ms >= twin.source_timestamp_ms, 'stale departure')
        require(event.source_id == twin.owner, 'departure source must own entity')
        twin.association_status = 'detached'
        twin.owner = None
        twin.subscriptions = {}
        twin.sequence_number = event.sequence_number
        twin.source_timestamp_ms = event.source_timestamp_ms

    def verify(self, twin, operation):
        return twin.association_status == 'detached' and twin.owner is None and not twin.subscriptions and twin.sequence_number == operation.event['sequence_number']
