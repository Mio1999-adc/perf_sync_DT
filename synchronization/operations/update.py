from .base import Handler, require, newer


class Update(Handler):
    kind = 'Update'

    def apply(self, twin, operation):
        event = self.event(operation)
        require(twin.association_status == 'attached', 'association required')
        require(event.event_type in {'insertion', 'observation'}, 'observation required')
        require(newer(event, twin), 'stale observation')
        require(twin.owner == event.source_id, 'source must own entity')
        twin.process_variables.update(event.process_variables)
        twin.sequence_number = event.sequence_number
        twin.source_timestamp_ms = event.source_timestamp_ms

    def verify(self, twin, operation):
        event = self.event(operation)
        return (twin.association_status == 'attached' and twin.owner == event.source_id
                and twin.sequence_number == event.sequence_number
                and twin.source_timestamp_ms == event.source_timestamp_ms
                and all(twin.process_variables.get(k) == v for k, v in event.process_variables.items()))
