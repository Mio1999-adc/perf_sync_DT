from .base import Handler, require


class Attach(Handler):
    kind = 'Attach'

    def apply(self, twin, operation):
        require(twin.association_status != 'attached', 'already attached')
        require(self.event(operation).event_type in {'insertion', 'observation'}, 'association trigger required')
        require(twin.association_status != 'detached' or self.event(operation).event_type == 'insertion', 'explicit insertion required to reattach')
        twin.association_status = 'attached'
        twin.owner = self.event(operation).source_id
        twin.ownership_version += 1

    def verify(self, twin, operation):
        return twin.association_status == 'attached' and twin.physical_id == operation.event['entity_id'] and twin.owner == operation.event['source_id']
