from .base import Handler, require, newer


class Transfer(Handler):
    kind = 'Transfer'

    def apply(self, twin, operation):
        event = self.event(operation)
        require(twin.association_status == 'attached', 'association required')
        require(newer(event, twin), 'new observation required for ownership change')
        require(twin.owner == operation.payload['previous_owner'], 'previous owner mismatch')
        require(twin.ownership_version == operation.payload['ownership_version'], 'ownership version mismatch')
        require(event.source_id != twin.owner, 'different target owner required')
        twin.owner = event.source_id
        twin.ownership_version += 1

    def verify(self, twin, operation):
        return twin.association_status == 'attached' and twin.owner == operation.event['source_id'] and twin.ownership_version == operation.payload['ownership_version'] + 1
