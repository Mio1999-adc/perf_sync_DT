from .base import Handler, require


class Follow(Handler):
    kind = 'Follow'

    def apply(self, twin, operation):
        event = self.event(operation)
        require(event.event_type == 'follow_request' or bool(operation.payload.get('renewal_subscription')), 'explicit subscription request or authorized renewal required')
        require(twin.association_status == 'attached', 'known attached subscription target required')
        subscription = operation.payload.get('renewal_subscription') or event.subscription
        require(set(subscription['variables']) <= set(twin.process_variables), 'unknown process variables')
        previous = twin.subscriptions.get(subscription['subscriber_id'])
        if operation.payload.get('renewal_subscription'):
            require(previous and previous.get('auto_renew') is True and previous['expires_simulation_ms'] <= event.simulation_time_ms, 'subscription renewal not due or not authorized')
        expiry = event.simulation_time_ms + subscription['lifetime_ms']
        require(not previous or expiry > previous['expires_simulation_ms'], 'renewal must extend lifetime')
        twin.subscriptions[subscription['subscriber_id']] = dict(subscription, expires_simulation_ms=expiry)

    def verify(self, twin, operation):
        event = self.event(operation)
        subscription = operation.payload.get('renewal_subscription') or event.subscription
        expected = dict(subscription, expires_simulation_ms=event.simulation_time_ms + subscription['lifetime_ms'])
        return twin.subscriptions.get(subscription['subscriber_id']) == expected
