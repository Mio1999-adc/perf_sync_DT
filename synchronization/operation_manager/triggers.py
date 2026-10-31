from synchronization.operation_manager.preconditions import newer


def identify(event, twin):
    if event.event_type == 'follow_request':
        return ['Follow']
    if event.event_type in {'departure', 'removal'}:
        return ['Detach'] if twin and twin.association_status == 'attached' and newer(event, twin) else []
    if twin and not newer(event, twin):
        return []
    if twin and twin.association_status == 'detached' and event.event_type != 'insertion':
        return []
    kinds = []
    if twin is None or twin.association_status != 'attached':
        kinds.append('Attach')
    elif twin.owner != event.source_id:
        kinds.append('Transfer')
    kinds.append('Update')
    if due_subscriptions(event, twin):
        kinds.append('Follow')
    return kinds


def due_subscriptions(event, twin):
    if twin is None or twin.association_status != 'attached':
        return []
    return [dict((key, value) for key, value in subscription.items() if key != 'expires_simulation_ms')
            for subscriber, subscription in sorted(twin.subscriptions.items())
            if subscription.get('auto_renew') is True and subscription['expires_simulation_ms'] <= event.simulation_time_ms]
