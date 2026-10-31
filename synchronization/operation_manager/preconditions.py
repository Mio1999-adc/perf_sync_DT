class PreconditionError(ValueError):
    pass


def newer(event, twin):
    return event.sequence_number > twin.sequence_number and event.source_timestamp_ms >= twin.source_timestamp_ms


def require(condition, message):
    if not condition:
        raise PreconditionError(message)
