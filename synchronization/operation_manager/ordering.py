"""FIFO durable inbox order; an event's operations form a dependency chain.
An accepted Transfer completes before its Update. Detach drains earlier operations.
Later observations cannot resurrect a detached entity without explicit insertion.
"""
from synchronization.models import Status


def dependencies_committed(operation, known):
    return all(known[dependency].status == Status.COMMITTED for dependency in operation.dependencies)
