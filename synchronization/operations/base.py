from simulation.event_generator.schemas import TrafficEvent
from synchronization.operation_manager.preconditions import require, newer


class Handler:
    kind = ''

    def apply(self, twin, operation):
        raise NotImplementedError

    def verify(self, twin, operation):
        raise NotImplementedError

    def event(self, operation):
        return TrafficEvent.model_validate(operation.event)
