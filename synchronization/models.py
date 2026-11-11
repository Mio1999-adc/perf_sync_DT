import time
from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field


class Status(str, Enum):
    CREATED = 'CREATED'
    VALIDATED = 'VALIDATED'
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    COMMITTING = 'COMMITTING'
    COMMITTED = 'COMMITTED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    SUPERSEDED = 'SUPERSEDED'


TERMINAL = {Status.COMMITTED, Status.FAILED, Status.CANCELLED, Status.SUPERSEDED}
NEXT = {Status.CREATED: Status.VALIDATED, Status.VALIDATED: Status.QUEUED, Status.QUEUED: Status.RUNNING, Status.RUNNING: Status.COMMITTING, Status.COMMITTING: Status.COMMITTED}


class Operation(BaseModel):
    operation_id: str
    operation_type: str
    event: dict[str, Any]
    dt_id: str
    dependencies: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    status: Status = Status.CREATED
    request_unix_ns: int = Field(default_factory=time.time_ns)
    request_monotonic_ns: int = Field(default_factory=time.monotonic_ns)
    validated_unix_ns: Optional[int] = None
    queued_unix_ns: Optional[int] = None
    start_unix_ns: Optional[int] = None
    commit_unix_ns: Optional[int] = None
    commit_monotonic_ns: Optional[int] = None
    duration_ns: Optional[int] = None
    version_before: Optional[int] = None
    version_after: Optional[int] = None
    owner: Optional[str] = None
    error_category: Optional[str] = None
    error: Optional[str] = None
    clock_session: str = ''
    trace_context: dict[str, str] = Field(default_factory=dict)
    activation_id: Optional[str] = None
    function_metadata: dict[str, Any] = Field(default_factory=dict)
    retry_count: int = 0

    def transition(self, status):
        if self.status in TERMINAL or (status not in {Status.FAILED, Status.CANCELLED, Status.SUPERSEDED} and NEXT.get(self.status) != status):
            raise ValueError(f'invalid transition {self.status} -> {status}')
        self.status = status
