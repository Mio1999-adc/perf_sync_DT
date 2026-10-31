from pydantic import BaseModel, Field
from typing import Any, Optional
from urllib.parse import quote


def digital_id(event):
    return 'perfsync:' + quote(f'{event.run_id}/{event.entity_type}/{event.entity_id}', safe='')


class Twin(BaseModel):
    dt_id: str
    physical_id: str
    entity_type: str
    run_id: str
    process_variables: dict[str, Any] = Field(default_factory=dict)
    version: int = 0
    source_timestamp_ms: int = -1
    sequence_number: int = -1
    association_status: str = 'unlinked'
    subscriptions: dict[str, Any] = Field(default_factory=dict)
    owner: Optional[str] = None
    ownership_version: int = 0
    last_operation: Optional[str] = None
    synchronization_status: str = 'unlinked'
    applied_operations: dict[str, Any] = Field(default_factory=dict)
