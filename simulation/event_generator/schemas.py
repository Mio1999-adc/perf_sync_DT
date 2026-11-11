"""Simulation/source timestamps are simulation milliseconds; emission is Unix ms."""
import hashlib
import json
import math
from typing import Any, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


class TrafficEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["1.0"] = "1.0"
    event_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    entity_id: str = Field(min_length=1)
    entity_type: Literal["vehicle", "traffic_signal", "road_segment", "rsu"]
    event_type: Literal["insertion", "observation", "departure", "follow_request", "removal"]
    simulation_time_ms: int = Field(ge=0)
    source_timestamp_ms: int = Field(ge=0)
    sequence_number: int = Field(ge=0)
    source_id: str = Field(min_length=1)
    emitted_at_unix_ms: Optional[int] = Field(default=None, ge=0)
    process_variables: dict[str, Any] = Field(default_factory=dict)
    subscription: Optional[dict[str, Any]] = None
    trace_context: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_payload(self):
        if self.source_timestamp_ms > self.simulation_time_ms:
            raise ValueError("source timestamp exceeds simulation time")
        if self.event_type == "follow_request":
            s = self.subscription or {}
            if not isinstance(s.get("subscriber_id"), str) or not s["subscriber_id"].strip():
                raise ValueError("subscriber identity required")
            if not isinstance(s.get("variables"), list) or not s["variables"] or not all(isinstance(v, str) and v for v in s["variables"]):
                raise ValueError("nonempty process variable names required")
            if 'auto_renew' in s and type(s['auto_renew']) is not bool:
                raise ValueError('auto_renew must be boolean')
            if type(s.get("lifetime_ms")) is not int or not 0 < s["lifetime_ms"] <= 86400000:
                raise ValueError("invalid subscription lifetime")
        # Reject NaN/Infinity and non-JSON payloads before durable ingestion.
        json.dumps(self.model_dump(), allow_nan=False)
        return self

    @property
    def deduplication_id(self):
        return hashlib.sha256(f"{self.run_id}:{self.entity_type}:{self.entity_id}:{self.event_type}:{self.sequence_number}".encode()).hexdigest()

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.model_dump(exclude={"emitted_at_unix_ms", "trace_context"}), sort_keys=True).encode()).hexdigest()
