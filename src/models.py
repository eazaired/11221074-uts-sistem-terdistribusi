from pydantic import BaseModel, Field, AwareDatetime, field_validator
from typing import Any, List

class Event(BaseModel):
    topic: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    timestamp: AwareDatetime
    source: str = Field(min_length=1)
    payload: dict[str, Any]

    @field_validator("topic")
    @classmethod
    def no_whitespace_topic(cls, v: str) -> str:
        if v.strip() != v:
            raise ValueError("topic must not have leading/trailing spaces")
        return v

class PublishRequest(BaseModel):
    events: List[Event] | Event  # dukung single atau batch

class Stats(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float

class EventList(BaseModel):
    topic: str
    events: list[Event]
