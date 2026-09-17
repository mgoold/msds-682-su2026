"""HTTP, application, and Avro contracts for independent Assignment 2 data."""

from __future__ import annotations

import json
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from confluent_kafka.schema_registry import topic_subject_name_strategy
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "trip_event_v1.avsc"
BASE_TIME = datetime(2026, 7, 23, 16, 0, tzinfo=UTC)
ServiceZone = Literal["north", "south", "west"]


class CreateTripRequest(BaseModel):
    """Strict HTTP request accepted by ``POST /trip-requests``."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
    sequence_number: int = Field(ge=0, lt=100)
    request_id: str = Field(pattern=r"^request_[0-9]{4}$")
    rider_id: str = Field(pattern=r"^rider_[0-9]{3}$")
    requested_at: AwareDatetime
    zone: ServiceZone

    @field_validator("requested_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        """Normalize accepted timestamps to UTC."""

        return value.astimezone(UTC)


class TripEventV1(BaseModel):
    """Strict application model validated on both sides of Kafka."""

    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
    sequence_number: int = Field(ge=0, lt=100)
    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")
    event_type: Literal["trip_requested"]
    rider_id: str = Field(pattern=r"^rider_[0-9]{3}$")
    event_time: AwareDatetime
    zone: ServiceZone

    @field_validator("event_time")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        """Normalize deserialized timestamps to UTC."""

        return value.astimezone(UTC)


class PublishReceipt(BaseModel):
    """Secret-free delivery evidence returned by the publisher."""

    model_config = ConfigDict(extra="forbid")

    topic: str
    key: str
    partition: int
    offset: int
    delivery: Literal["broker_acknowledged"]


class TripAcceptedResponse(BaseModel):
    """HTTP 202 response after broker acknowledgement."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["accepted"]
    request_id: str
    trip_id: str
    topic: str
    delivery: Literal["broker_acknowledged"]


class PublishError(RuntimeError):
    """Raised when Kafka does not acknowledge a submitted event."""


def request_to_event(request: CreateTripRequest) -> TripEventV1:
    """Map the HTTP boundary model into the canonical Kafka event."""

    # ==================== CODE START HERE ====================
    # TODO: map every required request field into one strict TripEventV1.
    raise NotImplementedError("Implement request_to_event")
    # ===================== CODE ENDS HERE =====================


def schema_str() -> str:
    """Read and validate the distributed Avro writer schema."""

    raw = SCHEMA_PATH.read_text(encoding="utf-8")
    json.loads(raw)
    return raw


def event_to_avro_dict(event: TripEventV1, _ctx: Any = None) -> dict[str, Any]:
    """Convert a validated application event into an Avro writer record."""

    # ==================== CODE START HERE ====================
    # TODO: return every field required by schemas/trip_event_v1.avsc.
    raise NotImplementedError("Implement event_to_avro_dict")
    # ===================== CODE ENDS HERE =====================


def avro_dict_to_event(data: dict[str, Any], _ctx: Any = None) -> TripEventV1:
    """Validate one deserialized Avro record as the application model."""

    # ==================== CODE START HERE ====================
    # TODO: validate the decoded dictionary with TripEventV1.
    raise NotImplementedError("Implement avro_dict_to_event")
    # ===================== CODE ENDS HERE =====================


def serializer_conf() -> dict[str, Any]:
    """Use one explicit topic-based Schema Registry subject strategy."""

    return {
        "auto.register.schemas": True,
        "normalize.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
    }


def event_key(event: TripEventV1) -> bytes:
    """Return the stable UTF-8 Kafka key."""

    return event.trip_id.encode("utf-8")


def value_subject(topic: str) -> str:
    """Return the value subject under topic-name strategy."""

    return f"{topic}-value"


def deterministic_requests(run_id: str, count: int = 12) -> list[CreateTripRequest]:
    """Create bounded synthetic HTTP inputs without prior Kafka data."""

    if count != 12:
        raise ValueError("The Assignment 2 base seeder requires exactly 12 requests")
    offset = zlib.crc32(run_id.encode("utf-8")) % 180
    base = BASE_TIME + timedelta(minutes=offset)
    zones: tuple[ServiceZone, ...] = ("north", "south", "west")
    requests: list[CreateTripRequest] = []
    for index in range(count):
        number = 5000 + offset * 20 + index
        requests.append(
            CreateTripRequest(
                run_id=run_id,
                sequence_number=index,
                request_id=f"request_{number:04d}",
                rider_id=f"rider_{500 + index:03d}",
                requested_at=base + timedelta(seconds=index * 11),
                zone=zones[index % len(zones)],
            )
        )
    return requests
