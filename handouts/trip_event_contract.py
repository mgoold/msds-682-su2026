"""Canonical TripEvent application and Avro contract bundled with Demo 05.

This file is the single source of truth inside the self-contained Demo 05
package for its event model, Avro
conversion functions, schema loading, deterministic teaching data, serializer
settings, and default Confluent wire framing. Its contract remains aligned with
the frozen Demo 04 release, but Demo 05 never imports or runs Demo 04.
"""

from __future__ import annotations

import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from confluent_kafka.schema_registry import topic_subject_name_strategy
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

BUNDLE_DIR = Path(__file__).resolve().parent
SCHEMA_V1_PATH = BUNDLE_DIR / "trip_event_v1.avsc"
SCHEMA_V2_PATH = BUNDLE_DIR / "trip_event_v2_reader.avsc"

EventType = Literal[
    "trip_requested",
    "driver_matched",
    "trip_started",
    "trip_completed",
]
ServiceZone = Literal["north", "south", "west"]
VehicleType = Literal["standard", "xl", "accessible"]

SYNTHETIC_BASE_TIME = datetime(2026, 7, 16, 17, 0, tzinfo=UTC)
SYNTHETIC_EVENT_INTERVAL_SECONDS = 17
SYNTHETIC_LIFECYCLE: tuple[EventType, ...] = (
    "trip_requested",
    "driver_matched",
    "trip_started",
    "trip_completed",
)
SYNTHETIC_ZONES: tuple[ServiceZone, ...] = ("north", "south", "west")


# ============================================================================
# KEY CONCEPT
# Pydantic validates application meaning before serialization and again after
# deserialization. Kafka and Avro do not replace these business rules.
# ============================================================================
class TripEventV1(BaseModel):
    """Strict application model for the version-1 trip event contract."""

    model_config = ConfigDict(extra="forbid", strict=True)

    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")
    event_type: EventType
    rider_id: str = Field(pattern=r"^rider_[0-9]{3}$")
    event_time: AwareDatetime
    zone: ServiceZone
    driver_id: str | None = Field(default=None, pattern=r"^driver_[0-9]{3}$")
    fare: float | None = Field(default=None, ge=0)

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        """Normalize all accepted timestamps to timezone-aware UTC."""

        return value.astimezone(UTC)

    @model_validator(mode="after")
    def enforce_lifecycle_rules(self) -> "TripEventV1":
        """Enforce lifecycle meaning after field-level validation."""

        if self.event_type == "trip_requested":
            if self.driver_id is not None:
                raise ValueError("trip_requested must not include driver_id")
            if self.fare is not None:
                raise ValueError("trip_requested must not include fare")
            return self

        if self.driver_id is None:
            raise ValueError(f"{self.event_type} requires driver_id")

        if self.event_type == "trip_completed":
            if self.fare is None:
                raise ValueError("trip_completed requires fare")
        elif self.fare is not None:
            raise ValueError(f"{self.event_type} must not include fare")
        return self

    @property
    def event_date(self) -> str:
        """Return a downstream-derived UTC date; it is not sent on the wire."""

        return self.event_time.date().isoformat()

    @property
    def event_hour(self) -> int:
        """Return a downstream-derived UTC hour; it is not sent on the wire."""

        return self.event_time.hour

    @property
    def event_weekday(self) -> str:
        """Return a downstream-derived UTC weekday; it is not sent on the wire."""

        return self.event_time.strftime("%A")

    def report_dict(self) -> dict[str, Any]:
        """Return JSON-safe application data plus downstream-derived fields."""

        data = self.model_dump(mode="json")
        data.update(
            {
                "event_date": self.event_date,
                "event_hour": self.event_hour,
                "event_weekday": self.event_weekday,
            }
        )
        return data


class TripEventV2(TripEventV1):
    """Backward-compatible reader model with one optional field."""

    vehicle_type: VehicleType | None = None


def read_schema(path: Path) -> str:
    """Read one distributed Avro schema and fail early if it is malformed."""

    raw = path.read_text(encoding="utf-8")
    json.loads(raw)
    return raw


def schema_v1_str() -> str:
    """Return the canonical version-1 writer schema."""

    return read_schema(SCHEMA_V1_PATH)


def schema_v2_str() -> str:
    """Return the canonical version-2 reader schema."""

    return read_schema(SCHEMA_V2_PATH)


def value_subject(topic: str) -> str:
    """Return the TopicNameStrategy value subject for a topic."""

    return f"{topic}-value"


# ============================================================================
# KEY CONCEPT
# The serializer converts a validated application model into Avro binary. The
# inverse function validates the deserialized dictionary again.
# ============================================================================
def event_to_avro_dict(event: TripEventV1, _ctx: Any = None) -> dict[str, Any]:
    """Convert a validated application event into the Avro writer record."""

    return {
        "trip_id": event.trip_id,
        "event_type": event.event_type,
        "rider_id": event.rider_id,
        "event_time": event.event_time,
        "zone": event.zone,
        "driver_id": event.driver_id,
        "fare": event.fare,
    }


def avro_dict_to_event(data: dict[str, Any], _ctx: Any = None) -> TripEventV1:
    """Validate a deserialized Avro record as the application model."""

    return TripEventV1.model_validate(data)


def avro_dict_to_event_v2(data: dict[str, Any], _ctx: Any = None) -> TripEventV2:
    """Validate a version-1 writer record with the version-2 reader model."""

    return TripEventV2.model_validate(data)


# ============================================================================
# KEY CONCEPT
# The demos use bounded deterministic events. No prior topic data, external
# dataset, or personal data is required.
# ============================================================================
def deterministic_events(count: int, *, seed_offset: int = 0) -> list[TripEventV1]:
    """Create bounded deterministic TripEventV1 records."""

    if count < 1:
        raise ValueError("count must be at least 1")

    base = SYNTHETIC_BASE_TIME + timedelta(minutes=seed_offset)
    events: list[TripEventV1] = []
    for index in range(count):
        event_type = SYNTHETIC_LIFECYCLE[index % len(SYNTHETIC_LIFECYCLE)]
        trip_number = 1000 + seed_offset * 10 + index
        payload: dict[str, Any] = {
            "trip_id": f"trip_{trip_number:04d}",
            "event_type": event_type,
            "rider_id": f"rider_{100 + (index % 30):03d}",
            "event_time": base
            + timedelta(seconds=index * SYNTHETIC_EVENT_INTERVAL_SECONDS),
            "zone": SYNTHETIC_ZONES[index % len(SYNTHETIC_ZONES)],
        }
        if event_type != "trip_requested":
            payload["driver_id"] = f"driver_{200 + (index % 40):03d}"
        if event_type == "trip_completed":
            payload["fare"] = round(18.0 + index * 1.25, 2)
        events.append(TripEventV1.model_validate(payload))
    return events


def synthetic_data_report(
    events: list[TripEventV1],
    *,
    seed_offset: int,
) -> dict[str, Any]:
    """Describe deterministic input without duplicating generation rules."""

    if not events:
        raise ValueError("events must not be empty")
    return {
        "source": "synthetic deterministic events generated locally",
        "prior_kafka_data_required": False,
        "seed_offset": seed_offset,
        "count": len(events),
        "first_trip_id": events[0].trip_id,
        "last_trip_id": events[-1].trip_id,
        "first_event_time": events[0].event_time,
        "event_interval_seconds": SYNTHETIC_EVENT_INTERVAL_SECONDS,
        "lifecycle_cycle": list(SYNTHETIC_LIFECYCLE),
        "zone_cycle": list(SYNTHETIC_ZONES),
    }


def event_key(event: TripEventV1) -> bytes:
    """Use the stable trip identifier as the Kafka key."""

    return event.trip_id.encode("utf-8")


def serializer_conf() -> dict[str, Any]:
    """Return the explicit serializer settings used throughout Demo 05."""

    return {
        "auto.register.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
        "validate.strict": True,
        "validate.strict.allow.default": False,
    }


def deserializer_conf() -> dict[str, Any]:
    """Return the matching explicit deserializer subject strategy."""

    return {"subject.name.strategy": topic_subject_name_strategy}


# ============================================================================
# IMPORTANT NOTE
# This course uses the default five-byte Confluent framing for its Avro value:
# one magic byte and one 32-bit schema ID before the Avro body.
# ============================================================================
def parse_confluent_wire_header(payload: bytes) -> dict[str, int]:
    """Parse the course default magic-byte and schema-ID value prefix."""

    if len(payload) < 5:
        raise ValueError(
            "Confluent-framed Avro payload must contain at least five bytes"
        )
    magic_byte, schema_id = struct.unpack(">bI", payload[:5])
    return {
        "magic_byte": magic_byte,
        "schema_id": schema_id,
        "payload_bytes": len(payload),
        "avro_body_bytes": len(payload) - 5,
    }


def validation_error_summary(exc: ValidationError) -> list[dict[str, Any]]:
    """Return stable, JSON-safe Pydantic validation evidence."""

    return [
        {
            "location": ".".join(str(part) for part in item["loc"]),
            "type": item["type"],
            "message": item["msg"],
        }
        for item in exc.errors(include_url=False, include_context=False)
    ]
