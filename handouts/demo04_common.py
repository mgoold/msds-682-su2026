"""Shared models and helpers for Summer 2026 Demo 04.

This module deliberately separates four concerns:

1. Pydantic validates the application/domain contract.
2. Avro defines the binary wire contract.
3. Schema Registry stores versions and compatibility metadata.
4. Kafka transports key/value bytes without interpreting business fields.

The helpers write only secret-free evidence. Real credentials remain in an
ignored ``.env`` file and never enter reports.
"""

from __future__ import annotations

import json
import os
import re
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

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
from dotenv import load_dotenv

BUNDLE_DIR = Path(__file__).resolve().parent
SCHEMA_V1_PATH = BUNDLE_DIR / "trip_event_v1.avsc"
SCHEMA_V2_PATH = BUNDLE_DIR / "trip_event_v2_reader.avsc"
DEFAULT_TOPIC = "msds682.demo04.trip-events-avro.v1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")

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
    """Strict application model for the version-1 trip event contract.

    Avro enforces field structure and wire types. These model validators add
    business rules that an Avro schema alone does not express, such as which
    fields are legal for each event type.
    """

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
        """Enforce business meaning after field-level validation."""

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
        """Derived field used by consumers; it is not sent on the wire."""

        return self.event_time.date().isoformat()

    @property
    def event_hour(self) -> int:
        """Derived UTC hour used by downstream aggregation."""

        return self.event_time.hour

    @property
    def event_weekday(self) -> str:
        """Derived UTC weekday used by downstream enrichment."""

        return self.event_time.strftime("%A")

    def report_dict(self) -> dict[str, Any]:
        """Return JSON-safe application data plus derived fields."""

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


class ConnectionConfigError(RuntimeError):
    """Raised when required Kafka or Schema Registry settings are absent."""


def read_schema(path: Path) -> str:
    """Read and normalize one Avro schema file."""

    raw = path.read_text(encoding="utf-8")
    json.loads(raw)  # fail early if the distributed file is malformed
    return raw


def schema_v1_str() -> str:
    return read_schema(SCHEMA_V1_PATH)


def schema_v2_str() -> str:
    return read_schema(SCHEMA_V2_PATH)


def avro_subject(topic: str) -> str:
    """Return the explicit topic-name-strategy subject for a value schema."""

    return f"{topic}-value"


# ============================================================================
# KEY CONCEPT
# The serializer converts this validated application object into Avro binary.
# The matching function below validates the deserialized record again.
# ============================================================================
def event_to_avro_dict(event: TripEventV1, _ctx: Any = None) -> dict[str, Any]:
    """Convert the application model into the Avro writer record.

    ``fastavro`` understands timezone-aware ``datetime`` values for the
    ``timestamp-millis`` logical type, so no naive local timestamp is emitted.
    """

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
# Demo 04 uses synthetic deterministic data. No prior topic data or personal
# data is required. The same count and seed_offset create the same events.
# ============================================================================
def deterministic_events(count: int, *, seed_offset: int = 0) -> list[TripEventV1]:
    """Create bounded deterministic events without a random-number generator.

    ``seed_offset`` selects a reproducible scenario: it shifts the base time
    and numeric trip IDs. Lifecycle values and zones then cycle by index. Every
    generated dictionary passes through ``TripEventV1`` before it is returned.
    """

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
            "event_time": base + timedelta(
                seconds=index * SYNTHETIC_EVENT_INTERVAL_SECONDS
            ),
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
    """Describe the reproducible input without copying every generation rule."""

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
    """Use a stable trip identifier as the Kafka key."""

    return event.trip_id.encode("utf-8")


def load_dotenv_for_demo() -> Path | None:
    """Load ``.env`` from the working directory or the script directory."""

    candidates = (Path.cwd() / ".env", BUNDLE_DIR / ".env")
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(candidate, override=False)
            return candidate
    load_dotenv(override=False)
    return None


def kafka_config(*, client_id: str | None = None) -> dict[str, str]:
    """Load Kafka client settings and fail before making a network request."""

    load_dotenv_for_demo()
    config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }
    if client_id:
        config["client.id"] = client_id

    missing = [key for key in ("bootstrap.servers", "sasl.username", "sasl.password") if not config[key]]
    if missing:
        env_names = {
            "bootstrap.servers": "BOOTSTRAP_SERVERS",
            "sasl.username": "SASL_USERNAME",
            "sasl.password": "SASL_PASSWORD",
        }
        raise ConnectionConfigError(
            "Missing required Kafka .env values: " + ", ".join(env_names[key] for key in missing)
        )
    return config


def schema_registry_config() -> dict[str, str]:
    """Load Schema Registry URL and credentials separately from Kafka."""

    load_dotenv_for_demo()
    url = os.getenv("SCHEMA_REGISTRY_URL", "")
    api_key = os.getenv("SCHEMA_REGISTRY_API_KEY", "")
    api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET", "")
    missing = [
        name
        for name, value in (
            ("SCHEMA_REGISTRY_URL", url),
            ("SCHEMA_REGISTRY_API_KEY", api_key),
            ("SCHEMA_REGISTRY_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise ConnectionConfigError("Missing required Schema Registry .env values: " + ", ".join(missing))
    return {
        "url": url,
        "basic.auth.user.info": f"{api_key}:{api_secret}",
    }


def topic_name() -> str:
    """Return the dedicated Avro topic, avoiding JSON/Avro mixing."""

    load_dotenv_for_demo()
    return os.getenv("DEMO04_TOPIC_NAME", DEFAULT_TOPIC)


def consumer_group_id(suffix: str, run_id: str | None = None) -> str:
    """Build a safe, deterministic group ID from the configured prefix."""

    load_dotenv_for_demo()
    prefix = os.getenv("CONSUMER_GROUP_ID_PREFIX", "msds682-su2026")
    parts = [prefix, suffix]
    if run_id:
        parts.append(run_id)
    raw = "-".join(parts)
    return re.sub(r"[^A-Za-z0-9._-]+", "-", raw)[:220]


def validate_run_id(run_id: str) -> str:
    """Return a safe run ID for Kafka identifiers and evidence paths.

    Run IDs become one directory component under ``outputs/runs``. Rejecting
    path separators, whitespace, and traversal tokens prevents a CLI value
    from escaping that evidence directory or creating ambiguous run names.
    """

    value = run_id.strip()
    if not RUN_ID_PATTERN.fullmatch(value) or value in {".", ".."}:
        raise ValueError(
            "--run-id must be 1-80 characters, start with a letter or digit, "
            "and contain only letters, digits, '.', '_', or '-'"
        )
    return value


# ============================================================================
# IMPORTANT NOTE
# Evidence may show hosts and credential-presence booleans, never secret values.
# ============================================================================
def safe_kafka_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Summarize connection state without returning credential values."""

    bootstrap = str(config.get("bootstrap.servers", ""))
    host = bootstrap.split(",", 1)[0]
    return {
        "bootstrap_host": host,
        "security_protocol": config.get("security.protocol"),
        "sasl_mechanism": config.get("sasl.mechanisms"),
        "client_id": config.get("client.id"),
        "username_present": bool(config.get("sasl.username")),
        "password_present": bool(config.get("sasl.password")),
    }


def safe_registry_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Summarize Schema Registry state without returning key or secret."""

    url = str(config.get("url", ""))
    parsed = urlsplit(url if "://" in url else f"//{url}")
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        port = None
    url_host = f"{hostname}:{port}" if hostname and port is not None else hostname
    return {
        "url_host": url_host,
        "basic_auth_present": bool(config.get("basic.auth.user.info")),
    }


# ============================================================================
# KEY CONCEPT
# The default wire header stores a magic byte and schema ID. Schema Registry
# stores the schema itself; Kafka stores this framing plus the Avro body.
# ============================================================================
def parse_confluent_wire_header(payload: bytes) -> dict[str, int]:
    """Parse the default Confluent framing: magic byte + 32-bit schema ID."""

    if len(payload) < 5:
        raise ValueError("Confluent-framed Avro payload must contain at least five bytes")
    magic_byte, schema_id = struct.unpack(">bI", payload[:5])
    return {
        "magic_byte": magic_byte,
        "schema_id": schema_id,
        "payload_bytes": len(payload),
        "avro_body_bytes": len(payload) - 5,
    }


def serializer_conf() -> dict[str, Any]:
    """Use explicit serializer settings for the Summer 2026 course."""

    return {
        "auto.register.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
        "validate.strict": True,
        "validate.strict.allow.default": False,
    }


def deserializer_conf() -> dict[str, Any]:
    """Use the same explicit subject strategy on the read path."""

    return {"subject.name.strategy": topic_subject_name_strategy}


def validation_error_summary(exc: ValidationError) -> list[dict[str, Any]]:
    """Return stable, JSON-safe validation evidence."""

    rows: list[dict[str, Any]] = []
    for item in exc.errors(include_url=False, include_context=False):
        rows.append(
            {
                "location": ".".join(str(part) for part in item["loc"]),
                "type": item["type"],
                "message": item["msg"],
            }
        )
    return rows


def write_json_report(run_id: str, demo_name: str, report: dict[str, Any]) -> Path:
    """Write reproducible, secret-free evidence under ``outputs/runs``."""

    safe_run_id = validate_run_id(run_id)
    output_dir = Path("outputs") / "runs" / safe_run_id / demo_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "report.json"
    output_file.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=_json_default) + "\n",
        encoding="utf-8",
    )
    return output_file


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
