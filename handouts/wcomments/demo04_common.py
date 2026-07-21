"""
================================================================================
DEMO 04 - SHARED SCHEMA / AVRO MODULE  (annotated tutorial copy)
================================================================================

READ demo02_producer_common.py FIRST. This module is its Demo 04 counterpart,
but with a much stronger stance on data contracts.

THE BIG QUESTION DEMO 04 ANSWERS
    Demo 02 left a hole. Kafka stores OPAQUE BYTES and validates nothing, so
    what actually stops a producer from writing garbage that breaks every
    consumer? In Demo 02 the answer was "nothing but good manners" - both sides
    imported the same TripEvent class and chose to honor it.

    Demo 04 introduces real machinery. Crucially, it introduces TWO SEPARATE
    LAYERS, and telling them apart is the single most important idea here:

    ┌──────────────────────────────────────────────────────────────────────┐
    │ LAYER 1 - APPLICATION / DOMAIN   (Pydantic, in this file)            │
    │   Enforces MEANING: fare >= 0; a completed trip must have a fare;    │
    │   a requested trip must NOT have a driver.                           │
    │   Runs entirely client-side, on both write and read.                 │
    ├──────────────────────────────────────────────────────────────────────┤
    │ LAYER 2 - WIRE / STRUCTURE       (Avro + Schema Registry)            │
    │   Enforces SHAPE: field names, field types, defaults, and whether a  │
    │   new schema version is compatible with the old one.                 │
    │   The schema lives on a REGISTRY SERVER, separate from the brokers.  │
    └──────────────────────────────────────────────────────────────────────┘

    NEITHER LAYER REPLACES THE OTHER. Avro will happily encode fare = -10.0 -
    it is a valid `double`. Only Layer 1 rejects it. Conversely, Pydantic cannot
    stop a producer team from renaming a field and breaking every consumer;
    only the Registry's compatibility checks can.

THE FOUR CONCERNS THIS MODULE SEPARATES (from the original docstring)
    1. Pydantic validates the application/domain contract.
    2. Avro defines the binary wire contract.
    3. Schema Registry stores versions and compatibility metadata.
    4. Kafka transports key/value bytes without interpreting business fields.

WHAT IS NEW COMPARED WITH DEMO 02
    - A far stricter Pydantic model (forbids unknown fields, requires
      timezone-aware timestamps, enforces cross-field lifecycle rules).
    - Avro schema files (.avsc) and converters to/from Avro records.
    - A SECOND set of credentials: Schema Registry is a different service from
      Kafka, with its own URL, key, and secret.
    - A parser for the 5-byte "Confluent wire format" header.
================================================================================
"""

from __future__ import annotations

import json
import os
import re
import struct   # for unpacking the binary wire header (see the bottom of the file)
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

# The Schema Registry client's standard naming strategy. It maps a topic to the
# "subject" under which its schema is registered - conventionally
# "<topic>-value". Being explicit about this rather than relying on a default
# means the read and write paths cannot silently disagree.
from confluent_kafka.schema_registry import topic_subject_name_strategy

from pydantic import (
    AwareDatetime,      # a datetime that MUST carry timezone info
    BaseModel,
    ConfigDict,         # per-model settings (strict mode, extra fields, ...)
    Field,
    ValidationError,
    field_validator,    # validates/normalizes ONE field
    model_validator,    # validates the WHOLE object, after fields are set
)
from dotenv import load_dotenv


# =============================================================================
# PATHS AND CONSTANTS
# =============================================================================

# The directory containing this file. Path(__file__).resolve().parent is the
# reliable way to find files that ship alongside your code, independent of the
# directory the user happened to run the command from.
BUNDLE_DIR = Path(__file__).resolve().parent

# THE AVRO SCHEMA FILES. ".avsc" is the conventional extension for an Avro
# schema, which is itself a JSON document describing fields and types.
#   v1        - the WRITER schema: what producers use to encode.
#   v2 reader - a newer READER schema, used to demonstrate schema evolution
#               (Demo 04B reads v1-encoded data with this v2 schema).
SCHEMA_V1_PATH = BUNDLE_DIR / "trip_event_v1.avsc"
SCHEMA_V2_PATH = BUNDLE_DIR / "trip_event_v2_reader.avsc"

# A DEDICATED TOPIC, separate from the Demo 02 JSON topic.
#
# This separation is deliberate and important: Demo 02's topic contains UTF-8
# JSON, while this one contains Confluent-framed Avro binary. Mixing the two
# formats in one topic would break consumers, because a consumer has to pick a
# deserializer in advance and there is no reliable way to tell the formats apart
# per message. ONE TOPIC, ONE FORMAT.
DEFAULT_TOPIC = "msds682.demo04.trip-events-avro.v1"

# Validates the --run-id CLI argument. Anchored (^...$), 1-80 characters,
# starting alphanumeric. See validate_run_id() below for why this matters.
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")

# --- Type aliases: closed sets of allowed values --------------------------
EventType = Literal[
    "trip_requested",
    "driver_matched",
    "trip_started",
    "trip_completed",
]
ServiceZone = Literal["north", "south", "west"]

# Only used by the V2 model - this is the field ADDED in the evolved schema.
VehicleType = Literal["standard", "xl", "accessible"]

# --- Synthetic data settings ---------------------------------------------
# Note: no RNG at all in Demo 04. The generator is a pure function of the index,
# which is even stronger reproducibility than Demo 02's seeded approach.
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

    COMPARE WITH DEMO 02's TripEvent. That model was permissive: plain string
    fields, a loose timestamp, and no cross-field rules. This one is strict in
    four distinct ways, each explained below.
    """

    # ---- STRICTNESS SETTING 1 AND 2 ---------------------------------
    # extra="forbid":
    #   Reject any field that is not declared here. The default would silently
    #   IGNORE unknown fields, which is how schema drift hides: a producer adds
    #   "surge_multiplier", consumers quietly drop it, and nobody notices until
    #   a report is wrong. Forbidding makes the change loud and immediate.
    #
    # strict=True:
    #   Disable pydantic's helpful type coercion. By default pydantic would
    #   accept the STRING "27.50" for a float field and convert it. That
    #   convenience hides bugs - if a producer is sending strings where numbers
    #   belong, you want to know now, not after it reaches the topic.
    model_config = ConfigDict(extra="forbid", strict=True)

    # ---- STRICTNESS SETTING 3: format constraints via regex ---------
    # Not merely "a string", but a string matching an exact shape.
    # ^trip_[0-9]{4}$ means literally "trip_" followed by exactly four digits.
    # This catches a whole class of integration bugs where one service sends
    # "trip-1234" or a bare "1234".
    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")

    event_type: EventType

    rider_id: str = Field(pattern=r"^rider_[0-9]{3}$")

    # AwareDatetime REQUIRES timezone information. A "naive" datetime like
    # 2026-07-16T17:00:00 is genuinely ambiguous across systems - 17:00 where?
    # For event data crossing machines and regions, ambiguity is a real bug, so
    # the type system rejects it outright. (Demo 04A has a test case proving a
    # naive timestamp is refused.)
    event_time: AwareDatetime

    zone: ServiceZone

    # Optional, but still format-checked WHEN present.
    driver_id: str | None = Field(default=None, pattern=r"^driver_[0-9]{3}$")

    # ge=0 is the business rule Avro cannot express. Avro can say "this is a
    # double"; only this says "and it must not be negative".
    fare: float | None = Field(default=None, ge=0)

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        """Normalize all accepted timestamps to timezone-aware UTC.

        A field_validator runs on ONE field after its type is checked. This one
        does not reject anything - it CONVERTS. An input of 17:05:00-07:00 is
        valid (it is aware), but it is stored as 00:05:00+00:00.

        WHY NORMALIZE: downstream code compares, sorts, and buckets timestamps.
        Doing that across mixed offsets is a reliable source of off-by-hours
        bugs. Converting once, at the boundary, means everything inside the
        system speaks UTC.
        """
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def enforce_lifecycle_rules(self) -> "TripEventV1":
        """Enforce business meaning after field-level validation.

        THIS METHOD IS THE HEART OF THE "TWO LAYERS" ARGUMENT.

        mode="after" means it runs once every individual field has been
        validated, so `self` is fully populated and it can reason about
        RELATIONSHIPS BETWEEN FIELDS.

        Avro fundamentally cannot express any of this. An Avro schema can say
        "driver_id is an optional string". It has no way to say "optional when
        the event is a request, but REQUIRED for every other event type". That
        is conditional, cross-field logic - and it lives here, in application
        code, forever.
        """
        # RULE 1: a trip that has only been REQUESTED cannot already have a
        # driver or a fare. Those fields being present means someone
        # constructed a nonsensical event.
        if self.event_type == "trip_requested":
            if self.driver_id is not None:
                raise ValueError("trip_requested must not include driver_id")
            if self.fare is not None:
                raise ValueError("trip_requested must not include fare")
            return self

        # RULE 2: every later stage REQUIRES a driver. If we reach this line the
        # event is not a request, so a driver must have been assigned.
        if self.driver_id is None:
            raise ValueError(f"{self.event_type} requires driver_id")

        # RULE 3: exactly one stage carries a fare - the completed one.
        if self.event_type == "trip_completed":
            if self.fare is None:
                raise ValueError("trip_completed requires fare")
        elif self.fare is not None:
            # matched/started must NOT have a fare - the trip has not finished,
            # so a final price cannot be known yet.
            raise ValueError(f"{self.event_type} must not include fare")

        return self   # a model_validator must return the model

    # ---- DERIVED FIELDS: computed locally, never transmitted ---------
    # These are @property, not model fields, so they are NOT part of the Avro
    # record and never travel over the wire. They are computed by whoever holds
    # the object.
    #
    # WHY THIS IS THE RIGHT DESIGN: sending event_date, event_hour, and
    # event_weekday would waste bytes on every message and, worse, create three
    # ways for the data to become internally inconsistent (imagine a message
    # whose event_date disagrees with its event_time). Derive, do not duplicate.

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
        return self.event_time.strftime("%A")   # e.g. "Thursday"

    def report_dict(self) -> dict[str, Any]:
        """Return JSON-safe application data plus derived fields.

        model_dump(mode="json") converts to plain Python types that json.dumps
        can handle - datetimes become ISO strings, and so on. The derived
        properties are then attached, so the evidence file shows both what was
        transmitted and what a consumer would compute from it.
        """
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
    """Backward-compatible reader model with one optional field.

    SCHEMA EVOLUTION IN ONE CLASS.

    It inherits everything from V1 and adds exactly one field, which is
    OPTIONAL WITH A DEFAULT of None. That default is the entire reason this is
    "backward compatible":

        a V1 message has no vehicle_type
            -> a V2 reader supplies the default (None)
            -> it reads successfully

    Had the new field been REQUIRED, a V2 reader could not read V1 data at all,
    because there would be no value to use and no way to invent one. That is
    what "breaking change" means in practice.

    THE RULE TO REMEMBER: to add a field compatibly, give it a default.
    Removing or renaming a field is not backward compatible, which is why such
    changes usually mean a new topic (…v2) rather than an in-place edit.
    """

    vehicle_type: VehicleType | None = None


class ConnectionConfigError(RuntimeError):
    """Raised when required Kafka or Schema Registry settings are absent.

    A custom exception type (rather than a bare RuntimeError) lets callers catch
    THIS specific failure - "your .env is incomplete" - and print setup help,
    while letting genuine runtime errors propagate.
    """


# =============================================================================
# AVRO SCHEMA LOADING
# =============================================================================
def read_schema(path: Path) -> str:
    """Read and normalize one Avro schema file.

    Note it returns the RAW TEXT, not a parsed object - the Avro serializer
    wants a schema string.

    The json.loads() call in the middle is a deliberate fail-fast check: it
    parses the file purely to confirm it is valid JSON, then discards the
    result. If the shipped .avsc is malformed, you get a clear JSON error here
    rather than a confusing serializer failure much later.
    """
    raw = path.read_text(encoding="utf-8")
    json.loads(raw)  # fail early if the distributed file is malformed
    return raw


def schema_v1_str() -> str:
    """The WRITER schema - what producers use to encode."""
    return read_schema(SCHEMA_V1_PATH)


def schema_v2_str() -> str:
    """The evolved READER schema - used to prove backward compatibility."""
    return read_schema(SCHEMA_V2_PATH)


def avro_subject(topic: str) -> str:
    """Return the explicit topic-name-strategy subject for a value schema.

    WHAT A "SUBJECT" IS: Schema Registry does not store schemas under topic
    names; it stores them under SUBJECTS, and each subject holds an ordered list
    of schema versions. The standard convention (TopicNameStrategy) is:

        subject for the value schema  =  "<topic>-value"
        subject for the key schema    =  "<topic>-key"

    Keys and values get separate subjects because they can evolve independently.
    """
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

    THE BRIDGE FROM LAYER 1 TO LAYER 2. AvroSerializer does not understand
    pydantic models, so you hand it a `to_dict` function - this one - and it
    calls it for every event.

    Note what is ABSENT: the derived properties (event_date, event_hour,
    event_weekday). Only the seven real fields are listed, so only they are
    encoded. The dict keys must match the Avro schema's field names exactly.

    The `_ctx` argument is the SerializationContext the library passes in
    (topic and whether this is a key or value). The leading underscore marks it
    as required-by-signature but unused here.
    """
    return {
        "trip_id": event.trip_id,
        "event_type": event.event_type,
        "rider_id": event.rider_id,

        # Passed as a real datetime object, not a string. The Avro schema
        # declares this as a "timestamp-millis" LOGICAL TYPE - stored as a plain
        # int64 of milliseconds since the epoch, with a label telling readers to
        # interpret it as a time. Because the datetime is timezone-aware (the
        # model guarantees it), the conversion is unambiguous.
        "event_time": event.event_time,

        "zone": event.zone,
        "driver_id": event.driver_id,
        "fare": event.fare,
    }


def avro_dict_to_event(data: dict[str, Any], _ctx: Any = None) -> TripEventV1:
    """Validate a deserialized Avro record as the application model.

    THE RETURN JOURNEY, and the reason validation happens twice.

    AvroDeserializer produces a plain dict that satisfies the Avro STRUCTURE.
    Passing it through the model re-applies the BUSINESS RULES - because Avro
    checked shape, not meaning. A record with fare = -10.0 decodes perfectly and
    is still invalid; this is where that is caught.
    """
    return TripEventV1.model_validate(data)


def avro_dict_to_event_v2(data: dict[str, Any], _ctx: Any = None) -> TripEventV2:
    """Validate a version-1 writer record with the version-2 reader model.

    Used by Demo 04B to demonstrate evolution: the same V1 bytes are decoded
    with both models. The V2 result simply has vehicle_type = None, supplied by
    the default. That is backward compatibility, demonstrated rather than
    asserted.
    """
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

    NOTE THE ABSENCE OF `random` ENTIRELY. Demo 02 used a seeded RNG; here every
    value is a pure function of `index`, which is even more predictable - there
    is no generator state to get out of step. `seed_offset` just picks a
    distinct, repeatable scenario so different demos can generate
    non-overlapping data.
    """
    if count < 1:
        raise ValueError("count must be at least 1")

    base = SYNTHETIC_BASE_TIME + timedelta(minutes=seed_offset)
    events: list[TripEventV1] = []

    for index in range(count):
        # Cycle the four lifecycle stages, exactly as in Demo 02.
        event_type = SYNTHETIC_LIFECYCLE[index % len(SYNTHETIC_LIFECYCLE)]
        trip_number = 1000 + seed_offset * 10 + index

        payload: dict[str, Any] = {
            # :04d zero-pads to four digits, satisfying the ^trip_[0-9]{4}$
            # pattern on the model. The format constraint and the generator
            # have to agree, or nothing validates.
            "trip_id": f"trip_{trip_number:04d}",
            "event_type": event_type,
            "rider_id": f"rider_{100 + (index % 30):03d}",
            "event_time": base + timedelta(
                seconds=index * SYNTHETIC_EVENT_INTERVAL_SECONDS
            ),
            "zone": SYNTHETIC_ZONES[index % len(SYNTHETIC_ZONES)],
        }

        # ---- THE GENERATOR OBEYS THE LIFECYCLE RULES --------------------
        # It must, because every payload is validated below. Adding driver_id to
        # a trip_requested event here would raise immediately. The generator and
        # the model are two expressions of the same contract.
        if event_type != "trip_requested":
            payload["driver_id"] = f"driver_{200 + (index % 40):03d}"
        if event_type == "trip_completed":
            payload["fare"] = round(18.0 + index * 1.25, 2)

        # VALIDATE ON THE WAY OUT. The function cannot return an invalid event,
        # so any downstream code can trust what it receives.
        events.append(TripEventV1.model_validate(payload))

    return events


def synthetic_data_report(
    events: list[TripEventV1],
    *,
    seed_offset: int,
) -> dict[str, Any]:
    """Describe the reproducible input without copying every generation rule.

    Documents the INPUTS in the evidence file, so a reader can tell exactly what
    data a run used and regenerate it. "prior_kafka_data_required: False" is a
    deliberate statement that this demo depends on no pre-existing topic
    contents - it is self-contained.
    """
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
    """Use a stable trip identifier as the Kafka key.

    IDENTICAL IN PURPOSE to Demo 02's event_key. Note the KEY is still plain
    UTF-8 bytes even though the VALUE is now Avro. Keys and values are
    serialized independently, and a simple string key is perfectly normal -
    it only needs to hash consistently for partitioning.
    """
    return event.trip_id.encode("utf-8")


# =============================================================================
# CONFIGURATION - now for TWO separate services
# =============================================================================
def load_dotenv_for_demo() -> Path | None:
    """Load ``.env`` from the working directory or the script directory.

    override=False means real environment variables WIN over .env values, which
    is what you want in CI or production, where secrets are injected by the
    platform rather than by a file.

    Returns which file was used (or None), which is handy when debugging "why
    are my credentials not being picked up?".
    """
    candidates = (Path.cwd() / ".env", BUNDLE_DIR / ".env")
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(candidate, override=False)
            return candidate

    # Fall back to python-dotenv's own search, in case a .env lives elsewhere.
    load_dotenv(override=False)
    return None


def kafka_config(*, client_id: str | None = None) -> dict[str, str]:
    """Load Kafka client settings and fail before making a network request.

    Same five keys as Demo 02 - the connection settings did not change just
    because the payload format did.
    """
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

    # Only the three genuinely user-supplied values are checked; the other two
    # have sensible defaults that are correct for Confluent Cloud.
    missing = [key for key in ("bootstrap.servers", "sasl.username", "sasl.password") if not config[key]]
    if missing:
        # Translate internal client keys back to the .env names the user edits.
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
    """Load Schema Registry URL and credentials separately from Kafka.

    A POINT STUDENTS MISS CONSTANTLY: Schema Registry is a DIFFERENT SERVICE
    from the Kafka brokers. Different hostname, different API key, different
    secret. Your Kafka credentials will not authenticate against it.

        Kafka brokers    <- BOOTSTRAP_SERVERS + SASL_USERNAME/PASSWORD
        Schema Registry  <- SCHEMA_REGISTRY_URL + SCHEMA_REGISTRY_API_KEY/SECRET

    So an Avro producer talks to two services: it fetches or registers a schema
    with the Registry, then sends the encoded bytes to a broker.

    Note the credential format: the client wants ONE string "key:secret" under
    the key "basic.auth.user.info" (HTTP Basic authentication), rather than two
    separate settings.
    """
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
    """Return the dedicated Avro topic, avoiding JSON/Avro mixing.

    The docstring states the rule directly: this demo must not write Avro into
    the Demo 02 JSON topic, because a consumer cannot switch deserializers per
    message.
    """
    load_dotenv_for_demo()
    return os.getenv("DEMO04_TOPIC_NAME", DEFAULT_TOPIC)


def consumer_group_id(suffix: str, run_id: str | None = None) -> str:
    """Build a safe, deterministic group ID from the configured prefix.

    Same idea as Demo 03's default_group_id: a predictable identity so progress
    is remembered across runs, with the shared prefix keeping students on one
    cluster from colliding.

    The re.sub replaces any disallowed character, and [:220] truncates to stay
    within Kafka's identifier length limit.
    """
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

    THIS IS A SECURITY CHECK, and worth understanding as a general habit.
    The run_id is interpolated into a filesystem path. Without validation,
    --run-id "../../etc/something" would write OUTSIDE the intended directory -
    a path traversal bug. The regex allows only letters, digits, dot,
    underscore and hyphen (no "/" and no whitespace), and the explicit
    {".", ".."} check blocks the two directory names that are dangerous even
    though they match the pattern.

    Rule of thumb: never build a path from user input without validating it.
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
    """Summarize connection state without returning credential values.

    Same principle as Demo 02: hostnames and BOOLEANS, never secrets.
    The split(",", 1)[0] keeps only the first host when bootstrap.servers holds
    a comma-separated list.
    """
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
    """Summarize Schema Registry state without returning key or secret.

    Extra care is needed here because the Registry credential is embedded IN a
    string ("key:secret") and, in some setups, could even appear inside a URL.
    So rather than printing the URL, this parses it and emits only host:port.

    urlsplit needs a scheme to parse correctly, hence the `f"//{url}"` fallback
    for a bare "host:port" value. The try/except around .port handles a
    malformed port that would otherwise raise.

    Result: "url_host": "psrc-abc12.us-east-1.aws.confluent.cloud" plus a single
    boolean confirming credentials were present.
    """
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
    """Parse the default Confluent framing: magic byte + 32-bit schema ID.

    THE CONFLUENT WIRE FORMAT - the mechanism that makes Avro-on-Kafka work.

    Every Avro message produced by a Confluent serializer looks like this:

        byte 0        bytes 1-4                  bytes 5..end
        ┌──────────┬────────────────────────┬──────────────────────────┐
        │ magic=0  │ schema ID (big-endian) │ Avro binary body         │
        └──────────┴────────────────────────┴──────────────────────────┘
             1 byte          4 bytes            everything else

    WHY THE SCHEMA ID INSTEAD OF THE SCHEMA:
        Avro binary contains NO field names - just values packed in schema
        order. That is what makes it so compact, and it means you cannot decode
        a payload without knowing its schema. Shipping the whole schema with
        every message would be enormously wasteful, so Confluent ships a 4-byte
        ID and the consumer looks the schema up in the Registry (and caches it).

    CONSEQUENCES WORTH INTERNALIZING:
        - Avro bytes are NOT text. Attempting json.loads(payload.decode()) fails
          - Demo 04B deliberately proves this.
        - Without Schema Registry access, a consumer literally cannot decode the
          data. The Registry is not optional infrastructure.
        - The magic byte is currently always 0; it exists so the format can
          change later without ambiguity.

    struct.unpack(">bI", payload[:5]) decodes the header:
        >   big-endian (network byte order)
        b   signed 1-byte integer  -> the magic byte
        I   unsigned 4-byte integer -> the schema ID
    """
    if len(payload) < 5:
        raise ValueError("Confluent-framed Avro payload must contain at least five bytes")

    magic_byte, schema_id = struct.unpack(">bI", payload[:5])
    return {
        "magic_byte": magic_byte,
        "schema_id": schema_id,
        "payload_bytes": len(payload),

        # The Avro body size, i.e. total minus the 5-byte header. Comparing this
        # against the equivalent JSON length is the concrete way to see how much
        # smaller Avro is.
        "avro_body_bytes": len(payload) - 5,
    }


def serializer_conf() -> dict[str, Any]:
    """Use explicit serializer settings for the Summer 2026 course.

    Four settings, each a deliberate choice rather than a default:

    auto.register.schemas = True
        On first use, upload this schema to the Registry automatically.
        Convenient for a course. In PRODUCTION this is often set to False, so
        schemas are registered deliberately through a review process rather
        than by whichever service happens to deploy first.

    subject.name.strategy = topic_subject_name_strategy
        Use "<topic>-value" as the subject. Stated explicitly so the read and
        write paths cannot drift apart if a library default ever changes.

    validate.strict = True
        Check the outgoing record against the schema BEFORE sending, instead of
        letting the broker or a consumer discover the problem later.

    validate.strict.allow.default = False
        Do not silently substitute schema defaults for missing fields. If a
        field is absent, that is an error worth surfacing - the same philosophy
        as extra="forbid" on the pydantic model.
    """
    return {
        "auto.register.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
        "validate.strict": True,
        "validate.strict.allow.default": False,
    }


def deserializer_conf() -> dict[str, Any]:
    """Use the same explicit subject strategy on the read path.

    The read side must agree with the write side about how subjects are named,
    or the consumer will look up the wrong subject and fail to find a schema.
    """
    return {"subject.name.strategy": topic_subject_name_strategy}


def validation_error_summary(exc: ValidationError) -> list[dict[str, Any]]:
    """Return stable, JSON-safe validation evidence.

    A pydantic ValidationError carries rich structured detail. This flattens
    each error to three stable fields so they can be written to a report:

        location - which field failed, e.g. "fare"
        type     - the machine-readable error code, e.g. "greater_than_equal"
        message  - the human-readable explanation

    include_url=False / include_context=False strip pydantic documentation links
    and internals, keeping the evidence file stable across library versions -
    useful when a test asserts on it.
    """
    rows: list[dict[str, Any]] = []
    for item in exc.errors(include_url=False, include_context=False):
        rows.append(
            {
                # "loc" is a tuple like ("fare",) or ("items", 0, "fare") for
                # nested data; joining with dots produces a readable path.
                "location": ".".join(str(part) for part in item["loc"]),
                "type": item["type"],
                "message": item["msg"],
            }
        )
    return rows


def write_json_report(run_id: str, demo_name: str, report: dict[str, Any]) -> Path:
    """Write reproducible, secret-free evidence under ``outputs/runs``.

    Two differences from Demo 02's version:
      - the run_id is VALIDATED first (path-traversal protection);
      - sort_keys=True makes the output deterministic, so two runs with the same
        data produce byte-identical files and `diff` is meaningful.
    """
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
    """Teach json.dumps how to serialize types it does not natively handle.

    json.dumps calls this for any object it cannot serialize. Three cases:

      datetime -> normalized to UTC and rendered as ISO-8601 with a "Z" suffix,
                  matching the format used everywhere else in the course.
      Path     -> plain string.
      pydantic -> anything with .model_dump() is dumped in JSON mode, so a raw
                  TripEventV1 can be dropped straight into a report.

    Raising TypeError for anything else is required: returning a fallback like
    str(value) would silently produce misleading evidence for an unexpected type.

    The leading underscore marks this as module-private.
    """
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
