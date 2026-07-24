"""
================================================================================
DEMO 05 COMMON - THE HTTP CONTRACT  (annotated tutorial copy)
================================================================================

READ demo04_common.py (in this same wcomments folder) FIRST if you have not
already. Demo 04 taught the "two layers" idea - Pydantic for application
meaning, Avro for wire structure - using a producer/consumer script. Demo 05
reuses that exact TripEventV1 contract (imported from trip_event_contract.py,
a shared file this course bundles alongside every Demo 05/06 script) and adds
a THIRD layer in front of it: an HTTP API built with FastAPI.

WHAT THIS DEMO TEACHES
    Demo 05 puts a web API in front of Kafka. A rider's phone or browser sends
    JSON over HTTP; this module is where that JSON becomes the same strict
    TripEventV1 object Demo 04 already knew how to serialize and validate.

WHY A SEPARATE HTTP MODEL AT ALL - THE KEY IDEA OF THIS FILE
    It would be tempting to make FastAPI accept a TripEventV1 directly and skip
    a step. This module deliberately does NOT do that, for a concrete reason:

        HTTP JSON often needs to be a little more FORGIVING than the internal
        event. A client sends a timestamp as a STRING (JSON has no datetime
        type), possibly with a non-UTC offset like "-07:00". TripEventV1
        already requires an aware datetime, so something has to accept the
        string, parse it, and normalize its offset - and that "something"
        should not be the same object the rest of the system trusts as final.

    So there are now THREE separate contracts, each with one job:

    ┌────────────────────────────────────────────────────────────────────┐
    │ CreateTripRequest  (this file)                                     │
    │   The HTTP boundary. What a client is allowed to POST.             │
    │   Parses a string timestamp; normalizes its timezone.               │
    ├────────────────────────────────────────────────────────────────────┤
    │ TripEventV1        (trip_event_contract.py, i.e. Demo 04's model)  │
    │   The APPLICATION contract. What Kafka ultimately carries.         │
    │   Built from a CreateTripRequest by request_to_event() below.      │
    ├────────────────────────────────────────────────────────────────────┤
    │ TripAcceptedResponse / PublishReceipt   (also this file)            │
    │   What the API hands BACK to the caller, and what a publisher      │
    │   implementation hands back internally. Neither is the Kafka event │
    │   - they are receipts ABOUT it.                                    │
    └────────────────────────────────────────────────────────────────────┘

WHAT THIS FILE DOES NOT DO
    It does not open a Kafka connection, build a FastAPI app, or know whether
    delivery ends up "local" (Demo 05A/05B) or "broker_acknowledged" (Demo
    05C/05D). Those live in demo05_app.py and demo05_kafka.py. This file is
    pure data shape plus two pieces of deterministic test data machinery.
================================================================================
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

# confluent_demo_common.py and trip_event_contract.py are shared infrastructure
# bundled with every Demo 05/06 script (not duplicated per-lecture the way
# demo02_producer_common.py and demo04_common.py were). load_dotenv_for_demo
# loads .env exactly as in Demo 04; TripEventV1 and ServiceZone are the SAME
# classes Demo 04 defined - Demo 05 does not redefine the event contract.
from confluent_demo_common import load_dotenv_for_demo
from trip_event_contract import ServiceZone, TripEventV1

# A DEDICATED TOPIC, separate from every Demo 01-04 topic. Even though the
# wire contract is the same Avro shape as Demo 04, this topic exists so a
# Demo 05 run never mixes with Demo 04's data or offsets.
DEFAULT_TOPIC = "msds682.demo05.trip-events-api-avro.v1"

# Deterministic HTTP request data, in the same spirit as Demo 04's
# SYNTHETIC_BASE_TIME: a fixed anchor time plus a fixed per-request interval
# means two runs with the same count/seed_offset produce byte-identical input.
REQUEST_BASE_TIME = datetime(2026, 7, 20, 17, 0, tzinfo=UTC)
REQUEST_INTERVAL_SECONDS = 13


# ============================================================================
# KEY CONCEPT
# HTTP JSON is a boundary contract, not the Kafka event contract. FastAPI and
# Pydantic parse the request first; request_to_event() then creates TripEventV1.
# ============================================================================
class CreateTripRequest(BaseModel):
    """HTTP request contract accepted by ``POST /trip-requests``.

    THIS is the model FastAPI attaches to the request body. When a route
    parameter is annotated with a pydantic BaseModel, FastAPI automatically:
    parses incoming JSON against it, returns HTTP 422 with a field-by-field
    error list if it does not match, and documents its shape in OpenAPI/Swagger.
    None of that machinery is visible here - it is exactly what makes FastAPI
    convenient - but every field and validator below determines what students
    will see reflected in that generated documentation and error response.
    """

    # extra="forbid": reject any JSON field not declared below, the same
    # strictness rule TripEventV1 uses in Demo 04. A client that sends an
    # unexpected extra key gets a loud 422 instead of the key being silently
    # dropped.
    model_config = ConfigDict(extra="forbid")

    # Format-checked strings, same pattern-based approach as Demo 04's
    # trip_id/rider_id fields. "request_" plus four digits.
    request_id: str = Field(pattern=r"^request_[0-9]{4}$")
    rider_id: str = Field(pattern=r"^rider_[0-9]{3}$")

    # AwareDatetime: pydantic will happily parse an ISO-8601 STRING like
    # "2026-07-20T10:00:00-07:00" out of JSON into a real, timezone-aware
    # datetime object. This is exactly the flexibility TripEventV1 itself does
    # not offer - JSON strings are welcome here, but only here.
    requested_at: AwareDatetime

    # ServiceZone is the same Literal["north", "south", "west"] type Demo 04
    # defined. Reusing it (rather than redeclaring the allowed values) means
    # the HTTP contract and the Kafka contract cannot silently drift apart.
    zone: ServiceZone

    @field_validator("requested_at")
    @classmethod
    def normalize_requested_at(cls, value: datetime) -> datetime:
        """Normalize the HTTP timestamp before constructing the Kafka event.

        A field_validator runs once the raw JSON string has already been
        parsed into an aware datetime. This does not reject anything - it
        CONVERTS. A client in any timezone offset gets normalized to UTC here,
        at the boundary, so the TripEventV1 built downstream never has to
        reason about the caller's local offset. (TripEventV1.event_time also
        normalizes to UTC, in trip_event_contract.py - this is the same
        practice applied one layer earlier, right where the ambiguous string
        first enters the system.)
        """

        return value.astimezone(UTC)


class TripAcceptedResponse(BaseModel):
    """Small API response; Kafka evidence remains in the secret-free report.

    This is deliberately thin. It tells the caller "your request was accepted,
    here is the trip_id it became, and here is what kind of delivery
    confirmation backs that" - it does NOT echo partition numbers, offsets, or
    schema IDs. Detailed Kafka evidence belongs in the JSON report files
    (demo05_common.write_json_report-based reports built by 05A/05C/05D),
    never in the HTTP response body a real client would parse.
    """

    # strict=True here (unlike CreateTripRequest) because this model is built
    # entirely by OUR OWN code from already-validated data, not parsed from an
    # untrusted external string. There is nothing left to coerce.
    model_config = ConfigDict(extra="forbid", strict=True)

    status: Literal["accepted"]
    request_id: str
    trip_id: str
    topic: str

    # THE DELIVERY-MODE FIELD. "local" comes from Demo 05A/05B's
    # credential-free in-memory publisher; "broker_acknowledged" comes from
    # Demo 05C/05D's real Confluent publisher, and specifically means the
    # Kafka delivery future has already completed by the time this response is
    # returned - not merely that the message was handed to a background queue.
    delivery: Literal["local", "broker_acknowledged"]


class PublishReceipt(BaseModel):
    """Internal delivery evidence returned by a publisher implementation.

    THIS IS NOT THE HTTP RESPONSE. It is a slightly richer internal record
    that demo05_app.py reads from to build a TripAcceptedResponse, and that
    Demo 05C's report embeds directly (as evidence, not as the live API
    contract). Compare its optional partition/offset/wire fields, present only
    for the Cloud path, with TripAcceptedResponse above, which never exposes
    them to an HTTP caller.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    delivery: Literal["local", "broker_acknowledged"]
    topic: str
    key: str

    # None for the local publisher (Demo 05A/05B never talks to a real
    # partition); populated by the Cloud publisher in demo05_kafka.py once the
    # broker has actually acknowledged the record.
    partition: int | None = None
    offset: int | None = None
    wire: dict[str, int] | None = None


class PublishError(RuntimeError):
    """Raised when an event cannot be accepted by the configured publisher.

    A dedicated exception type, exactly as Demo 04's ConnectionConfigError was
    dedicated to one failure mode. demo05_app.py catches SPECIFICALLY this
    type and turns it into an HTTP 503 - "the publisher could not confirm
    acceptance" - while letting any other, unexpected exception surface as a
    genuine 500-level bug instead of being mistaken for a routine delivery
    failure.
    """


def topic_name() -> str:
    """Return the dedicated Demo 05 topic, independent of Demo 01-04 data.

    Same override pattern as Demo 04's topic_name(): an environment variable
    wins if set, otherwise the course default. Reading .env here (rather than
    trusting a value cached at import time) means a student can change
    DEMO05_TOPIC_NAME between runs without restarting a long-lived process.
    """

    load_dotenv_for_demo()
    return os.getenv("DEMO05_TOPIC_NAME", DEFAULT_TOPIC)


def request_to_event(request: CreateTripRequest) -> TripEventV1:
    """Map the HTTP command into Demo 05's canonical Kafka event contract.

    THE BRIDGE BETWEEN THE TWO OUTER LAYERS OF THE DIAGRAM ABOVE. Every field
    on the right comes from the validated request on the left; nothing here
    re-validates format, because CreateTripRequest and TripEventV1 already
    did, each in its own layer.

    Note the event_type is hardcoded to "trip_requested" - an HTTP POST to
    this endpoint only ever represents the FIRST lifecycle stage from Demo
    04's TripEventV1 (trip_requested -> driver_matched -> trip_started ->
    trip_completed). Because TripEventV1's own model_validator forbids a
    driver_id or fare on a trip_requested event, this mapping could never
    accidentally construct an invalid event even if it tried.
    """

    # A readable transformation from the caller's request_id (e.g.
    # "request_5000") into the trip_id TripEventV1 expects (e.g. "trip_5000").
    # `.replace(..., 1)` limits the substitution to the first occurrence, so a
    # rider_id embedded later in the string (there isn't one here, but as a
    # general habit) could never be affected by accident.
    trip_id = request.request_id.replace("request_", "trip_", 1)
    return TripEventV1(
        trip_id=trip_id,
        event_type="trip_requested",
        rider_id=request.rider_id,
        event_time=request.requested_at,
        zone=request.zone,
    )


# ============================================================================
# KEY CONCEPT
# Demo 05 creates its own deterministic HTTP requests. It does not read prior
# Kafka records, call another producer, or use personal data.
# ============================================================================
def deterministic_requests(
    count: int,
    *,
    seed_offset: int = 0,
) -> list[CreateTripRequest]:
    """Create bounded reproducible API requests for tests and Cloud demos.

    Same purpose as Demo 04's deterministic_events(), one layer up: instead of
    generating TripEventV1 objects directly, this generates the HTTP REQUESTS
    that will eventually be turned into them by request_to_event(). Every
    demo05*.py script posts these to the FastAPI app rather than reading real
    user input, so a grader can regenerate identical evidence from the same
    (count, seed_offset) pair.
    """

    # Bounds double as a sanity check on CLI/test input. 100 is generous for a
    # classroom demo; a value outside this range almost certainly indicates a
    # mistake rather than a legitimate large run.
    if not 1 <= count <= 100:
        raise ValueError("count must be between 1 and 100")
    if not 0 <= seed_offset <= 400:
        raise ValueError("seed_offset must be between 0 and 400")

    base = REQUEST_BASE_TIME + timedelta(minutes=seed_offset)
    zones: tuple[ServiceZone, ...] = ("north", "south", "west")
    requests: list[CreateTripRequest] = []
    for index in range(count):
        # Folding seed_offset into the numeric ID (not just the timestamp)
        # keeps requests from two different seed_offsets from colliding on the
        # same request_id/rider_id even if their counts overlap.
        request_number = 5000 + seed_offset * 10 + index
        requests.append(
            CreateTripRequest(
                request_id=f"request_{request_number:04d}",
                rider_id=f"rider_{500 + (index % 30):03d}",
                requested_at=base + timedelta(seconds=index * REQUEST_INTERVAL_SECONDS),
                zone=zones[index % len(zones)],
            )
        )
    return requests


def request_input_report(
    requests: list[CreateTripRequest],
    *,
    seed_offset: int,
) -> dict[str, Any]:
    """Describe the independent deterministic API input.

    Same role as Demo 04's synthetic_data_report(): document the INPUT in the
    evidence file so a reader can tell exactly what a run posted without
    replaying every generation rule by hand, and can confirm this demo never
    depended on a pre-existing topic or external dataset.
    """

    if not requests:
        raise ValueError("requests must not be empty")
    return {
        "source": "synthetic deterministic HTTP requests generated by Demo 05",
        "prior_demo_topic_required": False,
        "prior_kafka_data_required": False,
        "seed_offset": seed_offset,
        "count": len(requests),
        "first_request_id": requests[0].request_id,
        "last_request_id": requests[-1].request_id,
        "request_interval_seconds": REQUEST_INTERVAL_SECONDS,
    }
