"""
================================================================================
DEMO 06 COMMON - CONNECT AND STREAM-PROCESSING SHARED MODULE
(annotated tutorial copy)
================================================================================

READ demo04_common.py and demo05_common.py (in this same wcomments folder)
FIRST for the "two layers" (Pydantic + Avro) idea and the deterministic-data
pattern this module reuses once more.

THE BIG QUESTION DEMO 06 ANSWERS
    Demos 02-05 all wrote their OWN producer, in Python, to get data into
    Kafka. Demo 06 asks: what if the data source is not something you wrote
    at all - a database, a SaaS API, a synthetic generator - and you cannot
    (or should not) hand-write a producer for it? The answer is KAFKA
    CONNECT: a managed integration runtime that reads from an external system
    and writes Avro-encoded records into Kafka, with no custom producer code.

    This module supports BOTH ways Demo 06 can get input data:

        06A: a REAL managed Kafka Connect "Datagen Source" connector, which
             this file never talks to directly - it only prepares topics and
             prints the Cloud Console fields a student enters by hand.

        Fallback (demo06_seed_source.py): a finite Python producer used ONLY
             when a classroom account cannot create a managed connector. It
             deliberately uses THIS SAME Avro value schema, so 06B-06D behave
             identically regardless of which source produced the data.

WHAT DEMO 06 ADDS ON TOP OF THAT INPUT
    Once records exist in the input topic (from either source), a Python
    STREAM PROCESSOR (06C) does what Connect cannot: read each record,
    validate it, DERIVE a new fact from it, publish that derived fact to a
    second topic, and only THEN commit progress on the first. That exact
    sequence - consume -> validate -> derive -> produce -> output ack ->
    commit input offset - is why "commit" in this demo always means a Kafka
    CONSUMER OFFSET COMMIT, never a producer acknowledgement and never a Git
    commit.

TWO PYDANTIC MODELS, ONE PER TOPIC
    DatagenOrderV1   - validates INPUT records, whichever source produced them
    OrderMetricV1    - the DERIVED event the processor itself creates and
                       publishes to the second topic
================================================================================
"""

from __future__ import annotations

import json
import os
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import KafkaError, OFFSET_BEGINNING
from confluent_kafka.schema_registry import topic_subject_name_strategy
from pydantic import BaseModel, ConfigDict, Field

from confluent_demo_common import load_dotenv_for_demo

BUNDLE_DIR = Path(__file__).resolve().parent
DATAGEN_ORDER_SCHEMA_PATH = BUNDLE_DIR / "demo06_datagen_order_v1.avsc"
ORDER_METRIC_SCHEMA_PATH = BUNDLE_DIR / "demo06_order_metric_v1.avsc"

# TWO TOPICS, one per event contract - the input Connect/fallback records, and
# the processor's own derived output. Mixing derived events into the input
# topic (or vice versa) would break both this schema's owner and every
# consumer expecting one shape per topic - the same "one topic, one format"
# rule Demo 04 established.
DEFAULT_INPUT_TOPIC = "msds682.demo06.connector-orders-avro.v1"
DEFAULT_OUTPUT_TOPIC = "msds682.demo06.connector-order-metrics-avro.v1"

# The exact Datagen "quickstart" dataset this course standardizes on. Datagen
# ships several (USERS, PAGEVIEWS, ORDERS, ...); using a fixed one means the
# managed-connector path and the Python fallback path can share one schema.
DATAGEN_QUICKSTART = "ORDERS"

# THE PROCESSOR'S BUSINESS RULE: an order at or above this many units is
# classified "large" rather than "standard". This is exactly the kind of
# judgment Avro alone cannot express (see demo_order_metric derivation below)
# - it is applied by the Python processor, not by the wire format.
LARGE_ORDER_THRESHOLD = 0.5

# A fixed epoch-milliseconds anchor for the fallback's synthetic ordertime,
# analogous to Demo 04/05's SYNTHETIC_BASE_TIME / REQUEST_BASE_TIME - it
# exists purely so the fallback's deterministic values are reproducible.
FALLBACK_BASE_TIME_MS = 1_720_000_000_000


class OrderAddress(BaseModel):
    """Address shape emitted by the Datagen ORDERS quickstart.

    A NESTED model, embedded inside DatagenOrderV1 below. Avro itself supports
    nested records the same way; Pydantic's nested BaseModel mirrors that
    structure so validation happens at every level, not just the top one.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    city: str = Field(min_length=1)
    state: str = Field(min_length=1)
    zipcode: int = Field(ge=0)


class DatagenOrderV1(BaseModel):
    """Strict application view of one Datagen ORDERS value.

    THIS is the Layer 1 (application/domain) model for the INPUT side of
    Demo 06, in the same sense Demo 04's TripEventV1 was Layer 1 for its own
    topic. It validates whatever the Avro deserializer hands back - whether
    that record originated from the real managed Datagen connector or from
    demo06_seed_source.py's Python fallback. Both sources must produce data
    that satisfies this exact shape, which is precisely why the fallback
    reuses the managed connector's own ORDERS schema rather than inventing one.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    ordertime: int = Field(ge=0)
    orderid: int = Field(ge=0)
    # Format-checked, same pattern-based strictness as every other event
    # contract in this course: "Item_" followed by digits, nothing else.
    itemid: str = Field(pattern=r"^Item_[0-9]+$")
    orderunits: float = Field(ge=0)
    address: OrderAddress


class OrderMetricV1(BaseModel):
    """Derived event produced by the Demo 06 stream processor.

    THIS is Layer 1 for the OUTPUT side: the fact the processor itself
    invents from one validated input record. Note the four "source_*" fields
    - they are not part of the Datagen data at all. They exist so a reader of
    the derived topic can always trace a metric back to the exact input
    record (topic, partition, offset) that produced it - see
    derive_order_metric() below.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    source_topic: str = Field(min_length=1)
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    # A single human-readable string combining the three fields above -
    # "<topic>:<partition>:<offset>" - used both as the Kafka KEY for this
    # derived event (see metric_key() below) and as the stable identity
    # Demo 06D compares across resume/replay passes.
    source_record_id: str = Field(min_length=1)
    orderid: int = Field(ge=0)
    itemid: str = Field(pattern=r"^Item_[0-9]+$")
    orderunits: float = Field(ge=0)
    size_band: Literal["standard", "large"]


def read_schema(path: Path) -> str:
    """Read and validate one distributed Avro schema.

    Same fail-fast pattern as Demo 04's read_schema(): json.loads() here
    purely to confirm the shipped .avsc file parses as valid JSON, discarding
    the parsed result, so a malformed schema file fails with a clear error
    here rather than a confusing one deep inside the Avro serializer later.
    """

    raw = path.read_text(encoding="utf-8")
    json.loads(raw)
    return raw


def datagen_order_schema_str() -> str:
    """Return the managed Datagen ORDERS value schema."""

    return read_schema(DATAGEN_ORDER_SCHEMA_PATH)


def order_metric_schema_str() -> str:
    """Return the derived-event value schema."""

    return read_schema(ORDER_METRIC_SCHEMA_PATH)


def serializer_conf() -> dict[str, Any]:
    """Use one explicit TopicNameStrategy configuration for Demo 06.

    Identical philosophy to Demo 04/05's serializer_conf(): auto-register on
    first use (convenient for a course; often disabled in production),
    explicit subject naming so reads and writes cannot silently disagree, and
    strict validation so a malformed outgoing record fails locally rather
    than corrupting the topic.
    """

    return {
        "auto.register.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
        "validate.strict": True,
        "validate.strict.allow.default": False,
    }


def input_topic_name() -> str:
    """Return the dedicated Connect source topic."""

    load_dotenv_for_demo()
    return os.getenv("DEMO06_INPUT_TOPIC_NAME", DEFAULT_INPUT_TOPIC)


def output_topic_name() -> str:
    """Return the dedicated derived-event topic."""

    load_dotenv_for_demo()
    return os.getenv("DEMO06_OUTPUT_TOPIC_NAME", DEFAULT_OUTPUT_TOPIC)


def stable_seed_offset(run_id: str) -> int:
    """Derive a reproducible small integer without Python hash randomization.

    Same crc32-based technique Demo 04C/05C used for their own seed_offset:
    Python's built-in hash() is randomized per-process by design (a security
    feature against hash-flooding attacks), so it would give a DIFFERENT
    number every run even for the identical run_id. zlib.crc32 has no such
    randomization, which is exactly what "reproducible" requires here.
    """

    return zlib.crc32(run_id.encode("utf-8")) % 100


# ============================================================================
# KEY CONCEPT
# The fallback source is deterministic and finite. It exists only when a
# student cannot create a managed connector. It uses the managed Datagen
# ORDERS value schema so 06B-06D retain one input-value contract.
# ============================================================================
def deterministic_orders(count: int, *, seed_offset: int) -> list[DatagenOrderV1]:
    """Create bounded values compatible with the Datagen ORDERS schema.

    Note this function is a PURE function of (count, seed_offset) - no
    randomness, no I/O - exactly Demo 04's deterministic_events() philosophy,
    applied here to a schema this course did not design (Datagen's own
    ORDERS shape) rather than one of its own.
    """

    if not 1 <= count <= 100:
        raise ValueError("count must be between 1 and 100")
    if not 0 <= seed_offset <= 999:
        raise ValueError("seed_offset must be between 0 and 999")

    rows: list[DatagenOrderV1] = []
    for index in range(count):
        # Folding seed_offset into `sequence` (rather than just the index)
        # keeps two different seed_offsets' generated orderids from colliding
        # even when their counts overlap - the same technique Demo 05's
        # deterministic_requests() used for request numbers.
        sequence = seed_offset * 100 + index
        rows.append(
            DatagenOrderV1(
                ordertime=FALLBACK_BASE_TIME_MS + sequence * 1_000,
                orderid=7_000 + sequence,
                itemid=f"Item_{100 + (sequence % 900)}",
                orderunits=round(0.15 + (sequence % 9) * 0.10, 6),
                address=OrderAddress(
                    city=f"City_{1 + sequence % 50}",
                    state=f"State_{1 + sequence % 20}",
                    zipcode=10_000 + sequence % 80_000,
                ),
            )
        )
    return rows


def order_to_avro_dict(order: DatagenOrderV1, _ctx: Any = None) -> dict[str, Any]:
    """Convert one validated fallback value into its Avro record.

    Only used by the Python fallback (demo06_seed_source.py) - the real
    managed connector encodes its own records itself, entirely inside Kafka
    Connect, without ever calling this function.
    """

    return order.model_dump()


def avro_dict_to_order(data: dict[str, Any], _ctx: Any = None) -> DatagenOrderV1:
    """Apply strict application validation after Avro deserialization.

    Note this is NOT actually called by demo06b/06c below - both of those
    scripts validate with `DatagenOrderV1.model_validate(raw)` directly after
    a plain AvroDeserializer(registry) call (no from_dict). It is provided
    here as the from_dict counterpart to order_to_avro_dict() for symmetry and
    for any script that prefers wiring validation directly into the
    deserializer instead of as a separate step.
    """

    return DatagenOrderV1.model_validate(data)


def metric_to_avro_dict(metric: OrderMetricV1, _ctx: Any = None) -> dict[str, Any]:
    """Convert one validated derived event into its Avro record."""

    return metric.model_dump()


def avro_dict_to_metric(data: dict[str, Any], _ctx: Any = None) -> OrderMetricV1:
    """Validate a deserialized derived event."""

    return OrderMetricV1.model_validate(data)


def derive_order_metric(
    order: DatagenOrderV1,
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
) -> OrderMetricV1:
    """Create a deterministic derived fact from one input record.

    THE HEART OF "STREAM PROCESSING" IN THIS DEMO: given one validated input
    record plus the Kafka COORDINATES it was read from, produce a new,
    independently meaningful fact. size_band is genuinely NEW information -
    the input record itself never says whether an order counts as "large".

    THE SOURCE COORDINATE IS BAKED INTO THE OUTPUT ON PURPOSE. Because it
    is built from topic:partition:offset - values that are IDENTICAL if the
    same input record is ever read again - re-deriving from the same input
    always produces the same source_record_id. Demo 06D exploits exactly
    this: replaying the same input naturally reproduces the same derived key,
    which is what makes a duplicate observable rather than silently distinct.
    """

    source_record_id = f"{source_topic}:{source_partition}:{source_offset}"
    size_band: Literal["standard", "large"] = (
        "large" if order.orderunits >= LARGE_ORDER_THRESHOLD else "standard"
    )
    return OrderMetricV1(
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        source_record_id=source_record_id,
        orderid=order.orderid,
        itemid=order.itemid,
        orderunits=order.orderunits,
        size_band=size_band,
    )


def fallback_order_key(order: DatagenOrderV1) -> bytes:
    """Return a readable stable key for fallback-source records.

    NOTE THE COMMENT IN demo06.md: the managed connector encodes its
    configured `orderid` key field using ITS OWN (opaque, likely Avro-encoded)
    key serialization, while this fallback uses plain readable UTF-8 decimal
    digits instead. The two source modes intentionally do NOT share key
    encoding - only the VALUE schema is shared - which is why demo06.md warns
    not to infer the value schema from key length or to mix both sources in
    one exercise.
    """

    return str(order.orderid).encode("utf-8")


def metric_key(metric: OrderMetricV1) -> bytes:
    """Use source coordinates as the stable derived-event key.

    Choosing the INPUT coordinate (not, say, orderid) as the derived event's
    key is what makes replayed output identity observable downstream (see
    derive_order_metric()'s docstring above) - two derived records with the
    same key are provably derived from the same original input record.
    """

    return metric.source_record_id.encode("utf-8")


def connector_console_plan(
    *,
    topic: str,
    max_interval_ms: int,
) -> dict[str, Any]:
    """Return secret-free Cloud Console fields for the managed connector.

    THIS FUNCTION NEVER TALKS TO CONFLUENT CLOUD. Kafka Connect connectors in
    this course are created BY HAND, through the Cloud Console UI - there is
    no Confluent Cloud API call here that could create one automatically.
    Instead, this returns the exact field VALUES a student should type into
    that UI, so demo06a_connect_source_plan.py's report becomes a checklist
    rather than leaving the student to guess topic names or settings.
    """

    if max_interval_ms < 1_000:
        # A floor on how often Datagen emits new records. This is a
        # classroom courtesy: a much smaller interval would flood the shared
        # topic and cluster with more traffic than the exercise needs.
        raise ValueError("max_interval_ms must be at least 1000 for this class demo")
    return {
        "connector": "Datagen Source",
        "name": "msds682-demo06-orders-<usf_username>",
        "kafka_topic": topic,
        "output_data_format": "AVRO",
        "quickstart": DATAGEN_QUICKSTART,
        "schema_keyfield": "orderid",
        "tasks_max": 1,
        "max_interval_ms": max_interval_ms,
        "credential_instruction": (
            "Select or create connector credentials in Confluent Cloud; "
            "never paste secrets into source code or evidence."
        ),
        "stop_condition": (
            "After at least 8 records are visible, pause the connector while "
            "finishing the exercise. Delete it after the exercise, and revoke "
            "a demo-only API key when it is no longer needed."
        ),
    }


@dataclass
class AssignmentTracker:
    """Record assignments and optionally force an explicit replay.

    Compare with Demo 04C's own AssignmentTracker: same on_assign/on_revoke
    role (drive the classic-protocol rebalance callback and record what was
    assigned), PLUS one new capability - force_beginning - that Demo 06D uses
    to implement an explicit replay rather than merely relying on
    auto.offset.reset.
    """

    # When True, on_assign rewrites every assigned partition's starting
    # offset to OFFSET_BEGINNING before accepting the assignment. This is
    # what Demo 06D calls "forcing" a replay: an explicit, code-driven
    # override, not the passive auto.offset.reset fallback (which only
    # applies when a group has NO prior committed position at all).
    force_beginning: bool = False
    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    @staticmethod
    def rows(partitions: Any) -> list[dict[str, int | str]]:
        return [
            {
                "topic": partition.topic,
                "partition": partition.partition,
                "offset": partition.offset,
            }
            for partition in partitions
        ]

    def on_assign(self, consumer: Any, partitions: Any) -> None:
        """Accept assigned partitions, optionally overriding them to beginning."""

        if self.force_beginning:
            # Mutating partition.offset BEFORE consumer.assign() is what
            # actually overrides the starting position - assign() reads the
            # offset field on each TopicPartition object at the moment it is
            # called.
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        self.assigned.append(self.rows(partitions))
        consumer.assign(partitions)

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        """Record partitions revoked during close or rebalance."""

        self.revoked.append(self.rows(partitions))


def wait_for_assignment(
    consumer: Any,
    tracker: AssignmentTracker,
    *,
    timeout: float,
) -> tuple[float, list[Any]]:
    """Wait for assignment and preserve any data returned by the same poll.

    Same "drive the poll loop until on_assign has actually fired" discipline
    as Demo 04C's wait_for_assignment(), with one addition this demo needs:
    the poll that TRIGGERS assignment can also return the FIRST real data
    message in the same call. Discarding that message (as Demo 04C's simpler
    version does, since it always starts from a fresh position) would here
    silently skip offset 0 - so this version collects any such message into
    `pending_messages` for the caller to process first.
    """

    started = time.monotonic()
    deadline = started + timeout
    pending_messages: list[Any] = []
    while not tracker.assigned and time.monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        error = message.error()
        if error is not None and error.code() != KafkaError._PARTITION_EOF:
            raise RuntimeError(f"Consumer error while waiting for assignment: {error}")
        if error is None:
            # ================================================================
            # IMPORTANT NOTE
            # The poll that triggers on_assign may also return the first data
            # record. Preserve it so the processing loop never skips offset 0.
            # ================================================================
            pending_messages.append(message)
    if not tracker.assigned:
        raise RuntimeError(
            "Consumer assignment timed out. Check the topic, credentials, "
            "group access, and cluster connectivity."
        )
    return round(time.monotonic() - started, 6), pending_messages


def source_coordinates(rows: list[dict[str, Any]]) -> list[str]:
    """Return comparable source-coordinate IDs from processor evidence.

    Used exclusively by Demo 06D to compare the FIRST, RESUME, and REPLAY
    passes' source_record_id values against one another - see
    validate_resume_replay() in demo06d_confluent_resume_replay.py for what
    those comparisons prove.
    """

    return [str(row["source_record_id"]) for row in rows]
