"""Shared contracts, configuration, and deterministic data for Demo 06."""

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

DEFAULT_INPUT_TOPIC = "msds682.demo06.connector-orders-avro.v1"
DEFAULT_OUTPUT_TOPIC = "msds682.demo06.connector-order-metrics-avro.v1"
DATAGEN_QUICKSTART = "ORDERS"
LARGE_ORDER_THRESHOLD = 0.5
FALLBACK_BASE_TIME_MS = 1_720_000_000_000


class OrderAddress(BaseModel):
    """Address shape emitted by the Datagen ORDERS quickstart."""

    model_config = ConfigDict(extra="forbid", strict=True)

    city: str = Field(min_length=1)
    state: str = Field(min_length=1)
    zipcode: int = Field(ge=0)


class DatagenOrderV1(BaseModel):
    """Strict application view of one Datagen ORDERS value."""

    model_config = ConfigDict(extra="forbid", strict=True)

    ordertime: int = Field(ge=0)
    orderid: int = Field(ge=0)
    itemid: str = Field(pattern=r"^Item_[0-9]+$")
    orderunits: float = Field(ge=0)
    address: OrderAddress


class OrderMetricV1(BaseModel):
    """Derived event produced by the Demo 06 stream processor."""

    model_config = ConfigDict(extra="forbid", strict=True)

    source_topic: str = Field(min_length=1)
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    source_record_id: str = Field(min_length=1)
    orderid: int = Field(ge=0)
    itemid: str = Field(pattern=r"^Item_[0-9]+$")
    orderunits: float = Field(ge=0)
    size_band: Literal["standard", "large"]


def read_schema(path: Path) -> str:
    """Read and validate one distributed Avro schema."""

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
    """Use one explicit TopicNameStrategy configuration for Demo 06."""

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
    """Derive a reproducible small integer without Python hash randomization."""

    return zlib.crc32(run_id.encode("utf-8")) % 100


# ============================================================================
# KEY CONCEPT
# The fallback source is deterministic and finite. It exists only when a
# student cannot create a managed connector. It uses the managed Datagen
# ORDERS value schema so 06B-06D retain one input-value contract.
# ============================================================================
def deterministic_orders(count: int, *, seed_offset: int) -> list[DatagenOrderV1]:
    """Create bounded values compatible with the Datagen ORDERS schema."""

    if not 1 <= count <= 100:
        raise ValueError("count must be between 1 and 100")
    if not 0 <= seed_offset <= 999:
        raise ValueError("seed_offset must be between 0 and 999")

    rows: list[DatagenOrderV1] = []
    for index in range(count):
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
    """Convert one validated fallback value into its Avro record."""

    return order.model_dump()


def avro_dict_to_order(data: dict[str, Any], _ctx: Any = None) -> DatagenOrderV1:
    """Apply strict application validation after Avro deserialization."""

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
    """Create a deterministic derived fact from one input record."""

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
    """Return a readable stable key for fallback-source records."""

    return str(order.orderid).encode("utf-8")


def metric_key(metric: OrderMetricV1) -> bytes:
    """Use source coordinates as the stable derived-event key."""

    return metric.source_record_id.encode("utf-8")


def connector_console_plan(
    *,
    topic: str,
    max_interval_ms: int,
) -> dict[str, Any]:
    """Return secret-free Cloud Console fields for the managed connector."""

    if max_interval_ms < 1_000:
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
    """Record assignments and optionally force an explicit replay."""

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
    """Wait for assignment and preserve any data returned by the same poll."""

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
    """Return comparable source-coordinate IDs from processor evidence."""

    return [str(row["source_record_id"]) for row in rows]
