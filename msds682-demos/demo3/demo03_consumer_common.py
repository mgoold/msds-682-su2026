"""Shared consumer contracts for the bounded Lecture 3 Confluent demos.

Student note: this module deliberately reuses Demo 02's topic, TripEvent model,
connection loader, and secret-free report helper. Consumer-specific behavior
lives here so Demo 03A–03D do not duplicate configuration or offset rules.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from confluent_kafka import KafkaError, KafkaException, OFFSET_BEGINNING

from demo02_producer_common import (
    TOPIC_NAME,
    TripEvent,
    load_dotenv_for_demo,
    require_producer_config,
    safe_config_report,
)


CommitMode = Literal["none", "sync", "async"]
OffsetReset = Literal["earliest", "latest", "error"]


def normalize_identifier(value: str) -> str:
    """Return a Kafka-friendly identifier while preserving useful words."""

    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip())
    return normalized.strip("-") or "demo"


def default_group_id(demo_name: str, run_id: str | None = None) -> str:
    """Build a predictable consumer group ID from one optional environment prefix."""

    load_dotenv_for_demo()
    prefix = normalize_identifier(os.getenv("CONSUMER_GROUP_ID_PREFIX", "msds682-su2026"))
    parts = [prefix, normalize_identifier(demo_name)]
    if run_id:
        parts.append(normalize_identifier(run_id))
    return "-".join(parts)


def build_consumer_config(
    kafka_config: dict[str, str],
    *,
    group_id: str,
    auto_offset_reset: OffsetReset,
    enable_auto_commit: bool,
    client_id: str,
    on_commit: Any | None = None,
) -> dict[str, Any]:
    """Extend the shared Kafka connection config with consumer-only settings."""

    config: dict[str, Any] = {
        **kafka_config,
        "group.id": group_id,
        "client.id": client_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
    }
    if not enable_auto_commit:
        # The application decides when a successfully processed message advances
        # the stored/committed position.
        config["enable.auto.offset.store"] = False
    if on_commit is not None:
        config["on_commit"] = on_commit
    return config


def require_consumer_config(
    *,
    group_id: str,
    auto_offset_reset: OffsetReset = "earliest",
    enable_auto_commit: bool = True,
    client_id: str,
    on_commit: Any | None = None,
) -> dict[str, Any]:
    """Load the shared Confluent config and require all consumer settings."""

    return build_consumer_config(
        require_producer_config(),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        client_id=client_id,
        on_commit=on_commit,
    )


def safe_consumer_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Return consumer configuration evidence without credential values."""

    connection = safe_config_report(config)
    return {
        **connection,
        "group_id": config["group.id"],
        "client_id": config["client.id"],
        "auto_offset_reset": config["auto.offset.reset"],
        "enable_auto_commit": bool(config["enable.auto.commit"]),
        "enable_auto_offset_store": bool(config.get("enable.auto.offset.store", True)),
    }


def decode_utf8(value: bytes | None, field_name: str) -> str | None:
    """Decode one optional Kafka byte field and raise a useful error."""

    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Kafka {field_name} is not valid UTF-8") from exc


def message_to_record(message: Any) -> dict[str, Any]:
    """Decode one Kafka message and validate its JSON value as a TripEvent."""

    raw_value = decode_utf8(message.value(), "value")
    if raw_value is None:
        raise ValueError("Kafka message value is missing")
    try:
        event = TripEvent.model_validate_json(raw_value)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Kafka value is not a valid Demo 02 TripEvent JSON document") from exc

    timestamp_type: int | None = None
    timestamp_ms: int | None = None
    if hasattr(message, "timestamp"):
        timestamp_type, timestamp_ms = message.timestamp()

    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp_type": timestamp_type,
        "timestamp_ms": timestamp_ms,
        "key": decode_utf8(message.key(), "key"),
        "event": event.model_dump(exclude_none=True),
    }


def topic_partition_records(partitions: Any | None) -> list[dict[str, int | str]]:
    """Convert TopicPartition objects into JSON-safe evidence rows."""

    if not partitions:
        return []
    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": partition.offset,
        }
        for partition in partitions
    ]


@dataclass
class CommitTracker:
    """Collect asynchronous offset-commit acknowledgements."""

    acknowledged: list[list[dict[str, int | str]]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, partitions: Any) -> None:
        if error is not None:
            self.failed.append(str(error))
            return
        self.acknowledged.append(topic_partition_records(partitions))


@dataclass
class AssignmentTracker:
    """Record group rebalances and optionally request an explicit replay."""

    force_beginning: bool = False
    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    def on_assign(self, consumer: Any, partitions: Any) -> None:
        if self.force_beginning:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
        rows = topic_partition_records(partitions)
        self.assigned.append(rows)
        print(f"Assigned partitions: {rows}")

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        rows = topic_partition_records(partitions)
        self.revoked.append(rows)
        print(f"Revoked partitions: {rows}")


@dataclass
class ConsumeResult:
    """Bounded consumer-loop result used by the three synchronous demos."""

    records: list[dict[str, Any]]
    stop_reason: str
    commit_requests: int
    synchronous_commit_results: list[list[dict[str, int | str]]]


def consume_records(
    consumer: Any,
    *,
    max_messages: int,
    poll_timeout: float,
    idle_timeout: float,
    run_timeout: float | None = None,
    commit_mode: CommitMode = "none",
) -> ConsumeResult:
    """Poll, validate, optionally commit, and stop at visible finite limits."""

    if max_messages < 1:
        raise ValueError("max_messages must be at least 1")
    if poll_timeout <= 0 or idle_timeout <= 0:
        raise ValueError("poll_timeout and idle_timeout must be positive")
    if run_timeout is not None and run_timeout <= 0:
        raise ValueError("run_timeout must be positive when provided")

    started = time.monotonic()
    last_message_at = started
    records: list[dict[str, Any]] = []
    commit_requests = 0
    synchronous_results: list[list[dict[str, int | str]]] = []
    stop_reason = "max_messages"

    while len(records) < max_messages:
        now = time.monotonic()
        if run_timeout is not None and now - started >= run_timeout:
            stop_reason = "run_timeout"
            break
        if now - last_message_at >= idle_timeout:
            stop_reason = "idle_timeout"
            break

        message = consumer.poll(poll_timeout)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(message.error())

        # Validate/process first. Manual commits happen only after this succeeds.
        record = message_to_record(message)
        records.append(record)
        last_message_at = time.monotonic()
        print(
            f"Consumed {record['topic']}[{record['partition']}] "
            f"offset={record['offset']} key={record['key']}"
        )

        if commit_mode != "none":
            asynchronous = commit_mode == "async"
            committed = consumer.commit(message=message, asynchronous=asynchronous)
            commit_requests += 1
            if not asynchronous:
                synchronous_results.append(topic_partition_records(committed))

    return ConsumeResult(
        records=records,
        stop_reason=stop_reason,
        commit_requests=commit_requests,
        synchronous_commit_results=synchronous_results,
    )


def assert_expected_topic(records: list[dict[str, Any]]) -> None:
    """Fail if a demo record came from a topic outside the shared Lec 2/3 thread."""

    unexpected = sorted({record["topic"] for record in records if record["topic"] != TOPIC_NAME})
    if unexpected:
        raise RuntimeError(f"Unexpected topic(s): {', '.join(unexpected)}")
