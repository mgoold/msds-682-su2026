"""Reusable bounded consumer loop and assignment evidence for Assignment 2."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import KafkaError, KafkaException, OFFSET_BEGINNING
from confluent_kafka.serialization import MessageField, SerializationContext

from contracts import TripEventV1


def partition_rows(partitions: Any | None) -> list[dict[str, int | str]]:
    """Convert TopicPartition values into JSON-safe evidence."""

    if not partitions:
        return []
    return [
        {
            "topic": item.topic,
            "partition": item.partition,
            "offset": item.offset,
        }
        for item in partitions
    ]


@dataclass
class AssignmentTracker:
    """Record assignments and optionally request an explicit replay."""

    force_beginning: bool = False
    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    def on_assign(self, consumer: Any, partitions: Any) -> None:
        """Record assignment and override offsets only for explicit replay."""

        # ==================== CODE START HERE ====================
        # TODO: in force_beginning mode, set every assigned partition offset to
        # OFFSET_BEGINNING and call consumer.assign(partitions). Always record
        # the resulting partition rows in self.assigned.
        raise NotImplementedError("Implement explicit replay assignment")
        # ===================== CODE ENDS HERE =====================

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        """Record partition revocation during group cleanup or rebalance."""

        self.revoked.append(partition_rows(partitions))


def message_to_record(message: Any, deserializer: Any) -> dict[str, Any]:
    """Deserialize, validate, and verify one Kafka key/value record."""

    # ==================== CODE START HERE ====================
    # TODO:
    # 1. require a nonempty message value;
    # 2. deserialize it with a VALUE SerializationContext;
    # 3. require/validate TripEventV1;
    # 4. decode the UTF-8 key and ensure it equals event.trip_id; and
    # 5. return topic/partition/offset/key plus JSON-safe event data.
    raise NotImplementedError("Implement schema-aware message validation")
    # ===================== CODE ENDS HERE =====================


class JsonlWriter:
    """Write and flush one processing result before its input commit."""

    def __init__(self, path: Path, mode: str) -> None:
        self.path = path
        self.mode = mode
        self._handle: Any | None = None

    def __enter__(self) -> "JsonlWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open(self.mode, encoding="utf-8")
        return self

    def write(self, record: dict[str, Any]) -> None:
        """Persist and flush a secret-free processing result."""

        if self._handle is None:
            raise RuntimeError("JsonlWriter is not open")
        output = {
            "run_id": record["event"]["run_id"],
            "sequence_number": record["event"]["sequence_number"],
            "trip_id": record["event"]["trip_id"],
            "event_time": record["event"]["event_time"],
            "zone": record["event"]["zone"],
            "source": {
                "topic": record["topic"],
                "partition": record["partition"],
                "offset": record["offset"],
            },
            "processing_status": "accepted",
        }
        self._handle.write(json.dumps(output, sort_keys=True) + "\n")
        self._handle.flush()

    def __exit__(self, *_args: Any) -> None:
        if self._handle is not None:
            self._handle.close()


@dataclass
class ConsumeResult:
    """Evidence returned by one bounded consume/process/commit phase."""

    records: list[dict[str, Any]]
    commit_results: list[list[dict[str, int | str]]]
    skipped_other_runs: int
    stop_reason: str


def consume_bounded(
    consumer: Any,
    deserializer: Any,
    *,
    run_id: str,
    max_messages: int,
    poll_timeout: float,
    idle_timeout: float,
    run_timeout: float,
    record_writer: Callable[[dict[str, Any]], None],
) -> ConsumeResult:
    """Poll, validate, process, synchronously commit, and stop visibly."""

    # ==================== CODE START HERE ====================
    # TODO: implement a finite poll loop. Handle None and partition EOF,
    # surface real errors, skip other run IDs, call record_writer(record)
    # before consumer.commit(message=..., asynchronous=False), collect commit
    # evidence, and stop on max_messages, idle_timeout, or run_timeout.
    raise NotImplementedError("Implement the bounded process-before-commit loop")
    # ===================== CODE ENDS HERE =====================
