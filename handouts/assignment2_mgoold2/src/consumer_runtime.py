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

        if self.force_beginning:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
        rows = partition_rows(partitions)
        self.assigned.append(rows)

        # ===================== CODE ENDS HERE =====================

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        """Record partition revocation during group cleanup or rebalance."""

        self.revoked.append(partition_rows(partitions))


def message_to_record(message: Any, deserializer: Any) -> dict[str, Any]:
    """Deserialize, validate, and verify one Kafka key/value record."""

    # ==================== CODE START HERE ====================
    # TODO:
    # 1. require a nonempty message value;
    raw_value = message.value()
    if not raw_value:
        raise ValueError("Kafka message value is missing")

    # 2. deserialize it with a VALUE SerializationContext;
    context = SerializationContext(message.topic(), MessageField.VALUE)
    event = deserializer(raw_value, context)

    # 3. require/validate TripEventV1;
    if not isinstance(event, TripEventV1):
        raise TypeError("Expected AvroDeserializer to return TripEventV1")

    message_key = message.key()

    # Check if message key exists
    if not message_key:
        raise ValueError("Kafka message key is missing")

    # 4. decode the UTF-8 key and ensure it equals event.trip_id; and
    message_key_str = message_key.decode("utf-8")
    if message_key_str != event.trip_id:
        raise ValueError(f"{message_key_str} is not equal to event.trip_id")

    # 5. return topic/partition/offset/key plus JSON-safe event data.

    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": message_key_str,
        "event": event.model_dump(mode="json"),
    }

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
    # before consumer.commit(message=..., asynchronous=False), reject a missing
    # result or any partition-level commit error, confirm offset+1, collect
    # successful commit evidence, and stop on max_messages, idle_timeout, or
    # run_timeout.

    started = time.monotonic()
    last_message_at = started
    records: list[dict[str, Any]] = []
    commit_results: list[list[dict[str, int | str]]] = []
    skipped_other_runs = 0
    stop_reason = "max_messages"

    while len(records) < max_messages:
        now = time.monotonic()
        if now - started >= run_timeout:
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
        record = message_to_record(message, deserializer)
        if run_id != record["event"]["run_id"]:
            skipped_other_runs += 1
            last_message_at = time.monotonic()
            continue
        record_writer(record)
        records.append(record)
        last_message_at = time.monotonic()        

        print(
            f"Consumed {record['topic']}[{record['partition']}] "
            f"offset={record['offset']} key={record['key']}"
        )

        committed = consumer.commit(message=message, asynchronous=False)
        if not committed:
            raise ValueError("Kafka returned no commit result.")
        for committed_partition in committed:
            if committed_partition.error is not None:
                raise KafkaException(committed_partition.error)
            if committed_partition.offset != message.offset() + 1:
                raise ValueError(
                    f"committed_partition.offset {committed_partition.offset} "
                    f"!= message.offset + 1 {message.offset() + 1}"
                )
        commit_results.append(partition_rows(committed))

    return ConsumeResult(
        records=records,
        stop_reason=stop_reason,
        commit_results=commit_results,
        skipped_other_runs=skipped_other_runs,
    )

    # ===================== CODE ENDS HERE =====================
