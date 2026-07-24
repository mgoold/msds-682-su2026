"""Reusable Kafka mechanics for the bounded Demo 07 processors."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaException, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient

from confluent_demo_common import ensure_topic
from demo07_common import topic_names


@dataclass
class DeliveryTracker:
    """Capture broker acknowledgements without storing payloads or secrets."""

    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, message: Any) -> None:
        if error is not None:
            self.failed.append(str(error))
            return
        self.delivered.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }
        )


@dataclass
class AssignmentTracker:
    """Record assignment and optionally force explicit replay from beginning."""

    force_beginning: bool = False
    assigned: list[dict[str, Any]] = field(default_factory=list)

    def on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        if self.force_beginning:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)
        self.assigned = [
            {
                "topic": partition.topic,
                "partition": partition.partition,
                "offset": partition.offset,
            }
            for partition in partitions
        ]

    def on_revoke(self, _consumer: Any, _partitions: list[Any]) -> None:
        return None


def wait_for_assignment(
    consumer: Any,
    tracker: AssignmentTracker,
    *,
    timeout: float,
) -> tuple[float, list[Any]]:
    """Wait for assignment without discarding a record returned during the wait."""

    if timeout <= 0:
        raise ValueError("timeout must be positive")
    started = time.monotonic()
    pending: list[Any] = []
    while not tracker.assigned and time.monotonic() - started < timeout:
        message = consumer.poll(0.25)
        if message is not None and not message.error():
            pending.append(message)
    if not tracker.assigned:
        raise RuntimeError("Consumer did not receive an assignment before timeout")
    return round(time.monotonic() - started, 6), pending


def ensure_demo07_topics(
    admin: AdminClient,
    *,
    create: bool,
    partitions: int,
    replication_factor: int,
) -> dict[str, str]:
    """Verify or create every topic in the Demo 07 lifecycle."""

    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    return {
        owner: ensure_topic(
            admin,
            topic=topic,
            create=create,
            partitions=partitions,
            replication_factor=replication_factor,
            create_option="--create-topics",
        )
        for owner, topic in topic_names().items()
    }


def message_coordinate(message: Any) -> str:
    return f"{message.topic()}:{message.partition()}:{message.offset()}"


def acknowledged_produce(
    producer: Any,
    *,
    topic: str,
    key: bytes,
    value: bytes,
    delivery_timeout: float,
    headers: list[tuple[str, bytes]] | None = None,
) -> dict[str, Any]:
    """Produce one value and require its broker acknowledgement."""

    if delivery_timeout <= 0:
        raise ValueError("delivery_timeout must be positive")
    tracker = DeliveryTracker()
    producer.produce(
        topic,
        key=key,
        value=value,
        headers=headers,
        on_delivery=tracker.callback,
    )
    producer.poll(0)
    remaining = producer.flush(delivery_timeout)
    if remaining or tracker.failed or len(tracker.delivered) != 1:
        raise RuntimeError(
            f"Output to {topic!r} was not acknowledged; input must not be committed"
        )
    return tracker.delivered[0]


def commit_message(consumer: Any, message: Any) -> list[dict[str, Any]]:
    """Synchronously commit exactly the next offset for one input record."""

    committed = consumer.commit(message=message, asynchronous=False)
    if committed is None:
        raise RuntimeError("Synchronous input commit returned no result")
    failures = [
        partition
        for partition in committed
        if getattr(partition, "error", None) is not None
    ]
    if failures:
        raise KafkaException(failures[0].error)
    expected = message.offset() + 1
    if not any(
        partition.topic == message.topic()
        and partition.partition == message.partition()
        and partition.offset == expected
        for partition in committed
    ):
        raise RuntimeError("Input commit did not confirm the expected next offset")
    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": partition.offset,
        }
        for partition in committed
    ]


def commit_message_batch(
    consumer: Any,
    messages: list[Any],
) -> list[dict[str, Any]]:
    """Commit highest next offsets after every derived output is acknowledged."""

    if not messages:
        raise ValueError("messages must not be empty")
    highest: dict[tuple[str, int], int] = {}
    for message in messages:
        coordinate = (message.topic(), message.partition())
        highest[coordinate] = max(
            highest.get(coordinate, 0),
            message.offset() + 1,
        )
    requested = [
        TopicPartition(topic, partition, offset)
        for (topic, partition), offset in sorted(highest.items())
    ]
    committed = consumer.commit(offsets=requested, asynchronous=False)
    if committed is None:
        raise RuntimeError("Synchronous batch commit returned no result")
    failures = [
        partition
        for partition in committed
        if getattr(partition, "error", None) is not None
    ]
    if failures:
        raise KafkaException(failures[0].error)
    observed = {
        (partition.topic, partition.partition): partition.offset
        for partition in committed
    }
    expected = {
        (partition.topic, partition.partition): partition.offset
        for partition in requested
    }
    if observed != expected:
        raise RuntimeError(
            f"Batch commit mismatch: expected {expected}, observed {observed}"
        )
    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": partition.offset,
        }
        for partition in committed
    ]
