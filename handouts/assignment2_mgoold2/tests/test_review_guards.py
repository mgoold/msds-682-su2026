"""Extra-credit review tests for guards the provided suite never executes.

The provided tests/test_consumer_application.py always builds messages whose
Kafka key matches the payload and whose run ID matches the consumer's, so the
key-verification guard and the run-ID filter are never reached. Removing either
one leaves all eleven provided tests passing. These tests exercise both.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from consumer_runtime import consume_bounded, message_to_record  # noqa: E402
from contracts import TripEventV1, deterministic_requests, request_to_event  # noqa: E402


class StubMessage:
    """Kafka message stand-in whose key is set independently of its payload."""

    def __init__(self, event: TripEventV1, offset: int, key: bytes | None) -> None:
        self.event = event
        self._offset = offset
        self._key = key

    def topic(self) -> str:
        return "assignment2-review-topic"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes:
        return b"fake-avro"

    def error(self) -> None:
        return None


class ReviewConsumer:
    """Finite consumer double that records commit order for the filter test."""

    def __init__(self, messages: list[StubMessage], operations: list[tuple]) -> None:
        self.messages = list(messages)
        self.operations = operations
        self.current: StubMessage | None = None

    def poll(self, _timeout: float) -> StubMessage | None:
        self.current = self.messages.pop(0) if self.messages else None
        return self.current

    def commit(self, *, message: StubMessage, asynchronous: bool) -> list[Any]:
        self.operations.append(("commit", message.event.run_id, message.offset()))
        return [
            SimpleNamespace(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset() + 1,
                error=None,
            )
        ]


def event_for(run_id: str, index: int = 0) -> TripEventV1:
    """Build one deterministic event without touching Kafka."""

    return request_to_event(deterministic_requests(run_id)[index])


def matching_key(event: TripEventV1) -> bytes:
    """Return the key a correct producer would set for this event."""

    return event.trip_id.encode("utf-8")


def test_key_that_disagrees_with_payload_is_rejected() -> None:
    """A key naming a different trip must stop the record before processing."""

    event = event_for("review-key")
    message = StubMessage(event, 0, b"trip_9999")
    assert message.key().decode("utf-8") != event.trip_id

    with pytest.raises(ValueError):
        message_to_record(message, lambda _value, _context: event)


def test_missing_key_is_rejected() -> None:
    """An absent key cannot be verified, so it must not be accepted silently."""

    event = event_for("review-nokey")
    message = StubMessage(event, 0, None)

    with pytest.raises(ValueError):
        message_to_record(message, lambda _value, _context: event)


def test_matching_key_is_accepted() -> None:
    """Control case: the same guard passes a key that agrees with the payload."""

    event = event_for("review-ok")
    message = StubMessage(event, 7, matching_key(event))
    record = message_to_record(message, lambda _value, _context: event)

    assert record["key"] == event.trip_id
    assert record["offset"] == 7


def test_foreign_run_id_is_skipped_without_write_or_commit() -> None:
    """Records from another run are counted, never written, never committed."""

    mine = event_for("mine")
    theirs = event_for("theirs")
    operations: list[tuple] = []
    consumer = ReviewConsumer(
        [
            StubMessage(theirs, 0, matching_key(theirs)),
            StubMessage(mine, 1, matching_key(mine)),
        ],
        operations,
    )

    def deserializer(_value: bytes, _context: Any) -> TripEventV1:
        assert consumer.current is not None
        return consumer.current.event

    def writer(record: dict[str, Any]) -> None:
        operations.append(("write", record["event"]["run_id"], record["offset"]))

    result = consume_bounded(
        consumer,
        deserializer,
        run_id="mine",
        max_messages=1,
        poll_timeout=0.01,
        idle_timeout=0.5,
        run_timeout=2.0,
        record_writer=writer,
    )

    assert result.skipped_other_runs == 1
    assert len(result.records) == 1
    assert result.records[0]["event"]["run_id"] == "mine"
    assert operations == [("write", "mine", 1), ("commit", "mine", 1)]
