"""Credential-free tests for Assignment 2 contracts and control flow."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from app import create_app  # noqa: E402
from config import (  # noqa: E402
    group_id_for_run,
    safe_kafka_config_report,
    safe_registry_config_report,
)
from consumer_runtime import (  # noqa: E402
    AssignmentTracker,
    consume_bounded,
    message_to_record,
)
from contracts import (  # noqa: E402
    CreateTripRequest,
    PublishError,
    PublishReceipt,
    TripEventV1,
    avro_dict_to_event,
    deterministic_requests,
    event_to_avro_dict,
    request_to_event,
    schema_str,
)
from run_consumer import settings_for_phase  # noqa: E402


class FakePublisher:
    """In-memory async publisher used by the FastAPI contract test."""

    def __init__(self) -> None:
        self.events: list[TripEventV1] = []
        self.closed = False

    async def publish(self, event: TripEventV1) -> PublishReceipt:
        self.events.append(event)
        return PublishReceipt(
            topic="assignment2-test-topic",
            key=event.trip_id,
            partition=0,
            offset=len(self.events) - 1,
            delivery="broker_acknowledged",
        )

    async def close(self) -> None:
        self.closed = True


class FailingPublisher(FakePublisher):
    """Publisher double that proves the route's secret-free 503 boundary."""

    async def publish(self, event: TripEventV1) -> PublishReceipt:
        raise PublishError("internal broker detail that must not reach HTTP")


class FakeMessage:
    """Minimal successful Kafka message used by the consumer loop."""

    def __init__(self, event: TripEventV1, offset: int) -> None:
        self.event = event
        self._offset = offset

    def topic(self) -> str:
        return "assignment2-test-topic"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return self.event.trip_id.encode("utf-8")

    def value(self) -> bytes:
        return b"fake-avro"

    def error(self) -> None:
        return None


class FakeConsumer:
    """Finite consumer double that records synchronous commit order."""

    def __init__(self, messages: list[FakeMessage], operations: list[tuple]) -> None:
        self.messages = list(messages)
        self.operations = operations

    def poll(self, _timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> list[Any]:
        self.operations.append(("commit", message.offset(), asynchronous))
        return [
            SimpleNamespace(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset() + 1,
            )
        ]


class FakeReplayConsumer:
    """Assignment double used to prove explicit beginning override."""

    def __init__(self) -> None:
        self.assigned: list[Any] | None = None

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = partitions


def fake_deserializer_for(message: FakeMessage):
    """Return a deserializer that exposes the message's validated event."""

    def deserialize(_value: bytes, _context: Any) -> TripEventV1:
        return message.event

    return deserialize


def test_deterministic_request_and_avro_contract_round_trip() -> None:
    """The independent 12-event source and application contract agree."""

    requests = deterministic_requests("test-run")
    assert len(requests) == 12
    assert [item.sequence_number for item in requests] == list(range(12))
    events = [request_to_event(item) for item in requests]
    assert all(event.run_id == "test-run" for event in events)
    assert len({event.trip_id for event in events}) == 12
    avro_record = event_to_avro_dict(events[0])
    assert avro_dict_to_event(avro_record) == events[0]
    assert json.loads(schema_str())["name"] == "TripEventV1"


def test_models_reject_unexpected_fields_and_naive_time() -> None:
    """Application validation is stricter than accepting an arbitrary dict."""

    payload = deterministic_requests("strict-test")[0].model_dump()
    payload["unexpected"] = "not allowed"
    with pytest.raises(ValidationError):
        CreateTripRequest.model_validate(payload)

    event = request_to_event(deterministic_requests("strict-test")[0])
    event_payload = event.model_dump()
    event_payload["event_time"] = event.event_time.replace(tzinfo=None)
    with pytest.raises(ValidationError):
        TripEventV1.model_validate(event_payload)


def test_fastapi_returns_202_after_publisher_receipt_and_closes() -> None:
    """The route uses one lifespan publisher and returns its acknowledged topic."""

    publisher = FakePublisher()

    async def factory() -> FakePublisher:
        return publisher

    payload = deterministic_requests("api-test")[0]
    with TestClient(create_app(factory)) as client:
        response = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        assert response.status_code == 202
        assert response.json()["delivery"] == "broker_acknowledged"
        assert response.json()["topic"] == "assignment2-test-topic"
        assert len(publisher.events) == 1
    assert publisher.closed is True


def test_fastapi_maps_publish_failure_to_secret_free_503() -> None:
    """A failed acknowledgement never becomes HTTP 202 or leaks internals."""

    publisher = FailingPublisher()

    async def factory() -> FailingPublisher:
        return publisher

    payload = deterministic_requests("api-failure")[0]
    with TestClient(create_app(factory)) as client:
        response = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
    assert response.status_code == 503
    assert response.json() == {
        "detail": "The event publisher is temporarily unavailable."
    }
    assert "internal broker detail" not in response.text
    assert publisher.closed is True


def test_message_deserializes_validates_and_checks_key() -> None:
    """Schema output must become a strict model with a matching Kafka key."""

    event = request_to_event(deterministic_requests("message-test")[0])
    message = FakeMessage(event, 7)
    record = message_to_record(message, fake_deserializer_for(message))
    assert record["offset"] == 7
    assert record["key"] == event.trip_id
    assert record["event"]["run_id"] == "message-test"


def test_consumer_writes_before_synchronous_commit() -> None:
    """Every accepted result is persisted before its input offset advances."""

    events = [
        request_to_event(item)
        for item in deterministic_requests("loop-test")[:2]
    ]
    operations: list[tuple] = []
    messages = [FakeMessage(event, index) for index, event in enumerate(events)]
    consumer = FakeConsumer(messages, operations)
    current = {"message": messages[0]}

    def deserializer(_value: bytes, _context: Any) -> TripEventV1:
        return current["message"].event

    original_poll = consumer.poll

    def poll(timeout: float) -> FakeMessage | None:
        message = original_poll(timeout)
        if message is not None:
            current["message"] = message
        return message

    consumer.poll = poll  # type: ignore[method-assign]

    def writer(record: dict[str, Any]) -> None:
        operations.append(("write", record["offset"]))

    result = consume_bounded(
        consumer,
        deserializer,
        run_id="loop-test",
        max_messages=2,
        poll_timeout=0.01,
        idle_timeout=0.2,
        run_timeout=1.0,
        record_writer=writer,
    )
    assert result.stop_reason == "max_messages"
    assert operations == [
        ("write", 0),
        ("commit", 0, False),
        ("write", 1),
        ("commit", 1, False),
    ]


def test_processing_failure_prevents_offset_commit() -> None:
    """A failed local write leaves the input uncommitted for later recovery."""

    event = request_to_event(deterministic_requests("failure-test")[0])
    message = FakeMessage(event, 0)
    operations: list[tuple] = []
    consumer = FakeConsumer([message], operations)

    def writer(_record: dict[str, Any]) -> None:
        raise OSError("simulated processing failure")

    with pytest.raises(OSError, match="simulated processing failure"):
        consume_bounded(
            consumer,
            fake_deserializer_for(message),
            run_id="failure-test",
            max_messages=1,
            poll_timeout=0.01,
            idle_timeout=0.2,
            run_timeout=1.0,
            record_writer=writer,
        )
    assert operations == []


def test_first_resume_and_replay_group_contracts(tmp_path: Path) -> None:
    """Base phases share history while replay has a separate beginning."""

    first = settings_for_phase(
        "first",
        run_id="run-a",
        base_group="student-base",
        results_dir=tmp_path,
    )
    resume = settings_for_phase(
        "resume",
        run_id="run-a",
        base_group="student-base",
        results_dir=tmp_path,
    )
    replay = settings_for_phase(
        "replay",
        run_id="run-a",
        base_group="student-base",
        results_dir=tmp_path,
    )
    assert first.group_id == resume.group_id
    assert replay.group_id != first.group_id
    assert first.output_mode == "w"
    assert resume.output_mode == "a"
    assert replay.force_beginning is True
    assert replay.report_filename == "consumer_replay_run.json"
    assert group_id_for_run("student-base", "run-a", replay=True) == replay.group_id


def test_replay_assignment_forces_beginning() -> None:
    """Explicit replay changes assigned offsets rather than trusting fallback."""

    partitions = [
        SimpleNamespace(topic="topic", partition=0, offset=42),
        SimpleNamespace(topic="topic", partition=1, offset=99),
    ]
    consumer = FakeReplayConsumer()
    tracker = AssignmentTracker(force_beginning=True)
    tracker.on_assign(consumer, partitions)
    assert consumer.assigned is partitions
    assert all(item.offset < 0 for item in partitions)
    assert len(tracker.assigned) == 1


def test_safe_reports_exclude_secret_values() -> None:
    """Evidence confirms presence without copying credentials."""

    kafka = {
        "bootstrap.servers": "pkc.example:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "secret-key",
        "sasl.password": "secret-password",
        "client.id": "test",
    }
    registry = {
        "url": "https://psrc.example",
        "basic.auth.user.info": "registry-key:registry-secret",
    }
    serialized = json.dumps(
        {
            "kafka": safe_kafka_config_report(kafka),
            "registry": safe_registry_config_report(registry),
        }
    )
    assert "secret-key" not in serialized
    assert "secret-password" not in serialized
    assert "registry-key" not in serialized
    assert "registry-secret" not in serialized
