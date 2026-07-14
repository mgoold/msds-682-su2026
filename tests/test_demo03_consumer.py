from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest


HANDOUTS = Path(__file__).resolve().parents[1] / "handouts"
sys.path.insert(0, str(HANDOUTS))

from demo02_producer_common import TOPIC_NAME, make_trip_events, serialize_event  # noqa: E402
import demo03d_confluent_asyncio_produce_consume as demo03d  # noqa: E402
from demo03_consumer_common import (  # noqa: E402
    AssignmentTracker,
    build_consumer_config,
    consume_records,
    default_group_id,
    message_to_record,
    safe_consumer_config_report,
)


class FakeTopicPartition:
    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FakeMessage:
    def __init__(
        self,
        *,
        value: bytes,
        key: bytes = b"trip_981",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = value
        self._key = key
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes:
        return self._key

    def topic(self) -> str:
        return TOPIC_NAME

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_720_087_200_000 + self._offset)

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = list(messages)
        self.commits: list[tuple[FakeMessage, bool]] = []
        self.assigned: list[FakeTopicPartition] = []

    def poll(self, _timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> Any:
        self.commits.append((message, asynchronous))
        if asynchronous:
            return None
        return [FakeTopicPartition(TOPIC_NAME, message.partition(), message.offset() + 1)]

    def assign(self, partitions: list[FakeTopicPartition]) -> None:
        self.assigned = partitions


class FakeDeliveredMessage:
    def __init__(self, key: bytes) -> None:
        self._key = key

    def topic(self) -> str:
        return TOPIC_NAME

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 21

    def key(self) -> bytes:
        return self._key


class FakeAIOProducer:
    produce_calls: list[tuple[str, bytes, bytes]] = []

    def __init__(self, _config: dict[str, str]) -> None:
        pass

    async def produce(self, topic: str, *, key: bytes, value: bytes) -> asyncio.Future[Any]:
        self.produce_calls.append((topic, key, value))
        future = asyncio.get_running_loop().create_future()
        future.set_result(FakeDeliveredMessage(key))
        return future

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class FakeAIOConsumer:
    def __init__(self, _config: dict[str, Any]) -> None:
        event = make_trip_events(1, 682)[0]
        self.message = FakeMessage(value=serialize_event(event), offset=20)
        self.on_assign: Any = None
        self.on_revoke: Any = None
        self.assigned: list[FakeTopicPartition] = []

    async def subscribe(self, _topics: list[str], *, on_assign: Any, on_revoke: Any) -> None:
        self.on_assign = on_assign
        self.on_revoke = on_revoke

    async def assign(self, partitions: list[FakeTopicPartition]) -> None:
        self.assigned = partitions

    async def poll(self, timeout: float) -> FakeMessage | None:
        del timeout
        if self.message is None:
            return None
        await self.on_assign(self, [FakeTopicPartition(TOPIC_NAME, 0, 20)])
        message, self.message = self.message, None
        return message

    async def unsubscribe(self) -> None:
        await self.on_revoke(self, self.assigned)

    async def close(self) -> None:
        pass


def kafka_connection_config() -> dict[str, str]:
    return {
        "bootstrap.servers": "example.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "not-reported",
        "sasl.password": "not-reported",
    }


def test_consumer_config_extends_shared_connection_without_duplicating_it() -> None:
    config = build_consumer_config(
        kafka_connection_config(),
        group_id="demo-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        client_id="demo-client",
    )

    assert config["bootstrap.servers"] == "example.confluent.cloud:9092"
    assert config["group.id"] == "demo-group"
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False


def test_safe_consumer_report_contains_flags_but_not_credentials() -> None:
    config = build_consumer_config(
        kafka_connection_config(),
        group_id="demo-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        client_id="demo-client",
    )
    report = safe_consumer_config_report(config)

    assert report["has_username"] is True
    assert report["has_password"] is True
    assert "sasl.username" not in report
    assert "sasl.password" not in report
    assert "not-reported" not in str(report)


def test_message_is_decoded_and_validated_as_the_demo02_trip_contract() -> None:
    event = make_trip_events(1, 682)[0]
    record = message_to_record(FakeMessage(value=serialize_event(event), offset=7))

    assert record["topic"] == TOPIC_NAME
    assert record["offset"] == 7
    assert record["key"] == "trip_981"
    assert record["event"]["trip_id"] == "trip_981"


def test_invalid_utf8_fails_before_offset_commit() -> None:
    consumer = FakeConsumer([FakeMessage(value=b"\xff")])

    with pytest.raises(ValueError, match="not valid UTF-8"):
        consume_records(
            consumer,
            max_messages=1,
            poll_timeout=0.01,
            idle_timeout=0.1,
            commit_mode="sync",
        )
    assert consumer.commits == []


def test_manual_sync_commit_occurs_after_each_valid_message() -> None:
    events = make_trip_events(2, 682)
    consumer = FakeConsumer(
        [
            FakeMessage(value=serialize_event(events[0]), offset=3),
            FakeMessage(value=serialize_event(events[1]), offset=4),
        ]
    )

    result = consume_records(
        consumer,
        max_messages=2,
        poll_timeout=0.01,
        idle_timeout=0.1,
        commit_mode="sync",
    )

    assert len(result.records) == 2
    assert result.commit_requests == 2
    assert [asynchronous for _, asynchronous in consumer.commits] == [False, False]
    assert result.synchronous_commit_results[-1][0]["offset"] == 5


def test_force_beginning_is_explicit_and_records_the_assignment() -> None:
    consumer = FakeConsumer([])
    partitions = [FakeTopicPartition(TOPIC_NAME, 0, 42)]
    tracker = AssignmentTracker(force_beginning=True)

    tracker.on_assign(consumer, partitions)

    assert consumer.assigned[0].offset < 0
    assert tracker.assigned[0][0]["partition"] == 0


def test_group_id_uses_one_environment_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONSUMER_GROUP_ID_PREFIX", "Student Name")

    assert default_group_id("demo03d asyncio", "run 1") == "Student-Name-demo03d-asyncio-run-1"


def test_async_producer_waits_for_consumer_assignment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(demo03d, "AIOProducer", FakeAIOProducer)
    FakeAIOProducer.produce_calls.clear()

    async def scenario() -> tuple[list[dict[str, Any]], float]:
        assignment_ready = asyncio.Event()
        task = asyncio.create_task(
            demo03d.produce_events(
                {},
                count=1,
                seed=682,
                assignment_ready=assignment_ready,
                assignment_timeout=0.5,
                interval=0,
            )
        )
        await asyncio.sleep(0)
        assert FakeAIOProducer.produce_calls == []
        assignment_ready.set()
        return await task

    delivered, wait_seconds = asyncio.run(scenario())
    assert len(delivered) == 1
    assert len(FakeAIOProducer.produce_calls) == 1
    assert wait_seconds >= 0


def test_async_producer_assignment_timeout_is_actionable() -> None:
    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="assignment.*not ready"):
            await demo03d.produce_events(
                {},
                count=1,
                seed=682,
                assignment_ready=asyncio.Event(),
                assignment_timeout=0.01,
                interval=0,
            )

    asyncio.run(scenario())


def test_async_consumer_signals_only_after_assignment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(demo03d, "AIOConsumer", FakeAIOConsumer)

    async def scenario() -> tuple[
        bool,
        list[dict[str, Any]],
        list[list[dict[str, int | str]]],
        list[list[dict[str, int | str]]],
    ]:
        assignment_ready = asyncio.Event()
        records, assignments, revocations = await demo03d.consume_events(
            {},
            expected_count=1,
            timeout=0.5,
            assignment_ready=assignment_ready,
        )
        return assignment_ready.is_set(), records, assignments, revocations

    ready, records, assignments, revocations = asyncio.run(scenario())
    assert ready is True
    assert len(records) == 1
    assert assignments[0][0]["partition"] == 0
    assert revocations[0][0]["partition"] == 0
