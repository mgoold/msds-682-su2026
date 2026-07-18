from __future__ import annotations

import asyncio
import json

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

import demo04c_confluent_avro_roundtrip as demo04c
import demo04d_asyncio_avro_roundtrip as demo04d
from demo04_common import (
    TripEventV1,
    avro_dict_to_event,
    avro_dict_to_event_v2,
    deserializer_conf,
    deterministic_events,
    event_key,
    event_to_avro_dict,
    parse_confluent_wire_header,
    schema_v1_str,
    schema_v2_str,
    safe_registry_config_report,
    serializer_conf,
    synthetic_data_report,
    validate_run_id,
)
from demo04a_schema_validation import run_validation


def test_validation_cases_match_expectations() -> None:
    rows = run_validation()
    assert len(rows) >= 8
    assert all(row["expectation_met"] for row in rows)


def test_timezone_is_normalized_and_derived() -> None:
    event = TripEventV1.model_validate_json(
        '{"trip_id":"trip_4200","event_type":"trip_completed",'
        '"rider_id":"rider_420","event_time":"2026-07-16T17:05:00-07:00",'
        '"zone":"west","driver_id":"driver_420","fare":28.0}'
    )
    assert event.event_time.isoformat() == "2026-07-17T00:05:00+00:00"
    assert event.event_date == "2026-07-17"
    assert event.event_hour == 0


def test_synthetic_events_are_reproducible_and_self_describing() -> None:
    first = deterministic_events(4, seed_offset=7)
    second = deterministic_events(4, seed_offset=7)
    different = deterministic_events(4, seed_offset=8)
    assert first == second
    assert first != different
    report = synthetic_data_report(first, seed_offset=7)
    assert report["prior_kafka_data_required"] is False
    assert report["count"] == 4
    assert report["first_trip_id"] == first[0].trip_id


def test_naive_timestamp_is_rejected() -> None:
    try:
        TripEventV1.model_validate_json(
            '{"trip_id":"trip_4201","event_type":"trip_requested",'
            '"rider_id":"rider_421","event_time":"2026-07-16T17:05:00",'
            '"zone":"north"}'
        )
    except ValidationError:
        return
    raise AssertionError("A timezone-naive event_time should be rejected")


def test_avro_v1_roundtrip_and_v2_reader_default() -> None:
    with SchemaRegistryClient.new_client({"url": "mock://test-demo04"}) as registry:
        context = SerializationContext("test.demo04.avro", MessageField.VALUE)
        serializer = AvroSerializer(
            registry,
            schema_v1_str(),
            to_dict=event_to_avro_dict,
            conf=serializer_conf(),
        )
        deserializer_v1 = AvroDeserializer(
            registry,
            schema_v1_str(),
            from_dict=avro_dict_to_event,
            conf=deserializer_conf(),
        )
        deserializer_v2 = AvroDeserializer(
            registry,
            schema_v2_str(),
            from_dict=avro_dict_to_event_v2,
            conf=deserializer_conf(),
        )
        event = deterministic_events(4)[3]
        payload = serializer(event, context)
        assert payload is not None
        header = parse_confluent_wire_header(payload)
        assert header["magic_byte"] == 0
        assert header["schema_id"] > 0
        decoded_v1 = deserializer_v1(payload, context)
        decoded_v2 = deserializer_v2(payload, context)
        assert decoded_v1 == event
        assert decoded_v2.vehicle_type is None


def test_avro_type_does_not_replace_business_validation() -> None:
    invalid = {
        "trip_id": "trip_4999",
        "event_type": "trip_completed",
        "rider_id": "rider_499",
        "event_time": deterministic_events(1)[0].event_time,
        "zone": "north",
        "driver_id": "driver_499",
        "fare": -1.0,
    }
    with SchemaRegistryClient.new_client({"url": "mock://test-business-rule"}) as registry:
        serializer = AvroSerializer(registry, schema_v1_str(), conf=serializer_conf())
        payload = serializer(
            invalid,
            SerializationContext("test.demo04.rules", MessageField.VALUE),
        )
        assert payload is not None
    try:
        TripEventV1.model_validate(invalid)
    except ValidationError:
        return
    raise AssertionError("Pydantic should reject a negative fare")


def test_async_avro_serdes_roundtrip() -> None:
    from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
    from confluent_kafka.schema_registry.avro import (
        AsyncAvroDeserializer,
        AsyncAvroSerializer,
    )

    async def run() -> None:
        async with AsyncSchemaRegistryClient.new_client(
            {"url": "mock://test-async-demo04"}
        ) as registry:
            serializer = await AsyncAvroSerializer(
                registry,
                schema_v1_str(),
                to_dict=event_to_avro_dict,
                conf=serializer_conf(),
            )
            deserializer = await AsyncAvroDeserializer(
                registry,
                schema_v1_str(),
                from_dict=avro_dict_to_event,
                conf=deserializer_conf(),
            )
            context = SerializationContext("test.demo04.async", MessageField.VALUE)
            event = deterministic_events(1)[0]
            payload = await serializer(event, context)
            assert payload is not None
            decoded = await deserializer(payload, context)
            assert decoded == event

    asyncio.run(run())


def test_standard_demo_topic_creation_contract() -> None:
    class TopicMetadata:
        error = None

    class Metadata:
        def __init__(self, topics: dict[str, object]) -> None:
            self.topics = topics

    class CreationFuture:
        def __init__(self) -> None:
            self.timeout: float | None = None

        def result(self, timeout: float) -> None:
            self.timeout = timeout

    class Admin:
        def __init__(self, *, exists: bool) -> None:
            self.exists = exists
            self.created: list[object] = []
            self.future = CreationFuture()

        def list_topics(self, timeout: float) -> Metadata:
            assert timeout == 15
            return Metadata({"test.demo04": TopicMetadata()} if self.exists else {})

        def create_topics(self, topics: list[object]) -> dict[str, CreationFuture]:
            self.created = topics
            return {"test.demo04": self.future}

    existing = Admin(exists=True)
    assert demo04c.ensure_topic(
        existing,  # type: ignore[arg-type]
        topic="test.demo04",
        create=False,
        partitions=3,
        replication_factor=3,
    ) == "already_exists"
    assert not existing.created

    missing = Admin(exists=False)
    with pytest.raises(RuntimeError, match="--create-topic"):
        demo04c.ensure_topic(
            missing,  # type: ignore[arg-type]
            topic="test.demo04",
            create=False,
            partitions=3,
            replication_factor=3,
        )

    creating = Admin(exists=False)
    assert demo04c.ensure_topic(
        creating,  # type: ignore[arg-type]
        topic="test.demo04",
        create=True,
        partitions=3,
        replication_factor=3,
    ) == "created"
    assert len(creating.created) == 1
    assert creating.future.timeout == 30


def test_run_id_rejects_path_traversal() -> None:
    assert validate_run_id("lec4-demo04-safe_1.0") == "lec4-demo04-safe_1.0"
    for unsafe in ("../../outside", "nested/run", "two words", "..", "-starts-with-dash"):
        with pytest.raises(ValueError):
            validate_run_id(unsafe)


def test_registry_report_redacts_credentials_and_url_userinfo() -> None:
    report = safe_registry_config_report(
        {
            "url": "https://url-user:url-password@registry.example.test:8443/path",
            "basic.auth.user.info": "api-key:api-secret",
        }
    )
    serialized = json.dumps(report, sort_keys=True)
    assert report == {
        "url_host": "registry.example.test:8443",
        "basic_auth_present": True,
    }
    for secret in ("url-user", "url-password", "api-key", "api-secret"):
        assert secret not in serialized


def test_async_producer_uses_supported_key_value_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Guard against reintroducing unsupported AIOProducer headers."""

    events = deterministic_events(2, seed_offset=21)
    created: list[object] = []

    class DeliveryMessage:
        def __init__(self, topic: str, key: bytes, value: bytes, offset: int) -> None:
            self._topic = topic
            self._key = key
            self._value = value
            self._offset = offset

        def topic(self) -> str:
            return self._topic

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return self._offset

        def key(self) -> bytes:
            return self._key

        def value(self) -> bytes:
            return self._value

    class SupportedContractProducer:
        def __init__(self, config: dict[str, object]) -> None:
            self.config = config
            self.calls: list[dict[str, object]] = []
            self.flush_timeout: float | None = None
            self.closed = False
            created.append(self)

        async def produce(self, topic: str, *, key: bytes, value: bytes) -> asyncio.Future[object]:
            self.calls.append({"topic": topic, "key": key, "value": value})
            future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
            future.set_result(DeliveryMessage(topic, key, value, len(self.calls) - 1))
            return future

        async def flush(self, timeout: float) -> int:
            self.flush_timeout = timeout
            return 0

        async def close(self) -> None:
            self.closed = True

    async def serializer(_event: TripEventV1, _context: object) -> bytes:
        return b"\x00\x00\x00\x00\x01payload"

    async def run() -> list[dict[str, object]]:
        ready = asyncio.Event()
        ready.set()
        delivered, _wait = await demo04d.produce_events(
            {"bootstrap.servers": "unused"},
            serializer,  # type: ignore[arg-type]
            SerializationContext("test.demo04.aio", MessageField.VALUE),
            topic="test.demo04.aio",
            events=events,
            assignment_ready=ready,
            assignment_timeout=1.0,
            delivery_timeout=2.0,
            interval=0.0,
        )
        return delivered

    monkeypatch.setattr(demo04d, "AIOProducer", SupportedContractProducer)
    delivered = asyncio.run(run())
    producer = created[0]
    assert isinstance(producer, SupportedContractProducer)
    assert [call["key"] for call in producer.calls] == [event_key(event) for event in events]
    assert all(set(call) == {"topic", "key", "value"} for call in producer.calls)
    assert producer.flush_timeout == 2.0
    assert producer.closed
    assert len(delivered) == 2


def test_async_producer_close_failure_does_not_mask_primary_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CloseTimeoutProducer:
        def __init__(self, _config: dict[str, object]) -> None:
            pass

        async def close(self) -> None:
            raise TimeoutError("cleanup timeout")

    async def failing_serializer(_event: TripEventV1, _context: object) -> bytes:
        raise ValueError("primary serialization failure")

    async def run() -> None:
        ready = asyncio.Event()
        ready.set()
        await demo04d.produce_events(
            {"bootstrap.servers": "unused"},
            failing_serializer,  # type: ignore[arg-type]
            SerializationContext("test.demo04.aio", MessageField.VALUE),
            topic="test.demo04.aio",
            events=deterministic_events(1),
            assignment_ready=ready,
            assignment_timeout=1.0,
            delivery_timeout=1.0,
            interval=0.0,
        )

    monkeypatch.setattr(demo04d, "AIOProducer", CloseTimeoutProducer)
    with pytest.raises(ValueError, match="primary serialization failure"):
        asyncio.run(run())


def test_async_consumer_filters_with_precomputed_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only deterministic keys for this bounded run may be deserialized and committed."""

    events = deterministic_events(2, seed_offset=31)
    expected_keys = frozenset(event_key(event) for event in events)
    payloads = [
        b"\x00\x00\x00\x00\x01first",
        b"\x00\x00\x00\x00\x01second",
    ]
    by_payload = dict(zip(payloads, events, strict=True))
    instances: list[object] = []

    class Partition:
        topic = "test.demo04.aio"
        partition = 0
        offset = 0

    class Message:
        def __init__(self, key: bytes, value: bytes, offset: int) -> None:
            self._key = key
            self._value = value
            self._offset = offset

        def topic(self) -> str:
            return "test.demo04.aio"

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return self._offset

        def key(self) -> bytes:
            return self._key

        def value(self) -> bytes:
            return self._value

        def error(self) -> None:
            return None

    queue = [
        Message(b"trip_9999", b"not-deserialized", 1),
        Message(event_key(events[0]), payloads[0], 2),
        Message(event_key(events[1]), payloads[1], 3),
    ]

    class KeyFilteringConsumer:
        def __init__(self, _config: dict[str, object]) -> None:
            self.messages = list(queue)
            self.committed: list[Message] = []
            self.closed = False
            instances.append(self)

        async def subscribe(self, _topics: list[str], *, on_assign: object, on_revoke: object) -> None:
            del on_revoke
            await on_assign(self, [Partition()])  # type: ignore[operator]

        async def assign(self, _partitions: object) -> None:
            return None

        async def poll(self, timeout: float) -> Message | None:
            del timeout
            return self.messages.pop(0) if self.messages else None

        async def commit(self, *, message: Message, asynchronous: bool) -> None:
            assert asynchronous is False
            self.committed.append(message)

        async def unsubscribe(self) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

    async def deserializer(payload: bytes, _context: object) -> TripEventV1:
        return by_payload[payload]

    async def run() -> tuple[list[dict[str, object]], int]:
        records, _assigned, _revoked, skipped = await demo04d.consume_events(
            {"group.id": "unused"},
            deserializer,  # type: ignore[arg-type]
            SerializationContext("test.demo04.aio", MessageField.VALUE),
            topic="test.demo04.aio",
            expected_keys=expected_keys,
            assignment_ready=asyncio.Event(),
            timeout=1.0,
            cleanup_timeout=1.0,
        )
        return records, skipped

    monkeypatch.setattr(demo04d, "AIOConsumer", KeyFilteringConsumer)
    records, skipped = asyncio.run(run())
    consumer = instances[0]
    assert isinstance(consumer, KeyFilteringConsumer)
    assert skipped == 1
    assert {record["key"] for record in records} == {
        key.decode("utf-8") for key in expected_keys
    }
    assert len(consumer.committed) == 2
    assert consumer.closed


def test_async_consumer_cleanup_failure_does_not_mask_primary_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = deterministic_events(1, seed_offset=44)[0]
    expected_key = event_key(event)
    cleanup_calls: list[str] = []

    class Partition:
        topic = "test.demo04.aio"
        partition = 0
        offset = 0

    class Message:
        def error(self) -> None:
            return None

        def key(self) -> bytes:
            return expected_key

        def value(self) -> bytes:
            return b"payload-that-triggers-primary-error"

    class FailingCleanupConsumer:
        def __init__(self, _config: dict[str, object]) -> None:
            self.sent = False

        async def subscribe(self, _topics: list[str], *, on_assign: object, on_revoke: object) -> None:
            del on_revoke
            await on_assign(self, [Partition()])  # type: ignore[operator]

        async def assign(self, _partitions: object) -> None:
            return None

        async def poll(self, timeout: float) -> Message | None:
            del timeout
            if self.sent:
                return None
            self.sent = True
            return Message()

        async def unsubscribe(self) -> None:
            cleanup_calls.append("unsubscribe")
            raise TimeoutError("unsubscribe cleanup failure")

        async def close(self) -> None:
            cleanup_calls.append("close")
            raise TimeoutError("close cleanup failure")

    async def failing_deserializer(_payload: bytes, _context: object) -> TripEventV1:
        raise ValueError("primary deserialization failure")

    async def run() -> None:
        await demo04d.consume_events(
            {"group.id": "unused"},
            failing_deserializer,  # type: ignore[arg-type]
            SerializationContext("test.demo04.aio", MessageField.VALUE),
            topic="test.demo04.aio",
            expected_keys=frozenset({expected_key}),
            assignment_ready=asyncio.Event(),
            timeout=1.0,
            cleanup_timeout=1.0,
        )

    monkeypatch.setattr(demo04d, "AIOConsumer", FailingCleanupConsumer)
    with pytest.raises(ValueError, match="primary deserialization failure"):
        asyncio.run(run())
    assert cleanup_calls == ["unsubscribe", "close"]
