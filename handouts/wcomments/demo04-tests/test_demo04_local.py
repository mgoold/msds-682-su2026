"""
================================================================================
DEMO 04 - LOCAL TEST SUITE  (annotated tutorial copy - LIGHT PASS)
================================================================================

This is a TEST SUITE, not a demo script, so it is annotated more lightly than
the demos: a strategy overview here, plus a short note above each test saying
what it proves. The assertions themselves are left to speak for themselves.

WHAT IT IS FOR
    Fourteen tests that verify the Demo 04 contracts WITHOUT any cloud account:
    no Kafka broker, no Schema Registry, no credentials, no network. Run with:

        python -m pytest demo04-tests -q

WHY A CREDENTIAL-FREE TEST SUITE MATTERS
    Cloud tests are slow, cost money, need secrets, and fail for reasons
    unrelated to your code (see the SSL-handshake timeouts that can hit a real
    run). Anything that can be verified locally should be. These tests catch
    contract regressions in seconds.

THE THREE TECHNIQUES USED HERE, worth recognizing generally

    1. mock:// SCHEMA REGISTRY
       A real SchemaRegistryClient backed by an in-memory store. Registration,
       schema IDs, and reader/writer resolution behave genuinely; only the
       network is absent. Used by the Avro round-trip tests.

    2. FAKE CLIENTS (test doubles)
       Small stand-in classes implementing just the methods the code calls -
       produce(), poll(), commit(), close(). They let a test drive the real
       producer/consumer LOGIC while controlling exactly what "Kafka" returns,
       including failures that would be hard to trigger for real.

    3. monkeypatch
       A pytest fixture that temporarily replaces a name in a module - swapping
       the real AIOProducer for a fake one - and restores it automatically when
       the test ends. This is how the async tests run without a cluster.

WHAT THE SUITE COVERS, in four groups
    - VALIDATION CONTRACT      the Demo 04A rules still hold
    - AVRO / SCHEMA EVOLUTION  round trips are lossless; V2 reads V1
    - SAFETY                   path traversal blocked; credentials redacted
    - ERROR HANDLING           cleanup failures never mask the original error

WHAT IT DOES NOT COVER
    Anything requiring the real cloud: authentication, permissions, network
    behavior, and Schema Registry compatibility ENFORCEMENT. Those live in
    Demos 04C and 04D. Knowing the boundary of your test double is part of
    using one honestly.
================================================================================
"""

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


# WHAT THIS PROVES: Runs Demo 04A's whole case table and checks every case behaved as expected.
# A regression guard on the validation contract: loosen a model rule (say,
# drop ge=0 from fare) and a case that should FAIL starts passing, failing here.
def test_validation_cases_match_expectations() -> None:
    rows = run_validation()
    assert len(rows) >= 8
    assert all(row["expectation_met"] for row in rows)


# WHAT THIS PROVES: Proves normalize_event_time converts an offset timestamp to UTC, and that the
# derived properties (event_date/hour/weekday) are computed from the NORMALIZED
# value - not the original offset. Off-by-hours bugs live in exactly this gap.
def test_timezone_is_normalized_and_derived() -> None:
    event = TripEventV1.model_validate_json(
        '{"trip_id":"trip_4200","event_type":"trip_completed",'
        '"rider_id":"rider_420","event_time":"2026-07-16T17:05:00-07:00",'
        '"zone":"west","driver_id":"driver_420","fare":28.0}'
    )
    assert event.event_time.isoformat() == "2026-07-17T00:05:00+00:00"
    assert event.event_date == "2026-07-17"
    assert event.event_hour == 0


# WHAT THIS PROVES: The determinism guarantee: the same count and seed_offset regenerate identical
# events. Everything comparative in Demo 04 depends on this holding.
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


# WHAT THIS PROVES: A timestamp with no timezone must be REFUSED, not guessed at. Asserting that
# invalid input fails is as important as asserting valid input succeeds.
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


# WHAT THIS PROVES: Two things at once, against the mock registry:
#   (a) encode -> decode returns an equal object (lossless round trip)
#   (b) V1 bytes read with the V2 reader schema yield vehicle_type=None,
#       supplied by the schema default. That IS backward compatibility.
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


# WHAT THIS PROVES: THE THESIS OF DEMO 04, as a test. A negative fare encodes to valid Avro (it is
# a valid double) and is still rejected by the pydantic rules. Structure and
# meaning are separate contracts enforced by different tools.
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


# WHAT THIS PROVES: The same round trip through the ASYNC serializer/deserializer, confirming the
# async path produces identical results to the blocking one.
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


# WHAT THIS PROVES: Exercises Demo 04C's ensure_topic() against a fake AdminClient: it must not
# create a topic unless asked, must accept one that already exists, and must
# pass through the requested partition and replication settings.
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


# WHAT THIS PROVES: A SECURITY test. --run-id becomes a directory name, so values like '../..' must
# be rejected before they can write outside the evidence directory.
def test_run_id_rejects_path_traversal() -> None:
    assert validate_run_id("lec4-demo04-safe_1.0") == "lec4-demo04-safe_1.0"
    for unsafe in ("../../outside", "nested/run", "two words", "..", "-starts-with-dash"):
        with pytest.raises(ValueError):
            validate_run_id(unsafe)


# WHAT THIS PROVES: A SECURITY test. The Schema Registry summary must expose only host and a
# presence boolean - never the key or secret, including credentials smuggled
# into a URL as user:pass@host.
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


# WHAT THIS PROVES: Guards the constraint documented in Demo 04D: the AIOProducer in this library
# version does not support headers, so the producer must send key/value only.
# Without this test, someone could reintroduce headers and break the demo.
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


# WHAT THIS PROVES: ERROR-HANDLING test. If the producer fails AND closing also fails, the ORIGINAL
# error must survive - otherwise you would debug the cleanup instead of the bug.
# This is the kind of path that is nearly impossible to trigger for real, which
# is exactly why a fake client earns its keep.
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


# WHAT THIS PROVES: Verifies Demo 04D's key-based run filter: only this run's precomputed keys are
# deserialized and committed. Other runs' messages are skipped and counted, and
# duplicates are not double-counted.
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


# WHAT THIS PROVES: The consumer-side twin of the producer cleanup test: a failure in unsubscribe
# or close must not replace the real processing error.
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
