"""Demo 04D: native asyncio Avro round trip on Confluent Cloud.

Student focus: run a finite producer and consumer concurrently on one asyncio
event loop. The producer waits for Kafka's real assignment callback instead of
guessing readiness with a fixed sleep.

Run Demo 04C with --create-topic first, or create the dedicated Avro topic in
Confluent Cloud. This is an optional extension for applications that already
own an asyncio event loop, such as FastAPI services.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import zlib
from typing import Any

from confluent_kafka.aio import AIOConsumer, AIOProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AsyncAvroDeserializer,
    AsyncAvroSerializer,
)
from confluent_kafka.serialization import MessageField, SerializationContext

from demo04_common import (
    ConnectionConfigError,
    TripEventV1,
    avro_dict_to_event,
    avro_subject,
    consumer_group_id,
    deserializer_conf,
    deterministic_events,
    event_key,
    event_to_avro_dict,
    kafka_config,
    parse_confluent_wire_header,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    schema_v1_str,
    serializer_conf,
    synthetic_data_report,
    topic_name,
    validate_run_id,
    write_json_report,
)


def topic_partition_rows(partitions: Any) -> list[dict[str, int | str]]:
    return [
        {"topic": partition.topic, "partition": partition.partition, "offset": partition.offset}
        for partition in partitions
    ]


async def produce_events(
    producer_config: dict[str, Any],
    serializer: AsyncAvroSerializer,
    context: SerializationContext,
    *,
    topic: str,
    events: list[TripEventV1],
    assignment_ready: asyncio.Event,
    assignment_timeout: float,
    delivery_timeout: float,
    interval: float,
) -> tuple[list[dict[str, Any]], float]:
    """Wait for assignment, serialize asynchronously, then produce finite data."""

    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        await asyncio.wait_for(assignment_ready.wait(), timeout=assignment_timeout)
    except TimeoutError as exc:
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check topic access, Kafka credentials, and cluster connectivity."
        ) from exc
    assignment_wait_seconds = round(loop.time() - started, 6)

    producer = AIOProducer(producer_config)
    delivery_futures: list[Any] = []
    primary_error: BaseException | None = None
    try:
        for event in events:
            value_bytes = await serializer(event, context)
            if value_bytes is None:
                raise RuntimeError("AsyncAvroSerializer unexpectedly returned None")
            # confluent-kafka 2.15 AIOProducer batch mode does not support
            # headers. The precomputed stable keys identify this bounded run.
            delivery_future = await producer.produce(
                topic,
                key=event_key(event),
                value=value_bytes,
            )
            delivery_futures.append(delivery_future)
            if interval:
                await asyncio.sleep(interval)
        remaining = await asyncio.wait_for(
            producer.flush(delivery_timeout),
            timeout=delivery_timeout + 1.0,
        )
        if remaining:
            raise RuntimeError(
                f"AIOProducer still had {remaining} queued messages after flush"
            )
        messages = await asyncio.wait_for(
            asyncio.gather(*delivery_futures),
            timeout=delivery_timeout,
        )
        delivered = [
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
                "wire": parse_confluent_wire_header(message.value() or b""),
            }
            for message in messages
        ]
        return delivered, assignment_wait_seconds
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            await asyncio.wait_for(producer.close(), timeout=delivery_timeout)
        except TimeoutError as exc:
            if primary_error is None:
                raise RuntimeError(
                    "AIOProducer did not close before --delivery-timeout"
                ) from exc
        except BaseException:
            if primary_error is None:
                raise


async def consume_events(
    consumer_config: dict[str, Any],
    deserializer: AsyncAvroDeserializer,
    context: SerializationContext,
    *,
    topic: str,
    expected_keys: frozenset[bytes],
    assignment_ready: asyncio.Event,
    timeout: float,
    cleanup_timeout: float,
) -> tuple[
    list[dict[str, Any]],
    list[list[dict[str, int | str]]],
    list[list[dict[str, int | str]]],
    int,
]:
    """Consume only this run's records and commit after successful validation."""

    consumer = AIOConsumer(consumer_config)
    records: list[dict[str, Any]] = []
    consumed_keys: set[bytes] = set()
    assignments: list[list[dict[str, int | str]]] = []
    revocations: list[list[dict[str, int | str]]] = []
    skipped = 0
    loop = asyncio.get_running_loop()
    # ========================================================================
    # KEY CONCEPT
    # The receive budget starts only after a real assignment. Group join time
    # must not silently consume the data budget on a cold Cloud connection.
    # ========================================================================
    deadline: float | None = None

    async def on_assign(aio_consumer: Any, partitions: Any) -> None:
        await aio_consumer.assign(partitions)
        rows = topic_partition_rows(partitions)
        assignments.append(rows)
        print(f"Async assigned: {rows}")
        assignment_ready.set()

    async def on_revoke(_aio_consumer: Any, partitions: Any) -> None:
        rows = topic_partition_rows(partitions)
        revocations.append(rows)
        print(f"Async revoked: {rows}")

    primary_error: BaseException | None = None
    cleanup_failure: tuple[str, BaseException] | None = None
    try:
        await consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        while len(records) < len(expected_keys) and (
            deadline is None or loop.time() < deadline
        ):
            remaining = timeout if deadline is None else max(deadline - loop.time(), 0.0)
            message = await consumer.poll(timeout=min(1.0, remaining))
            if assignment_ready.is_set() and deadline is None:
                deadline = loop.time() + timeout
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Consumer error: {message.error()}")
            message_key = message.key()
            # Filter before deserialization: only keys generated for this run
            # are allowed to become evidence or trigger an explicit commit.
            if message_key not in expected_keys or message_key in consumed_keys:
                skipped += 1
                continue

            event = await deserializer(message.value(), context)
            if not isinstance(event, TripEventV1):
                raise TypeError("Expected AsyncAvroDeserializer to return TripEventV1")
            if event_key(event) != message_key:
                raise ValueError("Deserialized trip_id does not match the Kafka key")
            consumed_keys.add(message_key)
            records.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "wire": parse_confluent_wire_header(message.value()),
                    "event": event.report_dict(),
                }
            )
            await consumer.commit(message=message, asynchronous=False)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        # Attempt both cleanup operations, bound each wait, and never let a
        # cleanup failure hide the original deserialize/validate/commit error.
        for label, operation in (
            ("unsubscribe", consumer.unsubscribe),
            ("close", consumer.close),
        ):
            try:
                await asyncio.wait_for(operation(), timeout=cleanup_timeout)
            except BaseException as exc:
                if primary_error is None and cleanup_failure is None:
                    cleanup_failure = (label, exc)
        if primary_error is None and cleanup_failure is not None:
            label, exc = cleanup_failure
            raise RuntimeError(
                f"AIOConsumer {label} failed during bounded cleanup"
            ) from exc
    return records, assignments, revocations, skipped


async def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    """Create async serdes, coordinate two finite Kafka tasks, and report."""

    topic = topic_name()
    registry_conf = schema_registry_config()
    seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 850
    events = deterministic_events(args.count, seed_offset=seed_offset)
    expected_keys = frozenset(event_key(event) for event in events)
    if len(expected_keys) != args.count:
        raise RuntimeError("Deterministic Demo 04D event keys must be unique")

    async with AsyncSchemaRegistryClient(registry_conf) as registry:
        schema = schema_v1_str()
        serializer = await AsyncAvroSerializer(
            registry,
            schema,
            to_dict=event_to_avro_dict,
            conf=serializer_conf(),
        )
        deserializer = await AsyncAvroDeserializer(
            registry,
            schema,
            from_dict=avro_dict_to_event,
            conf=deserializer_conf(),
        )
        context = SerializationContext(topic, MessageField.VALUE)

        producer_config = kafka_config(client_id="msds682-demo04d-aio-avro-producer")
        producer_config["delivery.timeout.ms"] = int(args.delivery_timeout * 1000)
        group_id = args.group_id or consumer_group_id("demo04d-aio-avro", args.run_id)
        consumer_config: dict[str, Any] = {
            **kafka_config(client_id="msds682-demo04d-aio-avro-consumer"),
            "group.id": group_id,
            "group.protocol": "classic",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
        assignment_ready = asyncio.Event()

        # ====================================================================
        # KEY CONCEPT
        # Both tasks share one event loop. The producer waits for the consumer's
        # real assignment event; this demo never guesses readiness with sleep.
        # ====================================================================
        producer_task = asyncio.create_task(
            produce_events(
                producer_config,
                serializer,
                context,
                topic=topic,
                events=events,
                assignment_ready=assignment_ready,
                assignment_timeout=args.assignment_timeout,
                delivery_timeout=args.delivery_timeout,
                interval=args.interval,
            )
        )
        consumer_task = asyncio.create_task(
            consume_events(
                consumer_config,
                deserializer,
                context,
                topic=topic,
                expected_keys=expected_keys,
                assignment_ready=assignment_ready,
                timeout=args.consumer_timeout,
                cleanup_timeout=args.delivery_timeout,
            )
        )
        try:
            producer_result, consumer_result = await asyncio.gather(
                producer_task,
                consumer_task,
            )
        except BaseException:
            for task in (producer_task, consumer_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
            raise

        delivered, assignment_wait_seconds = producer_result
        consumed, assignments, revocations, skipped = consumer_result
        latest = await registry.get_latest_version(avro_subject(topic))
        try:
            compatibility = await registry.get_compatibility(avro_subject(topic))
        except Exception as exc:
            compatibility = f"unavailable: {type(exc).__name__}: {exc}"

        report = {
            "demo": "demo04d_asyncio_avro_roundtrip",
            "topic": topic,
            "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),
            "subject": avro_subject(topic),
            "schema_id": latest.schema_id,
            "schema_version": latest.version,
            "compatibility": compatibility,
            "group_id": group_id,
            "group_protocol": consumer_config["group.protocol"],
            "requested": args.count,
            "delivered": len(delivered),
            "consumed": len(consumed),
            "run_filter": "precomputed deterministic Kafka keys",
            "expected_keys": sorted(key.decode("utf-8") for key in expected_keys),
            "skipped_records_from_other_runs": skipped,
            "assignment_wait_seconds": assignment_wait_seconds,
            "partition_assignments": assignments,
            "partition_revocations": revocations,
            "producer_connection": safe_kafka_config_report(producer_config),
            "consumer_connection": safe_kafka_config_report(consumer_config),
            "schema_registry": safe_registry_config_report(registry_conf),
            "delivered_messages": delivered,
            "consumed_records": consumed,
            "commit_rule": "await deserialize -> validate TripEventV1 -> await synchronous commit",
        }
    return report


def main() -> dict[str, Any]:
    """Validate prerequisites, execute the async demo, and write evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04d")
    parser.add_argument("--group-id")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--consumer-timeout", type=float, default=20.0)
    parser.add_argument("--interval", type=float, default=0.1)
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if min(args.assignment_timeout, args.delivery_timeout, args.consumer_timeout) <= 0:
        parser.error("timeout values must be positive")
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    # Fail before starting the event loop if the dedicated topic is absent.
    topic = topic_name()
    try:
        topic_check_config = kafka_config(client_id="msds682-demo04d-topic-check")
        schema_registry_config()
    except ConnectionConfigError as exc:
        raise SystemExit(str(exc)) from exc
    admin = AdminClient(topic_check_config)
    metadata = admin.list_topics(timeout=15)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise SystemExit(
            f"Topic {topic!r} does not exist. Run Demo 04C with --create-topic first."
        )

    # ====================================================================
    # STUDENT CHECKPOINT
    # What other nonblocking I/O needs to share this event loop? If there is
    # none, why is the standard synchronous Demo 04C the simpler design?
    # ====================================================================
    report = asyncio.run(run_demo(args))
    output_file = write_json_report(args.run_id, "demo04d_asyncio_avro_roundtrip", report)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")
    if report["delivered"] != args.count or report["consumed"] != args.count:
        raise SystemExit("Demo 04D did not deliver and consume the requested count.")
    return report


if __name__ == "__main__":
    main()
