"""Demo 04C: real Confluent Cloud Avro producer/consumer round trip.

Student focus: follow one bounded Cloud cycle from validated objects to Avro
bytes and back. Wait for real assignment, validate after deserialization, then
commit only after successful processing.

Prerequisite: copy .env.example to .env and supply both Kafka and Schema
Registry credentials. Use the dedicated Demo 04 Avro topic.
"""

from __future__ import annotations

import argparse
import json
import time
import zlib
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
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

@dataclass
class AssignmentTracker:
    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    @staticmethod
    def rows(partitions: Any) -> list[dict[str, int | str]]:
        return [
            {"topic": partition.topic, "partition": partition.partition, "offset": partition.offset}
            for partition in partitions
        ]

    def on_assign(self, consumer: Consumer, partitions: Any) -> None:
        rows = self.rows(partitions)
        self.assigned.append(rows)
        print(f"Assigned: {rows}")
        consumer.assign(partitions)

    def on_revoke(self, _consumer: Consumer, partitions: Any) -> None:
        rows = self.rows(partitions)
        self.revoked.append(rows)
        print(f"Revoked: {rows}")


@dataclass
class DeliveryTracker:
    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, message: Any) -> None:
        if error is not None:
            self.failed.append(str(error))
            return
        value = message.value() or b""
        self.delivered.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
                "wire": parse_confluent_wire_header(value),
            }
        )


def ensure_topic(
    admin: AdminClient,
    *,
    topic: str,
    create: bool,
    partitions: int,
    replication_factor: int,
) -> str:
    """Confirm the dedicated topic exists, optionally creating it once."""

    metadata = admin.list_topics(timeout=15)
    if topic in metadata.topics and metadata.topics[topic].error is None:
        return "already_exists"
    if not create:
        raise RuntimeError(
            f"Topic {topic!r} does not exist. Re-run with --create-topic or create it in Confluent Cloud first."
        )
    future = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=replication_factor,
                config={"cleanup.policy": "delete"},
            )
        ]
    )[topic]
    future.result(timeout=30)
    return "created"


def wait_for_assignment(
    consumer: Consumer,
    tracker: AssignmentTracker,
    *,
    timeout: float,
) -> float:
    """Drive the poll loop until Kafka confirms partition assignment."""

    started = time.monotonic()
    deadline = started + timeout
    while not tracker.assigned and time.monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        error = message.error()
        # A partition EOF event is not a data failure; it can appear when
        # partition EOF reporting is enabled. All other errors should stop
        # the demo before any records are produced.
        if error is not None and error.code() != KafkaError._PARTITION_EOF:
            raise RuntimeError(f"Consumer error while waiting for assignment: {error}")
    if not tracker.assigned:
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check topic access, Kafka credentials, and cluster connectivity."
        )
    return round(time.monotonic() - started, 6)


def headers_as_dict(message: Any) -> dict[str, bytes | None]:
    return {name: value for name, value in (message.headers() or [])}


def run_cloud_roundtrip(
    args: argparse.Namespace,
    *,
    topic: str,
    topic_status: str,
    registry_conf: dict[str, Any],
    registry: SchemaRegistryClient,
) -> dict[str, Any]:
    """Run the bounded Kafka cycle with an open Registry client."""

    schema = schema_v1_str()
    serializer = AvroSerializer(
        registry,
        schema,
        to_dict=event_to_avro_dict,
        conf=serializer_conf(),
    )
    deserializer = AvroDeserializer(
        registry,
        schema,
        from_dict=avro_dict_to_event,
        conf=deserializer_conf(),
    )
    context = SerializationContext(topic, MessageField.VALUE)

    group_id = consumer_group_id("demo04c-avro", args.run_id)
    consumer_conf: dict[str, Any] = {
        **kafka_config(client_id="msds682-demo04c-avro-consumer"),
        "group.id": group_id,
        "group.protocol": "classic",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf = kafka_config(client_id="msds682-demo04c-avro-producer")

    tracker = AssignmentTracker()
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    delivery = DeliveryTracker()
    marker = args.run_id.encode("utf-8")
    assignment_wait_seconds = 0.0
    consumed: list[dict[str, Any]] = []
    skipped_other_runs = 0
    primary_error: BaseException | None = None

    try:
        consumer.subscribe(
            [topic],
            on_assign=tracker.on_assign,
            on_revoke=tracker.on_revoke,
        )
        assignment_wait_seconds = wait_for_assignment(
            consumer,
            tracker,
            timeout=args.assignment_timeout,
        )

        # ====================================================================
        # STUDENT CHECKPOINT
        # Why must assignment be confirmed before producing when this new group
        # uses auto.offset.reset="latest"? What failure would a fixed sleep risk?
        # ====================================================================
        seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 850
        events = deterministic_events(args.count, seed_offset=seed_offset)
        for event in events:
            value_bytes = serializer(event, context)
            if value_bytes is None:
                raise RuntimeError("Avro serializer unexpectedly returned None")
            producer.produce(
                topic,
                key=event_key(event),
                value=value_bytes,
                headers=[("demo04-run-id", marker)],
                on_delivery=delivery.callback,
            )
            producer.poll(0)

        remaining = producer.flush(15.0)
        if remaining:
            raise RuntimeError(f"Producer still had {remaining} queued messages after flush")
        if delivery.failed:
            raise RuntimeError("At least one delivery failed: " + "; ".join(delivery.failed))

        deadline = time.monotonic() + args.consumer_timeout
        while len(consumed) < args.count and time.monotonic() < deadline:
            message = consumer.poll(args.poll_timeout)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Consumer error: {message.error()}")

            if headers_as_dict(message).get("demo04-run-id") != marker:
                skipped_other_runs += 1
                continue

            event = deserializer(message.value(), context)
            if not isinstance(event, TripEventV1):
                raise TypeError("Expected AvroDeserializer to return TripEventV1")
            consumed.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "wire": parse_confluent_wire_header(message.value()),
                    "event": event.report_dict(),
                }
            )
            # ====================================================================
            # KEY CONCEPT
            # Progress moves only after deserialize -> validate -> process.
            # Commit last so a failed record is not acknowledged as completed.
            # ====================================================================
            consumer.commit(message=message, asynchronous=False)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[BaseException] = []
        try:
            producer.flush(5.0)
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            consumer.close()
        except BaseException as exc:
            cleanup_errors.append(exc)
        if primary_error is None and cleanup_errors:
            raise cleanup_errors[0]

    latest = registry.get_latest_version(avro_subject(topic))
    try:
        compatibility = registry.get_compatibility(avro_subject(topic))
    except Exception as exc:  # permission and inherited-config behavior vary by account
        compatibility = f"unavailable: {type(exc).__name__}: {exc}"

    report = {
        "demo": "demo04c_confluent_avro_roundtrip",
        "topic": topic,
        "topic_status": topic_status,
        "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),
        "subject": avro_subject(topic),
        "schema_id": latest.schema_id,
        "schema_version": latest.version,
        "compatibility": compatibility,
        "group_id": group_id,
        "requested": args.count,
        "delivered": len(delivery.delivered),
        "consumed": len(consumed),
        "skipped_records_from_other_runs": skipped_other_runs,
        "assignment_wait_seconds": assignment_wait_seconds,
        "partition_assignments": tracker.assigned,
        "partition_revocations": tracker.revoked,
        "producer_connection": safe_kafka_config_report(producer_conf),
        "consumer_connection": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
        "delivered_messages": delivery.delivered,
        "consumed_records": consumed,
        "commit_rule": "deserialize Avro -> validate TripEventV1 -> application record -> synchronous commit",
    }
    output_file = write_json_report(args.run_id, "demo04c_confluent_avro_roundtrip", report)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")

    if len(delivery.delivered) != args.count or len(consumed) != args.count:
        raise SystemExit("Demo 04C did not deliver and consume the requested count.")
    return report


def main() -> dict[str, Any]:
    """Run one bounded real-Cloud Avro write/read cycle."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04c")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--consumer-timeout", type=float, default=20.0)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("--partitions and --replication-factor must be positive")
    if min(args.assignment_timeout, args.consumer_timeout, args.poll_timeout) <= 0:
        parser.error("all timeout values must be positive")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    topic = topic_name()
    try:
        kafka = kafka_config(client_id="msds682-demo04c-admin")
        registry_conf = schema_registry_config()
    except ConnectionConfigError as exc:
        raise SystemExit(str(exc)) from exc

    admin = AdminClient(kafka)
    topic_status = ensure_topic(
        admin,
        topic=topic,
        create=args.create_topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )

    with SchemaRegistryClient(registry_conf) as registry:
        return run_cloud_roundtrip(
            args,
            topic=topic,
            topic_status=topic_status,
            registry_conf=registry_conf,
            registry=registry,
        )


if __name__ == "__main__":
    main()
