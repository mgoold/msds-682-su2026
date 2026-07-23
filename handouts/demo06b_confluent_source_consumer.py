"""Demo 06B: inspect bounded Avro records written by the source path."""

from __future__ import annotations

import argparse
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from confluent_demo_common import (
    consumer_group_id,
    kafka_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    validate_run_id,
    write_json_report,
)
from demo06_common import (
    AssignmentTracker,
    DatagenOrderV1,
    input_topic_name,
    wait_for_assignment,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-messages", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if not 1 <= args.max_messages <= 100:
        parser.error("--max-messages must be between 1 and 100")
    if args.assignment_timeout <= 0 or args.idle_timeout <= 0:
        parser.error("timeouts must be positive")

    topic = input_topic_name()
    group_id = consumer_group_id("demo06b-inspect", run_id)
    kafka_conf: dict[str, Any] = {
        **kafka_config(client_id="msds682-demo06b-source-consumer"),
        "group.id": group_id,
        # Pin the classic protocol because AssignmentTracker accepts the full
        # assignment with consumer.assign(). KIP-848 callbacks are incremental.
        "group.protocol": "classic",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    registry_conf = schema_registry_config()
    tracker = AssignmentTracker()
    records: list[dict[str, Any]] = []
    consumer = Consumer(kafka_conf)
    started = time.monotonic()

    try:
        consumer.subscribe(
            [topic],
            on_assign=tracker.on_assign,
            on_revoke=tracker.on_revoke,
        )
        assignment_wait, pending_messages = wait_for_assignment(
            consumer,
            tracker,
            timeout=args.assignment_timeout,
        )
        context = SerializationContext(topic, MessageField.VALUE)
        with SchemaRegistryClient(registry_conf) as registry:
            deserializer = AvroDeserializer(registry)
            idle_deadline = time.monotonic() + args.idle_timeout
            while len(records) < args.max_messages and time.monotonic() < idle_deadline:
                message = (
                    pending_messages.pop(0)
                    if pending_messages
                    else consumer.poll(0.5)
                )
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Consumer error: {message.error()}")
                raw = deserializer(message.value(), context)
                order = DatagenOrderV1.model_validate(raw)
                records.append(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "key_present": message.key() is not None,
                        "key_bytes": len(message.key() or b""),
                        "order": order.model_dump(),
                    }
                )
                idle_deadline = time.monotonic() + args.idle_timeout
    finally:
        consumer.close()

    if len(records) != args.max_messages:
        raise RuntimeError(
            f"Expected {args.max_messages} records but consumed {len(records)}. "
            "Run the managed connector or the finite fallback seed first."
        )

    report = {
        "demo": "06B",
        "run_id": run_id,
        "input_source": "managed Datagen connector or explicit fallback seed",
        "topic": topic,
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "manual_commits": 0,
        "why_no_commit": (
            "06B is an isolated inspection group. Demo 06C owns processor commits."
        ),
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "assignments": tracker.assigned,
        "consumed": len(records),
        "records": records,
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo06b", report)
    print(f"Consumed {len(records)} validated Avro source records")
    print(f"Secret-free report: {path}")


if __name__ == "__main__":
    main()
