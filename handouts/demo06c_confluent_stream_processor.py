"""Demo 06C: consume, validate, derive, produce, acknowledge, and commit."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

from confluent_demo_common import (
    ensure_topic,
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
    derive_order_metric,
    input_topic_name,
    metric_key,
    metric_to_avro_dict,
    order_metric_schema_str,
    output_topic_name,
    serializer_conf,
    wait_for_assignment,
)


@dataclass
class DeliveryTracker:
    """Capture one output acknowledgement before the input commit."""

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


def process_one_message(
    *,
    message: Any,
    consumer: Any,
    producer: Any,
    input_deserializer: Any,
    output_serializer: Any,
    input_context: SerializationContext,
    output_context: SerializationContext,
    output_topic: str,
    delivery_timeout: float,
) -> dict[str, Any]:
    """Process one input and commit only after its output is acknowledged."""

    raw = input_deserializer(message.value(), input_context)
    try:
        order = DatagenOrderV1.model_validate(raw)
    except ValidationError as exc:
        coordinate = (
            f"{message.topic()}:{message.partition()}:{message.offset()}"
        )
        raise RuntimeError(
            f"Input validation failed at source coordinate {coordinate}"
        ) from exc
    metric = derive_order_metric(
        order,
        source_topic=message.topic(),
        source_partition=message.partition(),
        source_offset=message.offset(),
    )
    output_value = output_serializer(metric, output_context)
    if output_value is None:
        raise RuntimeError("AvroSerializer unexpectedly returned None")

    tracker = DeliveryTracker()
    producer.produce(
        output_topic,
        key=metric_key(metric),
        value=output_value,
        headers=[
            ("source-topic", message.topic().encode("utf-8")),
            ("source-partition", str(message.partition()).encode("utf-8")),
            ("source-offset", str(message.offset()).encode("utf-8")),
        ],
        on_delivery=tracker.callback,
    )
    producer.poll(0)
    remaining = producer.flush(delivery_timeout)
    if remaining or tracker.failed or len(tracker.delivered) != 1:
        raise RuntimeError(
            "Derived output was not acknowledged; input offset was not committed"
        )

    # ========================================================================
    # KEY CONCEPT
    # The commit below is a Kafka consumer offset commit. It is not a producer
    # acknowledgement and it is unrelated to a Git commit. Output delivery is
    # confirmed first; only then may this consumer record input progress.
    # ========================================================================
    consumer.commit(message=message, asynchronous=False)
    return {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "source_record_id": metric.source_record_id,
        "orderid": order.orderid,
        "itemid": order.itemid,
        "orderunits": order.orderunits,
        "size_band": metric.size_band,
        "output": tracker.delivered[0],
        "input_commit": "sync_after_output_ack",
    }


def run_processor(
    *,
    run_id: str,
    group_id: str,
    max_messages: int,
    assignment_timeout: float,
    idle_timeout: float,
    delivery_timeout: float,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    report_demo_name: str | None,
    force_beginning: bool = False,
) -> dict[str, Any]:
    """Run one bounded at-least-once processor pass."""

    validate_run_id(run_id)
    if not 1 <= max_messages <= 100:
        raise ValueError("max_messages must be between 1 and 100")
    if min(assignment_timeout, idle_timeout, delivery_timeout) <= 0:
        raise ValueError("timeouts must be positive")
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")

    input_topic = input_topic_name()
    output_topic = output_topic_name()
    base_kafka_conf = kafka_config(client_id="msds682-demo06c")
    registry_conf = schema_registry_config()
    admin = AdminClient(base_kafka_conf)
    topic_status = {
        "input": ensure_topic(
            admin,
            topic=input_topic,
            create=create_topics,
            partitions=partitions,
            replication_factor=replication_factor,
            create_option="--create-topics",
        ),
        "output": ensure_topic(
            admin,
            topic=output_topic,
            create=create_topics,
            partitions=partitions,
            replication_factor=replication_factor,
            create_option="--create-topics",
        ),
    }

    consumer_conf: dict[str, Any] = {
        **base_kafka_conf,
        "client.id": "msds682-demo06c-consumer",
        "group.id": group_id,
        # Pin the classic protocol because this bounded teaching callback uses
        # the full assignment with consumer.assign(). KIP-848 callbacks are
        # incremental and require incremental_assign().
        "group.protocol": "classic",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf: dict[str, Any] = {
        **base_kafka_conf,
        "client.id": "msds682-demo06c-derived-producer",
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    assignment = AssignmentTracker(force_beginning=force_beginning)
    processed: list[dict[str, Any]] = []
    started = time.monotonic()

    try:
        consumer.subscribe(
            [input_topic],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        assignment_wait, pending_messages = wait_for_assignment(
            consumer,
            assignment,
            timeout=assignment_timeout,
        )
        input_context = SerializationContext(input_topic, MessageField.VALUE)
        output_context = SerializationContext(output_topic, MessageField.VALUE)

        with SchemaRegistryClient(registry_conf) as registry:
            input_deserializer = AvroDeserializer(registry)
            output_serializer = AvroSerializer(
                registry,
                order_metric_schema_str(),
                to_dict=metric_to_avro_dict,
                conf=serializer_conf(),
            )
            idle_deadline = time.monotonic() + idle_timeout
            while len(processed) < max_messages and time.monotonic() < idle_deadline:
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
                processed.append(
                    process_one_message(
                        message=message,
                        consumer=consumer,
                        producer=producer,
                        input_deserializer=input_deserializer,
                        output_serializer=output_serializer,
                        input_context=input_context,
                        output_context=output_context,
                        output_topic=output_topic,
                        delivery_timeout=delivery_timeout,
                    )
                )
                idle_deadline = time.monotonic() + idle_timeout
    finally:
        consumer.close()

    if len(processed) != max_messages:
        raise RuntimeError(
            f"Expected {max_messages} input records but processed {len(processed)}. "
            "Run the managed connector or fallback seed first."
        )

    report = {
        "demo": report_demo_name or "06C-internal-pass",
        "run_id": run_id,
        "group_id": group_id,
        "force_beginning": force_beginning,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "topic_status": topic_status,
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "processed": len(processed),
        "records": processed,
        "commit_order": [
            "deserialize Avro",
            "validate with Pydantic",
            "derive output",
            "produce output",
            "wait for output acknowledgement",
            "commit input offset",
        ],
        "delivery_semantics": {
            "baseline": "at_least_once",
            "duplicate_window": (
                "A crash after output acknowledgement but before input commit "
                "can produce the same derived record again."
            ),
            "mitigation": (
                "The derived Kafka key is the stable input topic-partition-offset."
            ),
        },
        "assignments": assignment.assigned,
        "kafka": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    if report_demo_name is not None:
        report_path = write_json_report(run_id, report_demo_name.lower(), report)
        report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--group-id")
    parser.add_argument("--max-messages", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    group_id = args.group_id or f"msds682-su2026-demo06c-{run_id}"
    report = run_processor(
        run_id=run_id,
        group_id=group_id,
        max_messages=args.max_messages,
        assignment_timeout=args.assignment_timeout,
        idle_timeout=args.idle_timeout,
        delivery_timeout=args.delivery_timeout,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        report_demo_name="demo06c",
        force_beginning=False,
    )
    print(
        f"Processed {report['processed']} input records and committed only "
        "after output acknowledgement"
    )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
