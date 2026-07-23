"""Optional fallback: seed finite records with the Datagen ORDERS Avro schema."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

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
    datagen_order_schema_str,
    deterministic_orders,
    fallback_order_key,
    input_topic_name,
    order_to_avro_dict,
    output_topic_name,
    serializer_conf,
    stable_seed_offset,
)


@dataclass
class DeliveryTracker:
    """Collect broker acknowledgements without recording credentials."""

    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, message: Any) -> None:
        if error is not None:
            self.failed.append("delivery_failed")
            return
        self.delivered.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("--partitions and --replication-factor must be positive")

    input_topic = input_topic_name()
    output_topic = output_topic_name()
    kafka_conf = kafka_config(client_id="msds682-demo06-fallback-seed")
    registry_conf = schema_registry_config()
    admin = AdminClient(kafka_conf)
    input_status = ensure_topic(
        admin,
        topic=input_topic,
        create=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    output_status = ensure_topic(
        admin,
        topic=output_topic,
        create=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )

    seed_offset = stable_seed_offset(run_id)
    orders = deterministic_orders(args.count, seed_offset=seed_offset)
    producer = Producer(kafka_conf)
    tracker = DeliveryTracker()
    context = SerializationContext(input_topic, MessageField.VALUE)
    with SchemaRegistryClient(registry_conf) as registry:
        serializer = AvroSerializer(
            registry,
            datagen_order_schema_str(),
            to_dict=order_to_avro_dict,
            conf=serializer_conf(),
        )
        for order in orders:
            value = serializer(order, context)
            if value is None:
                raise RuntimeError("AvroSerializer unexpectedly returned None")
            producer.produce(
                input_topic,
                key=fallback_order_key(order),
                value=value,
                headers=[("demo06-source", b"fallback-seed")],
                on_delivery=tracker.callback,
            )
            producer.poll(0)

    remaining = producer.flush(15.0)
    if remaining:
        raise RuntimeError(f"Producer still had {remaining} queued records")
    if tracker.failed or len(tracker.delivered) != len(orders):
        raise RuntimeError("Not every fallback record received a broker acknowledgement")

    report = {
        "demo": "06-seed-fallback",
        "run_id": run_id,
        "source": "finite deterministic Python fallback, not Kafka Connect",
        "why_it_exists": (
            "Use only when the classroom account cannot create a managed "
            "connector. Do not mix this source with a managed connector in "
            "one exercise. Demo 06B-06D remain unchanged."
        ),
        "input_topic": input_topic,
        "output_topic": output_topic,
        "topic_status": {"input": input_status, "output": output_status},
        "seed_offset": seed_offset,
        "attempted": len(orders),
        "delivered": len(tracker.delivered),
        "failed": len(tracker.failed),
        "first_order_id": orders[0].orderid,
        "last_order_id": orders[-1].orderid,
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo06-seed-fallback", report)
    print(f"Delivered {len(orders)} fallback records to {input_topic}")
    print(f"Secret-free report: {path}")


if __name__ == "__main__":
    main()
