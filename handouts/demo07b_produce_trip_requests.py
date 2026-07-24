"""Demo 07B: publish deterministic trip requests for both pricing versions."""

from __future__ import annotations

import argparse
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from confluent_demo_common import (
    kafka_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    validate_run_id,
    write_json_report,
)
from demo07_common import (
    deterministic_trip_requests,
    model_to_avro_dict,
    serializer_conf,
    topic_names,
    trip_request_schema_str,
)
from demo07_kafka import acknowledged_produce, ensure_demo07_topics


def run_producer(
    *,
    run_id: str,
    count: int,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    delivery_timeout: float,
) -> dict[str, object]:
    """Publish one finite request batch and return secret-free evidence."""

    validate_run_id(run_id)
    requests = deterministic_trip_requests(run_id, count)
    topics = topic_names()
    kafka_conf = kafka_config(client_id="msds682-demo07b-producer")
    registry_conf = schema_registry_config()
    topic_status = ensure_demo07_topics(
        AdminClient(kafka_conf),
        create=create_topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    producer = Producer(kafka_conf)
    context = SerializationContext(topics["requests"], MessageField.VALUE)
    delivered: list[dict[str, object]] = []
    started = time.monotonic()

    with SchemaRegistryClient(registry_conf) as registry:
        serializer = AvroSerializer(
            registry,
            trip_request_schema_str(),
            to_dict=model_to_avro_dict,
            conf=serializer_conf(),
        )
        for request in requests:
            value = serializer(request, context)
            if value is None:
                raise RuntimeError("AvroSerializer unexpectedly returned None")
            delivered.append(
                {
                    "trip_id": request.trip_id,
                    "delivery": acknowledged_produce(
                        producer,
                        topic=topics["requests"],
                        key=request.trip_id.encode("utf-8"),
                        value=value,
                        delivery_timeout=delivery_timeout,
                        headers=[
                            ("demo", b"07B"),
                            ("run-id", run_id.encode("utf-8")),
                        ],
                    ),
                }
            )

    report: dict[str, object] = {
        "demo": "07B",
        "run_id": run_id,
        "source": "finite deterministic synthetic San Francisco coordinates",
        "prior_demo_topic_required": False,
        "prior_kafka_data_required": False,
        "topics": topics,
        "topic_status": topic_status,
        "attempted": len(requests),
        "delivered": len(delivered),
        "failed": 0,
        "trip_ids": [request.trip_id for request in requests],
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo07b", report)
    report["report_path"] = str(path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    args = parser.parse_args()

    report = run_producer(
        run_id=args.run_id,
        count=args.count,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        delivery_timeout=args.delivery_timeout,
    )
    print(
        f"Published {report['delivered']} requests to "
        f"{report['topics']['requests']}"
    )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
