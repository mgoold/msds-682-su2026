"""Demo 07D: simulate and publish delayed trip-outcome cost labels.

This bounded teaching source does not consume the requests topic, observe GPS,
or wait for a real trip. It recreates the declared synthetic requests, obtains
route measurements from the same selected provider as 07C, applies stable
realization adjustments, and publishes TripOutcomeV1 records.
"""

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
    outcome_from_route,
    serializer_conf,
    topic_names,
    trip_outcome_schema_str,
)
from demo07_kafka import acknowledged_produce, ensure_demo07_topics
from demo07_routing import routing_client


def run_outcome_producer(
    *,
    run_id: str,
    count: int,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    delivery_timeout: float,
    route_mode: str,
    route_timeout: float,
) -> dict[str, object]:
    """Simulate post-trip outcomes using the same declared route boundary."""

    validate_run_id(run_id)
    if route_mode not in {"osrm", "fixture"}:
        raise ValueError("route_mode must be osrm or fixture")
    if route_timeout <= 0:
        raise ValueError("route_timeout must be positive")
    requests = deterministic_trip_requests(run_id, count)
    router = routing_client(route_mode, timeout_seconds=route_timeout)
    outcomes = [
        outcome_from_route(
            request,
            router.estimate(request.pickup, request.dropoff),
        )
        for request in requests
    ]
    topics = topic_names()
    kafka_conf = kafka_config(client_id="msds682-demo07d-outcome-producer")
    registry_conf = schema_registry_config()
    topic_status = ensure_demo07_topics(
        AdminClient(kafka_conf),
        create=create_topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    producer = Producer(kafka_conf)
    context = SerializationContext(topics["outcomes"], MessageField.VALUE)
    delivered: list[dict[str, object]] = []
    started = time.monotonic()

    with SchemaRegistryClient(registry_conf) as registry:
        serializer = AvroSerializer(
            registry,
            trip_outcome_schema_str(),
            to_dict=model_to_avro_dict,
            conf=serializer_conf(),
        )
        for outcome in outcomes:
            value = serializer(outcome, context)
            if value is None:
                raise RuntimeError("AvroSerializer unexpectedly returned None")
            delivered.append(
                {
                    "trip_id": outcome.trip_id,
                    "actual_cost_cents": outcome.actual_cost_cents,
                    "delivery": acknowledged_produce(
                        producer,
                        topic=topics["outcomes"],
                        key=outcome.trip_id.encode("utf-8"),
                        value=value,
                        delivery_timeout=delivery_timeout,
                        headers=[
                            ("demo", b"07D"),
                            ("run-id", run_id.encode("utf-8")),
                            ("label", b"actual-cost"),
                        ],
                    ),
                }
            )

    report: dict[str, object] = {
        "demo": "07D",
        "run_id": run_id,
        "meaning": (
            "Synthetic delayed labels that become available after trip completion"
        ),
        "routing_mode": route_mode,
        "routing_calls": len(outcomes),
        "data_source": (
            "Declared route provider plus deterministic realization factors "
            "and synthetic cost process"
        ),
        "topics": topics,
        "topic_status": topic_status,
        "attempted": len(outcomes),
        "delivered": len(delivered),
        "failed": 0,
        "records": [outcome.model_dump(mode="json") for outcome in outcomes],
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo07d", report)
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
    parser.add_argument(
        "--routing-mode",
        choices=("osrm", "fixture"),
        default="osrm",
    )
    parser.add_argument("--route-timeout", type=float, default=10.0)
    args = parser.parse_args()

    report = run_outcome_producer(
        run_id=args.run_id,
        count=args.count,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        delivery_timeout=args.delivery_timeout,
        route_mode=args.routing_mode,
        route_timeout=args.route_timeout,
    )
    print(
        f"Published {report['delivered']} delayed outcomes to "
        f"{report['topics']['outcomes']}"
    )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
