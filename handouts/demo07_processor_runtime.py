"""Bounded consumer-to-producer runtime for Demo 07 fare quotes."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
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
from demo07_common import (
    CostModelArtifactV1,
    PricingMethod,
    RIDGE_V2_MODEL_VERSION,
    TripRequestV1,
    avro_dict_to_request,
    fare_quote_schema_str,
    load_model_artifact,
    model_to_avro_dict,
    quote_from_route,
    serializer_conf,
    topic_names,
    trip_request_schema_str,
)
from demo07_kafka import (
    AssignmentTracker,
    acknowledged_produce,
    commit_message,
    ensure_demo07_topics,
    message_coordinate,
    wait_for_assignment,
)
from demo07_routing import routing_client

RoutingMode = Literal["osrm", "fixture"]


def run_quote_processor(
    *,
    run_id: str,
    pricing_method: PricingMethod,
    model_artifact_path: Path | None,
    max_messages: int,
    route_mode: RoutingMode,
    route_timeout: float,
    assignment_timeout: float,
    idle_timeout: float,
    delivery_timeout: float,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    max_scanned: int = 1_000,
) -> dict[str, Any]:
    """Publish one quote per matching request, then commit its input offset."""

    validate_run_id(run_id)
    if not 1 <= max_messages <= 25:
        raise ValueError("max_messages must be between 1 and 25")
    if not max_messages <= max_scanned <= 10_000:
        raise ValueError("max_scanned must be between max_messages and 10000")
    if min(route_timeout, assignment_timeout, idle_timeout, delivery_timeout) <= 0:
        raise ValueError("timeouts must be positive")

    artifact: CostModelArtifactV1 | None = None
    if pricing_method == RIDGE_V2_MODEL_VERSION:
        if model_artifact_path is None:
            raise ValueError("ridge-v2 requires --model-artifact")
        artifact = load_model_artifact(model_artifact_path)
    elif model_artifact_path is not None:
        raise ValueError("rule-v1 does not use a model artifact")

    topics = topic_names()
    group_id = consumer_group_id(
        f"demo07-quotes-{pricing_method}",
        run_id,
    )
    base_conf = kafka_config(client_id=f"msds682-demo07c-{pricing_method}")
    registry_conf = schema_registry_config()
    topic_status = ensure_demo07_topics(
        AdminClient(base_conf),
        create=create_topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    consumer_conf: dict[str, Any] = {
        **base_conf,
        "client.id": f"msds682-demo07c-{pricing_method}-consumer",
        "group.id": group_id,
        "group.protocol": "classic",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf: dict[str, Any] = {
        **base_conf,
        "client.id": f"msds682-demo07c-{pricing_method}-producer",
    }
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    router = routing_client(route_mode, timeout_seconds=route_timeout)
    assignment = AssignmentTracker()
    processed: list[dict[str, Any]] = []
    skipped: list[str] = []
    scanned = 0
    started = time.monotonic()

    try:
        consumer.subscribe(
            [topics["requests"]],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        assignment_wait, pending = wait_for_assignment(
            consumer,
            assignment,
            timeout=assignment_timeout,
        )
        request_context = SerializationContext(
            topics["requests"], MessageField.VALUE
        )
        quote_context = SerializationContext(topics["quotes"], MessageField.VALUE)
        with SchemaRegistryClient(registry_conf) as registry:
            deserializer = AvroDeserializer(
                registry,
                trip_request_schema_str(),
                from_dict=avro_dict_to_request,
            )
            serializer = AvroSerializer(
                registry,
                fare_quote_schema_str(),
                to_dict=model_to_avro_dict,
                conf=serializer_conf(),
            )
            idle_deadline = time.monotonic() + idle_timeout
            while (
                len(processed) < max_messages
                and scanned < max_scanned
                and time.monotonic() < idle_deadline
            ):
                message = pending.pop(0) if pending else consumer.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Consumer error: {message.error()}")
                scanned += 1
                request = deserializer(message.value(), request_context)
                if not isinstance(request, TripRequestV1):
                    request = TripRequestV1.model_validate(request)
                if request.run_id != run_id:
                    commit_message(consumer, message)
                    skipped.append(message_coordinate(message))
                    idle_deadline = time.monotonic() + idle_timeout
                    continue

                coordinate = message_coordinate(message)
                route = router.estimate(request.pickup, request.dropoff)
                quote = quote_from_route(
                    request,
                    route,
                    source_id=coordinate,
                    pricing_method=pricing_method,
                    artifact=artifact,
                    quoted_at=datetime.now(UTC),
                )
                value = serializer(quote, quote_context)
                if value is None:
                    raise RuntimeError("AvroSerializer unexpectedly returned None")
                delivery = acknowledged_produce(
                    producer,
                    topic=topics["quotes"],
                    key=request.trip_id.encode("utf-8"),
                    value=value,
                    delivery_timeout=delivery_timeout,
                    headers=[
                        ("demo", b"07C"),
                        ("run-id", run_id.encode("utf-8")),
                        ("model-version", pricing_method.encode("utf-8")),
                        ("source-record-id", coordinate.encode("utf-8")),
                    ],
                )

                # ============================================================
                # KEY CONCEPT
                # This is a Kafka consumer offset commit. The quote is broker
                # acknowledged first; only then is input progress committed.
                # It is not a Git commit and not a producer-side commit.
                # ============================================================
                commit_result = commit_message(consumer, message)
                processed.append(
                    {
                        "trip_id": request.trip_id,
                        "source_record_id": coordinate,
                        "routing_provider": route.provider,
                        "quote": quote.model_dump(mode="json"),
                        "delivery": delivery,
                        "input_commit": commit_result,
                    }
                )
                idle_deadline = time.monotonic() + idle_timeout
    finally:
        consumer.close()

    if len(processed) != max_messages:
        raise RuntimeError(
            f"{pricing_method} expected {max_messages} records for run {run_id!r} "
            f"but processed {len(processed)} after scanning {scanned}. "
            "Run Demo 07B first with the same --run-id."
        )

    report: dict[str, Any] = {
        "demo": "07C",
        "run_id": run_id,
        "pricing_method": pricing_method,
        "model_artifact": (
            artifact.model_dump(mode="json") if artifact is not None else None
        ),
        "group_id": group_id,
        "route_mode": route_mode,
        "routing_calls": len(processed),
        "input_topic": topics["requests"],
        "output_topic": topics["quotes"],
        "topic_status": topic_status,
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "scanned": scanned,
        "skipped_other_runs": len(skipped),
        "processed": len(processed),
        "records": processed,
        "commit_order": [
            "deserialize Avro request",
            "validate TripRequestV1",
            "call selected routing provider once",
            "execute versioned pricing method",
            "produce FareQuoteV1",
            "wait for broker acknowledgement",
            "commit Kafka input offset",
        ],
        "assignments": assignment.assigned,
        "kafka": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    report_name = f"demo07c-{pricing_method}"
    path = write_json_report(run_id, report_name, report)
    report["report_path"] = str(path)
    return report
