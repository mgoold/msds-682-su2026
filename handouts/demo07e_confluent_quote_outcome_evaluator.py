"""Demo 07E: join fare quotes with delayed outcomes and publish evaluations."""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime
from typing import Any

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
    FareQuoteV1,
    PricingEvaluationV1,
    TripOutcomeV1,
    avro_dict_to_outcome,
    avro_dict_to_quote,
    evaluate_quote,
    fare_quote_schema_str,
    model_to_avro_dict,
    pricing_evaluation_schema_str,
    serializer_conf,
    summarize_evaluations,
    topic_names,
    trip_outcome_schema_str,
)
from demo07_kafka import (
    AssignmentTracker,
    acknowledged_produce,
    commit_message,
    commit_message_batch,
    ensure_demo07_topics,
    message_coordinate,
    wait_for_assignment,
)


def run_evaluator(
    *,
    run_id: str,
    expected_trips: int,
    expected_models: tuple[str, ...],
    assignment_timeout: float,
    idle_timeout: float,
    delivery_timeout: float,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    max_scanned: int = 5_000,
) -> dict[str, Any]:
    """Collect one complete bounded quote/outcome set, then publish evaluations.

    The classroom implementation collects and validates the complete expected
    quote/outcome set
    before emitting its evaluation records. A continuous production processor
    could emit each matched pair as soon as it becomes eligible.
    """

    validate_run_id(run_id)
    if not 1 <= expected_trips <= 25:
        raise ValueError("expected_trips must be between 1 and 25")
    if set(expected_models) != {"rule-v1", "ridge-v2"}:
        raise ValueError("expected_models must be rule-v1 and ridge-v2")
    expected_input_records = expected_trips * (len(expected_models) + 1)
    if max_scanned < expected_input_records:
        raise ValueError("max_scanned cannot be smaller than expected inputs")
    if min(assignment_timeout, idle_timeout, delivery_timeout) <= 0:
        raise ValueError("timeouts must be positive")

    topics = topic_names()
    input_topics = [topics["quotes"], topics["outcomes"]]
    output_topic = topics["evaluations"]
    group_id = consumer_group_id("demo07-evaluator", run_id)
    base_conf = kafka_config(client_id="msds682-demo07e-evaluator")
    registry_conf = schema_registry_config()
    topic_status = ensure_demo07_topics(
        AdminClient(base_conf),
        create=create_topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    consumer_conf: dict[str, Any] = {
        **base_conf,
        "client.id": "msds682-demo07e-evaluator-consumer",
        "group.id": group_id,
        "group.protocol": "classic",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf: dict[str, Any] = {
        **base_conf,
        "client.id": "msds682-demo07e-evaluator-producer",
    }
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    assignment = AssignmentTracker()
    quotes: dict[str, dict[str, tuple[FareQuoteV1, Any]]] = {}
    outcomes: dict[str, tuple[TripOutcomeV1, Any]] = {}
    skipped: list[str] = []
    scanned = 0
    started = time.monotonic()

    try:
        consumer.subscribe(
            input_topics,
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        assignment_wait, pending = wait_for_assignment(
            consumer,
            assignment,
            timeout=assignment_timeout,
        )
        with SchemaRegistryClient(registry_conf) as registry:
            quote_deserializer = AvroDeserializer(
                registry,
                fare_quote_schema_str(),
                from_dict=avro_dict_to_quote,
            )
            outcome_deserializer = AvroDeserializer(
                registry,
                trip_outcome_schema_str(),
                from_dict=avro_dict_to_outcome,
            )
            evaluation_serializer = AvroSerializer(
                registry,
                pricing_evaluation_schema_str(),
                to_dict=model_to_avro_dict,
                conf=serializer_conf(),
            )
            idle_deadline = time.monotonic() + idle_timeout
            while (
                (
                    sum(len(by_model) for by_model in quotes.values())
                    < expected_trips * len(expected_models)
                    or len(outcomes) < expected_trips
                )
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
                context = SerializationContext(message.topic(), MessageField.VALUE)
                if message.topic() == topics["quotes"]:
                    event = quote_deserializer(message.value(), context)
                    if not isinstance(event, FareQuoteV1):
                        event = FareQuoteV1.model_validate(event)
                    if event.run_id != run_id:
                        commit_message(consumer, message)
                        skipped.append(message_coordinate(message))
                        idle_deadline = time.monotonic() + idle_timeout
                        continue
                    if event.model_version not in expected_models:
                        raise RuntimeError(
                            f"Unexpected model version {event.model_version!r}"
                        )
                    by_model = quotes.setdefault(event.trip_id, {})
                    if event.model_version in by_model:
                        raise RuntimeError(
                            f"Duplicate quote for {event.quote_id} in run {run_id!r}"
                        )
                    by_model[event.model_version] = (event, message)
                elif message.topic() == topics["outcomes"]:
                    event = outcome_deserializer(message.value(), context)
                    if not isinstance(event, TripOutcomeV1):
                        event = TripOutcomeV1.model_validate(event)
                    if event.run_id != run_id:
                        commit_message(consumer, message)
                        skipped.append(message_coordinate(message))
                        idle_deadline = time.monotonic() + idle_timeout
                        continue
                    if event.trip_id in outcomes:
                        raise RuntimeError(
                            f"Duplicate outcome for {event.trip_id} in run {run_id!r}"
                        )
                    outcomes[event.trip_id] = (event, message)
                else:
                    raise RuntimeError(f"Unexpected input topic {message.topic()}")
                idle_deadline = time.monotonic() + idle_timeout

            quote_trip_ids = set(quotes)
            outcome_trip_ids = set(outcomes)
            incomplete_quotes = {
                trip_id: sorted(set(expected_models) - set(by_model))
                for trip_id, by_model in quotes.items()
                if set(by_model) != set(expected_models)
            }
            if (
                len(quote_trip_ids) != expected_trips
                or len(outcome_trip_ids) != expected_trips
                or quote_trip_ids != outcome_trip_ids
                or incomplete_quotes
            ):
                raise RuntimeError(
                    "Quote-outcome join did not receive a complete matched set: "
                    f"quotes={sorted(quote_trip_ids)}, "
                    f"outcomes={sorted(outcome_trip_ids)}, "
                    f"incomplete={incomplete_quotes}"
                )

            output_context = SerializationContext(
                output_topic, MessageField.VALUE
            )
            evaluations: list[PricingEvaluationV1] = []
            output_records: list[dict[str, Any]] = []
            messages_to_commit: list[Any] = []
            for trip_id in sorted(quote_trip_ids):
                outcome, outcome_message = outcomes[trip_id]
                messages_to_commit.append(outcome_message)
                for model_version in expected_models:
                    quote, quote_message = quotes[trip_id][model_version]
                    evaluation = evaluate_quote(
                        quote,
                        outcome,
                        evaluated_at=datetime.now(UTC),
                    )
                    value = evaluation_serializer(evaluation, output_context)
                    if value is None:
                        raise RuntimeError(
                            "AvroSerializer unexpectedly returned None"
                        )
                    delivery = acknowledged_produce(
                        producer,
                        topic=output_topic,
                        key=evaluation.quote_id.encode("utf-8"),
                        value=value,
                        delivery_timeout=delivery_timeout,
                        headers=[
                            ("demo", b"07E"),
                            ("run-id", run_id.encode("utf-8")),
                            (
                                "model-version",
                                evaluation.model_version.encode("utf-8"),
                            ),
                            ("join-key", trip_id.encode("utf-8")),
                        ],
                    )
                    evaluations.append(evaluation)
                    output_records.append(
                        {
                            "evaluation": evaluation.model_dump(mode="json"),
                            "quote_input": message_coordinate(quote_message),
                            "outcome_input": message_coordinate(outcome_message),
                            "delivery": delivery,
                        }
                    )
                    messages_to_commit.append(quote_message)

            # ================================================================
            # KEY CONCEPT
            # This finite join first collected and validated the complete run.
            # It now holds those quote and outcome inputs until every derived
            # evaluation is acknowledged, then commits both topics together.
            # It is bounded teaching state, not production durable state or a
            # per-pair continuous-emission implementation.
            # ================================================================
            commit_result = commit_message_batch(consumer, messages_to_commit)
    finally:
        consumer.close()

    summary = summarize_evaluations(evaluations)
    report: dict[str, Any] = {
        "demo": "07E",
        "run_id": run_id,
        "group_id": group_id,
        "input_topics": input_topics,
        "output_topic": output_topic,
        "topic_status": topic_status,
        "expected_models": list(expected_models),
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "scanned": scanned,
        "skipped_other_runs": len(skipped),
        "quotes": sum(len(by_model) for by_model in quotes.values()),
        "outcomes": len(outcomes),
        "evaluations": len(evaluations),
        "records": output_records,
        "model_summary": summary,
        "input_commit": commit_result,
        "join_state": {
            "type": "bounded in-memory dictionaries keyed by trip_id",
            "production_limit": (
                "A continuously running join needs durable state, event-time "
                "bounds, checkpointing, expiration, and late-event policy."
            ),
        },
        "kafka": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo07e", report)
    report["report_path"] = str(path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-trips", type=int, default=4)
    parser.add_argument(
        "--expected-models",
        nargs=2,
        default=("rule-v1", "ridge-v2"),
    )
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=20.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--max-scanned", type=int, default=5_000)
    args = parser.parse_args()

    report = run_evaluator(
        run_id=args.run_id,
        expected_trips=args.expected_trips,
        expected_models=tuple(args.expected_models),
        assignment_timeout=args.assignment_timeout,
        idle_timeout=args.idle_timeout,
        delivery_timeout=args.delivery_timeout,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        max_scanned=args.max_scanned,
    )
    print(
        f"Published {report['evaluations']} evaluations to "
        f"{report['output_topic']}"
    )
    print(f"Model summary: {report['model_summary']}")
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
