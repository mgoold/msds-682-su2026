"""Verify Python, ksqlDB, and Flink SQL with one Demo 07 business contract.

The verifier uses ``confluent-kafka`` plus Schema Registry directly.  It does
not use Confluent CLI and it does not trust a SQL result merely because eight
rows exist: every field is recalculated from the original quote and outcome.
"""

from __future__ import annotations

import argparse
import math
import time
from datetime import datetime
from typing import Any, Callable, TypeVar

from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

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
    avro_dict_to_evaluation,
    avro_dict_to_outcome,
    avro_dict_to_quote,
    deterministic_trip_requests,
    evaluate_quote,
    fare_quote_schema_str,
    pricing_evaluation_schema_str,
    summarize_evaluations,
    topic_names,
    trip_outcome_schema_str,
)
from demo07f_compare_models import compare_model_summaries
from demo09_common import ComputeEngine, engine_evaluation_topics

ModelT = TypeVar("ModelT", bound=BaseModel)


def _read_topic_snapshot(
    *,
    topic: str,
    run_id: str,
    base_conf: dict[str, str],
    registry: SchemaRegistryClient,
    schema_str: str,
    from_dict: Callable[..., ModelT],
    model_type: type[ModelT],
    identity: Callable[[ModelT], str],
    group_suffix: str,
    idle_timeout: float,
    max_scanned: int,
) -> tuple[list[ModelT], dict[str, Any]]:
    """Read a bounded beginning-to-current-end snapshot of one Kafka topic.

    KEY CONCEPT: continuous SQL never "finishes."  The verifier creates a
    finite observation boundary by snapshotting each partition's high
    watermark, then reading only up to that boundary.
    """

    if idle_timeout <= 0 or max_scanned < 1:
        raise ValueError("idle_timeout and max_scanned must be positive")
    consumer_conf: dict[str, Any] = {
        **base_conf,
        "client.id": f"msds682-demo09-verify-{group_suffix}",
        "group.id": consumer_group_id(f"demo09-verify-{group_suffix}", run_id),
        "group.protocol": "classic",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        # Emit partition-EOF events so a snapshot also terminates correctly
        # when Kafka retention or compaction has left gaps near the high
        # watermark.  Without this, a quiet partition could wait until timeout.
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    records: list[ModelT] = []
    seen: set[str] = set()
    skipped_other_runs = 0
    scanned = 0
    try:
        metadata = consumer.list_topics(topic=topic, timeout=15)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic {topic!r} is unavailable")
        assignments = [
            TopicPartition(topic, partition_id, OFFSET_BEGINNING)
            for partition_id in sorted(topic_metadata.partitions)
        ]
        if not assignments:
            raise RuntimeError(f"Kafka topic {topic!r} has no partitions")
        boundaries: dict[int, int] = {}
        positions: dict[int, int] = {}
        for partition in assignments:
            low, high = consumer.get_watermark_offsets(
                partition,
                timeout=15,
                cached=False,
            )
            boundaries[partition.partition] = high
            positions[partition.partition] = low
        consumer.assign(assignments)
        completed_partitions = {
            partition_id
            for partition_id in boundaries
            if positions[partition_id] >= boundaries[partition_id]
        }
        if completed_partitions:
            consumer.pause(
                [
                    TopicPartition(topic, partition_id)
                    for partition_id in sorted(completed_partitions)
                ]
            )
        deserializer = AvroDeserializer(
            registry,
            schema_str,
            from_dict=from_dict,
        )
        deadline = time.monotonic() + idle_timeout
        while (
            any(
                positions[partition] < boundaries[partition]
                for partition in boundaries
            )
            and scanned < max_scanned
        ):
            message = consumer.poll(0.5)
            if message is None:
                if time.monotonic() >= deadline:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    partition_id = message.partition()
                    positions[partition_id] = boundaries[partition_id]
                    if partition_id not in completed_partitions:
                        consumer.pause([TopicPartition(topic, partition_id)])
                        completed_partitions.add(partition_id)
                    continue
                raise RuntimeError(f"Consumer error: {message.error()}")
            partition_id = message.partition()
            boundary = boundaries[partition_id]
            # A continuous topic may receive new records after the watermarks
            # above were captured.  They belong to a later observation, not
            # this finite snapshot, so never let them change this run's proof.
            if message.offset() >= boundary:
                positions[partition_id] = boundary
                if partition_id not in completed_partitions:
                    consumer.pause([TopicPartition(topic, partition_id)])
                    completed_partitions.add(partition_id)
                continue
            scanned += 1
            positions[partition_id] = message.offset() + 1
            if positions[partition_id] >= boundary:
                consumer.pause([TopicPartition(topic, partition_id)])
                completed_partitions.add(partition_id)
            deadline = time.monotonic() + idle_timeout
            if message.value() is None:
                raise RuntimeError(
                    f"Unexpected tombstone in append-only topic {topic!r} at "
                    f"partition {partition_id}, offset {message.offset()}"
                )
            context = SerializationContext(topic, MessageField.VALUE)
            event = deserializer(message.value(), context)
            if not isinstance(event, model_type):
                event = model_type.model_validate(event)
            if getattr(event, "run_id") != run_id:
                skipped_other_runs += 1
                continue
            record_identity = identity(event)
            if record_identity in seen:
                raise RuntimeError(
                    f"Duplicate {record_identity!r} for run {run_id!r} "
                    f"in topic {topic!r}"
                )
            seen.add(record_identity)
            records.append(event)
        incomplete = {
            partition: {
                "position": positions[partition],
                "snapshot_end": boundaries[partition],
            }
            for partition in boundaries
            if positions[partition] < boundaries[partition]
        }
        if incomplete:
            raise RuntimeError(
                f"Timed out before reaching topic snapshot boundary: {incomplete}"
            )
        return records, {
            "topic": topic,
            "partitions": len(assignments),
            "scanned": scanned,
            "matched_run": len(records),
            "skipped_other_runs": skipped_other_runs,
            "snapshot_end_offsets": boundaries,
        }
    finally:
        consumer.close()


def _business_mismatches(
    actual: PricingEvaluationV1,
    expected: PricingEvaluationV1,
) -> list[str]:
    """Compare SQL output with the Demo 07 oracle at contract precision.

    Identifiers, money, nullable prediction error, and the boolean decision are
    exact.  Rounded floating-point business metrics allow at most 0.0001 to
    absorb representation differences between Python and the two SQL runtimes.
    """

    exact_fields = (
        "run_id",
        "trip_id",
        "quote_id",
        "model_version",
        "fare_cents",
        "actual_cost_cents",
        "profit_cents",
        "cost_prediction_error_cents",
        "within_target_tolerance",
    )
    float_fields = (
        "realized_markup_pct",
        "target_markup_pct",
        "markup_error_pp",
        "absolute_markup_error_pp",
    )
    mismatches = [
        field
        for field in exact_fields
        if getattr(actual, field) != getattr(expected, field)
    ]
    for field in float_fields:
        actual_value = float(getattr(actual, field))
        expected_value = float(getattr(expected, field))
        # NaN comparisons are always false, so a plain tolerance expression
        # would accidentally accept NaN.  SQL results used as evidence must be
        # finite before they are compared with the Demo 07 oracle.
        if not math.isfinite(actual_value) or (
            abs(actual_value - expected_value) > 0.0001000001
        ):
            mismatches.append(field)
    return mismatches


def verify_contract(
    *,
    run_id: str,
    quotes: list[FareQuoteV1],
    outcomes: list[TripOutcomeV1],
    evaluations_by_engine: dict[str, list[PricingEvaluationV1]],
) -> dict[str, Any]:
    """Apply one exact 4-outcome/8-quote/8-evaluation acceptance contract."""

    validate_run_id(run_id)
    expected_trip_ids = {
        request.trip_id for request in deterministic_trip_requests(run_id, 4)
    }
    if len(quotes) != 8 or len(outcomes) != 4:
        raise ValueError(
            f"Expected 8 quotes and 4 outcomes; got {len(quotes)} and "
            f"{len(outcomes)}"
        )
    quote_by_id = {quote.quote_id: quote for quote in quotes}
    outcome_by_trip = {outcome.trip_id: outcome for outcome in outcomes}
    if len(quote_by_id) != 8 or len(outcome_by_trip) != 4:
        raise ValueError("Quote IDs and outcome trip IDs must be unique")
    if any(quote.run_id != run_id for quote in quotes) or any(
        outcome.run_id != run_id for outcome in outcomes
    ):
        raise ValueError("Every quote and outcome must match the requested run_id")
    if set(outcome_by_trip) != expected_trip_ids:
        raise ValueError("Outcomes do not match the four deterministic trip IDs")
    expected_quote_ids = {
        f"{trip_id}:{model_version}"
        for trip_id in expected_trip_ids
        for model_version in ("rule-v1", "ridge-v2")
    }
    if set(quote_by_id) != expected_quote_ids:
        raise ValueError("Quotes do not contain both models for every trip")
    malformed_quotes = [
        quote.quote_id
        for quote in quotes
        if quote.quote_id != f"{quote.trip_id}:{quote.model_version}"
        or quote.pricing_method != quote.model_version
    ]
    if malformed_quotes:
        raise ValueError(
            "Quote identity or pricing/model version is inconsistent: "
            f"{sorted(malformed_quotes)}"
        )

    # Python's existing Demo 07 function is the oracle.  SQL is valuable here
    # because it moves computation ownership, not because it redefines markup.
    expected = {
        quote_id: evaluate_quote(
            quote,
            outcome_by_trip[quote.trip_id],
            evaluated_at=datetime.now().astimezone(),
        )
        for quote_id, quote in quote_by_id.items()
    }
    expected_summary = summarize_evaluations(list(expected.values()))
    expected_decision = compare_model_summaries(expected_summary)
    if expected_decision["recommended_version"] != "ridge-v2":
        raise ValueError("The bounded Demo 09 fixture must recommend ridge-v2")

    engines: dict[str, Any] = {}
    for engine, records in evaluations_by_engine.items():
        if engine not in {member.value for member in ComputeEngine}:
            raise ValueError(f"Unknown engine {engine!r}")
        if len(records) != 8:
            raise ValueError(f"{engine} produced {len(records)} rows, expected 8")
        by_quote = {record.quote_id: record for record in records}
        if len(by_quote) != 8 or set(by_quote) != expected_quote_ids:
            raise ValueError(f"{engine} output has missing or duplicate quote IDs")
        model_counts = {
            model: sum(record.model_version == model for record in records)
            for model in ("rule-v1", "ridge-v2")
        }
        if model_counts != {"rule-v1": 4, "ridge-v2": 4}:
            raise ValueError(f"{engine} model counts are {model_counts}")
        mismatches: dict[str, list[str]] = {}
        for quote_id, actual in by_quote.items():
            # SQL engines use their own processing time for this audit field.
            # We require a sane aware timestamp, but timestamps are not a
            # business-result equality field across independent engines.
            if actual.evaluated_at.utcoffset() is None or not (
                2020 <= actual.evaluated_at.year <= 2100
            ):
                raise ValueError(
                    f"{engine} has an invalid evaluated_at for {quote_id}"
                )
            fields = _business_mismatches(actual, expected[quote_id])
            if fields:
                mismatches[quote_id] = fields
        if mismatches:
            raise ValueError(
                f"{engine} disagrees with Demo 07 business rules for {mismatches}"
            )
        summary = summarize_evaluations(records)
        for model in ("rule-v1", "ridge-v2"):
            gap = abs(
                float(summary[model]["mean_absolute_markup_error_pp"])
                - float(
                    expected_summary[model]["mean_absolute_markup_error_pp"]
                )
            )
            if gap > 0.0001:
                raise ValueError(f"{engine} {model} MAE differs by {gap} pp")
        decision = compare_model_summaries(summary)
        if decision["recommended_version"] != "ridge-v2":
            raise ValueError(f"{engine} did not recommend ridge-v2")
        engines[engine] = {
            "records": len(records),
            "model_counts": model_counts,
            "model_summary": summary,
            "decision": decision,
        }
    if not engines:
        raise ValueError("Select at least one engine")
    return {
        "run_id": run_id,
        "input_counts": {"quotes": len(quotes), "outcomes": len(outcomes)},
        "expected_trip_ids": sorted(expected_trip_ids),
        "expected_model_summary": expected_summary,
        "engines": engines,
        "contract_passed": True,
    }


def run_verifier(
    *,
    run_id: str,
    engines: tuple[str, ...],
    idle_timeout: float,
    max_scanned: int,
) -> dict[str, Any]:
    """Read source/output topic snapshots and write secret-free evidence."""

    validate_run_id(run_id)
    allowed = {member.value for member in ComputeEngine}
    if not engines or set(engines) - allowed:
        raise ValueError(f"engines must be selected from {sorted(allowed)}")
    if len(set(engines)) != len(engines):
        raise ValueError("engines must not contain duplicates")

    topics = topic_names()
    engine_topics = engine_evaluation_topics()
    base_conf = kafka_config(client_id="msds682-demo09-verifier")
    registry_conf = schema_registry_config()
    snapshots: dict[str, Any] = {}
    with SchemaRegistryClient(registry_conf) as registry:
        quotes, snapshots["quotes"] = _read_topic_snapshot(
            topic=topics["quotes"],
            run_id=run_id,
            base_conf=base_conf,
            registry=registry,
            schema_str=fare_quote_schema_str(),
            from_dict=avro_dict_to_quote,
            model_type=FareQuoteV1,
            identity=lambda value: value.quote_id,
            group_suffix="quotes",
            idle_timeout=idle_timeout,
            max_scanned=max_scanned,
        )
        outcomes, snapshots["outcomes"] = _read_topic_snapshot(
            topic=topics["outcomes"],
            run_id=run_id,
            base_conf=base_conf,
            registry=registry,
            schema_str=trip_outcome_schema_str(),
            from_dict=avro_dict_to_outcome,
            model_type=TripOutcomeV1,
            identity=lambda value: value.trip_id,
            group_suffix="outcomes",
            idle_timeout=idle_timeout,
            max_scanned=max_scanned,
        )
        evaluations: dict[str, list[PricingEvaluationV1]] = {}
        for engine in engines:
            evaluations[engine], snapshots[engine] = _read_topic_snapshot(
                topic=engine_topics[engine],
                run_id=run_id,
                base_conf=base_conf,
                registry=registry,
                schema_str=pricing_evaluation_schema_str(),
                from_dict=avro_dict_to_evaluation,
                model_type=PricingEvaluationV1,
                identity=lambda value: value.quote_id,
                group_suffix=engine,
                idle_timeout=idle_timeout,
                max_scanned=max_scanned,
            )

    contract = verify_contract(
        run_id=run_id,
        quotes=quotes,
        outcomes=outcomes,
        evaluations_by_engine=evaluations,
    )
    report: dict[str, Any] = {
        "demo": "09-verify",
        **contract,
        "topic_snapshots": snapshots,
        "engine_topics": {engine: engine_topics[engine] for engine in engines},
        "tool_boundary": {
            "reader": "confluent-kafka Python client",
            "confluent_cli_used": False,
        },
        "kafka": safe_kafka_config_report(base_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    report_path = write_json_report(run_id, "demo09", report)
    report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=[member.value for member in ComputeEngine],
        default=[member.value for member in ComputeEngine],
    )
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--max-scanned", type=int, default=10_000)
    args = parser.parse_args()

    report = run_verifier(
        run_id=args.run_id,
        engines=tuple(args.engines),
        idle_timeout=args.idle_timeout,
        max_scanned=args.max_scanned,
    )
    print(
        "Demo 09 contract passed for: "
        + ", ".join(sorted(report["engines"]))
    )
    for engine, result in report["engines"].items():
        decision = result["decision"]
        print(
            f"{engine}: {result['records']} rows; "
            f"recommend {decision['recommended_version']}"
        )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
