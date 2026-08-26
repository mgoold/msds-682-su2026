"""Prepare Demo 09 topics and render SQL without using Confluent CLI.

By default this is a read-only cloud preflight.  The explicit
``--create-output-topics`` flag is required before it creates the two isolated
Demo 09 sink topics and registers Demo 07's existing evaluation value schema.
ksqlDB applications, Flink compute pools, and SQL statements remain manual
Confluent Cloud UI operations because they have lifecycle and billing impact.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from confluent_demo_common import (
    kafka_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    validate_run_id,
    write_json_report,
)
from demo07_common import pricing_evaluation_schema_str, topic_names
from demo07_kafka import ensure_demo07_topics
from demo09_common import output_topic_names, rendered_sql


def _topic_snapshot(admin: AdminClient, topics: list[str]) -> dict[str, Any]:
    """Return existence and partition counts without changing Kafka."""

    metadata = admin.list_topics(timeout=15)
    snapshot: dict[str, Any] = {}
    for topic in topics:
        topic_metadata = metadata.topics.get(topic)
        exists = topic_metadata is not None and topic_metadata.error is None
        snapshot[topic] = {
            "exists": exists,
            "partitions": (
                len(topic_metadata.partitions) if exists else None
            ),
        }
    return snapshot


def _subject_snapshot(
    registry: SchemaRegistryClient,
    topics: list[str],
) -> dict[str, Any]:
    """Read latest value-schema metadata; never include Registry credentials."""

    snapshot: dict[str, Any] = {}
    for topic in topics:
        subject = f"{topic}-value"
        try:
            latest = registry.get_latest_version(subject)
        except Exception as exc:
            snapshot[subject] = {
                "exists": False,
                "error_type": type(exc).__name__,
            }
        else:
            snapshot[subject] = {
                "exists": True,
                "schema_id": latest.schema_id,
                "version": latest.version,
            }
    return snapshot


def _write_rendered_sql(run_id: str, sql: dict[str, str]) -> dict[str, str]:
    """Write the exact statements that the professor pastes into Cloud UI."""

    output_dir = (
        Path("outputs") / "runs" / run_id / "demo09_prepare"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    for engine, contents in sql.items():
        filename = {
            "ksqldb": "demo09a_ksqldb.sql",
            "flinksql": "demo09b_flinksql.sql",
        }[engine]
        path = output_dir / filename
        path.write_text(contents, encoding="utf-8")
        paths[engine] = str(path)
    return paths


def run_prepare(
    *,
    run_id: str,
    create_output_topics: bool,
    partitions: int,
    replication_factor: int,
) -> dict[str, Any]:
    """Run the data-plane preflight and create only explicitly allowed sinks."""

    validate_run_id(run_id)
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")

    inputs = topic_names()
    outputs = output_topic_names()
    kafka_conf = kafka_config(client_id="msds682-demo09-prepare")
    registry_conf = schema_registry_config()
    admin = AdminClient(kafka_conf)

    # KEY CONCEPT: input topics are owned by Demo 07.  ``create=False`` makes
    # this a strict dependency check; Demo 09 is never allowed to recreate or
    # silently alter its upstream source.
    input_status = ensure_demo07_topics(
        admin,
        create=False,
        partitions=partitions,
        replication_factor=replication_factor,
    )

    output_status: dict[str, str] = {}
    if create_output_topics:
        from confluent_demo_common import ensure_topic

        for engine, topic in outputs.items():
            output_status[engine] = ensure_topic(
                admin,
                topic=topic,
                create=True,
                partitions=partitions,
                replication_factor=replication_factor,
                create_option="--create-output-topics",
            )

    all_topics = [*inputs.values(), *outputs.values()]
    topic_snapshot = _topic_snapshot(admin, all_topics)
    missing_inputs = [
        topic for topic in inputs.values() if not topic_snapshot[topic]["exists"]
    ]
    if missing_inputs:
        raise RuntimeError(f"Missing Demo 07 input topics: {missing_inputs}")

    with SchemaRegistryClient(registry_conf) as registry:
        if create_output_topics:
            # KEY CONCEPT: both engines reuse PricingEvaluationV1.  Registering
            # the same schema under each TopicNameStrategy subject gives Cloud
            # SQL two physical sinks but only one logical value contract.
            schema = Schema(pricing_evaluation_schema_str(), "AVRO")
            for topic in outputs.values():
                registry.register_schema(f"{topic}-value", schema)
        subjects = _subject_snapshot(registry, all_topics)

    # KEY CONCEPT: VALUE_SCHEMA_ID comes from the canonical Demo 07 evaluation
    # subject.  Therefore read-only mode can render SQL before Demo 09 sinks
    # exist, and no account-specific ID is hardcoded in the source template.
    canonical_subject = f"{inputs['evaluations']}-value"
    canonical_schema_id = subjects[canonical_subject].get("schema_id")
    if not isinstance(canonical_schema_id, int) or canonical_schema_id <= 0:
        raise RuntimeError(
            f"Missing canonical Demo 07 value subject {canonical_subject!r}. "
            "Complete Demo 07's Avro setup before Demo 09."
        )
    if create_output_topics:
        mismatched_subjects = [
            f"{topic}-value"
            for topic in outputs.values()
            if subjects[f"{topic}-value"].get("schema_id")
            != canonical_schema_id
        ]
        if mismatched_subjects:
            raise RuntimeError(
                "Demo 09 output subjects do not reuse the canonical "
                f"PricingEvaluationV1 schema: {mismatched_subjects}"
            )
    sql_paths = _write_rendered_sql(
        run_id,
        rendered_sql(
            run_id,
            ksqldb_value_schema_id=canonical_schema_id,
        ),
    )
    report: dict[str, Any] = {
        "demo": "09-prepare",
        "run_id": run_id,
        "mode": (
            "create-output-topics"
            if create_output_topics
            else "read-only-preflight"
        ),
        "tool_boundary": {
            "data_plane": "confluent-kafka Python client",
            "sql_execution": "manual Confluent Cloud UI",
            "confluent_cli_used": False,
        },
        "demo07_topics": inputs,
        "demo09_output_topics": outputs,
        "input_status": input_status,
        "output_create_status": output_status,
        "topic_snapshot": topic_snapshot,
        "schema_subject_snapshot": subjects,
        "canonical_evaluation_value_schema": {
            "subject": canonical_subject,
            "schema_id": canonical_schema_id,
        },
        "rendered_sql": sql_paths,
        "next_step": (
            "Open each rendered SQL file in Confluent Cloud UI; inspect and "
            "start the engine manually before producing the fresh Demo 07 run."
        ),
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    report_path = write_json_report(run_id, "demo09_prepare", report)
    report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--create-output-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    report = run_prepare(
        run_id=args.run_id,
        create_output_topics=args.create_output_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(f"Demo 09 prepare mode: {report['mode']}")
    for engine, path in report["rendered_sql"].items():
        print(f"Rendered {engine} SQL: {path}")
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
