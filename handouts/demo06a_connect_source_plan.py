"""Demo 06A: prepare the managed Datagen Source connector safely."""

from __future__ import annotations

import argparse

from confluent_kafka.admin import AdminClient

from confluent_demo_common import (
    ensure_topic,
    kafka_config,
    safe_kafka_config_report,
    validate_run_id,
    write_json_report,
)
from demo06_common import (
    connector_console_plan,
    input_topic_name,
    output_topic_name,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-interval-ms", type=int, default=2_000)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("--partitions and --replication-factor must be positive")

    input_topic = input_topic_name()
    output_topic = output_topic_name()
    topic_status: dict[str, str] = {
        "input": "not_checked",
        "output": "not_checked",
    }
    connection: dict[str, object] = {"cloud_checked": False}

    if args.create_topics:
        kafka_conf = kafka_config(client_id="msds682-demo06a-topic-setup")
        admin = AdminClient(kafka_conf)
        topic_status = {
            "input": ensure_topic(
                admin,
                topic=input_topic,
                create=True,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
                create_option="--create-topics",
            ),
            "output": ensure_topic(
                admin,
                topic=output_topic,
                create=True,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
                create_option="--create-topics",
            ),
        }
        connection = {
            "cloud_checked": True,
            "kafka": safe_kafka_config_report(kafka_conf),
        }

    report = {
        "demo": "06A",
        "run_id": run_id,
        "relationship": (
            "Kafka Connect owns source integration; the Python processor begins "
            "after records are durable in the input topic."
        ),
        "input_topic": input_topic,
        "output_topic": output_topic,
        "topic_status": topic_status,
        "connection": connection,
        "cloud_console_fields": connector_console_plan(
            topic=input_topic,
            max_interval_ms=args.max_interval_ms,
        ),
        "next_step": (
            "Create the connector in Confluent Cloud, wait for at least 8 "
            "records, then pause it before running Demo 06B."
        ),
    }
    path = write_json_report(run_id, "demo06a", report)
    print(f"Demo 06A plan written to {path}")
    print(f"Input topic: {input_topic}")
    print("Cloud Console: Connectors -> Add connector -> Datagen Source")
    print("Pause the connector after at least 8 records are visible.")
    print("Delete it after the exercise; revoke a demo-only key if unused.")


if __name__ == "__main__":
    main()
