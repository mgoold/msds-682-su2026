"""
================================================================================
DEMO 06A - PREPARING THE MANAGED CONNECT SOURCE  (annotated tutorial copy)
================================================================================

READ demo06_common.py FIRST. Everything imported below - connector_console_plan,
input_topic_name, output_topic_name - is defined and explained there.

WHAT THIS SCRIPT ACTUALLY DOES, AND WHAT IT DOES NOT DO
    THIS SCRIPT NEVER CREATES A KAFKA CONNECT CONNECTOR. There is no
    Confluent Cloud "Connect" API call anywhere below. What it DOES do:

        1. optionally create the two Demo 06 topics (input and derived), and
        2. print/report the EXACT field values a student should type into the
           Confluent Cloud Console UI to create the Datagen Source connector
           by hand.

    This split matters conceptually: Kafka Connect connector creation in this
    course is a manual, visible, Console-driven action - not something
    automated away by a script - because part of Demo 06A's lesson is seeing
    the connector, worker, task, and RUNNING status directly in the Cloud
    Console (see demo06.md's screenshots of this exact step).

WHY THE TOPICS ARE CREATED HERE, EAGERLY, BEFORE THE CONNECTOR EXISTS
    A Datagen Source connector needs its OUTPUT topic (Demo 06's INPUT topic)
    to exist, or it needs permission to auto-create it. Creating both Demo 06
    topics up front, with course-standard partition/replication settings,
    means the student's connector configuration in the Console can simply
    name an existing topic rather than depending on connector-side
    auto-creation with unpredictable settings.

WHAT 06A PROVES, AND WHAT IT DOES NOT
    Per demo06.md: 06A proves the integration runtime can create source
    records with no custom Python producer, that Datagen's Avro converter
    registers a value schema in Schema Registry, and that connector/task
    status is real operational evidence. It does NOT prove any Python
    processing logic is correct - that begins with Demo 06B.
================================================================================
"""

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
    # max_interval_ms: how often (in milliseconds) Datagen emits a new
    # synthetic record once running. 2000ms is the course default - fast
    # enough to see records arrive quickly in class, slow enough not to flood
    # a shared cluster.
    parser.add_argument("--max-interval-ms", type=int, default=2_000)
    # Opt-in, same safe default as every topic-creating script in this
    # course: never silently create infrastructure unless explicitly asked.
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("--partitions and --replication-factor must be positive")

    input_topic = input_topic_name()
    output_topic = output_topic_name()

    # DEFAULT, CLOUD-FREE STATE. If --create-topics is not passed, this
    # script never opens a Kafka connection at all - it can report the plan
    # (topic names, Console fields) purely from local configuration, which is
    # useful for reviewing the plan before touching any Cloud resource.
    topic_status: dict[str, str] = {
        "input": "not_checked",
        "output": "not_checked",
    }
    connection: dict[str, object] = {"cloud_checked": False}

    if args.create_topics:
        kafka_conf = kafka_config(client_id="msds682-demo06a-topic-setup")
        admin = AdminClient(kafka_conf)
        # BOTH topics, up front - the input topic Datagen will write to, and
        # the derived-event topic Demo 06C will need later. Preparing both
        # now means a student never has to remember to create the second one
        # mid-way through Demo 06C.
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
        # THE CHECKLIST a student reads directly off this report and re-types
        # into the Confluent Cloud Console's Datagen Source connector form -
        # see connector_console_plan() in demo06_common.py for every field's
        # meaning.
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
