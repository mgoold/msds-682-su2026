"""
================================================================================
DEMO 06B - INSPECTING CONNECTOR-CREATED SOURCE RECORDS
(annotated tutorial copy)
================================================================================

READ demo06_common.py FIRST. AssignmentTracker, DatagenOrderV1,
input_topic_name, and wait_for_assignment are all defined and explained there.

WHAT THIS DEMO TEACHES
    Demo 06A got records INTO Kafka using a managed connector (or the
    fallback). Demo 06B answers a narrower question: did that actually work,
    and are the records what you expect? It is a READ-ONLY INSPECTION - a
    consumer whose entire job is to look, validate, and report, never to
    advance any group's processing progress.

WHY THIS SCRIPT MAKES ZERO COMMITS - AND WHY THAT IS THE POINT
    Demo 06C is the real processor; ITS consumer group owns the "how far has
    processing gotten" bookkeeping for the input topic. If 06B committed
    offsets under a group name Demo 06C might reuse, running 06B could
    silently advance 06C's starting position and make it skip real input.
    So 06B deliberately:
        - uses "earliest" (always re-reads from the beginning, regardless of
          any group's committed position), and
        - uses a group ID from consumer_group_id("demo06b-inspect", run_id) -
          a name namespaced so it can never collide with 06C's own group.
    The report even states this reasoning directly under "why_no_commit".

WHAT THIS SCRIPT PROVES, CONCRETELY
    That real Avro-encoded records exist in the input topic, that each one's
    writer schema can be fetched from Schema Registry, and that each decoded
    value satisfies DatagenOrderV1 - independent of whether Demo 06A's
    managed connector or the Python fallback produced them.
================================================================================
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
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
from demo06_common import (
    AssignmentTracker,
    DatagenOrderV1,
    input_topic_name,
    wait_for_assignment,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-messages", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if not 1 <= args.max_messages <= 100:
        parser.error("--max-messages must be between 1 and 100")
    if args.assignment_timeout <= 0 or args.idle_timeout <= 0:
        parser.error("timeouts must be positive")

    topic = input_topic_name()
    group_id = consumer_group_id("demo06b-inspect", run_id)
    kafka_conf: dict[str, Any] = {
        **kafka_config(client_id="msds682-demo06b-source-consumer"),
        "group.id": group_id,
        # Pin the classic protocol because AssignmentTracker accepts the full
        # assignment with consumer.assign(). KIP-848 callbacks are incremental.
        "group.protocol": "classic",
        # "earliest": this inspection ALWAYS starts from the beginning of the
        # topic on a fresh group, regardless of anything Demo 06C's group has
        # committed - it is meant to see whatever is already durable, not
        # only new arrivals.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    registry_conf = schema_registry_config()
    tracker = AssignmentTracker()
    records: list[dict[str, Any]] = []
    consumer = Consumer(kafka_conf)
    started = time.monotonic()

    try:
        consumer.subscribe(
            [topic],
            on_assign=tracker.on_assign,
            on_revoke=tracker.on_revoke,
        )
        # Same coordination discipline as every consumer in this course:
        # confirm real partition assignment before trusting anything the
        # poll loop returns. wait_for_assignment() also hands back any
        # pending_messages the SAME poll happened to deliver, so offset 0 is
        # never silently skipped.
        assignment_wait, pending_messages = wait_for_assignment(
            consumer,
            tracker,
            timeout=args.assignment_timeout,
        )
        context = SerializationContext(topic, MessageField.VALUE)
        with SchemaRegistryClient(registry_conf) as registry:
            # AvroDeserializer(registry) WITH NO from_dict ARGUMENT: unlike
            # Demo 04/05's deserializers, this one returns a plain dict, not
            # a pydantic model. Validation happens explicitly, one line
            # below, via DatagenOrderV1.model_validate(raw) - a perfectly
            # valid alternative wiring to passing from_dict directly, used
            # here to make the validation step visible in this inspection
            # script.
            deserializer = AvroDeserializer(registry)
            idle_deadline = time.monotonic() + args.idle_timeout
            while len(records) < args.max_messages and time.monotonic() < idle_deadline:
                # Drain any message the assignment-triggering poll already
                # returned before asking the broker for more.
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
                raw = deserializer(message.value(), context)
                order = DatagenOrderV1.model_validate(raw)
                records.append(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        # key_present/key_bytes rather than a decoded string:
                        # this script deliberately does NOT assume a
                        # particular key encoding, because the managed
                        # connector and the Python fallback use DIFFERENT key
                        # encodings (see demo06_common.py's fallback_order_key
                        # docstring) - reporting only presence and length
                        # stays honest regardless of which source produced
                        # this record.
                        "key_present": message.key() is not None,
                        "key_bytes": len(message.key() or b""),
                        "order": order.model_dump(),
                    }
                )
                # RESET THE IDLE CLOCK on every record actually consumed, so
                # the timeout measures GAPS between arrivals, not total
                # elapsed time - a slow but steady trickle of records will
                # not be cut off just because the whole inspection runs
                # longer than idle_timeout.
                idle_deadline = time.monotonic() + args.idle_timeout
    finally:
        # consumer.close() leaves the group cleanly and commits nothing
        # extra - matching this script's promise of zero commits even on the
        # cleanup path.
        consumer.close()

    if len(records) != args.max_messages:
        raise RuntimeError(
            f"Expected {args.max_messages} records but consumed {len(records)}. "
            "Run the managed connector or the finite fallback seed first."
        )

    report = {
        "demo": "06B",
        "run_id": run_id,
        "input_source": "managed Datagen connector or explicit fallback seed",
        "topic": topic,
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "manual_commits": 0,
        # STATING THE REASONING DIRECTLY IN THE EVIDENCE FILE - a reader of
        # the JSON report, not just of this source file, can see why zero
        # commits is correct rather than an oversight.
        "why_no_commit": (
            "06B is an isolated inspection group. Demo 06C owns processor commits."
        ),
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "assignments": tracker.assigned,
        "consumed": len(records),
        "records": records,
        "kafka": safe_kafka_config_report(kafka_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    path = write_json_report(run_id, "demo06b", report)
    print(f"Consumed {len(records)} validated Avro source records")
    print(f"Secret-free report: {path}")


if __name__ == "__main__":
    main()
