"""
================================================================================
DEMO 06 SEED SOURCE - THE NO-PERMISSION FALLBACK  (annotated tutorial copy)
================================================================================

READ demo06_common.py FIRST. Everything imported below - the schema loader,
the deterministic generator, the key/value converters - is defined and
explained there.

WHAT THIS SCRIPT TEACHES
    This is NOT the main lesson of Demo 06 - it is a SAFETY VALVE. Demo 06A's
    real lesson is that Kafka Connect can populate a topic with no custom
    producer at all. But not every classroom Confluent Cloud account can
    create a managed connector (permissions vary), and Demo 06B-06D need SOME
    input data to work with regardless. This script is what a student runs
    ONLY when 06A's managed connector is unavailable.

WHY THIS IS SAFE TO SUBSTITUTE FOR A REAL CONNECTOR
    It writes values that satisfy the EXACT SAME Avro schema
    (DATAGEN_ORDER_SCHEMA_PATH, via datagen_order_schema_str()) the managed
    Datagen connector itself would produce. Demo 06B/06C/06D never need to
    know or care which source produced their input - they only ever validate
    against DatagenOrderV1. The one place this fallback visibly differs is the
    KEY ENCODING (see fallback_order_key() in demo06_common.py) - readable
    UTF-8 digits here versus the connector's own opaque key.

WHAT THIS SCRIPT IS HONEST ABOUT
    The report below labels itself explicitly: "finite deterministic Python
    fallback, not Kafka Connect". It never claims to prove anything about
    Connect, converters, or connector/task lifecycle - only 06A can prove
    those things, by using the real managed connector.
================================================================================
"""

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
    """Collect broker acknowledgements without recording credentials.

    Same pattern as every producer demo since Demo 02: a small dataclass
    whose `callback` method is handed to `on_delivery=`, populated later by
    the client's background I/O thread once each message's fate is known.
    """

    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, message: Any) -> None:
        if error is not None:
            self.failed.append(str(error))
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

    # -------------------------------------------------------------------
    # STEP 1: ENSURE BOTH TOPICS EXIST
    # -------------------------------------------------------------------
    # NOTE BOTH TOPICS ARE CREATED HERE, not just the input one. This
    # fallback runs standalone, without 06A having necessarily run first - so
    # it takes responsibility for the DERIVED-event topic Demo 06C will later
    # need too, exactly as 06A's own --create-topics flag would have.
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
        create_option="--create-topics",
    )
    output_status = ensure_topic(
        admin,
        topic=output_topic,
        create=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        create_option="--create-topics",
    )

    # -------------------------------------------------------------------
    # STEP 2: GENERATE AND PUBLISH DETERMINISTIC FALLBACK RECORDS
    # -------------------------------------------------------------------
    # stable_seed_offset(run_id) - a crc32-derived integer, so re-running this
    # script with the SAME --run-id regenerates the SAME orders, while a
    # different run_id (a different student) produces distinct ones.
    seed_offset = stable_seed_offset(run_id)
    orders = deterministic_orders(args.count, seed_offset=seed_offset)
    producer = Producer(kafka_conf)
    tracker = DeliveryTracker()
    context = SerializationContext(input_topic, MessageField.VALUE)

    # `with SchemaRegistryClient(...)` guarantees its connections are
    # released once this block exits, success or failure - the same
    # discipline every Avro-producing demo in this course follows.
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
                # fallback_order_key(): readable UTF-8 decimal digits, NOT the
                # managed connector's own key encoding - see the module
                # docstring and demo06_common.py's note on why the two source
                # modes intentionally differ here.
                key=fallback_order_key(order),
                value=value,
                # A HEADER MARKING THE SOURCE. Anyone inspecting this topic's
                # records later - or debugging why a key "looks different" -
                # can see directly, per message, that it came from this
                # Python fallback rather than the managed connector.
                headers=[("demo06-source", b"fallback-seed")],
                on_delivery=tracker.callback,
            )
            # producer.poll(0): the async pattern from Demo 02B - serves any
            # already-arrived delivery callbacks without blocking, so the
            # loop keeps handing off new records at full speed.
            producer.poll(0)

    # THE MANDATORY FINAL FLUSH, and a strict check on top of it: rather than
    # merely reporting leftovers (as Demo 02A did), this raises immediately if
    # anything is still queued or failed - there is no reason to continue if
    # the seed data itself did not fully land.
    remaining = producer.flush(15.0)
    if remaining:
        raise RuntimeError(f"Producer still had {remaining} queued records")
    if tracker.failed or len(tracker.delivered) != len(orders):
        raise RuntimeError("Not every fallback record received a broker acknowledgement")

    # -------------------------------------------------------------------
    # STEP 3: WRITE THE HONEST, SELF-IDENTIFYING REPORT
    # -------------------------------------------------------------------
    report = {
        "demo": "06-seed-fallback",
        "run_id": run_id,
        # THE HONESTY STATEMENT. This report never pretends to be a Kafka
        # Connect run - a grader or a future you reading this JSON later
        # should immediately know which input path produced this topic's data.
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
