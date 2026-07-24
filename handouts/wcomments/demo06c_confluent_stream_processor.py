"""
================================================================================
DEMO 06C - THE BOUNDED STREAM PROCESSOR  (annotated tutorial copy)
================================================================================

READ demo06_common.py AND demo06b_confluent_source_consumer.py FIRST. This is
where Demo 06's real lesson lives: a Python STREAM PROCESSOR that turns input
records into a new, derived fact - and commits its own progress only after
that derived fact is safely durable.

WHAT THIS DEMO TEACHES
    The exact processing sequence stated in demo06.md, and enforced literally
    by the order of statements inside process_one_message() below:

        poll input
          -> Avro deserialize
          -> Pydantic validate
          -> derive OrderMetricV1
          -> Avro serialize
          -> produce to derived topic
          -> wait for output broker acknowledgement
          -> commit the input offset synchronously

    Notice "commit" is the LAST step, and it is a Kafka CONSUMER OFFSET
    COMMIT on the INPUT topic - never a producer acknowledgement (that
    already happened one step earlier) and never a Git commit.

WHY THE OUTPUT KEY IS THE INPUT'S OWN COORDINATE
    Every derived record's key is "<input-topic>:<partition>:<offset>" (see
    derive_order_metric() in demo06_common.py). This makes REPLAYED output
    identifiable: if the same input record is ever processed twice, both
    derived records get the IDENTICAL key, so a downstream system doing
    key-based deduplication or compaction can recognize the duplicate instead
    of treating it as two unrelated facts.

THIS BASELINE IS AT-LEAST-ONCE - READ THIS CAREFULLY
    If the process crashes AFTER the output producer.flush() succeeds but
    BEFORE consumer.commit() completes, the input offset was never recorded,
    so this same input record will be reprocessed on restart - producing a
    SECOND derived record with the SAME stable key. This baseline does not
    prevent that; it only makes the duplicate observable. A production system
    wanting a true exactly-once guarantee would need Kafka transactions or
    idempotent downstream writes; this classroom baseline demonstrates why
    that extra machinery exists by showing you exactly where the gap is.

WHY THIS SCRIPT FLUSHES BEFORE EVERY SINGLE COMMIT
    Production stream processors normally BATCH several output
    acknowledgements before committing, or use Kafka transactions, because
    flushing per-record is comparatively slow (recall Demo 02A's ~200x
    slowdown from flushing per message). This classroom baseline pays that
    cost DELIBERATELY - one flush, one commit, one record at a time - purely
    so the acknowledgement-before-commit boundary stays visible to a student
    reading the code, rather than being hidden inside a batching optimization.
================================================================================
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

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
    AssignmentTracker,
    DatagenOrderV1,
    derive_order_metric,
    input_topic_name,
    metric_key,
    metric_to_avro_dict,
    order_metric_schema_str,
    output_topic_name,
    serializer_conf,
    wait_for_assignment,
)


@dataclass
class DeliveryTracker:
    """Capture one output acknowledgement before the input commit.

    A fresh instance is created PER MESSAGE inside process_one_message()
    below (unlike the producer demos, which use one long-lived tracker for
    an entire run) - because this script needs to know, for THIS ONE
    message specifically, whether ITS derived output was acknowledged before
    deciding whether ITS input offset may be committed.
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


def process_one_message(
    *,
    message: Any,
    consumer: Any,
    producer: Any,
    input_deserializer: Any,
    output_serializer: Any,
    input_context: SerializationContext,
    output_context: SerializationContext,
    output_topic: str,
    delivery_timeout: float,
) -> dict[str, Any]:
    """Process one input and commit only after its output is acknowledged.

    THIS FUNCTION'S STATEMENT ORDER *IS* THE LESSON. Read it top to bottom:
    deserialize, validate, derive, serialize, produce, flush (wait for
    acknowledgement), and only then commit. Reordering ANY of these - say,
    committing before the flush - would silently break the at-least-once
    guarantee this baseline relies on.
    """

    # STAGE 1: AVRO BYTES -> PLAIN DICT. Like Demo 06B, this deserializer has
    # no from_dict wired in, so the dict is validated explicitly below rather
    # than inside the deserializer call itself.
    raw = input_deserializer(message.value(), input_context)
    try:
        # STAGE 2: PYDANTIC VALIDATION - the application-meaning check Avro
        # itself cannot perform (Avro already confirmed the SHAPE by decoding
        # successfully; this confirms the VALUES make sense as a
        # DatagenOrderV1).
        order = DatagenOrderV1.model_validate(raw)
    except ValidationError as exc:
        # A validation failure is FATAL for this bounded classroom run - it
        # stops rather than skip the bad record, and reports exactly WHERE
        # (topic:partition:offset) it happened, so a student can go look at
        # that specific record in Confluent Cloud.
        coordinate = (
            f"{message.topic()}:{message.partition()}:{message.offset()}"
        )
        raise RuntimeError(
            f"Input validation failed at source coordinate {coordinate}"
        ) from exc

    # STAGE 3: DERIVE. The new fact this processor exists to create - see
    # derive_order_metric()'s docstring in demo06_common.py for why its
    # source_record_id is built from THIS message's own coordinates.
    metric = derive_order_metric(
        order,
        source_topic=message.topic(),
        source_partition=message.partition(),
        source_offset=message.offset(),
    )

    # STAGE 4: SERIALIZE THE DERIVED EVENT to Avro, against the SECOND schema
    # this demo owns (order_metric_schema_str(), not the input schema).
    output_value = output_serializer(metric, output_context)
    if output_value is None:
        raise RuntimeError("AvroSerializer unexpectedly returned None")

    # STAGE 5: PRODUCE TO THE DERIVED TOPIC. Headers record the exact input
    # coordinate this output was derived from - human-inspectable evidence,
    # separate from (but consistent with) the key itself.
    tracker = DeliveryTracker()
    producer.produce(
        output_topic,
        key=metric_key(metric),
        value=output_value,
        headers=[
            ("source-topic", message.topic().encode("utf-8")),
            ("source-partition", str(message.partition()).encode("utf-8")),
            ("source-offset", str(message.offset()).encode("utf-8")),
        ],
        on_delivery=tracker.callback,
    )
    producer.poll(0)

    # STAGE 6: FLUSH AND WAIT. This is the deliberately expensive
    # one-flush-per-record choice described in the module docstring - it
    # blocks until this SPECIFIC message's delivery callback has run.
    remaining = producer.flush(delivery_timeout)
    if remaining or tracker.failed or len(tracker.delivered) != 1:
        # If the output was NOT acknowledged, the function raises BEFORE
        # reaching the commit call below - this is the entire mechanism that
        # keeps the input offset uncommitted when its derived output did not
        # land.
        raise RuntimeError(
            "Derived output was not acknowledged; input offset was not committed"
        )

    # ========================================================================
    # KEY CONCEPT
    # The commit below is a Kafka consumer offset commit. It is not a producer
    # acknowledgement and it is unrelated to a Git commit. Output delivery is
    # confirmed first; only then may this consumer record input progress.
    # ========================================================================
    # STAGE 7: COMMIT THE INPUT OFFSET. asynchronous=False blocks until the
    # broker confirms the commit - the same "wait for the real answer" choice
    # every consumer in this course makes, cheap at this message volume.
    #
    # WHY THIS VERIFIES THE RETURN VALUE RATHER THAN TRUSTING SILENCE.
    # consumer.commit(asynchronous=False) does not raise just because ONE
    # partition in a multi-partition commit failed - it returns a list of
    # TopicPartition objects, EACH carrying its own .error. A commit() call
    # that merely "did not raise" is therefore not proof this message's
    # offset was actually recorded; the list has to be inspected.
    committed = consumer.commit(message=message, asynchronous=False)
    if committed is None:
        # Only possible if this were called asynchronously (it is not, here)
        # or against a mocked/misconfigured client - a defensive check that
        # documents the assumption the code below relies on.
        raise RuntimeError("Synchronous input commit returned no result")

    commit_failures = [
        partition
        for partition in committed
        if getattr(partition, "error", None) is not None
    ]
    if commit_failures:
        # Surface the FIRST partition-level error as a real KafkaException,
        # rather than silently returning as if the commit had succeeded.
        raise KafkaException(commit_failures[0].error)

    # THE OFFSET-VALUE CHECK. A Kafka committed offset is always "the offset
    # of the NEXT record to read", one past the message just processed - so
    # a genuinely successful commit of THIS message must show
    # message.offset() + 1 for THIS message's exact (topic, partition). This
    # confirms not merely "some commit succeeded somewhere" but "the specific
    # offset this function just processed was the one actually recorded".
    expected_offset = message.offset() + 1
    if not any(
        partition.topic == message.topic()
        and partition.partition == message.partition()
        and partition.offset == expected_offset
        for partition in committed
    ):
        raise RuntimeError(
            "Synchronous input commit did not confirm the expected next offset"
        )

    # Flatten the verified TopicPartition results into JSON-safe evidence -
    # proof, in the report itself, of exactly what was committed.
    commit_result = [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": partition.offset,
        }
        for partition in committed
    ]
    return {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "source_record_id": metric.source_record_id,
        "orderid": order.orderid,
        "itemid": order.itemid,
        "orderunits": order.orderunits,
        "size_band": metric.size_band,
        "output": tracker.delivered[0],
        "input_commit": "sync_after_output_ack",
        "input_commit_result": commit_result,
    }


def run_processor(
    *,
    run_id: str,
    group_id: str,
    max_messages: int,
    assignment_timeout: float,
    idle_timeout: float,
    delivery_timeout: float,
    create_topics: bool,
    partitions: int,
    replication_factor: int,
    report_demo_name: str | None,
    force_beginning: bool = False,
) -> dict[str, Any]:
    """Run one bounded at-least-once processor pass.

    FACTORED OUT OF main() so Demo 06D (resume/replay) can call this SAME
    function three times with different (group_id, force_beginning)
    combinations, rather than duplicating the whole processing loop. Notice
    the two parameters that make that reuse possible:

        group_id          - which consumer group's progress this pass uses
        force_beginning    - whether to override every assigned partition to
                              OFFSET_BEGINNING (an explicit replay) rather
                              than resuming from committed offsets

    report_demo_name is Optional specifically so Demo 06D can suppress the
    per-pass report file (passing None) and instead write ONE combined report
    covering all three passes plus its own resume/replay validation.
    """

    validate_run_id(run_id)
    if not 1 <= max_messages <= 100:
        raise ValueError("max_messages must be between 1 and 100")
    if min(assignment_timeout, idle_timeout, delivery_timeout) <= 0:
        raise ValueError("timeouts must be positive")
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")

    input_topic = input_topic_name()
    output_topic = output_topic_name()
    base_kafka_conf = kafka_config(client_id="msds682-demo06c")
    registry_conf = schema_registry_config()
    admin = AdminClient(base_kafka_conf)
    topic_status = {
        "input": ensure_topic(
            admin,
            topic=input_topic,
            create=create_topics,
            partitions=partitions,
            replication_factor=replication_factor,
            create_option="--create-topics",
        ),
        "output": ensure_topic(
            admin,
            topic=output_topic,
            create=create_topics,
            partitions=partitions,
            replication_factor=replication_factor,
            create_option="--create-topics",
        ),
    }

    consumer_conf: dict[str, Any] = {
        **base_kafka_conf,
        "client.id": "msds682-demo06c-consumer",
        "group.id": group_id,
        # Pin the classic protocol because this bounded teaching callback uses
        # the full assignment with consumer.assign(). KIP-848 callbacks are
        # incremental and require incremental_assign().
        "group.protocol": "classic",
        # "earliest" is only ever a FALLBACK: it applies solely when `group_id`
        # has NO committed offset yet. A resumed pass with prior commits
        # ignores this entirely and starts from its committed position - the
        # forced-replay case below overrides the starting offset explicitly
        # instead of relying on this setting at all.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf: dict[str, Any] = {
        **base_kafka_conf,
        "client.id": "msds682-demo06c-derived-producer",
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    # force_beginning flows straight into AssignmentTracker, which is what
    # actually rewrites each assigned partition's offset when this pass is a
    # forced replay - see AssignmentTracker.on_assign in demo06_common.py.
    assignment = AssignmentTracker(force_beginning=force_beginning)
    processed: list[dict[str, Any]] = []
    started = time.monotonic()

    try:
        consumer.subscribe(
            [input_topic],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        assignment_wait, pending_messages = wait_for_assignment(
            consumer,
            assignment,
            timeout=assignment_timeout,
        )
        input_context = SerializationContext(input_topic, MessageField.VALUE)
        output_context = SerializationContext(output_topic, MessageField.VALUE)

        with SchemaRegistryClient(registry_conf) as registry:
            input_deserializer = AvroDeserializer(registry)
            output_serializer = AvroSerializer(
                registry,
                order_metric_schema_str(),
                to_dict=metric_to_avro_dict,
                conf=serializer_conf(),
            )
            idle_deadline = time.monotonic() + idle_timeout
            while len(processed) < max_messages and time.monotonic() < idle_deadline:
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
                # THE ENTIRE consume -> validate -> derive -> produce ->
                # output-ack -> commit SEQUENCE happens inside this one call.
                processed.append(
                    process_one_message(
                        message=message,
                        consumer=consumer,
                        producer=producer,
                        input_deserializer=input_deserializer,
                        output_serializer=output_serializer,
                        input_context=input_context,
                        output_context=output_context,
                        output_topic=output_topic,
                        delivery_timeout=delivery_timeout,
                    )
                )
                idle_deadline = time.monotonic() + idle_timeout
    finally:
        consumer.close()

    if len(processed) != max_messages:
        raise RuntimeError(
            f"Expected {max_messages} input records but processed {len(processed)}. "
            "Run the managed connector or fallback seed first."
        )

    report = {
        "demo": report_demo_name or "06C-internal-pass",
        "run_id": run_id,
        "group_id": group_id,
        "force_beginning": force_beginning,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "topic_status": topic_status,
        "assignment_wait_seconds": assignment_wait,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "processed": len(processed),
        "records": processed,
        # STATING THE PROCESSING CONTRACT DIRECTLY IN THE EVIDENCE, exactly
        # as Demo 04C's report embedded its own "commit_rule" string - a
        # reader of the JSON alone, without the source code, can still see
        # what order of operations this run guarantees.
        "commit_order": [
            "deserialize Avro",
            "validate with Pydantic",
            "derive output",
            "produce output",
            "wait for output acknowledgement",
            "commit input offset",
        ],
        "delivery_semantics": {
            "baseline": "at_least_once",
            "duplicate_window": (
                "A crash after output acknowledgement but before input commit "
                "can produce the same derived record again."
            ),
            "mitigation": (
                "The derived Kafka key is the stable input topic-partition-offset."
            ),
        },
        "assignments": assignment.assigned,
        "kafka": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),
    }
    if report_demo_name is not None:
        # Demo 06C writes its own report file directly; Demo 06D passes None
        # for all three internal passes and instead folds their results into
        # one combined report of its own.
        report_path = write_json_report(run_id, report_demo_name.lower(), report)
        report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--group-id")
    parser.add_argument("--max-messages", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    # A DETERMINISTIC DEFAULT GROUP ID, derived from --run-id, unless the
    # caller overrides it with --group-id. Demo 06D needs to override this
    # explicitly (passing the SAME group_id to two of its three passes) to
    # prove same-group resume - that override is exactly what --group-id
    # exists to support.
    group_id = args.group_id or f"msds682-su2026-demo06c-{run_id}"
    report = run_processor(
        run_id=run_id,
        group_id=group_id,
        max_messages=args.max_messages,
        assignment_timeout=args.assignment_timeout,
        idle_timeout=args.idle_timeout,
        delivery_timeout=args.delivery_timeout,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        report_demo_name="demo06c",
        force_beginning=False,
    )
    print(
        f"Processed {report['processed']} input records and committed only "
        "after output acknowledgement"
    )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
