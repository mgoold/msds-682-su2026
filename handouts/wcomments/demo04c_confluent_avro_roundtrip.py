"""
================================================================================
DEMO 04C - REAL CLOUD AVRO ROUND TRIP  (annotated tutorial copy)
================================================================================

READ demo04_common.py, demo04a, AND demo04b FIRST. This is where everything
from Demos 02, 03, and 04 comes together against real infrastructure.

WHAT THIS DEMO TEACHES
    One complete, bounded cycle on real Confluent Cloud:

        validated object -> Avro bytes -> Kafka -> Avro bytes -> validated object

    In one script it exercises FOUR services/roles:
        AdminClient          - make sure the topic exists      (Demo 01 idea)
        Schema Registry      - register/fetch the Avro schema  (Demo 04B idea)
        Producer             - write encoded messages          (Demo 02 idea)
        Consumer             - read them back and commit       (Demo 03 idea)

WHAT IS GENUINELY NEW HERE (beyond "now it uses the network")

    1. MESSAGE HEADERS as a filtering mechanism. The topic is shared with other
       students and other runs, so each message is tagged with a run-id header
       and the consumer skips anything that is not its own. Headers are a third
       part of a Kafka message, alongside key and value.

    2. WAITING FOR REAL ASSIGNMENT. The consumer starts at "latest", so it must
       be confirmed as owning partitions BEFORE the producer writes anything.
       Demo 03D solved this with an asyncio.Event; the blocking client has no
       such signal, so here we drive the poll loop until on_assign has fired.

    3. CAREFUL SHUTDOWN. The finally block closes both clients while making
       sure a cleanup failure never masks the original error - a small piece of
       code with a lot of thought behind it.

    4. REAL REGISTRY METADATA. Unlike mock://, this asks the live Registry for
       the schema version and the subject's COMPATIBILITY setting - the thing
       mock:// cannot prove.

PREREQUISITES
    Both credential sets in .env: Kafka (BOOTSTRAP_SERVERS, SASL_*) AND Schema
    Registry (SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY/SECRET). They are
    different services with different credentials.
================================================================================
"""

from __future__ import annotations

import argparse
import json
import time
import zlib     # CRC32, used to derive a per-run data offset (see below)
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from demo04_common import (
    ConnectionConfigError,
    TripEventV1,
    avro_dict_to_event,
    avro_subject,
    consumer_group_id,
    deserializer_conf,
    deterministic_events,
    event_key,
    event_to_avro_dict,
    kafka_config,
    parse_confluent_wire_header,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    schema_v1_str,
    serializer_conf,
    synthetic_data_report,
    topic_name,
    validate_run_id,
    write_json_report,
)


@dataclass
class AssignmentTracker:
    """Records rebalances, and completes the assignment.

    Same role as Demo 03's version. The important line is `consumer.assign(...)`
    inside on_assign: with the classic protocol and a custom on_assign callback,
    YOU are responsible for completing the assignment. Omit that call and the
    consumer never actually owns any partitions and silently reads nothing.
    """

    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    @staticmethod
    def rows(partitions: Any) -> list[dict[str, int | str]]:
        """Flatten TopicPartition objects into JSON-safe dicts."""
        return [
            {"topic": partition.topic, "partition": partition.partition, "offset": partition.offset}
            for partition in partitions
        ]

    def on_assign(self, consumer: Consumer, partitions: Any) -> None:
        rows = self.rows(partitions)
        self.assigned.append(rows)
        print(f"Assigned: {rows}")

        # COMPLETE THE ASSIGNMENT. Also the signal wait_for_assignment() is
        # watching for: once self.assigned is non-empty, the consumer is live.
        consumer.assign(partitions)

    def on_revoke(self, _consumer: Consumer, partitions: Any) -> None:
        rows = self.rows(partitions)
        self.revoked.append(rows)
        print(f"Revoked: {rows}")


@dataclass
class DeliveryTracker:
    """Producer delivery callbacks - as in Demo 02, plus wire-format evidence."""

    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, message: Any) -> None:
        if error is not None:
            self.failed.append(str(error))
            return

        value = message.value() or b""
        self.delivered.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,

                # NEW vs DEMO 02: parse the 5-byte Confluent header off the
                # bytes that were actually sent. This is direct evidence that
                # the payload really is Avro-framed - magic byte, schema ID, and
                # the size split between framing and body.
                "wire": parse_confluent_wire_header(value),
            }
        )


def ensure_topic(
    admin: AdminClient,
    *,
    topic: str,
    create: bool,
    partitions: int,
    replication_factor: int,
) -> str:
    """Confirm the dedicated topic exists, optionally creating it once.

    THE ADMIN CLIENT AGAIN (Demo 01). Note the safe default: it only CREATES if
    you explicitly pass --create-topic. Otherwise a missing topic is an error
    with an actionable message.

    Why not always create? Because auto-creating a topic with wrong settings -
    say 1 partition when you wanted 3 - is worse than failing, since partition
    count is painful to change later.
    """
    # list_topics() fetches cluster metadata. Checking `.error is None` matters:
    # a topic can appear in metadata with an error attached (for instance if it
    # is mid-deletion), and treating that as "exists" would be wrong.
    metadata = admin.list_topics(timeout=15)
    if topic in metadata.topics and metadata.topics[topic].error is None:
        return "already_exists"

    if not create:
        raise RuntimeError(
            f"Topic {topic!r} does not exist. Re-run with --create-topic or create it in Confluent Cloud first."
        )

    # create_topics() takes a LIST and returns a dict of futures keyed by topic
    # name; [topic] pulls out the one we care about. .result() blocks until the
    # broker confirms, or raises. (Demo 01 covers this in detail.)
    future = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=replication_factor,
                config={"cleanup.policy": "delete"},
            )
        ]
    )[topic]
    future.result(timeout=30)
    return "created"


def wait_for_assignment(
    consumer: Consumer,
    tracker: AssignmentTracker,
    *,
    timeout: float,
) -> float:
    """Drive the poll loop until Kafka confirms partition assignment.

    THE BLOCKING-CLIENT ANSWER TO DEMO 03D's COORDINATION PROBLEM.

    The consumer uses auto.offset.reset="latest", so it will only ever see
    messages produced AFTER it owns partitions. Producing before assignment
    completes means those messages are missed permanently.

    The subtlety: subscribe() does not assign partitions. Assignment happens
    during a group rebalance, and REBALANCE CALLBACKS ONLY FIRE DURING poll().
    So you cannot simply wait - you must actively poll to let the client make
    progress. That is why this loop polls and discards messages: it is pumping
    the client, not reading data.

    Demo 03D used an asyncio.Event for the same purpose. The blocking client has
    no such signal, so "poll until tracker.assigned is non-empty" is the
    equivalent. Both beat a fixed sleep, which is a guess that fails on a slow
    network and wastes time on a fast one.
    """
    started = time.monotonic()
    deadline = started + timeout

    while not tracker.assigned and time.monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue

        error = message.error()
        # A partition EOF event is not a data failure; it can appear when
        # partition EOF reporting is enabled. All other errors should stop
        # the demo before any records are produced.
        if error is not None and error.code() != KafkaError._PARTITION_EOF:
            raise RuntimeError(f"Consumer error while waiting for assignment: {error}")

    # A TIMEOUT HERE IS FATAL, deliberately. Continuing would produce messages
    # the consumer can never see, and the run would fail later with a far more
    # confusing symptom ("delivered 4, consumed 0").
    if not tracker.assigned:
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check topic access, Kafka credentials, and cluster connectivity."
        )

    return round(time.monotonic() - started, 6)


def headers_as_dict(message: Any) -> dict[str, bytes | None]:
    """Turn a message's headers into a dict for easy lookup.

    MESSAGE HEADERS are the third component of a Kafka message, alongside key
    and value: a list of (name, bytes) pairs carrying METADATA about the
    message rather than business data. Typical uses are tracing IDs, source
    system names, content types - and, here, a run marker.

    message.headers() returns a list of tuples, or None if there are none;
    `or []` makes the None case safe.
    """
    return {name: value for name, value in (message.headers() or [])}


def run_cloud_roundtrip(
    args: argparse.Namespace,
    *,
    topic: str,
    topic_status: str,
    registry_conf: dict[str, Any],
    registry: SchemaRegistryClient,
) -> dict[str, Any]:
    """Run the bounded Kafka cycle with an open Registry client."""

    # ---- AVRO SERIALIZER AND DESERIALIZER, AGAINST THE REAL REGISTRY --
    # Same construction as Demo 04B, but `registry` now points at Confluent
    # Cloud rather than mock://. On first use the serializer registers the
    # schema and caches the ID it gets back.
    schema = schema_v1_str()
    serializer = AvroSerializer(
        registry,
        schema,
        to_dict=event_to_avro_dict,
        conf=serializer_conf(),
    )
    deserializer = AvroDeserializer(
        registry,
        schema,
        from_dict=avro_dict_to_event,
        conf=deserializer_conf(),
    )
    context = SerializationContext(topic, MessageField.VALUE)

    # ---- CONSUMER CONFIG ----------------------------------------------
    group_id = consumer_group_id("demo04c-avro", args.run_id)
    consumer_conf: dict[str, Any] = {
        **kafka_config(client_id="msds682-demo04c-avro-consumer"),
        "group.id": group_id,

        # Pin the classic rebalance protocol, because this code supplies its own
        # on_assign callback that calls consumer.assign(). The newer protocol
        # handles assignment server-side and does not use that call.
        "group.protocol": "classic",

        # "latest" is what creates the coordination requirement handled by
        # wait_for_assignment(). It also means this run reads only its OWN
        # messages, not the topic's history - appropriate for a round-trip test.
        "auto.offset.reset": "latest",

        # MANUAL COMMITS (Demo 03B). Both switches off, so the application is
        # the sole authority on what counts as processed.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    producer_conf = kafka_config(client_id="msds682-demo04c-avro-producer")

    tracker = AssignmentTracker()
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    delivery = DeliveryTracker()

    # THE RUN MARKER. The topic is shared - other students and earlier runs have
    # written to it - so every message this run produces is tagged with this
    # value in a header, and the consumer ignores anything else.
    marker = args.run_id.encode("utf-8")

    assignment_wait_seconds = 0.0
    consumed: list[dict[str, Any]] = []
    skipped_other_runs = 0

    # Remembers the original exception so the finally block can avoid masking it.
    primary_error: BaseException | None = None

    try:
        consumer.subscribe(
            [topic],
            on_assign=tracker.on_assign,
            on_revoke=tracker.on_revoke,
        )

        # BLOCK UNTIL THE CONSUMER REALLY OWNS PARTITIONS. Nothing may be
        # produced before this returns.
        assignment_wait_seconds = wait_for_assignment(
            consumer,
            tracker,
            timeout=args.assignment_timeout,
        )

        # ====================================================================
        # STUDENT CHECKPOINT
        # Why must assignment be confirmed before producing when this new group
        # uses auto.offset.reset="latest"? What failure would a fixed sleep risk?
        # ====================================================================
        #
        # (Answer, for this annotated copy: with "latest", a consumer only
        #  receives messages appended AFTER its position is established. Produce
        #  first and those records fall before that point - they are never
        #  delivered, and the run ends "delivered 4, consumed 0". A fixed sleep
        #  merely guesses how long the rebalance takes: too short and it fails
        #  intermittently on a slow network; too long and every run wastes time.
        #  Waiting for the actual assignment event is deterministic.)

        # A PER-RUN DATA OFFSET, derived from the run ID.
        # zlib.crc32 turns the run ID into a stable integer; % 850 bounds it.
        # Same run ID -> same events (reproducible); different run ID ->
        # different trip IDs, so concurrent students do not generate identical
        # records. Deterministic AND distinct.
        seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 850
        events = deterministic_events(args.count, seed_offset=seed_offset)

        for event in events:
            # OBJECT -> AVRO BYTES. Explicit, as in Demo 02D, but now the
            # serializer may consult the Registry and always prepends the
            # 5-byte header.
            value_bytes = serializer(event, context)
            if value_bytes is None:
                raise RuntimeError("Avro serializer unexpectedly returned None")

            producer.produce(
                topic,
                key=event_key(event),      # plain UTF-8 bytes; only the VALUE is Avro
                value=value_bytes,

                # THE HEADER. A list of (name, bytes) pairs. This is what makes
                # the consumer's filtering possible.
                headers=[("demo04-run-id", marker)],

                # Note the argument name is `on_delivery` here rather than
                # `callback`; the library accepts both spellings.
                on_delivery=delivery.callback,
            )
            producer.poll(0)     # async pattern from Demo 02B

        # ---- THE MANDATORY FINAL FLUSH ---------------------------------
        remaining = producer.flush(15.0)

        # STRICTER THAN DEMO 02: rather than merely reporting leftovers, this
        # raises. Nothing further makes sense if the write half did not
        # complete, since the consumer would wait for messages that never landed.
        if remaining:
            raise RuntimeError(f"Producer still had {remaining} queued messages after flush")
        if delivery.failed:
            raise RuntimeError("At least one delivery failed: " + "; ".join(delivery.failed))

        # ---- THE READ HALF ---------------------------------------------
        deadline = time.monotonic() + args.consumer_timeout

        while len(consumed) < args.count and time.monotonic() < deadline:
            message = consumer.poll(args.poll_timeout)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Consumer error: {message.error()}")

            # ---- FILTER BY HEADER --------------------------------------
            # Skip anything not tagged with this run's marker: messages from
            # other students, or from earlier runs. Without this, a shared topic
            # would make the "delivered == consumed" assertion meaningless.
            #
            # Counting the skips (rather than ignoring them silently) is good
            # practice - it shows in the report how busy the topic was.
            if headers_as_dict(message).get("demo04-run-id") != marker:
                skipped_other_runs += 1
                continue

            # AVRO BYTES -> VALIDATED OBJECT. The deserializer reads the schema
            # ID from the header, fetches that schema from the Registry (cached
            # after the first time), decodes the body, then hands the dict to
            # avro_dict_to_event, which re-applies the pydantic business rules.
            event = deserializer(message.value(), context)
            if not isinstance(event, TripEventV1):
                raise TypeError("Expected AvroDeserializer to return TripEventV1")

            consumed.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,

                    # Wire evidence from the READ side, to compare with the
                    # write side's header in delivered_messages.
                    "wire": parse_confluent_wire_header(message.value()),

                    "event": event.report_dict(),
                }
            )

            # ====================================================================
            # KEY CONCEPT
            # Progress moves only after deserialize -> validate -> process.
            # Commit last so a failed record is not acknowledged as completed.
            # ====================================================================
            #
            # THE AT-LEAST-ONCE ORDERING RULE FROM DEMO 03B, now with two extra
            # steps in front of it. If deserialization or validation raises, the
            # commit never happens and the record is redelivered next run.
            #
            # asynchronous=False blocks until the broker confirms the commit -
            # the safest choice, and cheap at this message count.
            consumer.commit(message=message, asynchronous=False)

    except BaseException as exc:
        # Remember the real error so cleanup below cannot obscure it, then
        # re-raise it unchanged.
        primary_error = exc
        raise

    finally:
        # ---- CAREFUL SHUTDOWN ------------------------------------------
        # Both clients must be closed even on failure. The subtlety: if cleanup
        # ITSELF raises while an exception is already propagating, Python would
        # replace the original error with the cleanup error - and you would
        # debug the wrong problem.
        #
        # So each cleanup step is individually guarded, and a cleanup error is
        # only raised when there was NO primary error to preserve.
        cleanup_errors: list[BaseException] = []
        try:
            producer.flush(5.0)
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            consumer.close()      # commits nothing extra; leaves the group promptly
        except BaseException as exc:
            cleanup_errors.append(exc)
        if primary_error is None and cleanup_errors:
            raise cleanup_errors[0]

    # ---- REAL REGISTRY METADATA (what mock:// could not prove) --------
    latest = registry.get_latest_version(avro_subject(topic))

    try:
        # THE SUBJECT'S COMPATIBILITY POLICY - e.g. BACKWARD, FORWARD, FULL,
        # NONE. This is the rule the Registry enforces when someone tries to
        # register a NEW version: BACKWARD (the usual default) means a new
        # schema must still be readable by consumers using the previous one.
        #
        # This is the enforcement Demo 04B's mock could only simulate.
        compatibility = registry.get_compatibility(avro_subject(topic))
    except Exception as exc:  # permission and inherited-config behavior vary by account
        # Deliberately non-fatal. Reading this setting may require permissions a
        # student key lacks, or the subject may inherit a global default with no
        # explicit value. Recording the reason as a string keeps the demo
        # working while staying honest about what could not be checked.
        compatibility = f"unavailable: {type(exc).__name__}: {exc}"

    report = {
        "demo": "demo04c_confluent_avro_roundtrip",
        "topic": topic,
        "topic_status": topic_status,       # "created" or "already_exists"
        "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),

        # REGISTRY EVIDENCE: the subject, the schema's global ID, its version
        # within the subject, and the compatibility policy.
        "subject": avro_subject(topic),
        "schema_id": latest.schema_id,
        "schema_version": latest.version,
        "compatibility": compatibility,

        "group_id": group_id,

        # THE ROUND-TRIP NUMBERS. On success all three agree.
        "requested": args.count,
        "delivered": len(delivery.delivered),
        "consumed": len(consumed),

        # How much other traffic was on the shared topic - context for anyone
        # reading the evidence later.
        "skipped_records_from_other_runs": skipped_other_runs,

        "assignment_wait_seconds": assignment_wait_seconds,
        "partition_assignments": tracker.assigned,
        "partition_revocations": tracker.revoked,

        # THREE secret-free connection summaries: producer, consumer, and the
        # separate Schema Registry service.
        "producer_connection": safe_kafka_config_report(producer_conf),
        "consumer_connection": safe_kafka_config_report(consumer_conf),
        "schema_registry": safe_registry_config_report(registry_conf),

        "delivered_messages": delivery.delivered,
        "consumed_records": consumed,

        # The processing contract, stated in the evidence itself.
        "commit_rule": "deserialize Avro -> validate TripEventV1 -> application record -> synchronous commit",
    }

    output_file = write_json_report(args.run_id, "demo04c_confluent_avro_roundtrip", report)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")

    # THE ROUND-TRIP ASSERTION: everything written must also have been read
    # back. This is what makes the demo a test rather than a demonstration.
    if len(delivery.delivered) != args.count or len(consumed) != args.count:
        raise SystemExit("Demo 04C did not deliver and consume the requested count.")

    return report


def main() -> dict[str, Any]:
    """Run one bounded real-Cloud Avro write/read cycle."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04c")
    parser.add_argument("--count", type=int, default=4)

    # Opt-in topic creation, so the script cannot silently create a topic with
    # settings you did not intend.
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)

    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--consumer-timeout", type=float, default=20.0)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    args = parser.parse_args()

    # Validate everything BEFORE touching the network, so misuse fails in
    # milliseconds rather than after a connection attempt.
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("--partitions and --replication-factor must be positive")
    if min(args.assignment_timeout, args.consumer_timeout, args.poll_timeout) <= 0:
        parser.error("all timeout values must be positive")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    topic = topic_name()

    # ---- LOAD BOTH CREDENTIAL SETS ------------------------------------
    # Kafka AND Schema Registry, each raising ConnectionConfigError if
    # incomplete. Catching it and converting to SystemExit gives a clean
    # one-line message instead of a traceback - this is a setup problem, not a
    # bug, and the user needs to know which variables to fill in.
    try:
        kafka = kafka_config(client_id="msds682-demo04c-admin")
        registry_conf = schema_registry_config()
    except ConnectionConfigError as exc:
        raise SystemExit(str(exc)) from exc

    # Admin work first: guarantee the topic exists before anyone tries to use it.
    admin = AdminClient(kafka)
    topic_status = ensure_topic(
        admin,
        topic=topic,
        create=args.create_topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )

    # `with` guarantees the Registry client's connections are released even if
    # the round trip raises.
    with SchemaRegistryClient(registry_conf) as registry:
        return run_cloud_roundtrip(
            args,
            topic=topic,
            topic_status=topic_status,
            registry_conf=registry_conf,
            registry=registry,
        )


if __name__ == "__main__":
    main()
