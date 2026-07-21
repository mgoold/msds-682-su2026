"""
================================================================================
DEMO 04D - ASYNCIO + AVRO ON REAL CLOUD  (annotated tutorial copy)
================================================================================

READ demo03d (asyncio) AND demo04c (cloud Avro) FIRST. This demo is their
combination and the most advanced script in the course.

WHAT THIS DEMO TEACHES
    Demo 04C with everything moved onto one asyncio event loop:

        AsyncSchemaRegistryClient   instead of SchemaRegistryClient
        AsyncAvroSerializer         instead of AvroSerializer
        AIOProducer / AIOConsumer   instead of Producer / Consumer

    Every network operation is awaited, so Kafka work can share a loop with a
    web framework, a database driver, or anything else async.

WHEN YOU SHOULD ACTUALLY USE THIS
    The STUDENT CHECKPOINT near the bottom asks exactly the right question:
    what OTHER non-blocking I/O needs this event loop? If the answer is
    "nothing", the synchronous Demo 04C is the simpler and better design.
    asyncio pays off when Kafka is one of several concurrent I/O sources - a
    FastAPI service, for instance. It is not faster for its own sake, and it
    adds real complexity, as the length of this file shows.

TWO THINGS DONE DIFFERENTLY FROM DEMO 04C, BOTH INSTRUCTIVE

    1. FILTERING BY KEY INSTEAD OF BY HEADER.
       Demo 04C tagged each message with a run-id HEADER. The AIOProducer in
       this library version does not support headers in batch mode, so this demo
       precomputes the exact set of keys it will produce and accepts only those.
       A nice illustration that a design constraint can be worked around by
       using a different part of the message - and that keys, being
       deterministic here, are already unique per run.

    2. THE RECEIVE BUDGET STARTS AFTER ASSIGNMENT.
       `deadline` begins as None and is only set once assignment completes. On a
       cold cloud connection, joining a group can take several seconds; if the
       clock started at subscribe(), that join time would silently eat the
       budget meant for receiving data. Measure the thing you actually mean.
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import json
import zlib
from typing import Any

# THE ASYNC VARIANTS OF EVERY CLIENT.
from confluent_kafka.aio import AIOConsumer, AIOProducer
from confluent_kafka.admin import AdminClient      # still synchronous - used once, before the loop
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AsyncAvroDeserializer,
    AsyncAvroSerializer,
)
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


def topic_partition_rows(partitions: Any) -> list[dict[str, int | str]]:
    """Flatten TopicPartition objects for JSON output (as in Demos 03 and 04C)."""
    return [
        {"topic": partition.topic, "partition": partition.partition, "offset": partition.offset}
        for partition in partitions
    ]


async def produce_events(
    producer_config: dict[str, Any],
    serializer: AsyncAvroSerializer,
    context: SerializationContext,
    *,
    topic: str,
    events: list[TripEventV1],
    assignment_ready: asyncio.Event,
    assignment_timeout: float,
    delivery_timeout: float,
    interval: float,
) -> tuple[list[dict[str, Any]], float]:
    """Wait for assignment, serialize asynchronously, then produce finite data."""

    # ---- THE COORDINATION GATE (as in Demo 03D) -----------------------
    # Park here until the consumer signals that it owns partitions. Because the
    # consumer starts at "latest", producing earlier would mean those messages
    # are never delivered to it.
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        await asyncio.wait_for(assignment_ready.wait(), timeout=assignment_timeout)
    except TimeoutError as exc:
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check topic access, Kafka credentials, and cluster connectivity."
        ) from exc
    assignment_wait_seconds = round(loop.time() - started, 6)

    producer = AIOProducer(producer_config)
    delivery_futures: list[Any] = []
    primary_error: BaseException | None = None

    try:
        for event in events:
            # AWAITED SERIALIZATION. This is new: the async serializer may need
            # to contact Schema Registry (to register or fetch a schema), which
            # is network I/O, so it is a coroutine. In Demo 04C the equivalent
            # call was blocking.
            value_bytes = await serializer(event, context)
            if value_bytes is None:
                raise RuntimeError("AsyncAvroSerializer unexpectedly returned None")

            # confluent-kafka 2.15 AIOProducer batch mode does not support
            # headers. The precomputed stable keys identify this bounded run.
            #
            # THE CONSTRAINT THAT SHAPES THIS DEMO. Demo 04C used a run-id
            # header to identify its own messages; that is unavailable here, so
            # filtering moves to the KEY instead (see expected_keys in
            # run_demo). The deterministic generator makes those keys both
            # predictable and unique per run, so they work as a run marker.
            delivery_future = await producer.produce(
                topic,
                key=event_key(event),
                value=value_bytes,
            )
            delivery_futures.append(delivery_future)

            # Optional pacing so the interleaving is visible. asyncio.sleep
            # yields to the loop, letting the consumer coroutine run - unlike
            # time.sleep(), which would block the loop and stall both tasks.
            if interval:
                await asyncio.sleep(interval)

        # ---- FLUSH, WITH A TIMEOUT ON THE TIMEOUT ---------------------
        # Belt and braces: flush() already takes a timeout, and wait_for adds an
        # outer bound one second longer. If flush ever failed to honor its own
        # timeout, the task would still not hang forever.
        remaining = await asyncio.wait_for(
            producer.flush(delivery_timeout),
            timeout=delivery_timeout + 1.0,
        )
        if remaining:
            raise RuntimeError(
                f"AIOProducer still had {remaining} queued messages after flush"
            )

        # Collect every delivery result at once. Each future resolves to a
        # Message that now knows its final partition and offset.
        messages = await asyncio.wait_for(
            asyncio.gather(*delivery_futures),
            timeout=delivery_timeout,
        )

        delivered = [
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
                "wire": parse_confluent_wire_header(message.value() or b""),
            }
            for message in messages
        ]
        return delivered, assignment_wait_seconds

    except BaseException as exc:
        primary_error = exc
        raise

    finally:
        # ---- BOUNDED CLEANUP THAT NEVER MASKS THE REAL ERROR ----------
        # Same discipline as Demo 04C: close the client, but if closing fails
        # while an exception is already in flight, keep the ORIGINAL error -
        # it is the one that explains what went wrong.
        try:
            await asyncio.wait_for(producer.close(), timeout=delivery_timeout)
        except TimeoutError as exc:
            if primary_error is None:
                raise RuntimeError(
                    "AIOProducer did not close before --delivery-timeout"
                ) from exc
        except BaseException:
            if primary_error is None:
                raise


async def consume_events(
    consumer_config: dict[str, Any],
    deserializer: AsyncAvroDeserializer,
    context: SerializationContext,
    *,
    topic: str,
    expected_keys: frozenset[bytes],
    assignment_ready: asyncio.Event,
    timeout: float,
    cleanup_timeout: float,
) -> tuple[
    list[dict[str, Any]],
    list[list[dict[str, int | str]]],
    list[list[dict[str, int | str]]],
    int,
]:
    """Consume only this run's records and commit after successful validation."""

    consumer = AIOConsumer(consumer_config)
    records: list[dict[str, Any]] = []

    # Tracks keys already accepted, so a redelivered message (at-least-once
    # delivery means duplicates happen) is not counted twice. This is
    # deduplication in practice - the idempotence idea made concrete.
    consumed_keys: set[bytes] = set()

    assignments: list[list[dict[str, int | str]]] = []
    revocations: list[list[dict[str, int | str]]] = []
    skipped = 0
    loop = asyncio.get_running_loop()

    # ========================================================================
    # KEY CONCEPT
    # The receive budget starts only after a real assignment. Group join time
    # must not silently consume the data budget on a cold Cloud connection.
    # ========================================================================
    #
    # `deadline` stays None until assignment completes, then becomes
    # "now + timeout". Joining a consumer group on a cold cloud connection can
    # take several seconds; starting the clock at subscribe() would let that
    # join time eat the budget meant for receiving data, producing flaky runs
    # that look like data problems. Measure the interval you actually mean.
    deadline: float | None = None

    async def on_assign(aio_consumer: Any, partitions: Any) -> None:
        # Complete the assignment (required with the classic protocol and a
        # custom callback), then release the producer.
        await aio_consumer.assign(partitions)
        rows = topic_partition_rows(partitions)
        assignments.append(rows)
        print(f"Async assigned: {rows}")

        # THE SIGNAL. Producer resumes at this instant - not before, not a
        # guessed interval later.
        assignment_ready.set()

    async def on_revoke(_aio_consumer: Any, partitions: Any) -> None:
        rows = topic_partition_rows(partitions)
        revocations.append(rows)
        print(f"Async revoked: {rows}")

    primary_error: BaseException | None = None
    cleanup_failure: tuple[str, BaseException] | None = None

    try:
        await consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

        # Loop until we have all expected records, or the (post-assignment)
        # deadline passes. `deadline is None` keeps the loop alive during the
        # join phase, when no budget is running yet.
        while len(records) < len(expected_keys) and (
            deadline is None or loop.time() < deadline
        ):
            remaining = timeout if deadline is None else max(deadline - loop.time(), 0.0)

            # Cap each poll at one second so the loop stays responsive, while
            # never overshooting the overall deadline.
            message = await consumer.poll(timeout=min(1.0, remaining))

            # START THE BUDGET the first time we notice assignment completed.
            if assignment_ready.is_set() and deadline is None:
                deadline = loop.time() + timeout

            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Consumer error: {message.error()}")

            message_key = message.key()

            # Filter before deserialization: only keys generated for this run
            # are allowed to become evidence or trigger an explicit commit.
            #
            # TWO CHECKS IN ONE CONDITION:
            #   not in expected_keys -> someone else's message; skip it
            #   in consumed_keys     -> a DUPLICATE of one we already handled
            #
            # Filtering BEFORE deserializing is deliberate: decoding another
            # run's message would waste a Registry lookup and could raise if it
            # used a different schema. Cheap check first.
            if message_key not in expected_keys or message_key in consumed_keys:
                skipped += 1
                continue

            # AWAITED DESERIALIZATION - may consult the Registry, hence async.
            event = await deserializer(message.value(), context)
            if not isinstance(event, TripEventV1):
                raise TypeError("Expected AsyncAvroDeserializer to return TripEventV1")

            # A CONSISTENCY CHECK worth noticing: the trip_id inside the decoded
            # PAYLOAD must match the KEY the message was sent with. They are set
            # independently at produce time, so a mismatch would mean a
            # serialization or routing bug - and would silently break the
            # per-trip ordering guarantee that keying exists to provide.
            if event_key(event) != message_key:
                raise ValueError("Deserialized trip_id does not match the Kafka key")

            consumed_keys.add(message_key)
            records.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "wire": parse_confluent_wire_header(message.value()),
                    "event": event.report_dict(),
                }
            )

            # COMMIT LAST - the at-least-once ordering rule, now awaited.
            await consumer.commit(message=message, asynchronous=False)

    except BaseException as exc:
        primary_error = exc
        raise

    finally:
        # Attempt both cleanup operations, bound each wait, and never let a
        # cleanup failure hide the original deserialize/validate/commit error.
        #
        # Looping over (label, operation) pairs keeps this compact while still
        # naming which step failed. Note the operations are passed UNCALLED -
        # `consumer.unsubscribe`, not `consumer.unsubscribe()` - so each is
        # invoked inside its own wait_for.
        for label, operation in (
            ("unsubscribe", consumer.unsubscribe),
            ("close", consumer.close),
        ):
            try:
                await asyncio.wait_for(operation(), timeout=cleanup_timeout)
            except BaseException as exc:
                # Record only the FIRST cleanup failure, and only when there is
                # no primary error to protect.
                if primary_error is None and cleanup_failure is None:
                    cleanup_failure = (label, exc)

        if primary_error is None and cleanup_failure is not None:
            label, exc = cleanup_failure
            raise RuntimeError(
                f"AIOConsumer {label} failed during bounded cleanup"
            ) from exc

    return records, assignments, revocations, skipped


async def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    """Create async serdes, coordinate two finite Kafka tasks, and report."""

    topic = topic_name()
    registry_conf = schema_registry_config()

    # Same per-run offset trick as Demo 04C: stable for a given run ID, distinct
    # across run IDs.
    seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 850
    events = deterministic_events(args.count, seed_offset=seed_offset)

    # ---- PRECOMPUTE THE RUN'S KEYS ------------------------------------
    # Because the generator is deterministic, we know EXACTLY which keys this
    # run will produce before producing anything. That set becomes the consumer's
    # filter - the substitute for Demo 04C's header marker.
    #
    # frozenset gives fast membership tests and cannot be modified by accident.
    expected_keys = frozenset(event_key(event) for event in events)

    # THE FILTER ONLY WORKS IF THE KEYS ARE UNIQUE. If two events shared a
    # trip_id, the set would be smaller than the event count and the consumer
    # could never reach `len(expected_keys)` records. Checking up front turns a
    # confusing hang into an immediate, explicit error.
    if len(expected_keys) != args.count:
        raise RuntimeError("Deterministic Demo 04D event keys must be unique")

    # `async with` on the Registry client: an asynchronous context manager, so
    # connections are released even if the body raises.
    async with AsyncSchemaRegistryClient(registry_conf) as registry:
        schema = schema_v1_str()

        # NOTE THE `await` ON CONSTRUCTION. These serdes may need to contact the
        # Registry while being built, so creating them is itself asynchronous -
        # unusual, and easy to forget.
        serializer = await AsyncAvroSerializer(
            registry,
            schema,
            to_dict=event_to_avro_dict,
            conf=serializer_conf(),
        )
        deserializer = await AsyncAvroDeserializer(
            registry,
            schema,
            from_dict=avro_dict_to_event,
            conf=deserializer_conf(),
        )
        context = SerializationContext(topic, MessageField.VALUE)

        producer_config = kafka_config(client_id="msds682-demo04d-aio-avro-producer")

        # Bound how long librdkafka will keep retrying a message internally.
        # Without it, the client's own retry budget could outlast our timeouts
        # and the task would appear to hang.
        producer_config["delivery.timeout.ms"] = int(args.delivery_timeout * 1000)

        group_id = args.group_id or consumer_group_id("demo04d-aio-avro", args.run_id)
        consumer_config: dict[str, Any] = {
            **kafka_config(client_id="msds682-demo04d-aio-avro-consumer"),
            "group.id": group_id,
            "group.protocol": "classic",   # required for the custom on_assign
            "auto.offset.reset": "latest", # hence the coordination gate
            "enable.auto.commit": False,   # manual commits
            "enable.auto.offset.store": False,
        }

        assignment_ready = asyncio.Event()

        # ====================================================================
        # KEY CONCEPT
        # Both tasks share one event loop. The producer waits for the consumer's
        # real assignment event; this demo never guesses readiness with sleep.
        # ====================================================================
        producer_task = asyncio.create_task(
            produce_events(
                producer_config,
                serializer,
                context,
                topic=topic,
                events=events,
                assignment_ready=assignment_ready,
                assignment_timeout=args.assignment_timeout,
                delivery_timeout=args.delivery_timeout,
                interval=args.interval,
            )
        )
        consumer_task = asyncio.create_task(
            consume_events(
                consumer_config,
                deserializer,
                context,
                topic=topic,
                expected_keys=expected_keys,
                assignment_ready=assignment_ready,
                timeout=args.consumer_timeout,
                cleanup_timeout=args.delivery_timeout,
            )
        )

        try:
            producer_result, consumer_result = await asyncio.gather(
                producer_task,
                consumer_task,
            )
        except BaseException:
            # Identical shutdown discipline to Demo 03D: cancel the sibling and
            # AWAIT it, so its finally block runs and closes its client, then
            # re-raise the original error.
            for task in (producer_task, consumer_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
            raise

        delivered, assignment_wait_seconds = producer_result
        consumed, assignments, revocations, skipped = consumer_result

        # Registry metadata - awaited here, unlike Demo 04C's blocking calls.
        latest = await registry.get_latest_version(avro_subject(topic))
        try:
            compatibility = await registry.get_compatibility(avro_subject(topic))
        except Exception as exc:
            # Non-fatal, as in 04C: permissions vary by account.
            compatibility = f"unavailable: {type(exc).__name__}: {exc}"

        report = {
            "demo": "demo04d_asyncio_avro_roundtrip",
            "topic": topic,
            "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),
            "subject": avro_subject(topic),
            "schema_id": latest.schema_id,
            "schema_version": latest.version,
            "compatibility": compatibility,
            "group_id": group_id,
            "group_protocol": consumer_config["group.protocol"],
            "requested": args.count,
            "delivered": len(delivered),
            "consumed": len(consumed),

            # Documents the filtering STRATEGY, which differs from 04C's header
            # approach - useful context when comparing the two reports.
            "run_filter": "precomputed deterministic Kafka keys",
            "expected_keys": sorted(key.decode("utf-8") for key in expected_keys),

            "skipped_records_from_other_runs": skipped,
            "assignment_wait_seconds": assignment_wait_seconds,
            "partition_assignments": assignments,
            "partition_revocations": revocations,
            "producer_connection": safe_kafka_config_report(producer_config),
            "consumer_connection": safe_kafka_config_report(consumer_config),
            "schema_registry": safe_registry_config_report(registry_conf),
            "delivered_messages": delivered,
            "consumed_records": consumed,
            "commit_rule": "await deserialize -> validate TripEventV1 -> await synchronous commit",
        }
    return report


def main() -> dict[str, Any]:
    """Validate prerequisites, execute the async demo, and write evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04d")
    parser.add_argument("--group-id")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--consumer-timeout", type=float, default=20.0)
    parser.add_argument("--interval", type=float, default=0.1)
    args = parser.parse_args()

    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if min(args.assignment_timeout, args.delivery_timeout, args.consumer_timeout) <= 0:
        parser.error("timeout values must be positive")
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    # Fail before starting the event loop if the dedicated topic is absent.
    #
    # A DELIBERATE ORDERING CHOICE. This check uses the ordinary SYNCHRONOUS
    # AdminClient and runs before asyncio.run(). Discovering a missing topic
    # inside the event loop would surface as a confusing timeout in one of the
    # tasks; catching it here gives a one-line message naming the fix.
    #
    # Note this demo does NOT create the topic - it tells you to run 04C with
    # --create-topic instead. Topic creation is a deliberate act with lasting
    # consequences (partition count), so it lives in exactly one place.
    topic = topic_name()
    try:
        topic_check_config = kafka_config(client_id="msds682-demo04d-topic-check")
        schema_registry_config()      # called only to validate that it is present
    except ConnectionConfigError as exc:
        raise SystemExit(str(exc)) from exc

    admin = AdminClient(topic_check_config)
    metadata = admin.list_topics(timeout=15)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise SystemExit(
            f"Topic {topic!r} does not exist. Run Demo 04C with --create-topic first."
        )

    # ====================================================================
    # STUDENT CHECKPOINT
    # What other nonblocking I/O needs to share this event loop? If there is
    # none, why is the standard synchronous Demo 04C the simpler design?
    # ====================================================================
    #
    # (Answer, for this annotated copy: asyncio buys nothing on its own here -
    #  it is not faster, and this file is markedly more complex than 04C. It
    #  earns its keep only when the event loop is ALREADY serving other I/O:
    #  HTTP handlers in FastAPI, async database queries, calls to other
    #  services. Then Kafka work interleaves with that instead of occupying a
    #  thread. With Kafka as the only I/O source, prefer 04C.)

    report = asyncio.run(run_demo(args))
    output_file = write_json_report(args.run_id, "demo04d_asyncio_avro_roundtrip", report)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")

    # The round-trip assertion, as in every demo: what went out must come back.
    if report["delivered"] != args.count or report["consumed"] != args.count:
        raise SystemExit("Demo 04D did not deliver and consume the requested count.")

    return report


if __name__ == "__main__":
    main()
