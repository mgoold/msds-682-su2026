"""
================================================================================
DEMO 03D - ASYNCIO PRODUCER AND CONSUMER TOGETHER  (annotated tutorial copy)
================================================================================

READ demo02b AND demo03a FIRST. This demo combines both directions and adds
Python's asyncio on top.

WHAT THIS DEMO TEACHES
    1. The native async Kafka clients (AIOProducer / AIOConsumer) and how they
       differ from the blocking ones used everywhere else in this course.
    2. A genuinely hard distributed-systems problem in miniature: how do you
       make a producer wait until a consumer is actually ready to receive?
       (The wrong answer is time.sleep(). The right answer is a SIGNAL.)

WHY ASYNCIO FOR KAFKA AT ALL
    The blocking Consumer.poll() occupies its thread while it waits. If your
    program is a web service that also handles HTTP requests, or talks to a
    database, a blocked thread is wasted capacity. asyncio lets Kafka work share
    ONE event loop with everything else: while poll() awaits data, the loop runs
    other coroutines.

    This matters when Kafka is one of several I/O sources. For a standalone
    consumer that does nothing else, the ordinary blocking client is simpler and
    perfectly fine.

ASYNCIO IN ONE PARAGRAPH (if you have not used it)
    `async def` defines a COROUTINE - a function that can pause. `await X` means
    "pause here, let the event loop run other work, and resume when X is done."
    asyncio.create_task() schedules a coroutine to run concurrently.
    asyncio.gather() waits for several tasks at once. It is CONCURRENCY, not
    parallelism: one thread, interleaved rather than simultaneous - which is
    ideal for I/O-bound work like network calls.

THE CENTRAL PROBLEM THIS DEMO SOLVES
    The consumer here starts at "latest", so it only sees messages produced
    AFTER it has been assigned partitions. But assignment takes a moment
    (subscribe -> group rebalance -> assignment). If the producer starts
    immediately, its messages land before the consumer is listening and are
    never seen.

    The naive fix is `await asyncio.sleep(2)` and hope. That is a guess: too
    short and it fails, too long and it wastes time - and it will fail on a slow
    network anyway. The correct fix is an explicit signal (asyncio.Event) that
    the consumer sets the instant assignment completes. Deterministic, not
    hopeful.
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

# THE ASYNC CLIENTS. Same underlying library, coroutine-based API: every call
# that touches the network is awaited.
from confluent_kafka.aio import AIOConsumer, AIOProducer

from demo02_producer_common import (
    TOPIC_NAME,
    event_key,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)
from demo03_consumer_common import (
    default_group_id,
    message_to_record,
    require_consumer_config,
    safe_consumer_config_report,
    topic_partition_records,
)


async def produce_events(
    config: dict[str, str],
    *,
    count: int,
    seed: int,
    assignment_ready: asyncio.Event,   # THE SIGNAL - see below
    assignment_timeout: float,
    interval: float,
) -> tuple[list[dict[str, Any]], float]:
    """Wait for the consumer to be ready, then produce a finite set of events."""

    # ---- THE COORDINATION GATE ----------------------------------------
    # asyncio.Event is a one-way flag shared between coroutines:
    #   .wait() pauses until someone calls .set()
    #   .set()  wakes every waiter
    #
    # The consumer calls .set() inside its on_assign callback - i.e. at the
    # exact moment it owns partitions and can receive. So this producer resumes
    # neither too early nor a moment later than necessary.
    #
    # asyncio.wait_for(..., timeout) adds a deadline so a broken consumer
    # cannot hang the program forever. Without it, a failure elsewhere would
    # present as a silent hang - the worst kind of bug.
    loop = asyncio.get_running_loop()
    assignment_wait_started = loop.time()
    try:
        await asyncio.wait_for(assignment_ready.wait(), timeout=assignment_timeout)
    except TimeoutError as exc:
        # Turn a bare timeout into a message that names the likely causes.
        # `from exc` preserves the original traceback.
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check cluster connectivity, topic access, and Kafka API credentials."
        ) from exc

    # How long we actually waited - reported later as evidence that the gate did
    # something real rather than being decorative.
    assignment_wait_seconds = loop.time() - assignment_wait_started

    producer = AIOProducer(config)

    # ---- HOW DELIVERY RESULTS DIFFER IN THE ASYNC CLIENT --------------
    # The blocking client used a CALLBACK (Demo 02). The async client returns a
    # FUTURE per message instead - a handle to a result that is not ready yet.
    # Same deferred-result idea, expressed in asyncio's vocabulary.
    delivery_futures = []

    try:
        for event in make_trip_events(count, seed):
            # `await producer.produce(...)` still does not mean "sent". It
            # queues the record and hands back a future that resolves when the
            # broker acknowledges it.
            #
            # Note there is no callback= argument here - the future replaces it.
            delivery_future = await producer.produce(
                TOPIC_NAME,
                key=event_key(event),        # same serialization helpers as Demo 02
                value=serialize_event(event),
            )
            delivery_futures.append(delivery_future)

            # A small deliberate pause between messages. Purely pedagogical: it
            # spreads the messages out in time so you can watch the consumer
            # print them as they arrive, instead of all at once. Remove it for
            # throughput.
            #
            # asyncio.sleep() yields to the event loop, so the consumer
            # coroutine actually runs during this pause - unlike time.sleep(),
            # which would block the entire loop and stall the consumer too.
            await asyncio.sleep(interval)

        # The same mandatory final flush as every producer in this course:
        # block until everything queued has reached a terminal state.
        await producer.flush()

        # asyncio.gather(*futures) waits for ALL delivery futures at once and
        # returns their results in order. Each result is a Message object that
        # now knows its final partition and offset.
        delivered_messages = await asyncio.gather(*delivery_futures)

        delivered = [
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
            }
            for message in delivered_messages
        ]
        return delivered, round(assignment_wait_seconds, 6)

    finally:
        # ALWAYS close, even if something above raised. Releases the connection
        # and background resources.
        await producer.close()


async def consume_events(
    config: dict[str, Any],
    *,
    expected_count: int,
    timeout: float,
    assignment_ready: asyncio.Event,   # the same Event object the producer waits on
) -> tuple[
    list[dict[str, Any]],
    list[list[dict[str, int | str]]],
    list[list[dict[str, int | str]]],
]:
    """Consume with AIOConsumer.poll() while allowing producer work to proceed."""

    consumer = AIOConsumer(config)
    records: list[dict[str, Any]] = []
    assignments: list[list[dict[str, int | str]]] = []
    revocations: list[list[dict[str, int | str]]] = []

    # loop.time() is the event loop's monotonic clock - the asyncio-native
    # equivalent of time.monotonic().
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async def on_assign(aio_consumer: Any, partitions: Any) -> None:
        """Called when this consumer is given partitions. NOTE: it is async."""

        # In the async client the rebalance callbacks are themselves coroutines,
        # so the assign() call must be awaited.
        #
        # The comment in the original source is worth keeping: AIOConsumer runs
        # the underlying blocking client on a small thread-pool executor, and
        # its default of two workers is what permits this re-entrant assign()
        # call from inside a callback without deadlocking.
        await aio_consumer.assign(partitions)

        rows = topic_partition_records(partitions)
        assignments.append(rows)
        print(f"Async assigned partitions: {rows}")

        # ---- THE SIGNAL FIRES HERE --------------------------------------
        # Assignment is complete, so this consumer can now receive messages.
        # Setting the Event releases the producer, which has been parked in
        # assignment_ready.wait().
        #
        # THIS LINE IS THE POINT OF THE DEMO. It converts "wait long enough and
        # hope" into "wait for the actual precondition". Deterministic
        # coordination beats a sleep every time.
        assignment_ready.set()

    async def on_revoke(_aio_consumer: Any, partitions: Any) -> None:
        """Called before partitions are taken away (rebalance or shutdown)."""
        rows = topic_partition_records(partitions)
        revocations.append(rows)
        print(f"Async revoked partitions: {rows}")

    await consumer.subscribe(
        [TOPIC_NAME],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )

    try:
        # The familiar poll loop, now with awaits. Two bounds:
        #   - stop once we have the expected number of records
        #   - stop at the wall-clock deadline, so a failure cannot hang forever
        while len(records) < expected_count and loop.time() < deadline:
            remaining = max(deadline - loop.time(), 0.0)

            # min(1.0, remaining) keeps each individual poll short (at most one
            # second) so the loop stays responsive, while never waiting past the
            # overall deadline.
            #
            # `await` here is what makes the whole demo work: while this poll
            # waits for data, the event loop runs the PRODUCER coroutine. One
            # thread, two Kafka clients, genuinely interleaved.
            message = await consumer.poll(timeout=min(1.0, remaining))

            # None simply means nothing arrived in that window - normal.
            if message is None:
                continue

            if message.error():
                raise RuntimeError(f"Consumer error: {message.error()}")

            # Same decode-and-validate helper the blocking demos use: bytes ->
            # JSON -> validated TripEvent.
            record = message_to_record(message)
            records.append(record)
            print(
                f"Async consumed {record['topic']}[{record['partition']}] "
                f"offset={record['offset']} key={record['key']}"
            )
    finally:
        # unsubscribe() then close(): leave the group cleanly, then release
        # resources. In `finally` so it happens even on error.
        await consumer.unsubscribe()
        await consumer.close()

    return records, assignments, revocations


async def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    """Run the producer and consumer concurrently and collect the evidence."""

    producer_config = require_producer_config()
    group_id = args.group_id or default_group_id("demo03d-asyncio", args.run_id)

    consumer_config = require_consumer_config(
        group_id=group_id,

        # "latest" IS WHAT CREATES THE COORDINATION PROBLEM. The consumer will
        # only see messages produced after it is assigned - which is precisely
        # why the assignment_ready gate is necessary. With "earliest" the demo
        # would work by accident, and would teach nothing.
        auto_offset_reset="latest",

        enable_auto_commit=True,
        client_id="msds682-demo03d-aio-consumer",
    )

    # Pin the rebalance protocol explicitly. The newer "consumer" protocol
    # handles assignment server-side and does not use the same client-side
    # assign() call this demo makes inside on_assign. Naming "classic" keeps the
    # demo's behavior stable rather than depending on a library default.
    consumer_config["group.protocol"] = "classic"

    # A distinct client.id for the producer, so broker-side logs can tell the
    # two clients apart.
    producer_config = {**producer_config, "client.id": "msds682-demo03d-aio-producer"}

    # THE SHARED SIGNAL. Created here, passed to BOTH coroutines. The consumer
    # sets it; the producer waits on it.
    assignment_ready = asyncio.Event()

    # ---- LAUNCH BOTH CONCURRENTLY -------------------------------------
    # create_task() schedules a coroutine to run on the event loop immediately.
    # Both tasks start together - but produce_events() blocks at its gate until
    # the consumer signals readiness, so the ORDER is enforced by the Event, not
    # by the launch sequence.
    producer_task = asyncio.create_task(
        produce_events(
            producer_config,
            count=args.count,
            seed=args.seed,
            assignment_ready=assignment_ready,
            assignment_timeout=args.assignment_timeout,
            interval=args.interval,
        )
    )
    consumer_task = asyncio.create_task(
        consume_events(
            consumer_config,
            expected_count=args.count,
            timeout=args.consumer_timeout,
            assignment_ready=assignment_ready,
        )
    )

    try:
        # gather() waits for both and returns their results in order. If either
        # raises, gather re-raises immediately.
        producer_result, consumer_result = await asyncio.gather(
            producer_task,
            consumer_task,
        )
    except BaseException:
        # ---- CLEAN SHUTDOWN ON FAILURE --------------------------------
        # If one task fails, the other may still be running - waiting on a
        # signal that will never come, or polling a topic nobody is feeding.
        # Left alone it would leak a connection and could hang the process.
        #
        # So: cancel the sibling, then AWAIT it. The await matters - cancelling
        # only requests cancellation; awaiting lets the task actually run its
        # `finally` block and close its client.
        #
        # return_exceptions=True stops the cleanup itself from raising and
        # masking the original error.
        #
        # BaseException (not Exception) also catches KeyboardInterrupt and
        # CancelledError, so Ctrl-C still shuts down cleanly.
        for task in (producer_task, consumer_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
        raise      # re-raise the original error after cleaning up

    delivered, assignment_wait_seconds = producer_result
    consumed, assignments, revocations = consumer_result

    return {
        "demo": "demo03d_confluent_asyncio_produce_consume",
        "topic": TOPIC_NAME,
        "requested": args.count,

        # THE HEADLINE NUMBERS. On a healthy run, delivered == consumed ==
        # requested: every message written was also read back, in one process,
        # within one event loop.
        "delivered": len(delivered),
        "consumed": len(consumed),

        # EVIDENCE THAT THE GATE MATTERED. A non-trivial value here is proof
        # that the producer genuinely waited for assignment rather than racing
        # ahead - the thing a fixed sleep could only have guessed at.
        "assignment_wait_seconds": assignment_wait_seconds,

        "group_protocol": consumer_config["group.protocol"],
        "partition_assignments": assignments,
        "partition_revocations": revocations,

        # Two separate secret-free connection summaries: this program is both a
        # producer and a consumer.
        "producer_connection": safe_config_report(producer_config),
        "consumer_connection": safe_consumer_config_report(consumer_config),

        "delivered_messages": delivered,
        "consumed_records": consumed,
    }


def main() -> dict[str, Any]:
    """Parse arguments, run the async demo, and write secret-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03d")
    parser.add_argument("--group-id")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--seed", type=int, default=682)

    # How long the producer will wait for the consumer's readiness signal.
    parser.add_argument("--assignment-timeout", type=float, default=15.0)

    # Pause between produced messages, so the interleaving is visible.
    parser.add_argument("--interval", type=float, default=0.1)

    # Overall bound on the consumer loop.
    parser.add_argument("--consumer-timeout", type=float, default=15.0)

    args = parser.parse_args()

    # Validate up front - clearer than failing deep inside a coroutine.
    if args.count < 1:
        parser.error("--count must be at least 1")
    if args.assignment_timeout <= 0 or args.consumer_timeout <= 0:
        parser.error("--assignment-timeout and --consumer-timeout must be positive")
    if args.interval < 0:
        parser.error("--interval cannot be negative")

    # asyncio.run() is the standard entry point from synchronous code: it
    # creates an event loop, runs the coroutine to completion, and shuts the
    # loop down cleanly afterwards.
    report = asyncio.run(run_demo(args))

    output_file = write_json_report(
        args.run_id,
        "demo03d_confluent_asyncio_produce_consume",
        report,
    )
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # THE ROUND-TRIP ASSERTION. Everything produced must also have been
    # consumed. If the coordination gate failed, the consumer would have missed
    # early messages and this check would catch it - the test that makes the
    # whole assignment_ready mechanism verifiable rather than merely plausible.
    if report["delivered"] != args.count or report["consumed"] != args.count:
        raise SystemExit("AsyncIO demo did not deliver and consume the requested count.")

    return report


if __name__ == "__main__":
    main()
