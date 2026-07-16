"""Demo 03D: run native Confluent AIO producer and consumer tasks safely.

Student focus: asyncio is useful because Kafka shares an event loop with other
async work. The producer waits for a real consumer assignment signal instead
of guessing readiness with a fixed sleep.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

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
    assignment_ready: asyncio.Event,
    assignment_timeout: float,
    interval: float,
) -> tuple[list[dict[str, Any]], float]:
    """Wait for consumer assignment, then produce a finite event set."""

    # Student checkpoint: latest-offset consumers must be assigned before new
    # records are produced. A signal is deterministic; a fixed sleep is not.
    loop = asyncio.get_running_loop()
    assignment_wait_started = loop.time()
    try:
        await asyncio.wait_for(assignment_ready.wait(), timeout=assignment_timeout)
    except TimeoutError as exc:
        raise RuntimeError(
            "Consumer assignment was not ready before --assignment-timeout. "
            "Check cluster connectivity, topic access, and Kafka API credentials."
        ) from exc
    assignment_wait_seconds = loop.time() - assignment_wait_started

    producer = AIOProducer(config)
    delivery_futures = []
    try:
        for event in make_trip_events(count, seed):
            delivery_future = await producer.produce(
                TOPIC_NAME,
                key=event_key(event),
                value=serialize_event(event),
            )
            delivery_futures.append(delivery_future)
            await asyncio.sleep(interval)
        await producer.flush()
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
        await producer.close()


async def consume_events(
    config: dict[str, Any],
    *,
    expected_count: int,
    timeout: float,
    assignment_ready: asyncio.Event,
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
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async def on_assign(aio_consumer: Any, partitions: Any) -> None:
        """Complete the assignment before allowing the producer to start."""

        # AIOConsumer uses an executor for the underlying blocking client. Its
        # default two workers allow this re-entrant assign call from a callback.
        await aio_consumer.assign(partitions)
        rows = topic_partition_records(partitions)
        assignments.append(rows)
        print(f"Async assigned partitions: {rows}")
        assignment_ready.set()

    async def on_revoke(_aio_consumer: Any, partitions: Any) -> None:
        """Record revocation evidence before shutdown or rebalance."""

        rows = topic_partition_records(partitions)
        revocations.append(rows)
        print(f"Async revoked partitions: {rows}")

    await consumer.subscribe(
        [TOPIC_NAME],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )
    try:
        while len(records) < expected_count and loop.time() < deadline:
            remaining = max(deadline - loop.time(), 0.0)
            message = await consumer.poll(timeout=min(1.0, remaining))
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Consumer error: {message.error()}")
            record = message_to_record(message)
            records.append(record)
            print(
                f"Async consumed {record['topic']}[{record['partition']}] "
                f"offset={record['offset']} key={record['key']}"
            )
    finally:
        await consumer.unsubscribe()
        await consumer.close()
    return records, assignments, revocations


async def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    """Run native asyncio producer and consumer clients concurrently."""

    producer_config = require_producer_config()
    group_id = args.group_id or default_group_id("demo03d-asyncio", args.run_id)
    consumer_config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        client_id="msds682-demo03d-aio-consumer",
    )
    # Student checkpoint: the callback uses assign(partitions), so this demo
    # makes the classic group protocol explicit instead of relying on a default.
    consumer_config["group.protocol"] = "classic"
    producer_config = {**producer_config, "client.id": "msds682-demo03d-aio-producer"}
    assignment_ready = asyncio.Event()

    # Student checkpoint: both tasks start together, but the producer has an
    # explicit assignment gate inside produce_events().
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
        producer_result, consumer_result = await asyncio.gather(
            producer_task,
            consumer_task,
        )
    except BaseException:
        # If either task fails, cancel and await its sibling so every client's
        # finally block still closes network resources before the error exits.
        for task in (producer_task, consumer_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
        raise
    delivered, assignment_wait_seconds = producer_result
    consumed, assignments, revocations = consumer_result

    return {
        "demo": "demo03d_confluent_asyncio_produce_consume",
        "topic": TOPIC_NAME,
        "requested": args.count,
        "delivered": len(delivered),
        "consumed": len(consumed),
        "assignment_wait_seconds": assignment_wait_seconds,
        "group_protocol": consumer_config["group.protocol"],
        "partition_assignments": assignments,
        "partition_revocations": revocations,
        "producer_connection": safe_config_report(producer_config),
        "consumer_connection": safe_consumer_config_report(consumer_config),
        "delivered_messages": delivered,
        "consumed_records": consumed,
    }


def main() -> dict[str, Any]:
    """Parse a finite asyncio demo, run it, and write secret-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03d")
    parser.add_argument("--group-id")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--consumer-timeout", type=float, default=15.0)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")
    if args.assignment_timeout <= 0 or args.consumer_timeout <= 0:
        parser.error("--assignment-timeout and --consumer-timeout must be positive")
    if args.interval < 0:
        parser.error("--interval cannot be negative")

    report = asyncio.run(run_demo(args))
    output_file = write_json_report(
        args.run_id,
        "demo03d_confluent_asyncio_produce_consume",
        report,
    )
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if report["delivered"] != args.count or report["consumed"] != args.count:
        raise SystemExit("AsyncIO demo did not deliver and consume the requested count.")
    return report


if __name__ == "__main__":
    main()
