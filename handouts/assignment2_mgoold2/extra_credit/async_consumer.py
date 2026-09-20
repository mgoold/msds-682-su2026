"""Extra credit: bounded native-asyncio consumer with an equivalence check.

Reads the Assignment 2 topic with ``confluent_kafka.aio.AIOConsumer`` and
``AsyncAvroDeserializer``, then compares the event identities it accepted
against the ones the synchronous consumer already wrote to
``results/processed_events.jsonl``.

Four properties this is built to demonstrate:

  * real assignment readiness - the receive budget starts only once the broker
    has actually assigned partitions, so a slow group join cannot eat it;
  * finite poll and time limits - a message cap, a post-assignment deadline,
    and an assignment timeout, all visible in the report;
  * schema-aware validation - Avro through Schema Registry, strict
    ``TripEventV1``, and the UTF-8 key checked against the payload, mirroring
    ``message_to_record``; and
  * correct cleanup - unsubscribe and close are each awaited under their own
    timeout, and a cleanup failure never hides the original error.

Uses its own consumer group, so the graded first/resume/replay history is
untouched; that claim is measured rather than asserted. Run from the
assignment's top-level folder after the synchronous phases:

    python extra_credit/async_consumer.py --run-id assignment2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from confluent_kafka import (  # noqa: E402
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    OFFSET_BEGINNING,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient  # noqa: E402
from confluent_kafka.aio import AIOConsumer  # noqa: E402
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from config import (  # noqa: E402
    RESULTS_DIR,
    base_group_id,
    group_id_for_run,
    kafka_config,
    normalize_identifier,
    registry_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    topic_name,
    validate_run_id,
    write_json_report,
)
from consumer_runtime import partition_rows  # noqa: E402
from contracts import TripEventV1, avro_dict_to_event, schema_str  # noqa: E402

EXPECTED_RECORDS = 12


def identity(run_id: str, sequence_number: int, trip_id: str) -> str:
    """One comparable event identity, independent of partition and offset."""

    return f"{run_id}:{sequence_number:02d}:{trip_id}"


def synchronous_identities(results_dir: Path = RESULTS_DIR) -> list[str]:
    """Identities the synchronous first + resume runs already accepted."""

    path = results_dir / "processed_events.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    return sorted(
        identity(row["run_id"], row["sequence_number"], row["trip_id"]) for row in rows
    )


def committed_offsets(topic: str, groups: dict[str, str]) -> dict[str, dict[str, int]]:
    """Read each group's committed offsets without joining the group."""

    admin = AdminClient(kafka_config(client_id="msds682-assignment2-xc-aio-probe"))
    snapshot: dict[str, dict[str, int]] = {}
    for label, group in groups.items():
        request = ConsumerGroupTopicPartitions(
            group, [TopicPartition(topic, partition) for partition in (0, 1, 2)]
        )
        result = admin.list_consumer_group_offsets([request])[group].result()
        snapshot[label] = {str(tp.partition): tp.offset for tp in result.topic_partitions}
    return snapshot


async def consume_async(
    *,
    group_id: str,
    run_id: str,
    topic: str,
    assignment_timeout: float,
    receive_timeout: float,
    poll_timeout: float,
    cleanup_timeout: float,
) -> dict[str, Any]:
    """Run one bounded AIOConsumer pass and return its evidence."""

    consumer = AIOConsumer(
        {
            **kafka_config(client_id="msds682-assignment2-xc-aio"),
            "group.id": group_id,
            "group.protocol": "classic",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    loop = asyncio.get_running_loop()
    assignment_ready = asyncio.Event()
    assignments: list[list[dict[str, int | str]]] = []
    revocations: list[list[dict[str, int | str]]] = []
    records: list[dict[str, Any]] = []
    skipped_other_runs = 0
    stop_reason = "max_messages"
    started = loop.time()
    assignment_ready_at: float | None = None
    # The receive budget is deliberately not started here. It opens only once a
    # real assignment arrives, so a cold group join cannot consume it.
    deadline: float | None = None

    async def on_assign(aio_consumer: Any, partitions: Any) -> None:
        # Explicit beginning, so a re-run reproduces the same coverage rather
        # than resuming past what the previous run committed.
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        await aio_consumer.assign(partitions)
        assignments.append(partition_rows(partitions))
        assignment_ready.set()

    async def on_revoke(_aio_consumer: Any, partitions: Any) -> None:
        revocations.append(partition_rows(partitions))

    primary_error: BaseException | None = None
    cleanup: dict[str, str] = {}
    async with AsyncSchemaRegistryClient(registry_config()) as registry:
        deserializer = await AsyncAvroDeserializer(
            registry, schema_str(), from_dict=avro_dict_to_event
        )
        context = SerializationContext(topic, MessageField.VALUE)
        try:
            await consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
            while len(records) < EXPECTED_RECORDS:
                now = loop.time()
                if not assignment_ready.is_set() and now - started >= assignment_timeout:
                    stop_reason = "assignment_timeout"
                    break
                if deadline is not None and now >= deadline:
                    stop_reason = "receive_timeout"
                    break

                message = await consumer.poll(timeout=poll_timeout)
                if assignment_ready.is_set() and deadline is None:
                    assignment_ready_at = round(loop.time() - started, 4)
                    deadline = loop.time() + receive_timeout
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                raw_value = message.value()
                if not raw_value:
                    raise ValueError("Kafka message value is missing")
                event = await deserializer(raw_value, context)
                if not isinstance(event, TripEventV1):
                    raise TypeError("Expected AsyncAvroDeserializer to return TripEventV1")

                message_key = message.key()
                if not message_key:
                    raise ValueError("Kafka message key is missing")
                message_key_str = message_key.decode("utf-8")
                if message_key_str != event.trip_id:
                    raise ValueError(
                        f"Kafka key {message_key_str} does not match payload "
                        f"trip_id {event.trip_id}"
                    )

                if event.run_id != run_id:
                    skipped_other_runs += 1
                    continue

                records.append(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "key": message_key_str,
                        "event": event.model_dump(mode="json"),
                    }
                )
                await consumer.commit(message=message, asynchronous=False)
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            # Bound each cleanup step separately and never let a cleanup error
            # replace the error that actually stopped the run.
            for label, operation in (
                ("unsubscribe", consumer.unsubscribe),
                ("close", consumer.close),
            ):
                try:
                    await asyncio.wait_for(operation(), timeout=cleanup_timeout)
                    cleanup[label] = "ok"
                except BaseException as exc:  # noqa: BLE001 - recorded, not hidden
                    cleanup[label] = f"{type(exc).__name__}: {exc}"

    return {
        "assignment_ready_seconds": assignment_ready_at,
        "partition_assignments": assignments,
        "partition_revocations": revocations,
        "records": records,
        "skipped_records_from_other_runs": skipped_other_runs,
        "stop_reason": stop_reason,
        "cleanup": cleanup,
        "cleanup_clean": all(value == "ok" for value in cleanup.values()),
        "primary_error": None if primary_error is None else str(primary_error),
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Consume asynchronously, then compare identities with the sync run."""

    run_id = validate_run_id(args.run_id)
    topic = topic_name()
    group_id = ".".join([base_group_id(), normalize_identifier(run_id), "xc-aio"])
    graded_groups = {
        "base": group_id_for_run(base_group_id(), run_id),
        "replay": group_id_for_run(base_group_id(), run_id, replay=True),
    }

    graded_before = committed_offsets(topic, graded_groups)
    outcome = await consume_async(
        group_id=group_id,
        run_id=run_id,
        topic=topic,
        assignment_timeout=args.assignment_timeout,
        receive_timeout=args.receive_timeout,
        poll_timeout=args.poll_timeout,
        cleanup_timeout=args.cleanup_timeout,
    )
    graded_after = committed_offsets(topic, graded_groups)

    async_ids = sorted(
        identity(r["event"]["run_id"], r["event"]["sequence_number"], r["event"]["trip_id"])
        for r in outcome["records"]
    )
    sync_ids = synchronous_identities()

    report = {
        "assignment": "assignment02",
        "artifact": "xc_native_asyncio_consumer",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "run_id": run_id,
        "topic": topic,
        "group_id": group_id,
        "client": "confluent_kafka.aio.AIOConsumer",
        "deserializer": "confluent_kafka.schema_registry.avro.AsyncAvroDeserializer",
        "bounds": {
            "expected_records": EXPECTED_RECORDS,
            "assignment_timeout_s": args.assignment_timeout,
            "receive_timeout_s": args.receive_timeout,
            "poll_timeout_s": args.poll_timeout,
            "cleanup_timeout_s": args.cleanup_timeout,
            "receive_budget_starts_after_assignment": True,
        },
        **outcome,
        "processed": len(outcome["records"]),
        "sequence_numbers": [r["event"]["sequence_number"] for r in outcome["records"]],
        "equivalence": {
            "synchronous_source": "results/processed_events.jsonl",
            "synchronous_identities": sync_ids,
            "asynchronous_identities": async_ids,
            "identical": sync_ids == async_ids,
            "missing_from_async": sorted(set(sync_ids) - set(async_ids)),
            "extra_in_async": sorted(set(async_ids) - set(sync_ids)),
        },
        "graded_group_ids": graded_groups,
        "graded_group_offsets_before": graded_before,
        "graded_group_offsets_after": graded_after,
        "base_and_replay_groups_untouched": graded_before == graded_after,
        "kafka_connection": safe_kafka_config_report(
            kafka_config(client_id="msds682-assignment2-xc-aio")
        ),
        "schema_registry": safe_registry_config_report(registry_config()),
    }

    output = write_json_report("xc_async_consumer.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"\nWrote {output}")
    return report


def main() -> dict[str, Any]:
    """Parse bounds and run the asyncio consumer once."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="assignment2")
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--assignment-timeout", type=float, default=45.0)
    parser.add_argument("--receive-timeout", type=float, default=45.0)
    parser.add_argument("--cleanup-timeout", type=float, default=20.0)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
