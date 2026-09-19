"""Extra credit: two bounded consumer members sharing one new consumer group.

Starts two members concurrently in a group that no other phase uses, records
every partition assignment and revocation with a shared monotonic timestamp,
and writes secret-free evidence showing:

  1. no partition was owned by both members at the same instant, and
  2. their combined accepted records cover the intended run.

The base and replay groups are untouched: this uses its own group ID, so the
first/resume/replay committed history is unaffected. Run from the assignment's
top-level folder after the base evidence exists:

    python extra_credit/two_member_group.py --run-id assignment2
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from typing import Any

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from confluent_kafka import (  # noqa: E402
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    OFFSET_BEGINNING,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient  # noqa: E402

from cloud import make_avro_deserializer  # noqa: E402
from config import (  # noqa: E402
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
from consumer_runtime import message_to_record, partition_rows  # noqa: E402

EXPECTED_RECORDS = 12


class MemberLog:
    """Timestamped ownership and processing record for one group member."""

    def __init__(self, name: str, origin: float) -> None:
        self.name = name
        self.origin = origin
        self.events: list[dict[str, Any]] = []
        self.records: list[dict[str, Any]] = []
        self.stop_reason = "max_messages"

    def elapsed(self) -> float:
        """Seconds since the shared origin, so both members share one clock."""

        return round(time.monotonic() - self.origin, 4)

    def on_assign(self, consumer: Any, partitions: Any) -> None:
        # Start at the beginning so re-running this script reproduces the same
        # coverage instead of resuming past what a previous run committed.
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)
        self.events.append(
            {"at": self.elapsed(), "event": "assign", "partitions": partition_rows(partitions)}
        )

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        self.events.append(
            {"at": self.elapsed(), "event": "revoke", "partitions": partition_rows(partitions)}
        )


def run_member(
    log: MemberLog,
    *,
    group_id: str,
    run_id: str,
    topic: str,
    accepted: dict[int, str],
    lock: threading.Lock,
    idle_timeout: float,
    run_timeout: float,
    poll_timeout: float,
) -> None:
    """Consume with this member until the run is covered or a bound is hit."""

    consumer = Consumer(
        {
            **kafka_config(client_id=f"msds682-assignment2-{log.name}"),
            "group.id": group_id,
            "group.protocol": "classic",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    deserializer = make_avro_deserializer(registry_config())
    started = time.monotonic()
    last_message_at = started
    try:
        consumer.subscribe([topic], on_assign=log.on_assign, on_revoke=log.on_revoke)
        while True:
            with lock:
                if len(accepted) >= EXPECTED_RECORDS:
                    log.stop_reason = "run_covered"
                    break
            now = time.monotonic()
            if now - started >= run_timeout:
                log.stop_reason = "run_timeout"
                break
            if now - last_message_at >= idle_timeout:
                log.stop_reason = "idle_timeout"
                break

            message = consumer.poll(poll_timeout)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            record = message_to_record(message, deserializer)
            last_message_at = time.monotonic()
            if record["event"]["run_id"] != run_id:
                continue

            log.records.append(record)
            with lock:
                accepted[record["event"]["sequence_number"]] = log.name
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


def committed_offsets(topic: str, groups: dict[str, str]) -> dict[str, dict[str, int]]:
    """Read each named group's committed offsets without joining the group."""

    admin = AdminClient(kafka_config(client_id="msds682-assignment2-xc-probe"))
    snapshot: dict[str, dict[str, int]] = {}
    for label, group in groups.items():
        request = ConsumerGroupTopicPartitions(
            group, [TopicPartition(topic, partition) for partition in (0, 1, 2)]
        )
        result = admin.list_consumer_group_offsets([request])[group].result()
        snapshot[label] = {
            str(tp.partition): tp.offset for tp in result.topic_partitions
        }
    return snapshot


def ownership_intervals(log: MemberLog, end: float) -> list[dict[str, Any]]:
    """Turn assign/revoke events into per-partition ownership windows."""

    open_windows: dict[int, float] = {}
    intervals: list[dict[str, Any]] = []
    for event in log.events:
        for row in event["partitions"]:
            partition = int(row["partition"])
            if event["event"] == "assign":
                open_windows[partition] = event["at"]
            elif partition in open_windows:
                intervals.append(
                    {
                        "member": log.name,
                        "partition": partition,
                        "from": open_windows.pop(partition),
                        "to": event["at"],
                    }
                )
    for partition, start in open_windows.items():
        intervals.append(
            {"member": log.name, "partition": partition, "from": start, "to": end}
        )
    return intervals


def overlaps(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """True when two ownership windows for one partition intersect in time."""

    return a["partition"] == b["partition"] and a["from"] < b["to"] and b["from"] < a["to"]


def main() -> dict[str, Any]:
    """Run both members concurrently and write the evidence report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="assignment2")
    parser.add_argument("--poll-timeout", type=float, default=0.5)
    parser.add_argument("--idle-timeout", type=float, default=20.0)
    parser.add_argument("--run-timeout", type=float, default=90.0)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    topic = topic_name()
    group_id = ".".join(
        [base_group_id(), normalize_identifier(run_id), "xc-two-members"]
    )

    # Measure the graded groups before and after, rather than asserting they
    # were untouched.
    graded_groups = {
        "base": group_id_for_run(base_group_id(), run_id),
        "replay": group_id_for_run(base_group_id(), run_id, replay=True),
    }
    graded_before = committed_offsets(topic, graded_groups)

    origin = time.monotonic()
    logs = [MemberLog("member-1", origin), MemberLog("member-2", origin)]
    accepted: dict[int, str] = {}
    lock = threading.Lock()
    errors: list[str] = []

    def target(log: MemberLog) -> None:
        try:
            run_member(
                log,
                group_id=group_id,
                run_id=run_id,
                topic=topic,
                accepted=accepted,
                lock=lock,
                idle_timeout=args.idle_timeout,
                run_timeout=args.run_timeout,
                poll_timeout=args.poll_timeout,
            )
        except BaseException as exc:  # noqa: BLE001 - reported, not swallowed
            errors.append(f"{log.name}: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=target, args=(log,), name=log.name) for log in logs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=args.run_timeout + 30)

    end = round(time.monotonic() - origin, 4)
    graded_after = committed_offsets(topic, graded_groups)
    intervals = [row for log in logs for row in ownership_intervals(log, end)]
    concurrent = [
        {"a": a, "b": b}
        for index, a in enumerate(intervals)
        for b in intervals[index + 1 :]
        if a["member"] != b["member"] and overlaps(a, b)
    ]

    combined = sorted(accepted)
    report = {
        "assignment": "assignment02",
        "artifact": "xc_two_member_consumer_group",
        "run_id": run_id,
        "topic": topic,
        "group_id": group_id,
        "group_protocol": "classic",
        "members": [
            {
                "member": log.name,
                "events": log.events,
                "processed": len(log.records),
                "sequence_numbers": [r["event"]["sequence_number"] for r in log.records],
                "trip_ids": [r["event"]["trip_id"] for r in log.records],
                "partitions_read": sorted({r["partition"] for r in log.records}),
                "stop_reason": log.stop_reason,
            }
            for log in logs
        ],
        "partition_ownership_windows": sorted(
            intervals, key=lambda row: (row["partition"], row["from"])
        ),
        "concurrent_shared_partitions": concurrent,
        "no_partition_shared_concurrently": not concurrent,
        "combined_sequence_numbers": combined,
        "combined_processed": sum(len(log.records) for log in logs),
        "covers_intended_run": combined == list(range(EXPECTED_RECORDS)),
        "errors": errors,
        "graded_group_ids": graded_groups,
        "graded_group_offsets_before": graded_before,
        "graded_group_offsets_after": graded_after,
        "base_and_replay_groups_untouched": graded_before == graded_after,
        "kafka_connection": safe_kafka_config_report(
            kafka_config(client_id="msds682-assignment2-xc-two-members")
        ),
        "schema_registry": safe_registry_config_report(registry_config()),
    }

    output = write_json_report("xc_two_member_group.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"\nWrote {output}")
    return report


if __name__ == "__main__":
    main()
