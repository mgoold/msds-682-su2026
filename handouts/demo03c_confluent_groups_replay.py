"""Demo 03C: observe group assignments and request replay explicitly.

Student focus: concurrent members of one group share partitions. Normal mode
respects group progress; only --force-beginning overrides assigned offsets, and
every run remains bounded by message and time limits.
"""

from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer

from demo02_producer_common import TOPIC_NAME, write_json_report
from demo03_consumer_common import (
    AssignmentTracker,
    assert_expected_topic,
    consume_records,
    default_group_id,
    require_consumer_config,
    safe_consumer_config_report,
)


def main() -> dict:
    """Show group partition assignment or explicitly replay from the beginning."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03c")
    parser.add_argument("--group-id")
    parser.add_argument("--member-id", default="member-a")
    parser.add_argument("--force-beginning", action="store_true")
    parser.add_argument("--max-messages", type=int, default=50)
    parser.add_argument("--poll-timeout", type=float, default=0.5)
    parser.add_argument("--run-seconds", type=float, default=20.0)
    args = parser.parse_args()

    group_id = args.group_id or default_group_id("demo03c-shared-group")
    # Student checkpoint: normal group mode and replay are different contracts.
    # Normal mode starts at latest only when the group has no committed offset;
    # replay explicitly overrides assignment to OFFSET_BEGINNING.
    auto_offset_reset = "earliest" if args.force_beginning else "latest"
    config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=not args.force_beginning,
        client_id=f"msds682-demo03c-{args.member_id}",
    )
    assignment = AssignmentTracker(force_beginning=args.force_beginning)
    consumer = Consumer(config)
    try:
        consumer.subscribe(
            [TOPIC_NAME],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        result = consume_records(
            consumer,
            max_messages=args.max_messages,
            poll_timeout=args.poll_timeout,
            idle_timeout=args.run_seconds,
            run_timeout=args.run_seconds,
        )
        assert_expected_topic(result.records)
    finally:
        consumer.close()

    report = {
        "demo": "demo03c_confluent_groups_replay",
        "mode": "force_beginning_replay" if args.force_beginning else "consumer_group",
        "topic": TOPIC_NAME,
        "group_id": group_id,
        "member_id": args.member_id,
        "consumed": len(result.records),
        "stop_reason": result.stop_reason,
        "connection": safe_consumer_config_report(config),
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,
        "records": result.records,
    }
    output_file = write_json_report(args.run_id, "demo03c_confluent_groups_replay", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    return report


if __name__ == "__main__":
    main()
