"""Demo 03B: process records, commit offsets, and prove same-group resume.

Student focus: disable automatic offset storage, validate each record before
committing, then run twice with the same group ID and compare offsets.
"""

from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer

from demo02_producer_common import TOPIC_NAME, write_json_report
from demo03_consumer_common import (
    AssignmentTracker,
    CommitTracker,
    assert_expected_topic,
    consume_records,
    default_group_id,
    require_consumer_config,
    safe_consumer_config_report,
)


def main() -> dict:
    """Process then commit offsets so the same group resumes on the next run."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03b")
    parser.add_argument("--group-id")
    parser.add_argument("--commit-mode", choices=["sync", "async"], default="sync")
    parser.add_argument("--max-messages", type=int, default=4)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--idle-timeout", type=float, default=8.0)
    args = parser.parse_args()

    # Student checkpoint: this default deliberately stays stable across runs.
    # Change the group ID only when you intentionally want a separate history.
    group_id = args.group_id or default_group_id("demo03b-resume")
    commits = CommitTracker()
    config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        client_id="msds682-demo03b-offset-consumer",
        on_commit=commits.callback,
    )
    assignment = AssignmentTracker()
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
            idle_timeout=args.idle_timeout,
            commit_mode=args.commit_mode,
        )
        # consume_records() decodes and validates before requesting each commit.
        assert_expected_topic(result.records)
    finally:
        consumer.close()

    report = {
        "demo": "demo03b_confluent_offsets_commit",
        "topic": TOPIC_NAME,
        "group_id": group_id,
        "commit_mode": args.commit_mode,
        "consumed": len(result.records),
        "commit_requests": result.commit_requests,
        "synchronous_commit_results": result.synchronous_commit_results,
        "asynchronous_commit_acknowledgements": commits.acknowledged,
        "asynchronous_commit_failures": commits.failed,
        "stop_reason": result.stop_reason,
        "connection": safe_consumer_config_report(config),
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,
        "records": result.records,
        "next_step": "Run again with the same group ID to resume after committed offsets.",
    }
    output_file = write_json_report(args.run_id, "demo03b_confluent_offsets_commit", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if commits.failed:
        raise SystemExit("At least one asynchronous offset commit failed.")
    return report


if __name__ == "__main__":
    main()
