"""Demo 03A: consume a bounded sample from the shared Confluent topic.

Student focus: subscribe, poll, decode, validate, inspect partition/offset, and
always close the consumer. The stable group ID makes saved progress observable
when the script is run again.
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
    """Consume a bounded set of Demo 02 events with the standard poll loop."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03a")
    parser.add_argument("--group-id")
    parser.add_argument("--max-messages", type=int, default=8)
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--idle-timeout", type=float, default=8.0)
    args = parser.parse_args()

    # Student checkpoint: the default group ID is intentionally stable. With
    # auto-commit enabled, rerunning can reach the current end of the topic.
    group_id = args.group_id or default_group_id("demo03a-basic")
    config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset="earliest",
        # close() commits the latest stored offsets when auto-commit is enabled.
        enable_auto_commit=True,
        client_id="msds682-demo03a-basic-consumer",
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
        )
        assert_expected_topic(result.records)
    finally:
        # close() leaves the group promptly and releases sockets even on failure.
        consumer.close()

    report = {
        "demo": "demo03a_confluent_basic_consumer",
        "topic": TOPIC_NAME,
        "requested_max_messages": args.max_messages,
        "consumed": len(result.records),
        "stop_reason": result.stop_reason,
        "connection": safe_consumer_config_report(config),
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,
        "records": result.records,
    }
    output_file = write_json_report(args.run_id, "demo03a_confluent_basic_consumer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    return report


if __name__ == "__main__":
    main()
