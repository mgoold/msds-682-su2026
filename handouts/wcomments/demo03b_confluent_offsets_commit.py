"""
================================================================================
DEMO 03B - MANUAL OFFSET COMMITS  (annotated tutorial copy)
================================================================================

READ demo03a FIRST. This is the same consumer with ONE setting changed:
auto-commit is turned OFF and the application commits explicitly.

WHAT THIS DEMO TEACHES
    Who decides when a message counts as "done", and why that decision is the
    difference between losing data and duplicating it.

THE CORE IDEA: COMMITTING IS SAVING A BOOKMARK
    Kafka stores, per (group, topic, partition), a single number: the offset
    this group has finished through. That number is the ONLY thing that makes a
    consumer restartable. Committing is the act of updating it.

    Reading a message does not commit it. Processing it does not commit it.
    Committing is a separate, explicit action - and this demo makes it explicit.

THE ORDERING RULE, WHICH IS THE WHOLE POINT

        poll  ->  decode  ->  validate  ->  process  ->  THEN commit

    Do it in that order and a crash costs you a REPEATED message.
    Do it backwards (commit first) and a crash costs you a LOST message.

    Kafka's default guarantee is therefore AT-LEAST-ONCE: every message arrives
    at least once, possibly more than once. You cannot engineer the duplicates
    away, because the gap between "processed" and "committed" can never be
    truly atomic. Instead you make duplicates harmless - IDEMPOTENT processing
    (upsert by trip_id, not "counter += 1").

SYNC vs ASYNC COMMITS
    --commit-mode sync   blocks until the broker confirms. Safe, slower.
    --commit-mode async  returns immediately; the result arrives via callback.
                         Faster, but a failed commit is silent unless you watch
                         the callback - which is why CommitTracker exists.

HOW TO SEE IT WORK
    Run this script TWICE with the same group ID. The second run starts after
    the offsets the first one committed. That resume is the observable proof
    that committing did something.
================================================================================
"""

from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer

from demo02_producer_common import TOPIC_NAME, write_json_report
from demo03_consumer_common import (
    AssignmentTracker,
    CommitTracker,              # NEW vs 03A: watches async commit outcomes
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

    # The choice this demo is built around. Note "none" is deliberately NOT
    # offered here - the point of 03B is to commit.
    parser.add_argument("--commit-mode", choices=["sync", "async"], default="sync")

    # Only 4 by default: small enough to run repeatedly and compare offsets
    # between runs without wading through output.
    parser.add_argument("--max-messages", type=int, default=4)

    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--idle-timeout", type=float, default=8.0)
    args = parser.parse_args()

    # STABLE BY DESIGN. The resume behavior only exists if the group ID stays
    # the same across runs - a new ID means a new, empty history.
    group_id = args.group_id or default_group_id("demo03b-resume")

    # Collects async commit results. Harmless in sync mode.
    commits = CommitTracker()

    config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset="earliest",

        # ---- THE KEY DIFFERENCE FROM DEMO 03A -------------------------
        # Auto-commit OFF. Nothing is saved unless this code says so.
        #
        # This also causes the shared module to set enable.auto.offset.store =
        # False, meaning even RECEIVING a message no longer implicitly marks it
        # as progress. Both switches together are what make the application the
        # sole authority on what "done" means.
        enable_auto_commit=False,

        client_id="msds682-demo03b-offset-consumer",

        # The async-commit result callback. Without it, a failed async commit
        # would be completely silent - and the next run would quietly re-read
        # messages you believed were committed.
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

            # PASSING commit_mode IS WHAT ACTIVATES COMMITTING. Inside
            # consume_records(), each message is decoded and validated BEFORE
            # consumer.commit() is called - the ordering rule from the header,
            # enforced in the shared code rather than restated here.
            commit_mode=args.commit_mode,
        )

        # consume_records() decodes and validates before requesting each commit.
        assert_expected_topic(result.records)

    finally:
        # Same discipline as 03A. In manual-commit mode close() does NOT
        # silently commit anything you did not ask it to - which is exactly the
        # control you opted into.
        consumer.close()

    report = {
        "demo": "demo03b_confluent_offsets_commit",
        "topic": TOPIC_NAME,

        # Recorded prominently because it is the variable that determines what
        # the next run will see.
        "group_id": group_id,
        "commit_mode": args.commit_mode,

        "consumed": len(result.records),

        # How many commits we ASKED for. Compare with the two fields below to
        # see how many were confirmed - they should agree on a healthy run.
        "commit_requests": result.commit_requests,

        # Sync commits return their results inline...
        "synchronous_commit_results": result.synchronous_commit_results,

        # ...async commits deliver theirs later, via the callback.
        "asynchronous_commit_acknowledgements": commits.acknowledged,
        "asynchronous_commit_failures": commits.failed,

        "stop_reason": result.stop_reason,
        "connection": safe_consumer_config_report(config),
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,
        "records": result.records,

        # An instruction embedded in the evidence, pointing at the experiment
        # that makes committing visible.
        "next_step": "Run again with the same group ID to resume after committed offsets.",
    }

    output_file = write_json_report(args.run_id, "demo03b_confluent_offsets_commit", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # FAIL LOUDLY ON A FAILED ASYNC COMMIT. This is the safety net that makes
    # async commits usable: without it, the script would exit 0 having consumed
    # messages whose progress was never actually saved - and the silent
    # re-reading would only show up on the next run.
    if commits.failed:
        raise SystemExit("At least one asynchronous offset commit failed.")

    return report


if __name__ == "__main__":
    main()
