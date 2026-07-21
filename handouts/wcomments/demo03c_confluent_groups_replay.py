"""
================================================================================
DEMO 03C - CONSUMER GROUPS AND REPLAY  (annotated tutorial copy)
================================================================================

READ demo03a AND demo03b FIRST.

WHAT THIS DEMO TEACHES - two distinct things that are easy to confuse:

    1. CONSUMER GROUPS: how several processes share the work of reading one
       topic, and what happens when membership changes (rebalancing).

    2. REPLAY: how to deliberately re-read a topic from the beginning - and why
       Kafka makes you ask for that explicitly rather than letting it happen by
       accident.

HOW GROUPS DISTRIBUTE WORK

    Topic with 3 partitions          Group "analytics"
    ┌────────────┐
    │ partition 0│ ───────────────▶  member A
    │ partition 1│ ───────────────▶  member A
    │ partition 2│ ───────────────▶  member B
    └────────────┘

    The rules:
      - Within ONE group, each partition goes to exactly ONE member. This is
        how you scale: add members, and the partitions redistribute.
      - You cannot usefully run more members than partitions. Extra members sit
        idle with nothing assigned - which is why partition count sets your
        maximum consumer parallelism.
      - DIFFERENT groups are completely independent. Each has its own committed
        offsets and each sees EVERY message. That is how one topic feeds an
        analytics job, an alerting service, and an archiver simultaneously.

    TRY IT: run this script in two terminals at once with the same group ID and
    different --member-id values, and watch the on_assign/on_revoke output as
    Kafka rebalances the partitions between them.

THE TWO MODES IN THIS SCRIPT

    NORMAL (default):
        auto_offset_reset="latest", auto-commit ON. Respects group progress;
        a brand-new group sees only messages produced from now on. This is
        ordinary group behavior.

    REPLAY (--force-beginning):
        auto_offset_reset="earliest", auto-commit OFF, AND on_assign explicitly
        overrides every assigned partition to OFFSET_BEGINNING. Re-reads
        everything, and deliberately does NOT commit, so it leaves the group's
        real progress untouched.

WHY REPLAY NEEDS AN EXPLICIT OVERRIDE
    Because auto.offset.reset alone CANNOT do it. That setting only applies
    when a group has no committed offset at all. Once a group has progress,
    Kafka resumes from it and ignores the setting entirely. To move an existing
    position you must set the offsets yourself and re-assign - see
    AssignmentTracker.on_assign in the shared module.

    That friction is intentional. Replaying a whole topic is expensive, and if
    your processing is not idempotent it is harmful. Kafka makes it a decision,
    not a default.
================================================================================
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

    # A label distinguishing concurrent members of the SAME group. It feeds
    # client.id, so broker logs and the on_assign output tell you which process
    # got which partitions. It does not affect assignment - it just makes the
    # experiment readable.
    parser.add_argument("--member-id", default="member-a")

    # THE MODE SWITCH. action="store_true" makes it a flag: absent -> False,
    # present -> True.
    parser.add_argument("--force-beginning", action="store_true")

    # Higher than the other demos because in group mode you may be sharing
    # partitions and want to see a reasonable sample.
    parser.add_argument("--max-messages", type=int, default=50)

    # Shorter poll timeout keeps the loop responsive during a rebalance.
    parser.add_argument("--poll-timeout", type=float, default=0.5)

    # A hard wall-clock bound, so a group-mode run that never receives anything
    # still terminates.
    parser.add_argument("--run-seconds", type=float, default=20.0)

    args = parser.parse_args()

    group_id = args.group_id or default_group_id("demo03c-shared-group")

    # ---- THE TWO CONTRACTS, SELECTED BY ONE FLAG ----------------------
    #
    # NORMAL MODE -> "latest":
    #   A new group starts at the END and sees only new messages. This is the
    #   sensible default for a live service: on first start, do not reprocess
    #   history you were never responsible for.
    #
    # REPLAY MODE -> "earliest":
    #   Combined with the OFFSET_BEGINNING override in on_assign. Strictly, the
    #   override is what forces the replay; "earliest" is belt-and-braces for
    #   the case where the group has no committed offsets at all.
    auto_offset_reset = "earliest" if args.force_beginning else "latest"

    config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,

        # ---- WHY REPLAY MODE DISABLES AUTO-COMMIT ---------------------
        # `enable_auto_commit=not args.force_beginning`
        #
        #   normal mode  -> True:  behave like a real group member and save progress
        #   replay mode  -> False: read everything, save NOTHING
        #
        # This is the thoughtful part of the design. A replay is a diagnostic
        # activity - you are inspecting history. If it committed, it would
        # overwrite the group's real position and the next normal run would
        # re-read everything too. Replay must be side-effect free.
        enable_auto_commit=not args.force_beginning,

        # The member ID makes concurrent members distinguishable in logs.
        client_id=f"msds682-demo03c-{args.member_id}",
    )

    # PASSING force_beginning INTO THE TRACKER is what arms the replay. Inside
    # on_assign, if this is True, every assigned partition's offset is set to
    # OFFSET_BEGINNING and re-assigned - overriding whatever position the group
    # had saved.
    assignment = AssignmentTracker(force_beginning=args.force_beginning)

    consumer = Consumer(config)

    try:
        # The on_assign callback is doing double duty here: it reports the
        # rebalance for the group demonstration, AND performs the offset
        # override for the replay demonstration.
        consumer.subscribe(
            [TOPIC_NAME],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )

        result = consume_records(
            consumer,
            max_messages=args.max_messages,
            poll_timeout=args.poll_timeout,

            # Both bounds are set to run_seconds. In normal ("latest") mode the
            # topic is often quiet, so without a wall-clock bound the script
            # could sit waiting indefinitely for messages that never come.
            idle_timeout=args.run_seconds,
            run_timeout=args.run_seconds,
        )

        assert_expected_topic(result.records)

    finally:
        # Especially important here: a member that leaves without close() keeps
        # its partitions locked until the session times out, so the OTHER member
        # in your two-terminal experiment would stall for ~10 seconds.
        consumer.close()

    report = {
        "demo": "demo03c_confluent_groups_replay",

        # Records which contract this run used, so the evidence is unambiguous.
        "mode": "force_beginning_replay" if args.force_beginning else "consumer_group",

        "topic": TOPIC_NAME,
        "group_id": group_id,
        "member_id": args.member_id,
        "consumed": len(result.records),
        "stop_reason": result.stop_reason,
        "connection": safe_consumer_config_report(config),

        # THE INTERESTING FIELDS IN THIS DEMO. With two concurrent members you
        # can watch partitions get revoked from one and assigned to the other -
        # a rebalance, captured as data.
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
