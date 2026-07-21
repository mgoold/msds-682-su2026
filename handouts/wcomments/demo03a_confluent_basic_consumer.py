"""
================================================================================
DEMO 03A - THE BASIC CONSUMER  (annotated tutorial copy)
================================================================================

READ demo03_consumer_common.py FIRST - the poll loop, group IDs, and config
helpers used here are all defined and explained there.

WHAT THIS DEMO TEACHES
    The minimal, complete shape of a Kafka consumer:

        1. build config (with a GROUP ID)
        2. create the Consumer
        3. subscribe() to a topic
        4. poll() in a loop, decoding and validating each message
        5. close() - always, even on failure

    It reads messages that Demo 02 produced. Same cluster, same topic, opposite
    direction.

THE MENTAL MODEL SHIFT FROM PRODUCING
    Producing is fire-and-confirm: hand over bytes, learn later whether they
    landed. Consuming is a PULL LOOP: you repeatedly ask "anything for me?" and
    Kafka answers with a message or with nothing.

    Crucially, READING DOES NOT CONSUME. The message is not removed, and other
    consumer groups still see it. All that changes is YOUR group's bookmark -
    its committed offset. The log is immutable; you are just moving a cursor
    through it.

WHAT TO TRY AFTER READING THIS
    Run it twice with the same group ID. The second run starts where the first
    stopped, because auto-commit saved the position. Then run it with
    --group-id something-new and watch it read from the beginning again.
================================================================================
"""

from __future__ import annotations

import argparse
import json

# THE CONSUMER CLASS. Constructing one joins a consumer group and starts a
# background thread that maintains the group membership heartbeat.
from confluent_kafka import Consumer

# Reused from the producer side: the same topic constant and the same report
# writer. Sharing them guarantees we read exactly what Demo 02 wrote.
from demo02_producer_common import TOPIC_NAME, write_json_report

from demo03_consumer_common import (
    AssignmentTracker,          # observes rebalances (on_assign / on_revoke)
    assert_expected_topic,      # safety check on where records came from
    consume_records,            # THE POLL LOOP
    default_group_id,           # builds a stable group ID
    require_consumer_config,    # credentials + consumer settings, or exit
    safe_consumer_config_report,
)


def main() -> dict:
    """Consume a bounded set of Demo 02 events with the standard poll loop."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03a")

    # Lets you override the group ID to get a fresh reading history on demand.
    parser.add_argument("--group-id")

    # BOUND 1: stop after this many messages, so the demo terminates.
    parser.add_argument("--max-messages", type=int, default=8)

    # How long each individual poll() waits for data before returning None.
    # Small values keep the loop responsive; this is NOT a total time limit.
    parser.add_argument("--poll-timeout", type=float, default=1.0)

    # BOUND 2: stop after this many seconds with no messages arriving - i.e.
    # we have drained the topic.
    parser.add_argument("--idle-timeout", type=float, default=8.0)

    args = parser.parse_args()

    # ---- THE GROUP ID: the most consequential choice in this script ----
    # Deliberately STABLE by default (no run-id mixed in), because that is what
    # makes the "run twice and watch it resume" behavior observable. If this
    # changed every run, every run would look like a first run.
    group_id = args.group_id or default_group_id("demo03a-basic")

    config = require_consumer_config(
        group_id=group_id,

        # If this group has NO saved position, start at the oldest message.
        # If it DOES have one, this setting is ignored and we resume there.
        auto_offset_reset="earliest",

        # AUTO-COMMIT ON (the easy mode). The client periodically saves your
        # position in the background, and close() commits the latest stored
        # offsets on the way out.
        #
        # The trade-off, made explicit in Demo 03B: auto-commit can save a
        # position for a message you have not finished processing, so a crash
        # can lose work. Fine for reading; not fine for side effects.
        enable_auto_commit=True,

        client_id="msds682-demo03a-basic-consumer",
    )

    assignment = AssignmentTracker()

    # CREATING THE CONSUMER: joins the group and starts heartbeating so the
    # broker knows this member is alive. No messages are fetched yet.
    consumer = Consumer(config)

    try:
        # ---- SUBSCRIBE ------------------------------------------------
        # Note it takes a LIST - one consumer can follow several topics.
        #
        # subscribe() does not immediately give you partitions. It declares
        # interest; the group coordinator then runs a rebalance and assigns
        # partitions among the members. The on_assign callback fires when that
        # completes, which is why nothing can be read for a moment after this.
        #
        # (There is also assign(), which takes explicit partitions and skips
        # groups entirely. subscribe() is what you normally want.)
        consumer.subscribe(
            [TOPIC_NAME],
            on_assign=assignment.on_assign,   # "you now own these partitions"
            on_revoke=assignment.on_revoke,   # "you are losing these partitions"
        )

        # ---- THE POLL LOOP --------------------------------------------
        # All the real work lives in consume_records() in the shared module:
        # poll, skip None, check errors, decode, validate, append.
        #
        # No commit_mode is passed, so it defaults to "none" - this demo does
        # not commit explicitly, relying on auto-commit instead.
        result = consume_records(
            consumer,
            max_messages=args.max_messages,
            poll_timeout=args.poll_timeout,
            idle_timeout=args.idle_timeout,
        )

        # Sanity check: everything really came from the expected topic.
        assert_expected_topic(result.records)

    finally:
        # ---- ALWAYS CLOSE, EVEN ON FAILURE ----------------------------
        # `finally` guarantees this runs whether the try block succeeded, raised,
        # or was interrupted with Ctrl-C.
        #
        # close() does three important things:
        #   1. commits final offsets (when auto-commit is enabled), so the next
        #      run resumes correctly;
        #   2. LEAVES THE GROUP promptly, so Kafka can rebalance immediately
        #      instead of waiting for the session timeout (~10s) to notice a
        #      dead member;
        #   3. releases sockets and threads.
        #
        # Skipping close() is why a restarted consumer sometimes appears to hang:
        # the group is waiting to time out the previous, apparently-alive member.
        consumer.close()

    report = {
        "demo": "demo03a_confluent_basic_consumer",
        "topic": TOPIC_NAME,
        "requested_max_messages": args.max_messages,
        "consumed": len(result.records),

        # WHY the loop ended - genuinely useful, not decoration:
        #   "max_messages" -> we hit our limit; more may still be waiting
        #   "idle_timeout" -> the topic is drained; we are caught up
        "stop_reason": result.stop_reason,

        "connection": safe_consumer_config_report(config),

        # Rebalance evidence. Run a second copy of this script at the same time
        # with the same group ID and these lists become genuinely interesting:
        # you will see partitions revoked from one member and assigned to another.
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,

        # The decoded, validated messages themselves.
        "records": result.records,
    }

    output_file = write_json_report(args.run_id, "demo03a_confluent_basic_consumer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    return report


if __name__ == "__main__":
    main()
