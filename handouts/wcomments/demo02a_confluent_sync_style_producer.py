"""
================================================================================
DEMO 02A - THE "SYNC-STYLE" PRODUCER  (annotated tutorial copy)
================================================================================

READ demo02_producer_common.py FIRST. Everything imported below is defined and
explained there.

WHAT THIS DEMO TEACHES
    The simplest possible way to produce messages: send one, WAIT until it is
    confirmed delivered, then send the next.

        produce -> flush   (wait)
        produce -> flush   (wait)
        produce -> flush   (wait)

    This is called "sync-style" rather than "synchronous" because the
    confluent-kafka library has no truly synchronous send. produce() is ALWAYS
    asynchronous. We merely *simulate* synchronous behavior by immediately
    blocking on flush() after every single message, so only one message is ever
    in flight.

WHY LEARN A PATTERN THAT IS DELIBERATELY SLOW
    1. It is the easiest to reason about. One message at a time, in order, with
       an immediate answer for each. If something fails you know exactly which
       message failed and when.
    2. It makes the ASYNCHRONOUS pattern in Demo 02B legible by contrast - you
       cannot appreciate what batching buys you until you have seen its absence.
    3. It is a genuinely useful debugging tool. When a connection is broken,
       this pattern surfaces the error on message #1 rather than at the end.

THE COST
    Every message pays a full network round trip to the cloud before the next
    one is even queued. In the course benchmark this works out to roughly 78 ms
    per message - about 12 messages/second, versus ~2,600/second for the async
    pattern. That is a ~200x difference, and it comes entirely from where the
    flush() call sits.

WHAT THIS SCRIPT DOES END TO END
    1. read command-line arguments
    2. load credentials and build the Kafka client
    3. generate N deterministic fake trip events
    4. for each event: produce it, then flush (wait for delivery)
    5. write a secret-free JSON report and exit non-zero if anything failed
================================================================================
"""

from __future__ import annotations

import argparse   # command-line argument parsing (--count, --seed, ...)
import json       # to pretty-print the final report to the terminal
import time       # for measuring elapsed time

# THE KAFKA CLIENT ITSELF.
# `Producer` is the class that speaks the Kafka protocol. Constructing one
# opens connections and starts a BACKGROUND THREAD that does all network I/O.
# Merely importing it does nothing; the client comes alive at Producer(config).
from confluent_kafka import Producer

# Everything below is our own shared module - see demo02_producer_common.py.
from demo02_producer_common import (
    TOPIC_NAME,               # the one topic all four demos write to
    DeliveryTracker,          # collects per-message delivery results
    event_dict,               # event -> plain dict (for the report)
    event_key,                # event -> key bytes (decides the partition)
    make_trip_events,         # deterministic fake data generator
    require_producer_config,  # loads .env credentials or exits
    safe_config_report,       # connection summary with NO secrets
    serialize_event,          # event -> value bytes (the payload)
    write_json_report,        # saves the report to outputs/
)


def main() -> dict:
    """Run the sync-style producer once and return the report dict."""

    # -------------------------------------------------------------------
    # STEP 1: COMMAND-LINE ARGUMENTS
    # -------------------------------------------------------------------
    parser = argparse.ArgumentParser()

    # Names the output folder, so separate runs do not overwrite each other.
    parser.add_argument("--run-id", default="lec2-demo02a")

    # How many messages to send. The default of 4 is deliberately tiny: this
    # pattern is slow, and 4 messages is enough to demonstrate the behavior.
    parser.add_argument("--count", type=int, default=4)

    # The RNG seed for the event generator. Fixed by default so the data is
    # reproducible across runs (see make_trip_events in the common module).
    parser.add_argument("--seed", type=int, default=682)

    # How many SECONDS flush() will wait before giving up. If the broker is
    # unreachable, this bounds how long the script hangs.
    parser.add_argument("--flush-timeout", type=float, default=30.0)

    args = parser.parse_args()

    # -------------------------------------------------------------------
    # STEP 2: BUILD THE CLIENT AND THE DATA
    # -------------------------------------------------------------------

    # Read .env, validate that all five settings are present, or exit with a
    # message naming what is missing. Credentials never appear in source code.
    config = require_producer_config()

    # CREATING THE PRODUCER. This is the moment the script becomes "a producer".
    # Behind this one line: connection setup, TLS handshake, SASL authentication,
    # a metadata fetch to learn the cluster layout, and the start of a background
    # I/O thread. (This startup cost is why the first batch in the Demo 02C
    # benchmark is measurably slower than later ones.)
    producer = Producer(config)

    # Our callback collector. Nothing has happened yet; it is just an empty
    # container waiting for delivery results.
    tracker = DeliveryTracker()

    # Generate all the events UP FRONT, before timing starts, so that data
    # generation time is not counted as producer time.
    events = make_trip_events(args.count, args.seed)

    # -------------------------------------------------------------------
    # STEP 3: THE PRODUCE LOOP  <-- the heart of this demo
    # -------------------------------------------------------------------

    # perf_counter() is a high-resolution monotonic clock. Use it for measuring
    # DURATIONS; never use time.time(), which can jump if the system clock is
    # adjusted mid-run.
    start = time.perf_counter()

    # Initialized to 0 so it is defined even if `events` is empty. It will hold
    # flush()'s return value: the count of messages still undelivered.
    remaining = 0

    for event in events:
        # ---- (a) HAND THE MESSAGE TO THE CLIENT -----------------------
        # This does NOT send anything over the network. It:
        #   - validates the arguments,
        #   - computes the target partition from the key,
        #   - appends the record to an in-memory queue,
        #   - returns immediately (microseconds).
        # The background thread transmits it at some later moment.
        producer.produce(
            # WHICH TOPIC. Same constant in all four demos.
            topic=TOPIC_NAME,

            # THE KEY, as bytes. Determines the partition via
            # hash(key) % num_partitions, so all events sharing a trip_id stay
            # on one partition, in order.
            key=event_key(event),

            # THE VALUE, as bytes: the compact UTF-8 JSON payload.
            value=serialize_event(event),

            # THE DELIVERY CALLBACK. This function reference stays entirely on
            # your machine - it is never transmitted to Kafka. The library
            # invokes it later, once this message's fate is known.
            callback=tracker.callback,
        )

        # ---- (b) BLOCK UNTIL THAT MESSAGE IS RESOLVED -----------------
        # THIS LINE IS WHAT MAKES THE DEMO "SYNC-STYLE".
        #
        # flush() blocks until the producer's queue is empty - meaning every
        # message has either been delivered or definitively failed - and it
        # serves the pending delivery callbacks while it waits. So by the time
        # this returns, tracker.callback has already run for this message.
        #
        # Because flush() is INSIDE the loop, we wait for message N before
        # producing message N+1. Only one message is ever in flight. All of
        # librdkafka's batching and pipelining machinery is effectively
        # disabled, which is exactly why this is slow.
        #
        # RETURN VALUE: the number of messages STILL unresolved (0 on success).
        # It is reassigned each iteration, so after the loop it reflects the
        # final flush. A non-zero value means the timeout expired with work
        # still outstanding - see the note in the report section below.
        remaining = producer.flush(args.flush_timeout)

    # Guard against a zero or negative duration so later division is safe.
    elapsed = max(time.perf_counter() - start, 0.000001)

    # -------------------------------------------------------------------
    # STEP 4: BUILD THE EVIDENCE REPORT
    # -------------------------------------------------------------------
    # Everything here is chosen to be safe to commit and submit.
    report = {
        "demo": "demo02a_confluent_sync_style_producer",

        # Names the pattern explicitly so a reader of the JSON knows which
        # strategy produced these numbers.
        "producer_mode": "sync_style_flush_each_message",

        "topic": TOPIC_NAME,

        # ATTEMPTED vs DELIVERED vs FAILED - three different things:
        #   attempted = how many we handed to produce()
        #   delivered = how many callbacks reported success
        #   failed    = how many callbacks reported an error
        # They should satisfy: attempted == delivered + failed + remaining.
        "attempted": len(events),
        "delivered": len(tracker.delivered),
        "failed": tracker.failed,

        # REMAINING is subtly different from FAILED, and the distinction matters:
        #   failed    -> Kafka gave us a definitive answer: it did not work.
        #   remaining -> we never got an answer at all; the flush timed out.
        # A failed message is a RESOLVED message. A remaining one is an UNKNOWN.
        # That is why the exit check at the bottom tests both.
        "remaining_after_flush": remaining,

        "elapsed_seconds": round(elapsed, 6),

        # Connection metadata with booleans instead of credentials.
        "connection": safe_config_report(config),

        # One example payload so a reader can see the shape of the data.
        "sample_value": event_dict(events[0]) if events else {},

        # Only the first 10 delivery records. With a large --count, embedding
        # every delivery would flood the terminal and bloat the JSON file.
        # Ten is plenty to prove delivery happened.
        "delivered_messages": tracker.delivered[:10],
    }

    # -------------------------------------------------------------------
    # STEP 5: SAVE, PRINT, AND SET AN HONEST EXIT CODE
    # -------------------------------------------------------------------
    output_file = write_json_report(args.run_id, "demo02a_confluent_sync_style_producer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # FAIL LOUDLY. If any message failed OR any is still unaccounted for, exit
    # non-zero. This is what makes the script usable in automation: a shell or
    # CI system checks the exit code, and a silent partial failure would be
    # worse than a crash. Note the report file is written FIRST, so the evidence
    # survives even when the run is judged unsuccessful.
    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")

    return report


# Standard Python idiom: only run main() when this file is executed directly
# (`python demo02a_....py`), not when it is imported by another module.
if __name__ == "__main__":
    main()
