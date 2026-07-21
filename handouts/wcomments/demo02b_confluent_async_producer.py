"""
================================================================================
DEMO 02B - THE ASYNCHRONOUS PRODUCER  (annotated tutorial copy)
================================================================================

READ demo02_producer_common.py FIRST, then demo02a (the slow version) - this
demo is best understood as "02A with the flush() moved".

WHAT THIS DEMO TEACHES
    The pattern you should actually use in production:

        produce, produce, produce, ...   (queue them all, do not wait)
        poll(0) along the way            (service completed callbacks)
        ONE final flush()                (wait once, at the end)

THE ONE-LINE DIFFERENCE FROM DEMO 02A
    02A:  flush() INSIDE the loop   -> one message in flight at a time
    02B:  flush() AFTER  the loop   -> hundreds in flight simultaneously

    That single change is worth roughly 200x in throughput. Nothing else about
    the two scripts differs in any meaningful way.

WHY IT IS SO MUCH FASTER
    produce() only queues a record. The background thread is free to gather
    many queued records into a single batched network request, compress it, and
    pipeline several such requests without waiting for each acknowledgement.
    Network latency gets paid CONCURRENTLY across many messages instead of
    serially, once per message.

    Think of it as the difference between mailing 500 letters one at a time -
    walking to the post office and back for each - versus filling a sack and
    making one trip.

THE TWO NEW RESPONSIBILITIES THIS PATTERN CREATES
    1. You MUST call poll(0) while producing, or delivery callbacks pile up
       unserviced and the internal queue can fill.
    2. You MUST call flush() exactly once before exiting, or messages still in
       flight when the process ends are simply LOST. This is the single most
       common Kafka producer bug.
================================================================================
"""

from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer

from demo02_producer_common import (
    TOPIC_NAME,
    DeliveryTracker,
    event_key,
    event_dict,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)


def main() -> dict:
    """Run the asynchronous producer once and return the report dict."""

    # -------------------------------------------------------------------
    # STEP 1: ARGUMENTS  (identical to Demo 02A)
    # -------------------------------------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02b")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    # -------------------------------------------------------------------
    # STEP 2: CLIENT AND DATA  (also identical to Demo 02A)
    # -------------------------------------------------------------------
    # Same seed as 02A means the SAME events - which is what makes any timing
    # comparison between the two demos meaningful.
    config = require_producer_config()
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(args.count, args.seed)

    # -------------------------------------------------------------------
    # STEP 3: THE PRODUCE LOOP  <-- where 02B diverges from 02A
    # -------------------------------------------------------------------
    start = time.perf_counter()

    for event in events:
        # ---- (a) QUEUE THE MESSAGE, DO NOT WAIT -----------------------
        # Byte-for-byte the same call as in Demo 02A. produce() has always been
        # asynchronous; 02A simply hid that by blocking immediately afterwards.
        producer.produce(
            topic=TOPIC_NAME,
            key=event_key(event),
            value=serialize_event(event),
            callback=tracker.callback,
        )

        # ---- (b) SERVE ANY CALLBACKS THAT ARE ALREADY READY -----------
        # WHAT poll(0) DOES: runs delivery callbacks that have already completed,
        # on THIS thread, and returns how many it handled.
        #
        # WHY THE ARGUMENT IS 0: that is the timeout in seconds. Zero means
        # "do not block" - handle whatever is ready right now and return
        # immediately, even if that is nothing. poll(1.0) would wait up to a
        # second, which would defeat the purpose here.
        #
        # WHY IT IS NEEDED AT ALL - two independent reasons:
        #
        #   1. CALLBACKS NEVER FIRE ON THEIR OWN. Results are produced by the
        #      background C thread and queued; they are handed to your Python
        #      code only during poll() or flush(). Without poll(0) here, no
        #      callback would run until the final flush().
        #
        #   2. THE QUEUE IS BOUNDED. librdkafka's send queue holds a finite
        #      number of messages (queue.buffering.max.messages, default about
        #      100,000). Completed-but-unserviced results also consume memory.
        #      On a long run, failing to poll leads to growing memory use and
        #      eventually a BufferError from produce().
        #
        # For only 4 messages none of this matters. At 2,000+ it does - which is
        # why the habit is established here, at small scale.
        producer.poll(0)

    # ---- (c) THE ONE MANDATORY WAIT -----------------------------------
    # OUTSIDE the loop, exactly once.
    #
    # At this instant, every message has been QUEUED but many are probably still
    # in flight. flush() blocks until each has reached a terminal state -
    # delivered or failed - serving the remaining callbacks as they complete.
    #
    # IF YOU DELETE THIS LINE the script still runs, prints a report, and exits
    # zero - while silently discarding whatever was still queued. The counts
    # would quietly under-report. There is no warning. This is THE classic
    # Kafka producer bug, and it is why every demo ends with a flush.
    #
    # Returns the number of messages still unresolved when it returned; 0 means
    # everything was accounted for.
    remaining = producer.flush(args.flush_timeout)

    elapsed = max(time.perf_counter() - start, 0.000001)

    # -------------------------------------------------------------------
    # STEP 4: REPORT  (same shape as 02A, for easy comparison)
    # -------------------------------------------------------------------
    report = {
        "demo": "demo02b_confluent_async_producer",

        # The mode label is the main difference from 02A's report. Comparing the
        # two JSON files side by side is the intended exercise: same topic, same
        # events, same counts - different elapsed_seconds.
        "producer_mode": "async",

        "topic": TOPIC_NAME,
        "attempted": len(events),
        "delivered": len(tracker.delivered),
        "failed": tracker.failed,
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "connection": safe_config_report(config),
        "sample_value": event_dict(events[0]) if events else {},

        # First 10 only: with a large --count, printing every delivery
        # would flood the terminal and bloat the JSON report.
        "delivered_messages": tracker.delivered[:10],
    }

    output_file = write_json_report(args.run_id, "demo02b_confluent_async_producer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # Same honest exit check as 02A: fail on either known failures or unknowns.
    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")

    return report


if __name__ == "__main__":
    main()
