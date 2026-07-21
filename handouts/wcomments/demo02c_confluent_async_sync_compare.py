"""
================================================================================
DEMO 02C - BENCHMARK: ASYNC vs SYNC-STYLE  (annotated tutorial copy)
================================================================================

READ demo02a AND demo02b FIRST. This demo introduces no new Kafka calls at all -
it simply runs both previous patterns back to back and measures them.

WHAT THIS DEMO TEACHES
    Two things, and the second is arguably more important than the first:

    1. HOW MUCH FASTER async is (spoiler: ~200x in the course benchmark).
    2. HOW TO MEASURE IT HONESTLY. Anyone can print a stopwatch value. Making a
       comparison that actually supports a conclusion requires deliberate care.

THE THREE RULES OF A FAIR COMPARISON, all visible in the code below:

    RULE 1 - IDENTICAL DATA.
        Both strategies use the same seed, so both send byte-for-byte identical
        payloads. Message size affects throughput, so different data would mean
        a timing difference might reflect the DATA rather than the STRATEGY.

    RULE 2 - A FRESH CLIENT EACH TIME.
        Each run_* function constructs its own Producer. If they shared one, the
        second strategy would inherit an already-warmed connection (TLS + SASL
        + metadata already done) and look artificially fast.

    RULE 3 - TIME THROUGH COMPLETED DELIVERY.
        The timer stops only AFTER flush() returns. Stopping earlier would
        measure how fast you can fill a queue - which is nearly instant and
        completely meaningless.

A NOTE ON HONESTY IN THE REPORT
    The report includes an explicit "note" field stating that sync_style is a
    teaching simplification. That matters: this benchmark does not show that
    "Kafka is slow" or that some library is inferior. It shows the cost of one
    specific choice - where you put flush() - and the write-up says so.
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
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)


def run_async(config: dict[str, str], count: int, seed: int, flush_timeout: float) -> dict:
    """Run the DEMO 02B pattern and return one row of measurements.

    Note the shape of this function: it takes CONFIG (not a producer) and builds
    its own client. That is Rule 2 above - a fresh, cold client per strategy so
    neither benefits from the other's warm connection.
    """
    # A brand-new client: fresh TCP connection, TLS handshake, SASL auth, and
    # metadata fetch. This startup cost lands entirely inside the timed region,
    # which is why the FIRST batch of any run is measurably slower than later
    # ones - a real effect you can see in the benchmark output.
    producer = Producer(config)
    tracker = DeliveryTracker()

    # Same (count, seed) as run_sync_style below => identical events. Rule 1.
    events = make_trip_events(count, seed)

    start = time.perf_counter()

    for event in events:
        # The async pattern: queue everything, never block inside the loop.
        producer.produce(TOPIC_NAME, key=event_key(event), value=serialize_event(event), callback=tracker.callback)
        # Serve completed callbacks without blocking (see Demo 02B for detail).
        producer.poll(0)

    # ONE blocking wait, after the loop. The timer is still running - Rule 3.
    remaining = producer.flush(flush_timeout)

    elapsed = max(time.perf_counter() - start, 0.000001)

    return {
        "strategy": "async",
        "attempted": count,
        "delivered": len(tracker.delivered),

        # Note: len() here, whereas the single-demo scripts embedded the full
        # list. In a comparison table you want a COUNT, not a wall of errors.
        "failed": len(tracker.failed),

        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),

        # THROUGHPUT: the headline number. Dividing by elapsed normalizes for
        # message count, so runs of different sizes remain comparable.
        "messages_per_sec": round(count / elapsed, 2),
    }


def run_sync_style(config: dict[str, str], count: int, seed: int, flush_timeout: float) -> dict:
    """Run the DEMO 02A pattern and return one row of measurements.

    Compare this function line by line with run_async above. They are identical
    except for the placement of flush() and the absence of poll(). That is the
    entire experiment.
    """
    producer = Producer(config)
    tracker = DeliveryTracker()

    # Same arguments as run_async => the exact same event objects. Rule 1.
    events = make_trip_events(count, seed)

    start = time.perf_counter()
    remaining = 0

    for event in events:
        producer.produce(TOPIC_NAME, key=event_key(event), value=serialize_event(event), callback=tracker.callback)

        # THE ONLY REAL DIFFERENCE: flush INSIDE the loop.
        #
        # No poll(0) is needed here because flush() already services callbacks -
        # and since we flush after every message, there is never a backlog.
        #
        # Each iteration now costs a full network round trip. At roughly 78 ms
        # per round trip to a cloud broker, 2,000 messages take about 2.5
        # minutes, versus under a second for the async path.
        remaining = producer.flush(flush_timeout)

    elapsed = max(time.perf_counter() - start, 0.000001)

    return {
        # The label spells out the mechanism rather than just saying "sync",
        # because there is no truly synchronous send in this library.
        "strategy": "sync_style_flush_each_message",

        "attempted": count,
        "delivered": len(tracker.delivered),
        "failed": len(tracker.failed),
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "messages_per_sec": round(count / elapsed, 2),
    }


def main() -> dict:
    """Run both strategies over identical data and report both rows."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02c")

    # The default of 4 keeps the teaching demo fast. The assignment version of
    # this benchmark uses 2,000 per strategy, which is where the difference
    # becomes dramatic and where per-run noise averages out.
    parser.add_argument("--count", type=int, default=4)

    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()

    # BOTH STRATEGIES, SAME ARGUMENTS. Passing identical (count, seed) to each
    # is what enforces Rule 1 at the call site.
    #
    # Both write to the SAME topic, so the topic ends up holding two copies of
    # each logical event. That is expected and harmless here: Kafka has no
    # notion of duplicates, and we are measuring producer behavior, not building
    # a clean dataset.
    rows = [
        run_async(config, args.count, args.seed, args.flush_timeout),
        run_sync_style(config, args.count, args.seed, args.flush_timeout),
    ]

    report = {
        "demo": "demo02c_confluent_async_sync_compare",
        "topic": TOPIC_NAME,
        "connection": safe_config_report(config),

        # A list of per-strategy rows - the natural shape for a comparison, and
        # the same shape the assignment version writes to CSV for plotting.
        "rows": rows,

        # HONEST CAVEAT, stored with the data rather than left to a README.
        # A benchmark result without its caveat invites misreading.
        "note": "sync_style is a teaching simplification; confluent-kafka produce() is asynchronous by default.",
    }

    output_file = write_json_report(args.run_id, "demo02c_confluent_async_sync_compare", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # A benchmark is only trustworthy if BOTH runs actually completed. If either
    # strategy failed to deliver everything, the timings are meaningless and the
    # script must not report success.
    #
    # any(...) with a generator: True if any row has failures or leftovers.
    if any(row["failed"] or row["remaining_after_flush"] for row in rows):
        raise SystemExit("Some messages were not delivered.")

    return report


if __name__ == "__main__":
    main()
