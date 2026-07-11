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
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(count, seed)
    start = time.perf_counter()
    for event in events:
        # Async default: enqueue all messages first.
        producer.produce(TOPIC_NAME, key=event_key(event), value=serialize_event(event), callback=tracker.callback)
        producer.poll(0)
    remaining = producer.flush(flush_timeout)
    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "strategy": "async",
        "attempted": count,
        "delivered": len(tracker.delivered),
        "failed": len(tracker.failed),
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "messages_per_sec": round(count / elapsed, 2),
    }


def run_sync_style(config: dict[str, str], count: int, seed: int, flush_timeout: float) -> dict:
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(count, seed)
    start = time.perf_counter()
    remaining = 0
    for event in events:
        # Sync-style teaching simplification: wait after each produce.
        producer.produce(TOPIC_NAME, key=event_key(event), value=serialize_event(event), callback=tracker.callback)
        remaining = producer.flush(flush_timeout)
    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "strategy": "sync_style_flush_each_message",
        "attempted": count,
        "delivered": len(tracker.delivered),
        "failed": len(tracker.failed),
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "messages_per_sec": round(count / elapsed, 2),
    }


def main() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02c")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()
    rows = [
        run_async(config, args.count, args.seed, args.flush_timeout),
        run_sync_style(config, args.count, args.seed, args.flush_timeout),
    ]
    report = {
        "demo": "demo02c_confluent_async_sync_compare",
        "topic": TOPIC_NAME,
        "connection": safe_config_report(config),
        "rows": rows,
        "note": "sync_style is a teaching simplification; confluent-kafka produce() is asynchronous by default.",
    }
    output_file = write_json_report(args.run_id, "demo02c_confluent_async_sync_compare", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if any(row["failed"] or row["remaining_after_flush"] for row in rows):
        raise SystemExit("Some messages were not delivered.")
    return report


if __name__ == "__main__":
    main()
