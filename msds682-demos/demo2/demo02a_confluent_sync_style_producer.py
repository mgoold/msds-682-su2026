from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer

from demo02_producer_common import (
    TOPIC_NAME,
    DeliveryTracker,
    event_dict,
    event_key,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)


def main() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02a")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(args.count, args.seed)

    start = time.perf_counter()
    remaining = 0
    for event in events:
        # Sync-style teaching simplification: produce one message, then wait.
        producer.produce(
            topic=TOPIC_NAME,
            key=event_key(event),
            value=serialize_event(event),
            callback=tracker.callback,
        )
        remaining = producer.flush(args.flush_timeout)

    elapsed = max(time.perf_counter() - start, 0.000001)
    report = {
        "demo": "demo02a_confluent_sync_style_producer",
        "producer_mode": "sync_style_flush_each_message",
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
    output_file = write_json_report(args.run_id, "demo02a_confluent_sync_style_producer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")
    return report


if __name__ == "__main__":
    main()
