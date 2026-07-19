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
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02b")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(args.count, args.seed)

    start = time.perf_counter()
    for event in events:
        # produce() is asynchronous: it queues the message and returns quickly.
        producer.produce(
            topic=TOPIC_NAME,
            key=event_key(event),
            value=serialize_event(event),
            callback=tracker.callback,
        )
        # poll(0) lets the client serve delivery callbacks without blocking.
        producer.poll(0)

    # flush() is the explicit wait point before the script exits.
    remaining = producer.flush(args.flush_timeout)
    elapsed = max(time.perf_counter() - start, 0.000001)

    report = {
        "demo": "demo02b_confluent_async_producer",
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
    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")
    return report


if __name__ == "__main__":
    main()
