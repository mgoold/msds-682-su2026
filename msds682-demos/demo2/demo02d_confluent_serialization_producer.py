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
    parser.add_argument("--run-id", default="lec2-demo02d")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()
    producer = Producer(config)
    tracker = DeliveryTracker()
    events = make_trip_events(args.count, args.seed)

    serialized_preview = serialize_event(events[0]).decode("utf-8") if events else ""
    start = time.perf_counter()
    for event in events:
        value_bytes = serialize_event(event)
        # Explicit serialization step: Pydantic model -> JSON string -> UTF-8 bytes.
        producer.produce(
            topic=TOPIC_NAME,
            key=event_key(event),
            value=value_bytes,
            callback=tracker.callback,
        )
        producer.poll(0)
    remaining = producer.flush(args.flush_timeout)
    elapsed = max(time.perf_counter() - start, 0.000001)

    report = {
        "demo": "demo02d_confluent_serialization_producer",
        "topic": TOPIC_NAME,
        "attempted": len(events),
        "delivered": len(tracker.delivered),
        "failed": tracker.failed,
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "connection": safe_config_report(config),
        "sample_python_object": event_dict(events[0]) if events else {},
        "sample_serialized_value": serialized_preview,
        "serialized_type": "UTF-8 JSON bytes",
    }
    output_file = write_json_report(args.run_id, "demo02d_confluent_serialization_producer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")
    return report


if __name__ == "__main__":
    main()
