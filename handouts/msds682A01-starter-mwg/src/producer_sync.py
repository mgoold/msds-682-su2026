"""Demo 02A assignment program: produce one message, then flush."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from producer_common import (
    DEFAULT_SEED,
    DeliveryTracker,
    TripEvent,
    event_dict,
    event_key,
    get_topic_name,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_file,
)


def run_sync_style(
    producer: Any,
    topic: str,
    events: list[TripEvent],
    flush_timeout: float,
) -> dict[str, Any]:
    """Send each event and wait for its delivery before sending the next."""

    tracker = DeliveryTracker()
    remaining = 0
    start = time.perf_counter()

    # ==================== CODE START HERE ====================
    # TODO: For every event, call produce() with topic, stable key, serialized
    # value, and tracker.callback; then call flush() inside the loop.
    raise NotImplementedError("Complete the sync-style producer loop")
    # ===================== CODE ENDS HERE =====================

    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "producer_mode": "sync_style_flush_each_message",
        "topic": topic,
        "attempted": len(events),
        "delivered": tracker.delivered_count,
        "failed": tracker.failed_count,
        "failure_messages": tracker.failed_messages[:10],
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "sample_value": event_dict(events[0]) if events else {},
        "delivery_samples": tracker.delivery_samples,
    }


def main() -> dict[str, Any]:
    """Parse arguments, run Demo 02A, and write secret-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="assignment1")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count must be positive")

    config = require_producer_config()
    topic = get_topic_name()
    report = run_sync_style(
        Producer(config),
        topic,
        make_trip_events(args.count, args.seed),
        args.flush_timeout,
    )
    report.update(
        {
            "demo": "demo02a_confluent_sync_style_producer",
            "run_id": args.run_id,
            "connection": safe_config_report(config, topic),
        }
    )
    output_path = write_json_file(Path("evidence/demo02a_report.json"), report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_path}")
    if report["failed"] or report["remaining_after_flush"]:
        raise SystemExit("Some messages were not delivered.")
    return report


if __name__ == "__main__":
    main()
