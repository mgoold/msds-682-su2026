"""Demo 02C assignment benchmark: compare async and sync-style delivery."""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from producer_common import (
    DEFAULT_SEED,
    DeliveryTracker,
    TripEvent,
    event_key,
    get_topic_name,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_file,
)


MINIMUM_MESSAGES = 2_000
REQUIRED_BATCH_SIZE = 500
CSV_COLUMNS = [
    "run_id",
    "strategy",
    "batch_index",
    "batch_message_count",
    "total_messages_so_far",
    "elapsed_seconds",
    "messages_per_second",
    "batch_delivered",
    "batch_failed",
    "remaining_after_flush",
]


def validate_benchmark_arguments(messages: int, batch_size: int) -> None:
    """Enforce the base-assignment benchmark size and complete batch rows."""

    if messages < MINIMUM_MESSAGES:
        raise ValueError(f"messages must be at least {MINIMUM_MESSAGES}")
    if batch_size != REQUIRED_BATCH_SIZE:
        raise ValueError(f"batch-size must be exactly {REQUIRED_BATCH_SIZE}")
    if messages % batch_size:
        raise ValueError("messages must be divisible by batch-size")


def run_strategy(
    producer: Any,
    topic: str,
    events: list[TripEvent],
    strategy: str,
    batch_size: int,
    flush_timeout: float,
    run_id: str,
) -> list[dict[str, Any]]:
    """Run one strategy and return one completed-delivery row per batch."""

    if strategy not in {"async", "sync_style"}:
        raise ValueError(f"Unknown strategy: {strategy}")
    if not events or len(events) % batch_size:
        raise ValueError("events must contain one or more complete batches")

    tracker = DeliveryTracker()
    rows: list[dict[str, Any]] = []

    # ==================== CODE START HERE ====================
    # TODO: Process events in batch_size slices. For async, produce and poll(0)
    # for every event, then flush once per batch. For sync_style, flush after
    # every event. Time each batch through completed delivery. Append one row
    # using every CSV_COLUMNS field and callback-count deltas for that batch.
    raise NotImplementedError("Complete run_strategy")
    # ===================== CODE ENDS HERE =====================

    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    """Write benchmark rows using the assignment's fixed CSV contract."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> list[dict[str, Any]]:
    """Run both strategies with identical events and write benchmark evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="assignment1")
    parser.add_argument("--messages", type=int, default=MINIMUM_MESSAGES)
    parser.add_argument("--batch-size", type=int, default=REQUIRED_BATCH_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, default=Path("results/producer_benchmark.csv"))
    args = parser.parse_args()
    validate_benchmark_arguments(args.messages, args.batch_size)

    config = require_producer_config()
    topic = get_topic_name()
    # One event list is intentionally reused so the logical payloads are equal.
    events = make_trip_events(args.messages, args.seed)
    rows: list[dict[str, Any]] = []
    for strategy in ("async", "sync_style"):
        rows.extend(
            run_strategy(
                Producer(config),
                topic,
                events,
                strategy,
                args.batch_size,
                args.flush_timeout,
                args.run_id,
            )
        )

    csv_path = write_csv(args.output, rows)
    config_path = write_json_file(
        Path("evidence/demo02c_config.json"),
        {
            "run_id": args.run_id,
            "messages_per_strategy": args.messages,
            "batch_size": args.batch_size,
            "seed": args.seed,
            "connection": safe_config_report(config, topic),
        },
    )
    expected_rows = 2 * (args.messages // args.batch_size)
    invalid_rows = [
        row
        for row in rows
        if row["batch_delivered"] != args.batch_size
        or row["batch_failed"] != 0
        or row["remaining_after_flush"] != 0
    ]
    if len(rows) != expected_rows or invalid_rows:
        raise SystemExit(
            f"Incomplete benchmark: expected {expected_rows} valid rows; "
            f"wrote {len(rows)} rows with {len(invalid_rows)} invalid rows."
        )
    print(f"Wrote {len(rows)} valid rows to {csv_path}")
    print(f"Wrote secret-free configuration to {config_path}")
    return rows


if __name__ == "__main__":
    main()
