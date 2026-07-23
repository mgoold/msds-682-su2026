"""Demo 06D: prove same-group resume and explicit new-group replay."""

from __future__ import annotations

import argparse
from typing import Any

from confluent_demo_common import (
    consumer_group_id,
    validate_run_id,
    write_json_report,
)
from demo06_common import source_coordinates
from demo06c_confluent_stream_processor import run_processor


def validate_resume_replay(
    *,
    first: dict[str, Any],
    resume: dict[str, Any],
    replay: dict[str, Any],
) -> dict[str, Any]:
    """Verify the observable offset contract across three bounded passes."""

    first_coordinates = source_coordinates(first["records"])
    resume_coordinates = source_coordinates(resume["records"])
    replay_coordinates = source_coordinates(replay["records"])
    checks = {
        "same_group_resumed_without_reprocessing_first_batch": (
            set(first_coordinates).isdisjoint(resume_coordinates)
        ),
        "new_group_replayed_first_batch": replay_coordinates == first_coordinates,
        "base_group_reused": first["group_id"] == resume["group_id"],
        "replay_group_is_distinct": replay["group_id"] != first["group_id"],
    }
    if not all(checks.values()):
        raise AssertionError(f"Resume/replay contract failed: {checks}")
    return {
        "checks": checks,
        "first_coordinates": first_coordinates,
        "resume_coordinates": resume_coordinates,
        "replay_coordinates": replay_coordinates,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--messages-per-pass", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if not 1 <= args.messages_per_pass <= 25:
        parser.error("--messages-per-pass must be between 1 and 25")

    base_group = consumer_group_id("demo06d-resume", run_id)
    replay_group = consumer_group_id("demo06d-replay", run_id)
    shared = {
        "max_messages": args.messages_per_pass,
        "assignment_timeout": args.assignment_timeout,
        "idle_timeout": args.idle_timeout,
        "delivery_timeout": args.delivery_timeout,
        "partitions": args.partitions,
        "replication_factor": args.replication_factor,
        "report_demo_name": None,
    }

    first = run_processor(
        run_id=f"{run_id}-first",
        group_id=base_group,
        create_topics=args.create_topics,
        **shared,
    )
    resume = run_processor(
        run_id=f"{run_id}-resume",
        group_id=base_group,
        create_topics=False,
        **shared,
    )
    replay = run_processor(
        run_id=f"{run_id}-replay",
        group_id=replay_group,
        create_topics=False,
        **shared,
    )
    validation = validate_resume_replay(
        first=first,
        resume=resume,
        replay=replay,
    )

    report = {
        "demo": "06D",
        "run_id": run_id,
        "messages_per_pass": args.messages_per_pass,
        "base_group": base_group,
        "replay_group": replay_group,
        **validation,
        "interpretation": {
            "resume": (
                "The same consumer group starts after its committed input offsets."
            ),
            "replay": (
                "A new group has no committed position, so earliest is used."
            ),
            "output_duplicates": (
                "Replay intentionally republishes derived events. Stable output "
                "keys make the duplicate identity observable."
            ),
        },
    }
    path = write_json_report(run_id, "demo06d", report)
    print("Same-group resume and new-group replay checks passed")
    print(f"Secret-free report: {path}")


if __name__ == "__main__":
    main()
