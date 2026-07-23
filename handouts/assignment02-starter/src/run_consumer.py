"""Run first, resume, or replay phases with visible group semantics."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import Consumer

from cloud import make_avro_deserializer
from config import (
    ConnectionConfigError,
    RESULTS_DIR,
    base_group_id,
    group_id_for_run,
    kafka_config,
    registry_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    topic_name,
    validate_run_id,
    write_json_report,
)
from consumer_runtime import AssignmentTracker, JsonlWriter, consume_bounded

Phase = Literal["first", "resume", "replay"]


@dataclass(frozen=True)
class PhaseSettings:
    """Resolved group, output, and replay behavior for one CLI phase."""

    group_id: str
    force_beginning: bool
    output_path: Path
    output_mode: str
    report_filename: str


def settings_for_phase(
    phase: Phase,
    *,
    run_id: str,
    base_group: str,
    results_dir: Path = RESULTS_DIR,
) -> PhaseSettings:
    """Keep phase-specific group and output rules in one testable function."""

    # ==================== CODE START HERE ====================
    # TODO:
    # - first: base run group, processed_events.jsonl, write mode;
    # - resume: same base run group, processed_events.jsonl, append mode;
    # - replay: separate replay group, replayed_events.jsonl, write mode, and
    #   force_beginning=True.
    raise NotImplementedError("Implement phase settings")
    # ===================== CODE ENDS HERE =====================


def main() -> dict[str, Any]:
    """Execute one bounded real-Confluent consumer phase."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["first", "resume", "replay"])
    parser.add_argument("--run-id", default="assignment2")
    parser.add_argument("--max-messages", type=int, required=True)
    parser.add_argument("--poll-timeout", type=float, default=0.5)
    parser.add_argument("--idle-timeout", type=float, default=12.0)
    parser.add_argument("--run-timeout", type=float, default=40.0)
    args = parser.parse_args()
    try:
        run_id = validate_run_id(args.run_id)
        if args.max_messages < 1:
            raise ValueError("max-messages must be positive")
        if min(args.poll_timeout, args.idle_timeout, args.run_timeout) <= 0:
            raise ValueError("all timeout values must be positive")
        selected = settings_for_phase(
            args.phase,
            run_id=run_id,
            base_group=base_group_id(),
        )
        base_config = kafka_config(
            client_id=f"msds682-assignment2-{args.phase}"
        )
        registry_configuration = registry_config()
    except (ValueError, ConnectionConfigError) as exc:
        parser.error(str(exc))

    topic = topic_name()
    consumer_config = {
        **base_config,
        "group.id": selected.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    assignment = AssignmentTracker(force_beginning=selected.force_beginning)
    consumer = Consumer(consumer_config)
    deserializer = make_avro_deserializer(registry_configuration)
    try:
        consumer.subscribe(
            [topic],
            on_assign=assignment.on_assign,
            on_revoke=assignment.on_revoke,
        )
        with JsonlWriter(selected.output_path, selected.output_mode) as writer:
            result = consume_bounded(
                consumer,
                deserializer,
                run_id=run_id,
                max_messages=args.max_messages,
                poll_timeout=args.poll_timeout,
                idle_timeout=args.idle_timeout,
                run_timeout=args.run_timeout,
                record_writer=writer.write,
            )
    finally:
        consumer.close()

    report = {
        "assignment": "assignment02",
        "phase": args.phase,
        "run_id": run_id,
        "topic": topic,
        "group_id": selected.group_id,
        "force_beginning": selected.force_beginning,
        "auto_offset_reset": consumer_config["auto.offset.reset"],
        "enable_auto_commit": consumer_config["enable.auto.commit"],
        "enable_auto_offset_store": consumer_config["enable.auto.offset.store"],
        "requested_max_messages": args.max_messages,
        "processed": len(result.records),
        "commit_requests": len(result.commit_results),
        "sequence_numbers": [
            record["event"]["sequence_number"] for record in result.records
        ],
        "trip_ids": [record["event"]["trip_id"] for record in result.records],
        "records": result.records,
        "commit_results": result.commit_results,
        "skipped_records_from_other_runs": result.skipped_other_runs,
        "stop_reason": result.stop_reason,
        "partition_assignments": assignment.assigned,
        "partition_revocations": assignment.revoked,
        "result_file": selected.output_path.relative_to(
            selected.output_path.parents[1]
        ).as_posix(),
        "kafka_connection": safe_kafka_config_report(consumer_config),
        "schema_registry": safe_registry_config_report(registry_configuration),
        "commit_rule": (
            "deserialize Avro -> validate TripEventV1 -> write result -> "
            "synchronous consumer offset commit"
        ),
    }
    output = write_json_report(selected.report_filename, report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if report["processed"] != args.max_messages:
        raise SystemExit(
            f"{args.phase} processed {report['processed']} of "
            f"{args.max_messages} required records"
        )
    if report["commit_requests"] != report["processed"]:
        raise SystemExit("Every processed record must have one commit request")
    return report


if __name__ == "__main__":
    main()
