"""Demo 04A: validate the application contract before Kafka.

Student focus: strict field types, timezone-aware event time, forbidden extra
fields, cross-field lifecycle rules, and derived consumer fields. This demo is
fully local and deterministic; it needs no Kafka or Schema Registry account.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from pydantic import ValidationError

from demo04_common import (
    TripEventV1,
    validate_run_id,
    validation_error_summary,
    write_json_report,
)


def validation_cases() -> list[dict[str, Any]]:
    """Return stable pass/fail cases that exercise the application contract."""

    valid_requested = {
        "trip_id": "trip_4100",
        "event_type": "trip_requested",
        "rider_id": "rider_410",
        "event_time": "2026-07-16T17:00:00Z",
        "zone": "north",
    }
    valid_completed = {
        "trip_id": "trip_4101",
        "event_type": "trip_completed",
        "rider_id": "rider_411",
        "event_time": "2026-07-16T17:05:00-07:00",
        "zone": "west",
        "driver_id": "driver_411",
        "fare": 27.5,
    }
    return [
        {"case_id": "valid_requested", "payload": valid_requested, "should_pass": True},
        {"case_id": "valid_completed", "payload": valid_completed, "should_pass": True},
        {
            "case_id": "naive_timestamp_rejected",
            "payload": {**valid_requested, "trip_id": "trip_4102", "event_time": "2026-07-16T17:00:00"},
            "should_pass": False,
        },
        {
            "case_id": "extra_field_rejected",
            "payload": {**valid_requested, "trip_id": "trip_4103", "unexpected": "drift"},
            "should_pass": False,
        },
        {
            "case_id": "string_fare_rejected",
            "payload": {**valid_completed, "trip_id": "trip_4104", "fare": "27.50"},
            "should_pass": False,
        },
        {
            "case_id": "negative_fare_rejected",
            "payload": {**valid_completed, "trip_id": "trip_4105", "fare": -1.0},
            "should_pass": False,
        },
        {
            "case_id": "completed_requires_fare",
            "payload": {key: value for key, value in valid_completed.items() if key != "fare"}
            | {"trip_id": "trip_4106"},
            "should_pass": False,
        },
        {
            "case_id": "requested_rejects_driver",
            "payload": {**valid_requested, "trip_id": "trip_4107", "driver_id": "driver_410"},
            "should_pass": False,
        },
    ]


def run_validation() -> list[dict[str, Any]]:
    """Validate every case through the same JSON boundary used by applications."""

    rows: list[dict[str, Any]] = []
    for case in validation_cases():
        try:
            # ====================================================================
            # KEY CONCEPT
            # Test every payload at the same JSON -> Pydantic boundary used by an
            # application. A plain Python dictionary does not prove the contract.
            # ====================================================================
            event = TripEventV1.model_validate_json(json.dumps(case["payload"]))
            actual_pass = True
            errors: list[dict[str, Any]] = []
            normalized = event.report_dict()
        except ValidationError as exc:
            actual_pass = False
            errors = validation_error_summary(exc)
            normalized = None

        rows.append(
            {
                "case_id": case["case_id"],
                "expected_pass": case["should_pass"],
                "actual_pass": actual_pass,
                "expectation_met": actual_pass == case["should_pass"],
                "normalized_event": normalized,
                "errors": errors,
            }
        )
    return rows


def main() -> dict[str, Any]:
    """Run local validation cases and write a secret-free report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04a")
    args = parser.parse_args()
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    cases = run_validation()
    report = {
        "demo": "demo04a_schema_validation",
        "purpose": "application/domain validation before serialization",
        "total_cases": len(cases),
        "expectations_met": sum(1 for row in cases if row["expectation_met"]),
        "cases": cases,
        "key_point": (
            "Pydantic enforces application meaning. Avro and Schema Registry do not replace these business rules."
        ),
    }
    output_file = write_json_report(args.run_id, "demo04a_schema_validation", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    if report["expectations_met"] != report["total_cases"]:
        raise SystemExit("At least one validation case did not match its expected result.")
    return report


if __name__ == "__main__":
    main()
