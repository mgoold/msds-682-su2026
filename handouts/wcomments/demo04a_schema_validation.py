"""
================================================================================
DEMO 04A - APPLICATION VALIDATION, BEFORE KAFKA  (annotated tutorial copy)
================================================================================

READ demo04_common.py FIRST - especially the TripEventV1 model, whose rules this
demo exercises.

WHAT THIS DEMO TEACHES
    LAYER 1 of the two-layer schema story, in isolation: what your APPLICATION
    considers a valid event, enforced by pydantic.

    NO KAFKA. NO SCHEMA REGISTRY. NO CREDENTIALS. NO NETWORK.
    This runs entirely on your machine, which makes it the ideal place to
    understand validation without any infrastructure in the way.

THE SHAPE OF THE DEMO
    It is a table-driven test. A list of PAYLOADS, each labelled with whether it
    SHOULD pass, is fed through the model. The script then checks that reality
    matched the expectation for every case, and exits non-zero if not.

    That "expected vs actual" structure is worth copying. Asserting that invalid
    data is REJECTED is just as important as asserting valid data is accepted -
    a validator that accepts everything would pass a test suite that only ever
    tried valid input.

THE DISTINCTION THIS DEMO IS BUILT TO SHOW
    Some failures are SHAPE/TYPE violations, which Avro could also catch:
        - a string where a float belongs
        - an unknown extra field
    Others are BUSINESS RULES that Avro fundamentally cannot express:
        - "a completed trip must have a fare"
        - "a requested trip must NOT have a driver"

    The second category is why application validation never goes away, no matter
    how good your wire schema is. The STUDENT CHECKPOINT comment further down
    asks you to classify each failing case - that is the exercise.

WHY VALIDATE AT THE JSON BOUNDARY
    Note the code uses model_validate_json(json.dumps(payload)) rather than
    model_validate(payload). Round-tripping through actual JSON text reproduces
    the boundary a real application faces - data arriving from a network as
    text. Validating a hand-built Python dict is a weaker test, because a dict
    can hold types (like a real datetime) that JSON never could.
================================================================================
"""

from __future__ import annotations

import argparse
import json
from typing import Any

# The exception pydantic raises when data violates the model.
from pydantic import ValidationError

from demo04_common import (
    TripEventV1,               # the strict model under test
    validate_run_id,           # path-traversal-safe run ID check
    validation_error_summary,  # ValidationError -> JSON-safe rows
    write_json_report,
)


def validation_cases() -> list[dict[str, Any]]:
    """Return stable pass/fail cases that exercise the application contract.

    Each case is: an ID, a payload, and whether it SHOULD pass. Two valid
    baselines followed by six deliberate violations.
    """

    # ---- BASELINE 1: a valid REQUESTED event --------------------------
    # No driver_id and no fare - correct, because the trip has only been asked
    # for. The "Z" suffix makes the timestamp timezone-aware (UTC).
    valid_requested = {
        "trip_id": "trip_4100",
        "event_type": "trip_requested",
        "rider_id": "rider_410",
        "event_time": "2026-07-16T17:00:00Z",
        "zone": "north",
    }

    # ---- BASELINE 2: a valid COMPLETED event --------------------------
    # Has BOTH driver_id and fare, as the lifecycle rules require.
    # Note the offset "-07:00" rather than "Z": also timezone-aware, so also
    # valid. The model's normalize_event_time validator will convert it to UTC,
    # demonstrating normalization rather than rejection.
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

        # ---- FAILURE 1: NAIVE TIMESTAMP  [type/shape] -----------------
        # "2026-07-16T17:00:00" with no Z and no offset. 17:00 WHERE? The
        # AwareDatetime type rejects it. This is the kind of ambiguity that
        # produces off-by-hours bugs across regions, so it is refused at the
        # boundary rather than guessed at.
        {
            "case_id": "naive_timestamp_rejected",
            "payload": {**valid_requested, "trip_id": "trip_4102", "event_time": "2026-07-16T17:00:00"},
            "should_pass": False,
        },

        # ---- FAILURE 2: UNKNOWN FIELD  [type/shape] -------------------
        # "unexpected" is not in the model, and extra="forbid" rejects it.
        # Without that setting, pydantic would silently DROP the field - which
        # is exactly how schema drift hides. A producer starts sending a new
        # field, consumers quietly discard it, and nobody notices for months.
        {
            "case_id": "extra_field_rejected",
            "payload": {**valid_requested, "trip_id": "trip_4103", "unexpected": "drift"},
            "should_pass": False,
        },

        # ---- FAILURE 3: STRING WHERE A FLOAT BELONGS  [type/shape] ----
        # "27.50" as text rather than 27.50 as a number. Normally pydantic would
        # helpfully coerce this; strict=True on the model disables that.
        #
        # Why refuse a convenience? Because silent coercion hides a real defect
        # in the upstream producer, and one day a value arrives that cannot be
        # coerced - at 3 a.m., in production, far from where the bug lives.
        {
            "case_id": "string_fare_rejected",
            "payload": {**valid_completed, "trip_id": "trip_4104", "fare": "27.50"},
            "should_pass": False,
        },

        # ---- FAILURE 4: NEGATIVE FARE  [BUSINESS RULE] ----------------
        # THE CANONICAL EXAMPLE for the whole demo. -1.0 is a perfectly valid
        # floating-point number and a perfectly valid Avro "double". Avro would
        # encode it without complaint. Only the ge=0 constraint rejects it.
        {
            "case_id": "negative_fare_rejected",
            "payload": {**valid_completed, "trip_id": "trip_4105", "fare": -1.0},
            "should_pass": False,
        },

        # ---- FAILURE 5: COMPLETED WITHOUT A FARE  [BUSINESS RULE] -----
        # Built by copying valid_completed and REMOVING "fare" via the dict
        # comprehension, then overriding trip_id with the | merge operator.
        #
        # Avro cannot express this: it can mark fare optional or required, but
        # not "required only when event_type is trip_completed". Conditional,
        # cross-field logic lives in application code.
        {
            "case_id": "completed_requires_fare",
            "payload": {key: value for key, value in valid_completed.items() if key != "fare"}
            | {"trip_id": "trip_4106"},
            "should_pass": False,
        },

        # ---- FAILURE 6: REQUESTED WITH A DRIVER  [BUSINESS RULE] ------
        # The mirror image of the previous case: a field that is present but
        # must not be. A trip that has only been requested cannot already have
        # a driver assigned - that would mean the events arrived out of order or
        # the producer is confused.
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
            #
            # json.dumps() turns the dict into a JSON STRING, then
            # model_validate_json() parses and validates it. The round trip
            # through text is the point: it is exactly what happens when a
            # message arrives from Kafka.
            event = TripEventV1.model_validate_json(json.dumps(case["payload"]))

            actual_pass = True
            errors: list[dict[str, Any]] = []

            # report_dict() shows the NORMALIZED result - note that the
            # valid_completed case, given as -07:00, appears here in UTC. That
            # is normalize_event_time doing its job, visible in the evidence.
            normalized = event.report_dict()

        except ValidationError as exc:
            # Reaching here means the model REJECTED the payload. For six of the
            # eight cases that is the desired outcome.
            actual_pass = False
            errors = validation_error_summary(exc)
            normalized = None

        rows.append(
            {
                "case_id": case["case_id"],
                "expected_pass": case["should_pass"],
                "actual_pass": actual_pass,

                # THE ACTUAL ASSERTION. Not "did it pass?" but "did it do what
                # we expected?" - so a case expected to FAIL counts as a success
                # when it fails.
                "expectation_met": actual_pass == case["should_pass"],

                "normalized_event": normalized,

                # For rejected cases, exactly WHICH rule fired - the most
                # instructive part of the output.
                "errors": errors,
            }
        )

    return rows


def main() -> dict[str, Any]:
    """Run local validation cases and write a secret-free report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04a")
    args = parser.parse_args()

    # The run ID becomes a directory name, so it is validated before use.
    # parser.error() prints usage and exits, which is friendlier than a traceback.
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    cases = run_validation()

    # ====================================================================
    # STUDENT CHECKPOINT
    # Which failures are shape/type violations, and which are business-rule
    # violations? Name one business rule that Avro alone would not enforce.
    # ====================================================================
    #
    # (Answer, for this annotated copy:
    #    SHAPE/TYPE  - naive_timestamp_rejected, extra_field_rejected,
    #                  string_fare_rejected
    #    BUSINESS    - negative_fare_rejected, completed_requires_fare,
    #                  requested_rejects_driver
    #  "trip_completed requires a fare" is a business rule Avro cannot express,
    #  because Avro has no way to make one field's requiredness depend on
    #  another field's value.)

    report = {
        "demo": "demo04a_schema_validation",
        "purpose": "application/domain validation before serialization",
        "total_cases": len(cases),

        # A single number summarizing the run: how many cases behaved as
        # expected. Should equal total_cases.
        "expectations_met": sum(1 for row in cases if row["expectation_met"]),

        "cases": cases,

        # The thesis of the demo, stored in the evidence rather than left in a
        # slide deck.
        "key_point": (
            "Pydantic enforces application meaning. Avro and Schema Registry do not replace these business rules."
        ),
    }

    output_file = write_json_report(args.run_id, "demo04a_schema_validation", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    # FAIL LOUDLY IF THE CONTRACT DRIFTS. If someone loosens the model - say,
    # removes ge=0 from fare - the negative-fare case would start passing, this
    # check would fail, and the change would be caught immediately.
    #
    # In effect this script is a TEST SUITE for the data contract that also
    # produces human-readable evidence.
    if report["expectations_met"] != report["total_cases"]:
        raise SystemExit("At least one validation case did not match its expected result.")

    return report


if __name__ == "__main__":
    main()
