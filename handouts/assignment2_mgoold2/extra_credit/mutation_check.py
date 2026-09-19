"""Credential-free mutation check for the AI-assisted engineering review.

Disables one consumer guard at a time in ``src/consumer_runtime.py``, runs the
provided test suite and the review suite against each mutated copy, and records
which suite noticed. The source file is always restored, including on failure.

A guard that no suite detects is unverified: the code could be deleted and the
tests would still report success. Run from the assignment's top-level folder:

    python extra_credit/mutation_check.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET = PROJECT_ROOT / "src" / "consumer_runtime.py"
PROVIDED_SUITE = "tests/test_consumer_application.py"
REVIEW_SUITE = "tests/test_review_guards.py"
OUTPUT = PROJECT_ROOT / "evidence" / "xc_review_evidence.json"

# Each guard is disabled by making its condition unreachable rather than by
# deleting lines, so the surrounding code keeps its original structure.
GUARDS = [
    {
        "guard": "key_matches_payload",
        "description": "Kafka key must equal the decoded event's trip_id",
        "condition": 'if message_key_str != event.trip_id:',
    },
    {
        "guard": "key_present",
        "description": "Kafka message key must be present and nonempty",
        "condition": 'if not message_key:',
    },
    {
        "guard": "run_id_filter",
        "description": "records from another run are skipped, not processed",
        "condition": 'if run_id != record["event"]["run_id"]:',
    },
]

SUMMARY = re.compile(r"(?:(\d+) failed)?,?\s*(\d+) passed")


def run_suite(path: str) -> dict[str, int | bool | str]:
    """Run one pytest file and return a JSON-safe outcome summary."""

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--tb=no", path],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
    summary = lines[-1] if lines else ""
    match = SUMMARY.search(summary)
    failed = int(match.group(1)) if match and match.group(1) else 0
    passed = int(match.group(2)) if match else 0
    return {
        "passed": passed,
        "failed": failed,
        "exit_code": result.returncode,
        "all_passed": result.returncode == 0,
    }


def main() -> dict:
    """Mutate each guard in turn and record which suite detected the change."""

    original = TARGET.read_text(encoding="utf-8")
    report: dict = {
        "assignment": "assignment02",
        "artifact": "xc_ai_review_mutation_check",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "purpose": (
            "Show which consumer guards the provided test suite can detect the "
            "removal of, and which are covered only by the review suite."
        ),
        "target_file": "src/consumer_runtime.py",
        "provided_suite": PROVIDED_SUITE,
        "review_suite": REVIEW_SUITE,
        "requires_credentials": False,
    }

    try:
        report["baseline"] = {
            "provided_suite": run_suite(PROVIDED_SUITE),
            "review_suite": run_suite(REVIEW_SUITE),
        }

        mutations = []
        for guard in GUARDS:
            condition = guard["condition"]
            if condition not in original:
                raise SystemExit(f"guard condition not found in source: {condition}")
            TARGET.write_text(
                original.replace(condition, "if False:  # MUTATED"),
                encoding="utf-8",
            )
            provided = run_suite(PROVIDED_SUITE)
            review = run_suite(REVIEW_SUITE)
            mutations.append(
                {
                    "guard": guard["guard"],
                    "description": guard["description"],
                    "disabled_condition": condition,
                    "provided_suite": provided,
                    "review_suite": review,
                    "detected_by_provided_suite": not provided["all_passed"],
                    "detected_by_review_suite": not review["all_passed"],
                }
            )
        report["mutations"] = mutations
    finally:
        TARGET.write_text(original, encoding="utf-8")

    report["source_restored"] = TARGET.read_text(encoding="utf-8") == original
    report["after_restore"] = {
        "provided_suite": run_suite(PROVIDED_SUITE),
        "review_suite": run_suite(REVIEW_SUITE),
    }
    report["guards_undetected_by_provided_suite"] = [
        m["guard"] for m in report["mutations"] if not m["detected_by_provided_suite"]
    ]
    report["guards_detected_by_review_suite"] = [
        m["guard"] for m in report["mutations"] if m["detected_by_review_suite"]
    ]

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"\nWrote {OUTPUT}")
    return report


if __name__ == "__main__":
    main()
