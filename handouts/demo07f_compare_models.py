"""Demo 07F: compare rule-v1 and ridge-v2, then recommend one version."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from confluent_demo_common import validate_run_id, write_json_report
from demo07_common import architecture_comparison


def default_evaluation_report(run_id: str) -> Path:
    safe_run_id = validate_run_id(run_id)
    return Path("outputs") / "runs" / safe_run_id / "demo07e" / "report.json"


def compare_model_summaries(
    model_summary: dict[str, dict[str, float | int]],
) -> dict[str, Any]:
    """Select the lower business-target error without hiding the baseline."""

    required = {"rule-v1", "ridge-v2"}
    if set(model_summary) != required:
        raise ValueError(
            f"Expected summaries for {sorted(required)}, got {sorted(model_summary)}"
        )
    baseline_error = float(
        model_summary["rule-v1"]["mean_absolute_markup_error_pp"]
    )
    candidate_error = float(
        model_summary["ridge-v2"]["mean_absolute_markup_error_pp"]
    )
    recommended_version = (
        "ridge-v2" if candidate_error < baseline_error else "rule-v1"
    )
    improvement = baseline_error - candidate_error
    return {
        "recommended_version": recommended_version,
        "baseline": "rule-v1",
        "candidate": "ridge-v2",
        "primary_metric": "mean_absolute_markup_error_pp",
        "baseline_error_pp": baseline_error,
        "candidate_error_pp": candidate_error,
        "candidate_improvement_pp": round(improvement, 4),
        "selection_recommendation": (
            "prefer ridge-v2 for this teaching comparison"
            if recommended_version == "ridge-v2"
            else "retain rule-v1 for this teaching comparison"
        ),
        "selection_rule": (
            "Recommend the version with lower mean absolute deviation from "
            "the 20% markup target on the same delayed outcomes."
        ),
    }


def run_comparison(
    *,
    run_id: str,
    evaluation_report_path: Path | None = None,
) -> dict[str, Any]:
    """Read evaluated outcomes and write a reproducible selection report."""

    validate_run_id(run_id)
    source_path = evaluation_report_path or default_evaluation_report(run_id)
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    if payload.get("run_id") != run_id:
        raise ValueError(
            f"Evaluation report run_id {payload.get('run_id')!r} "
            f"does not match {run_id!r}"
        )
    model_summary = payload.get("model_summary")
    if not isinstance(model_summary, dict):
        raise ValueError("Evaluation report omitted model_summary")
    decision = compare_model_summaries(model_summary)
    report: dict[str, Any] = {
        "demo": "07F",
        "run_id": run_id,
        "business_question": (
            "Which pricing method keeps realized markup closest to the 20% "
            "target, where markup is (fare - cost) / cost?"
        ),
        "model_summary": model_summary,
        "decision": decision,
        "online_architecture_comparison": architecture_comparison(),
        "architecture_decision": (
            "Implement A: one routing response produces distance and duration. "
            "Keep B as an anti-pattern because splitting those fields creates "
            "an artificial join. The meaningful join is quote plus outcome."
        ),
        "coverage_boundary": {
            "demonstrated": [
                "deterministic event source",
                "Kafka ingestion and event log",
                "Avro contracts and Schema Registry",
                "stateless route feature transformation",
                "versioned rule and trained-model inference",
                "prediction event",
                "bounded quote-outcome stateful join",
                "delayed labels",
                "evaluation and replayable comparison",
                "explicit model artifact and selection recommendation",
            ],
            "not_claimed_as_production_complete": [
                "durable state store and checkpoint recovery",
                "event-time windows and late-event handling",
                "continuous drift monitoring",
                "automated retraining orchestration",
                "model registry and controlled rollout",
            ],
        },
        "source_evaluation_report": str(source_path),
    }
    path = write_json_report(run_id, "demo07f", report)
    report["report_path"] = str(path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evaluation-report", type=Path)
    args = parser.parse_args()

    report = run_comparison(
        run_id=args.run_id,
        evaluation_report_path=args.evaluation_report,
    )
    decision = report["decision"]
    print(
        f"Recommended version: {decision['recommended_version']} | "
        f"baseline error={decision['baseline_error_pp']} pp | "
        f"candidate error={decision['candidate_error_pp']} pp"
    )
    print(f"Recommendation: {decision['selection_recommendation']}")
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
