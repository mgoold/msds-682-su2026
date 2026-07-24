"""Demo 07A: train and export a transparent Ridge cost-model artifact."""

from __future__ import annotations

import argparse
from pathlib import Path

from confluent_demo_common import validate_run_id, write_json_report
from demo07_common import save_model_artifact, train_ridge_cost_model


def default_artifact_path(run_id: str) -> Path:
    """Return the one documented artifact path for this run."""

    safe_run_id = validate_run_id(run_id)
    return (
        Path("outputs")
        / "runs"
        / safe_run_id
        / "demo07a"
        / "ridge-cost-v2.json"
    )


def run_training(
    *,
    run_id: str,
    alpha: float,
    output_path: Path | None = None,
) -> dict[str, object]:
    """Train once on deterministic synthetic labels and write safe evidence."""

    validate_run_id(run_id)
    artifact = train_ridge_cost_model(alpha=alpha)
    artifact_path = save_model_artifact(
        artifact,
        output_path or default_artifact_path(run_id),
    )
    report: dict[str, object] = {
        "demo": "07A",
        "run_id": run_id,
        "data_source": "deterministic synthetic cost records",
        "prior_demo_required": False,
        "kafka_required": False,
        "artifact_path": str(artifact_path),
        "artifact": artifact.model_dump(mode="json"),
        "deployment_contract": {
            "features": artifact.feature_names,
            "target": artifact.target_name,
            "version": artifact.model_version,
            "format": "validated JSON coefficients; no pickle execution",
        },
    }
    report_path = write_json_report(run_id, "demo07a", report)
    report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    report = run_training(
        run_id=args.run_id,
        alpha=args.alpha,
        output_path=args.output,
    )
    artifact = report["artifact"]
    assert isinstance(artifact, dict)
    print(
        f"Trained {artifact['model_version']} on "
        f"{artifact['training_records']} records; "
        f"validation MAE={artifact['validation_mae_cents']} cents"
    )
    print(f"Model artifact: {report['artifact_path']}")
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
