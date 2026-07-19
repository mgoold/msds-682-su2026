"""Extra credit +1: advanced evaluation and observability.

Aggregates three or more INDEPENDENT Confluent Cloud benchmark runs (each at
least 2,000 messages per strategy, each written to its own CSV) and reports:

  * per-run and pooled throughput,
  * p50 and p95 batch latency per strategy,
  * run-to-run variability (min/max/spread, standard deviation),
  * success and failure counts, and
  * a secret-free producer configuration snapshot.

These runs are ADDITIONAL evidence. The base assignment's own complete
2,000-message-per-strategy comparison remains in results/producer_benchmark.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

STRATEGIES = ("async", "sync_style")
INT_FIELDS = {
    "batch_index",
    "batch_message_count",
    "total_messages_so_far",
    "batch_delivered",
    "batch_failed",
    "remaining_after_flush",
}
FLOAT_FIELDS = {"elapsed_seconds", "messages_per_second"}


def load_run(path: Path) -> list[dict[str, Any]]:
    """Load one benchmark CSV, converting numeric columns to real numbers."""

    with path.open(newline="", encoding="utf-8") as handle:
        rows = []
        for raw in csv.DictReader(handle):
            row: dict[str, Any] = {}
            for column, value in raw.items():
                if column in INT_FIELDS:
                    row[column] = int(value)
                elif column in FLOAT_FIELDS:
                    row[column] = float(value)
                else:
                    row[column] = value
            row["source_csv"] = path.name
            rows.append(row)
    if not rows:
        raise ValueError(f"{path} contains no rows")
    return rows


def percentile(values: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile (p50/p95) of a small sample.

    statistics.quantiles needs more data points than a 4-batch run provides, so
    nearest-rank is both valid and easier to explain for n=4.
    """

    if not values:
        raise ValueError("percentile of an empty sample")
    ordered = sorted(values)
    rank = max(1, min(len(ordered), round(fraction * len(ordered) + 0.5)))
    return ordered[rank - 1]


def summarize(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    """Return latency, throughput, and delivery statistics for one row set."""

    latencies = [row["elapsed_seconds"] for row in rows]
    throughputs = [row["messages_per_second"] for row in rows]
    delivered = sum(row["batch_delivered"] for row in rows)
    failed = sum(row["batch_failed"] for row in rows)
    remaining = sum(row["remaining_after_flush"] for row in rows)

    return {
        "label": label,
        "batches": len(rows),
        "messages_delivered": delivered,
        "messages_failed": failed,
        "remaining_after_flush": remaining,
        "all_batches_complete": failed == 0 and remaining == 0,
        "batch_latency_seconds": {
            "p50": round(percentile(latencies, 0.50), 6),
            "p95": round(percentile(latencies, 0.95), 6),
            "min": round(min(latencies), 6),
            "max": round(max(latencies), 6),
            "mean": round(statistics.fmean(latencies), 6),
            "stdev": round(statistics.stdev(latencies), 6) if len(latencies) > 1 else 0.0,
        },
        "throughput_msgs_per_second": {
            "min": round(min(throughputs), 2),
            "max": round(max(throughputs), 2),
            "mean": round(statistics.fmean(throughputs), 2),
            "stdev": round(statistics.stdev(throughputs), 2) if len(throughputs) > 1 else 0.0,
        },
    }


def variability(per_run_means: list[float]) -> dict[str, Any]:
    """Quantify how much a strategy's mean throughput moved between runs."""

    mean = statistics.fmean(per_run_means)
    spread = max(per_run_means) - min(per_run_means)
    stdev = statistics.stdev(per_run_means) if len(per_run_means) > 1 else 0.0
    return {
        "per_run_mean_throughput": [round(value, 2) for value in per_run_means],
        "across_run_mean": round(mean, 2),
        "across_run_stdev": round(stdev, 2),
        "across_run_spread": round(spread, 2),
        # Coefficient of variation: stdev as a share of the mean, so async and
        # sync_style are comparable despite a ~200x scale difference.
        "coefficient_of_variation_pct": round((stdev / mean) * 100, 2) if mean else 0.0,
    }


def plot_latency(runs: dict[str, list[dict[str, Any]]], output_path: Path) -> Path:
    """Plot per-batch latency for every run, grouped by strategy."""

    colors = {"async": "#1f77b4", "sync_style": "#d62728"}
    plt.figure(figsize=(11, 6))
    for run_index, (run_name, rows) in enumerate(sorted(runs.items()), start=1):
        for strategy in STRATEGIES:
            subset = sorted(
                (row for row in rows if row["strategy"] == strategy),
                key=lambda row: row["batch_index"],
            )
            if not subset:
                continue
            plt.plot(
                [row["batch_index"] for row in subset],
                [row["elapsed_seconds"] for row in subset],
                marker="o",
                linewidth=1.8,
                alpha=0.85,
                color=colors[strategy],
                linestyle=["-", "--", ":", "-."][(run_index - 1) % 4],
                label=f"{strategy} ({run_name})",
            )

    plt.yscale("log")
    plt.title("Batch latency across independent runs (lower is faster)")
    plt.xlabel("Batch index (each batch = 500 messages)")
    plt.ylabel("Batch latency, seconds (log scale)")
    all_indexes = {row["batch_index"] for rows in runs.values() for row in rows}
    plt.xticks(sorted(all_indexes))
    plt.grid(True, which="both", alpha=0.3)
    plt.legend(fontsize=8, ncol=2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    return output_path


def main() -> dict[str, Any]:
    """Aggregate the independent runs and write the observability report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--config-snapshot", type=Path, default=Path("evidence/demo02c_config.json"))
    parser.add_argument("--output", type=Path, default=Path("evidence/xc_variability_report.json"))
    parser.add_argument("--plot", type=Path, default=Path("results/xc_latency_by_run.png"))
    args = parser.parse_args()

    if len(args.inputs) < 3:
        raise SystemExit("Extra credit requires at least three independent runs.")

    runs = {path.stem: load_run(path) for path in args.inputs}

    # Each run must independently satisfy the base benchmark contract.
    for name, rows in runs.items():
        for strategy in STRATEGIES:
            subset = [row for row in rows if row["strategy"] == strategy]
            if len(subset) < 4:
                raise SystemExit(f"{name}/{strategy}: fewer than 4 batches")
            if sum(row["batch_delivered"] for row in subset) < 2_000:
                raise SystemExit(f"{name}/{strategy}: fewer than 2,000 messages")

    per_strategy: dict[str, Any] = {}
    for strategy in STRATEGIES:
        pooled = [row for rows in runs.values() for row in rows if row["strategy"] == strategy]
        per_run = {
            name: summarize([r for r in rows if r["strategy"] == strategy], name)
            for name, rows in sorted(runs.items())
        }
        per_strategy[strategy] = {
            "pooled": summarize(pooled, f"{strategy} (all runs pooled)"),
            "per_run": per_run,
            "variability": variability(
                [run["throughput_msgs_per_second"]["mean"] for run in per_run.values()]
            ),
        }

    config_snapshot = None
    if args.config_snapshot.exists():
        config_snapshot = json.loads(args.config_snapshot.read_text())
        # Defensive: the base helper is already secret-free, but never ship a
        # value that looks like a credential.
        for banned in ("sasl.username", "sasl.password", "SASL_USERNAME", "SASL_PASSWORD"):
            assert banned not in json.dumps(config_snapshot), f"{banned} leaked into snapshot"

    plot_path = plot_latency(runs, args.plot)

    async_mean = per_strategy["async"]["pooled"]["throughput_msgs_per_second"]["mean"]
    sync_mean = per_strategy["sync_style"]["pooled"]["throughput_msgs_per_second"]["mean"]

    report = {
        "extra_credit_item": "+1 advanced evaluation and observability",
        "independent_runs": len(runs),
        "run_names": sorted(runs),
        "messages_per_strategy_per_run": 2_000,
        "batch_size": 500,
        "seed": 682,
        "per_strategy": per_strategy,
        "async_speedup_pooled": round(async_mean / sync_mean, 1) if sync_mean else None,
        "secret_free_config_snapshot": config_snapshot,
        "plot": str(plot_path),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"\nWrote {args.output} and {plot_path}")
    return report


if __name__ == "__main__":
    main()
