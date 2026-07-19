"""Validate the HW1 benchmark CSV and plot throughput by completed batch."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

from producer_compare import CSV_COLUMNS, MINIMUM_MESSAGES, REQUIRED_BATCH_SIZE


def load_and_validate_rows(path: Path) -> list[dict[str, Any]]:
    """Load CSV rows and enforce the base-assignment evidence contract."""

    # ==================== CODE START HERE ====================
    # TODO: Read the CSV with DictReader, verify all CSV_COLUMNS, convert numeric
    # fields, require async and sync_style, and verify at least
    # MINIMUM_MESSAGES // REQUIRED_BATCH_SIZE sequential valid rows per strategy
    # with zero failures/remaining after flush.
    
    int_fields = {
        "batch_index",
        "batch_message_count",
        "total_messages_so_far",
        "batch_delivered",
        "batch_failed",
        "remaining_after_flush",
    }
    float_fields = {"elapsed_seconds", "messages_per_second"}

    # 1. Read the file and check the header before trusting any row.
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        missing = [column for column in CSV_COLUMNS if column not in header]
        if missing:
            raise ValueError(f"Missing required CSV columns: {', '.join(missing)}")
        raw_rows = list(reader)

    if not raw_rows:
        raise ValueError("Benchmark CSV contains no data rows")

    # 2. Convert every column to its real type; CSV gives back only strings.
    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        row: dict[str, Any] = {}
        for column in CSV_COLUMNS:
            value = raw[column]
            if column in int_fields:
                row[column] = int(value)
            elif column in float_fields:
                row[column] = float(value)
            else:
                row[column] = value
        rows.append(row)

    # 3. Group by strategy so each one can be checked independently.
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        by_strategy[row["strategy"]].append(row)

    required_batches = MINIMUM_MESSAGES // REQUIRED_BATCH_SIZE
    for strategy in ("async", "sync_style"):
        strategy_rows = by_strategy.get(strategy, [])
        if len(strategy_rows) < required_batches:
            raise ValueError(
                f"{strategy} has {len(strategy_rows)} batches; "
                f"at least {required_batches} are required"
            )
        for row in strategy_rows:
            if row["batch_failed"] or row["remaining_after_flush"]:
                raise ValueError(
                    f"{strategy} batch {row['batch_index']} has incomplete delivery"
                )
        batch_indexes = [row["batch_index"] for row in strategy_rows]
        if batch_indexes != list(range(1, len(batch_indexes) + 1)):
            raise ValueError(f"{strategy} batch_index values are not sequential from 1")

    return rows    

    # ===================== CODE ENDS HERE =====================


def plot_rows(rows: list[dict[str, Any]], output_path: Path) -> Path:
    """Plot messages per second for each completed batch and strategy."""

    # ==================== CODE START HERE ====================
    # TODO: Draw one labeled line per strategy using batch_index on x and
    # messages_per_second on y. Add title, axis labels, grid, legend, and save.
    # same manipulation as above function:
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        by_strategy[row["strategy"]].append(row)

        # Fixed colors keyed to the strategy, so a line never changes color.
    colors = {"async": "#1f77b4", "sync_style": "#d62728"}

    # 2. Create the figure.
    plt.figure(figsize=(10, 6))

    # 3. One labeled line per strategy, sorted so x ascends.
    for strategy in ("async", "sync_style"):
        strategy_rows = sorted(by_strategy[strategy], key=lambda row: row["batch_index"])
        x_values = [row["batch_index"] for row in strategy_rows]
        y_values = [row["messages_per_second"] for row in strategy_rows]
        plt.plot(
            x_values,
            y_values,
            marker="o",
            linewidth=2,
            label=strategy,
            color=colors[strategy],
        )

    # 4. Required decorations.
    plt.title("Producer throughput by completed batch: async vs sync_style")
    plt.xlabel("Batch index (each batch = 500 messages)")
    # Log scale: sync_style is ~200x slower, so a linear axis flattens it to zero.
    plt.yscale("log")
    plt.ylabel("Messages per second (log scale)")
    # Batches are whole numbers, so only label the integer batch positions.
    plt.xticks(sorted({row["batch_index"] for row in rows}))
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()

    # 5. Make sure the destination folder exists, then save and close.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()

    return output_path

    # ===================== CODE ENDS HERE =====================


def main() -> Path:
    """Parse paths, validate benchmark evidence, and save the comparison plot."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("results/producer_benchmark.csv"))
    parser.add_argument("--output", type=Path, default=Path("results/producer_benchmark.png"))
    args = parser.parse_args()
    rows = load_and_validate_rows(args.input)
    output = plot_rows(rows, args.output)
    print(f"Validated {len(rows)} benchmark rows and wrote {output}")
    return output


if __name__ == "__main__":
    main()
