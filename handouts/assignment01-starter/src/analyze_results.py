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
    raise NotImplementedError("Complete load_and_validate_rows")
    # ===================== CODE ENDS HERE =====================


def plot_rows(rows: list[dict[str, Any]], output_path: Path) -> Path:
    """Plot messages per second for each completed batch and strategy."""

    # ==================== CODE START HERE ====================
    # TODO: Draw one labeled line per strategy using batch_index on x and
    # messages_per_second on y. Add title, axis labels, grid, legend, and save.
    raise NotImplementedError("Complete plot_rows")
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
