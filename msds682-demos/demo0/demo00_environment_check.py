from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import sys
from pathlib import Path


PACKAGES = [
    "fastapi",
    "pydantic",
    "uvicorn",
    "httpx2",
    "pandas",
    "matplotlib",
    "pytest",
    "python-dotenv",
    "apscheduler",
    "confluent-kafka",
    "fastmcp",
]


def collect_environment() -> dict:
    packages = {}
    for package in PACKAGES:
        try:
            packages[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            packages[package] = "not installed"

    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "packages": packages,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec1")
    args = parser.parse_args()

    report = collect_environment()
    output_dir = Path("outputs") / "runs" / args.run_id / "demo00_environment"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "environment_report.json"
    output_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")


if __name__ == "__main__":
    main()
