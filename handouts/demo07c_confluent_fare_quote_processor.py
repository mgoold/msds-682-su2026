"""Demo 07C: consume trip requests and publish rule-v1 or ridge-v2 quotes."""

from __future__ import annotations

import argparse
from pathlib import Path

from demo07_processor_runtime import run_quote_processor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--pricing-method",
        required=True,
        choices=("rule-v1", "ridge-v2"),
    )
    parser.add_argument("--model-artifact", type=Path)
    parser.add_argument("--max-messages", type=int, default=4)
    parser.add_argument(
        "--routing-mode",
        choices=("osrm", "fixture"),
        default="osrm",
    )
    parser.add_argument("--route-timeout", type=float, default=10.0)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--max-scanned", type=int, default=1_000)
    args = parser.parse_args()

    report = run_quote_processor(
        run_id=args.run_id,
        pricing_method=args.pricing_method,
        model_artifact_path=args.model_artifact,
        max_messages=args.max_messages,
        route_mode=args.routing_mode,
        route_timeout=args.route_timeout,
        assignment_timeout=args.assignment_timeout,
        idle_timeout=args.idle_timeout,
        delivery_timeout=args.delivery_timeout,
        create_topics=args.create_topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        max_scanned=args.max_scanned,
    )
    print(
        f"{report['pricing_method']} published {report['processed']} quotes to "
        f"{report['output_topic']}"
    )
    print(f"Secret-free report: {report['report_path']}")


if __name__ == "__main__":
    main()
