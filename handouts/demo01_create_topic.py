from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv


def load_config() -> dict[str, str]:
    cwd_env = Path.cwd() / ".env"
    script_env = Path(__file__).resolve().parent / ".env"
    if cwd_env.exists():
        load_dotenv(cwd_env)
    else:
        load_dotenv(script_env)
    return {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }


def missing_config(config: dict[str, str]) -> list[str]:
    env_by_client_key = {
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "security.protocol": "SECURITY_PROTOCOL",
        "sasl.mechanisms": "SASL_MECHANISMS",
        "sasl.username": "SASL_USERNAME",
        "sasl.password": "SASL_PASSWORD",
    }
    return [env_by_client_key[key] for key, value in config.items() if not value]


def topic_exists(admin_client: AdminClient, topic_name: str) -> bool:
    return topic_name in admin_client.list_topics(timeout=10).topics


def create_topic(
    admin_client: AdminClient,
    topic_name: str,
    partitions: int,
    replication_factor: int,
    cleanup_policy: str,
) -> str:
    if topic_exists(admin_client, topic_name):
        return "already_exists"

    topic = NewTopic(
        topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config={"cleanup.policy": cleanup_policy},
    )
    futures = admin_client.create_topics([topic])
    futures[topic_name].result(timeout=30)
    return "created"


def safe_report(
    config: dict[str, str],
    topic_name: str,
    partitions: int,
    replication_factor: int,
    cleanup_policy: str,
    status: str,
) -> dict:
    return {
        "status": status,
        "topic": topic_name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "cleanup_policy": cleanup_policy,
        "bootstrap_host": config["bootstrap.servers"].split("://")[-1],
        "has_username": bool(config["sasl.username"]),
        "has_password": bool(config["sasl.password"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=os.getenv("DEMO01_TOPIC_NAME", "msds682.demo01.trip-events.v1"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--cleanup-policy", choices=["delete", "compact"], default="delete")
    parser.add_argument("--run-id", default="lec2")
    args = parser.parse_args()

    config = load_config()
    missing = missing_config(config)
    if missing:
        raise SystemExit(f"Missing required .env values: {', '.join(missing)}")

    admin_client = AdminClient(config)
    status = create_topic(
        admin_client=admin_client,
        topic_name=args.topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        cleanup_policy=args.cleanup_policy,
    )

    report = safe_report(
        config=config,
        topic_name=args.topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        cleanup_policy=args.cleanup_policy,
        status=status,
    )
    output_dir = Path("outputs") / "runs" / args.run_id / "demo01_topic_creation"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "topic_report.json"
    output_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")


if __name__ == "__main__":
    main()
