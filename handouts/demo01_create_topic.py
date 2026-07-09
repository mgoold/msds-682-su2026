from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv


def load_config() -> dict[str, str]:
    # Credentials come from .env, never hardcoded in source code.
    # Look next to where you run from first, then next to this script.
    cwd_env = Path.cwd() / ".env"
    script_env = Path(__file__).resolve().parent / ".env"
    if cwd_env.exists():
        load_dotenv(cwd_env)
    else:
        load_dotenv(script_env)
    # librdkafka client config uses dot-notation keys ("bootstrap.servers"),
    # while env vars use upper snake case; this dict is the translation layer.
    return {
        # Kafka cluster address, for example:
        # pkc-abc12.us-west2.gcp.confluent.cloud:9092
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),

        # Confluent Cloud Kafka uses SASL_SSL + PLAIN for API-key auth.
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),

        # SASL username/password are the Kafka API key and secret.
        # They are intentionally loaded from .env, not hardcoded here.
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }


def missing_config(config: dict[str, str]) -> list[str]:
    # Fail fast: name every missing env var up front,
    # instead of a cryptic auth error after touching the network.
    env_by_client_key = {
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "security.protocol": "SECURITY_PROTOCOL",
        "sasl.mechanisms": "SASL_MECHANISMS",
        "sasl.username": "SASL_USERNAME",
        "sasl.password": "SASL_PASSWORD",
    }
    return [env_by_client_key[key] for key, value in config.items() if not value]


def topic_exists(admin_client: AdminClient, topic_name: str) -> bool:
    # list_topics() is a cluster metadata query. Checking first makes the
    # whole demo idempotent: safe to re-run in class any number of times.
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

    # The three knobs of a topic:
    #   num_partitions: parallelism (how many consumers can read at once)
    #   replication_factor: fault tolerance. This class uses 3 by default;
    #                       managed Kafka limits may depend on cluster settings.
    #   cleanup.policy: how the log forgets. "delete" expires old segments
    #                   by retention; "compact" keeps the latest per key.
    topic = NewTopic(
        topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config={"cleanup.policy": cleanup_policy},
    )
    # create_topics() is async: it returns {topic_name: Future}.
    # result() blocks until the broker confirms, or raises on failure.
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
    # Everything printed or saved is secret-free:
    # host only, and booleans instead of the actual username/password.
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
    # Default topic name follows the course convention:
    # <course>.<demo>.<entity>.<version>: lowercase, descriptive, versioned.
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
    # "already_exists" does not verify the existing topic matches the flags:
    # read back the actual partition count so a mismatch is visible.
    topic_metadata = admin_client.list_topics(timeout=10).topics.get(args.topic)
    if topic_metadata is not None:
        report["partitions_on_cluster"] = len(topic_metadata.partitions)
    # Evidence file for reproducibility: every run leaves a JSON report
    # under outputs/runs/<run-id>/ that can be checked without cluster access.
    output_dir = Path("outputs") / "runs" / args.run_id / "demo01_topic_creation"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "topic_report.json"
    output_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")


if __name__ == "__main__":
    main()
