"""Shared Confluent configuration, topic, and evidence helpers for class demos.

Lecture-specific modules provide their own topic names and orchestration. This
module owns the repeated connection boundary so credentials, run IDs, topic
creation, and secret-free evidence follow one implementation.
"""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

BUNDLE_DIR = Path(__file__).resolve().parent
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")


class ConnectionConfigError(RuntimeError):
    """Raised when required Kafka or Schema Registry settings are absent."""


class TopicSetupError(RuntimeError):
    """Raised when a Kafka topic cannot be verified or created."""


def load_dotenv_for_demo() -> Path | None:
    """Load ``.env`` from the working directory or this bundle directory."""

    candidates = (Path.cwd() / ".env", BUNDLE_DIR / ".env")
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(candidate, override=False)
            return candidate
    load_dotenv(override=False)
    return None


def kafka_config(*, client_id: str | None = None) -> dict[str, str]:
    """Load Kafka client settings and fail before a network request."""

    load_dotenv_for_demo()
    config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }
    if client_id:
        config["client.id"] = client_id

    required = ("bootstrap.servers", "sasl.username", "sasl.password")
    missing = [key for key in required if not config[key]]
    if missing:
        env_names = {
            "bootstrap.servers": "BOOTSTRAP_SERVERS",
            "sasl.username": "SASL_USERNAME",
            "sasl.password": "SASL_PASSWORD",
        }
        raise ConnectionConfigError(
            "Missing required Kafka .env values: "
            + ", ".join(env_names[key] for key in missing)
        )
    return config


def schema_registry_config() -> dict[str, str]:
    """Load the Registry URL and credentials separately from Kafka."""

    load_dotenv_for_demo()
    url = os.getenv("SCHEMA_REGISTRY_URL", "")
    api_key = os.getenv("SCHEMA_REGISTRY_API_KEY", "")
    api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET", "")
    missing = [
        name
        for name, value in (
            ("SCHEMA_REGISTRY_URL", url),
            ("SCHEMA_REGISTRY_API_KEY", api_key),
            ("SCHEMA_REGISTRY_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise ConnectionConfigError(
            "Missing required Schema Registry .env values: " + ", ".join(missing)
        )
    return {
        "url": url,
        "basic.auth.user.info": f"{api_key}:{api_secret}",
    }


def consumer_group_id(suffix: str, run_id: str | None = None) -> str:
    """Build a safe group ID from the configured course prefix."""

    load_dotenv_for_demo()
    prefix = os.getenv("CONSUMER_GROUP_ID_PREFIX", "msds682-su2026")
    parts = [prefix, suffix]
    if run_id:
        parts.append(run_id)
    return re.sub(r"[^A-Za-z0-9._-]+", "-", "-".join(parts))[:220]


def validate_run_id(run_id: str) -> str:
    """Validate a run ID used in Kafka identifiers and evidence paths."""

    value = run_id.strip()
    if not RUN_ID_PATTERN.fullmatch(value) or value in {".", ".."}:
        raise ValueError(
            "--run-id must be 1-80 characters, start with a letter or digit, "
            "and contain only letters, digits, '.', '_', or '-'"
        )
    return value


def ensure_topic(
    admin: AdminClient,
    *,
    topic: str,
    create: bool,
    partitions: int,
    replication_factor: int,
) -> str:
    """Confirm a demo topic exists, optionally creating it once."""

    try:
        metadata = admin.list_topics(timeout=15)
        if topic in metadata.topics and metadata.topics[topic].error is None:
            return "already_exists"
        if not create:
            raise TopicSetupError(
                f"Topic {topic!r} does not exist. Re-run with --create-topic or "
                "create it in Confluent Cloud first."
            )
        future = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    config={"cleanup.policy": "delete"},
                )
            ]
        )[topic]
        future.result(timeout=30)
        return "created"
    except TopicSetupError:
        raise
    except Exception as exc:
        raise TopicSetupError(
            f"Could not verify or create Kafka topic {topic!r}: {exc}"
        ) from exc


# ============================================================================
# IMPORTANT NOTE
# Reports show hosts and credential-presence booleans, never credential values.
# ============================================================================
def safe_kafka_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Summarize Kafka connection state without credential values."""

    bootstrap = str(config.get("bootstrap.servers", ""))
    return {
        "bootstrap_host": bootstrap.split(",", 1)[0],
        "security_protocol": config.get("security.protocol"),
        "sasl_mechanism": config.get("sasl.mechanisms"),
        "client_id": config.get("client.id"),
        "username_present": bool(config.get("sasl.username")),
        "password_present": bool(config.get("sasl.password")),
    }


def safe_registry_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Summarize Registry state without URL userinfo or credentials."""

    url = str(config.get("url", ""))
    parsed = urlsplit(url if "://" in url else f"//{url}")
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        port = None
    url_host = f"{hostname}:{port}" if hostname and port is not None else hostname
    return {
        "url_host": url_host,
        "basic_auth_present": bool(config.get("basic.auth.user.info")),
    }


def write_json_report(run_id: str, demo_name: str, report: dict[str, Any]) -> Path:
    """Write secret-free evidence under ``outputs/runs``."""

    safe_run_id = validate_run_id(run_id)
    output_dir = Path("outputs") / "runs" / safe_run_id / demo_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "report.json"
    output_file.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=_json_default) + "\n",
        encoding="utf-8",
    )
    return output_file


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
