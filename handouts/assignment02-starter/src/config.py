"""Single configuration and secret-free evidence boundary for Assignment 2."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
RESULTS_DIR = PROJECT_ROOT / "results"
DEFAULT_TOPIC = "msds682.assignment02.trip-events-api-avro.v1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
IDENTIFIER_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


class ConnectionConfigError(RuntimeError):
    """Raised when required local configuration is absent or unsafe."""


def load_assignment_env() -> None:
    """Load only the assignment-local ignored ``.env`` file."""

    load_dotenv(PROJECT_ROOT / ".env")


def validate_run_id(value: str) -> str:
    """Validate a short identifier used in events, groups, and evidence."""

    if not RUN_ID_PATTERN.fullmatch(value):
        raise ValueError(
            "run_id must start with a letter or digit and use at most 40 "
            "letters, digits, dots, underscores, or hyphens"
        )
    return value


def normalize_identifier(value: str) -> str:
    """Return a Kafka-friendly identifier without duplicating naming rules."""

    normalized = IDENTIFIER_PATTERN.sub("-", value.strip()).strip("-")
    return normalized or "assignment2"


def topic_name() -> str:
    """Return the independent Assignment 2 topic."""

    load_assignment_env()
    return os.getenv("ASSIGNMENT2_TOPIC_NAME", DEFAULT_TOPIC).strip() or DEFAULT_TOPIC


def base_group_id() -> str:
    """Return the student-specific base consumer group from ignored config."""

    load_assignment_env()
    value = os.getenv("ASSIGNMENT2_GROUP_ID", "").strip()
    if not value or "<usf_username>" in value:
        raise ConnectionConfigError(
            "Set ASSIGNMENT2_GROUP_ID in .env and replace <usf_username>"
        )
    return normalize_identifier(value)


def group_id_for_run(base: str, run_id: str, *, replay: bool = False) -> str:
    """Build stable base and replay group IDs for one logical run."""

    parts = [normalize_identifier(base), normalize_identifier(validate_run_id(run_id))]
    if replay:
        parts.append("replay")
    return ".".join(parts)


def _required_env(names: tuple[str, ...]) -> dict[str, str]:
    """Return required environment values or one combined missing-key error."""

    load_assignment_env()
    values = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ConnectionConfigError(
            "Missing required .env setting(s): " + ", ".join(missing)
        )
    return values


def kafka_config(*, client_id: str) -> dict[str, Any]:
    """Build current Confluent Kafka client configuration."""

    values = _required_env(
        (
            "BOOTSTRAP_SERVERS",
            "SECURITY_PROTOCOL",
            "SASL_MECHANISMS",
            "SASL_USERNAME",
            "SASL_PASSWORD",
        )
    )
    return {
        "bootstrap.servers": values["BOOTSTRAP_SERVERS"],
        "security.protocol": values["SECURITY_PROTOCOL"],
        "sasl.mechanisms": values["SASL_MECHANISMS"],
        "sasl.username": values["SASL_USERNAME"],
        "sasl.password": values["SASL_PASSWORD"],
        "client.id": client_id,
    }


def registry_config() -> dict[str, str]:
    """Build current Schema Registry client configuration."""

    values = _required_env(
        (
            "SCHEMA_REGISTRY_URL",
            "SCHEMA_REGISTRY_API_KEY",
            "SCHEMA_REGISTRY_API_SECRET",
        )
    )
    return {
        "url": values["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{values['SCHEMA_REGISTRY_API_KEY']}:"
            f"{values['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    }


def safe_kafka_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Confirm required Kafka settings without exposing credential values."""

    bootstrap = str(config.get("bootstrap.servers", ""))
    return {
        "bootstrap_host": bootstrap.split(",", 1)[0].split(":", 1)[0],
        "security_protocol": config.get("security.protocol"),
        "sasl_mechanisms": config.get("sasl.mechanisms"),
        "client_id": config.get("client.id"),
        "has_username": bool(config.get("sasl.username")),
        "has_password": bool(config.get("sasl.password")),
    }


def safe_registry_config_report(config: dict[str, str]) -> dict[str, Any]:
    """Confirm Registry configuration without exposing keys or secrets."""

    parsed = urlsplit(config.get("url", ""))
    return {
        "registry_host": parsed.hostname,
        "registry_scheme": parsed.scheme,
        "has_registry_credentials": bool(config.get("basic.auth.user.info")),
    }


def write_json_report(filename: str, report: dict[str, Any]) -> Path:
    """Write one deterministic, secret-free JSON evidence file."""

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / filename
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path
