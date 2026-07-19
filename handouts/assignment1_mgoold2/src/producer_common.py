"""Shared event, configuration, delivery, and reporting helpers for HW1."""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field


DEFAULT_TOPIC_NAME = "msds682.demo01.trip-events.v1"
DEFAULT_SEED = 682
BASE_EVENT_TIME = datetime(2026, 7, 4, 10, 0, tzinfo=timezone.utc)


class TripEvent(BaseModel):
    """Validated message-value contract shared by every producer strategy."""
    # note that basemodel, field imported on line 13 above
    # ==================== CODE START HERE ====================
    # TODO: Define trip_id, event_type, rider_id, event_time, zone,
        # Message value model shared by all Demo 02 producer scripts.
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"] # can only be these 4 strings
    rider_id: str 
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0) 
    # optional driver_id, and an optional nonnegative fare. Use the Demo 02
    # TripEvent model as the contract.
    # ===================== CODE ENDS HERE =====================



class DeliveryTracker:
    """Collect delivery callback outcomes without storing credentials."""

    # ==================== CODE START HERE ====================
    # TODO: Initialize delivered_count, failed_messages, and delivery_samples.
    # Then implement callback(err, msg): record an error string on failure; on
    # success increment the count and retain at most 10 secret-free samples.
    def __init__(self) -> None:
        """Initialize delivery counts, errors, and bounded evidence samples."""
        self.delivered_count = 0
        self.failed_messages: list[str] = []
        self.delivery_samples: list[str] = []

    def callback(self, err: Any, msg: Any) -> None:
        """Record one delivery success or failure from confluent-kafka."""
        if err is not None:
            self.failed_messages.append(str(err))
            return
        key = msg.key().decode("utf-8") if msg.key() else ""
        self.delivered_count+=1
        if len(self.delivery_samples) < 10:
            self.delivery_samples.append(
                {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": key,
                }
            )

    # ===================== CODE ENDS HERE =====================

    @property
    def failed_count(self) -> int:
        """Return the number of delivery failures observed by callbacks."""

        return len(self.failed_messages)


def load_dotenv_for_assignment() -> None:
    """Load `.env` from the current directory, falling back to project root."""

    cwd_env = Path.cwd() / ".env"
    project_env = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(cwd_env if cwd_env.exists() else project_env)


def load_producer_config() -> dict[str, str]:
    """Build a Confluent producer configuration from environment variables."""

    # ==================== CODE START HERE ====================
    # TODO: Load .env and return the five confluent-kafka keys demonstrated in
    # Demo 02. Credentials must come from environment variables, never literals.
    load_dotenv_for_assignment()
    return {
        # Confluent Cloud Kafka cluster endpoint.
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),
        # Confluent Cloud requires encrypted SASL authentication.
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),
        # PLAIN means API-key/API-secret authentication.
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),
        # Kafka API key. Do not hard-code it in source code.
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        # Kafka API secret. Keep it in .env only.
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }    

    # ===================== CODE ENDS HERE =====================


def require_producer_config() -> dict[str, str]:
    """Return producer configuration or stop with missing variable names."""

    config = load_producer_config()
    required = {
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "sasl.username": "SASL_USERNAME",
        "sasl.password": "SASL_PASSWORD",
    }
    missing = [env_name for key, env_name in required.items() if not config.get(key)]
    if missing:
        raise SystemExit(f"Missing required .env values: {', '.join(missing)}")
    return config


def get_topic_name() -> str:
    """Return the one topic shared by Demo 02A-02D."""

    load_dotenv_for_assignment()
    return os.getenv("ASSIGNMENT1_TOPIC_NAME", DEFAULT_TOPIC_NAME)


def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    """Create one deterministic logical trip event for a stream position."""

    # ==================== CODE START HERE ====================
    # TODO: Reproduce the Demo 02 deterministic event generator. The same index
    # and seeded Random instance must create the same logical event.
    event_types = ["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    event_type = event_types[index % len(event_types)]
    trip_number = 981 + (index // len(event_types))
    event_time = (BASE_EVENT_TIME + timedelta(seconds=index)).isoformat().replace("+00:00", "Z")

    return TripEvent(
        trip_id=f"trip_{trip_number}",
        event_type=event_type,
        rider_id=f"rider-{trip_number}",
        driver_id=None if event_type == "trip_requested" else f"driver-{rng.randint(1, 8):03d}",
        fare=round(rng.uniform(10.0, 90.0), 2) if event_type == "trip_completed" else None,
        zone=["north", "south", "west"][index % 3],
        event_time=event_time,
    )
    # ===================== CODE ENDS HERE =====================


def make_trip_events(count: int, seed: int = DEFAULT_SEED) -> list[TripEvent]:
    """Create a reproducible event sequence for fair strategy comparisons."""

    if count < 0:
        raise ValueError("count must be nonnegative")
    rng = random.Random(seed)
    return [make_trip_event(index, rng) for index in range(count)]


def event_key(event: TripEvent) -> bytes:
    """Serialize the stable trip identifier used as the Kafka message key."""

    # ==================== CODE START HERE ====================
    # TODO: Encode trip_id as UTF-8 bytes.
    return event.trip_id.encode("utf-8")
    # ===================== CODE ENDS HERE =====================


def serialize_event(event: TripEvent) -> bytes:
    """Serialize a validated event as compact UTF-8 JSON bytes."""

    # ==================== CODE START HERE ====================
    # TODO: Convert the Pydantic model to a JSON string, omit None fields, and
    # encode that string as UTF-8 bytes.
    return event.model_dump_json(exclude_none=True).encode("utf-8")
    # ===================== CODE ENDS HERE =====================


def event_dict(event: TripEvent) -> dict[str, Any]:
    """Return a secret-free Python representation for report previews."""

    return event.model_dump(exclude_none=True)


def safe_config_report(config: dict[str, str], topic: str) -> dict[str, Any]:
    """Return configuration metadata without usernames, passwords, or keys."""

    return {
        "topic": topic,
        "bootstrap_host": config["bootstrap.servers"].split("://")[-1],
        "security_protocol": config["security.protocol"],
        "sasl_mechanisms": config["sasl.mechanisms"],
        "has_username": bool(config["sasl.username"]),
        "has_password": bool(config["sasl.password"]),
    }


def write_json_file(path: Path, payload: dict[str, Any]) -> Path:
    """Write a stable, human-readable JSON evidence file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
