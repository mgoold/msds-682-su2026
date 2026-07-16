from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field


TOPIC_NAME = "msds682.demo01.trip-events.v1"
BASE_EVENT_TIME = datetime(2026, 7, 4, 10, 0, tzinfo=timezone.utc)


class TripEvent(BaseModel):
    # Message value model shared by all Demo 02 producer scripts.
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    rider_id: str
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0)


class DeliveryTracker:
    # Collect delivery callback results without printing secrets.
    def __init__(self) -> None:
        self.delivered: list[dict[str, Any]] = []
        self.failed: list[str] = []

    def callback(self, err, msg) -> None:
        if err is not None:
            self.failed.append(str(err))
            return
        key = msg.key().decode("utf-8") if msg.key() else ""
        self.delivered.append(
            {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
            }
        )


def load_dotenv_for_demo() -> None:
    # Prefer .env in the folder where the command runs; fall back to script folder.
    cwd_env = Path.cwd() / ".env"
    script_env = Path(__file__).resolve().parent / ".env"
    load_dotenv(cwd_env if cwd_env.exists() else script_env)


def load_producer_config() -> dict[str, str]:
    load_dotenv_for_demo()
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


def missing_config(config: dict[str, str]) -> list[str]:
    env_by_client_key = {
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "security.protocol": "SECURITY_PROTOCOL",
        "sasl.mechanisms": "SASL_MECHANISMS",
        "sasl.username": "SASL_USERNAME",
        "sasl.password": "SASL_PASSWORD",
    }
    return [env_by_client_key[key] for key, value in config.items() if not value]


def require_producer_config() -> dict[str, str]:
    config = load_producer_config()
    missing = missing_config(config)
    if missing:
        raise SystemExit(f"Missing required .env values: {', '.join(missing)}")
    return config


def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    # Deterministic fake events: same seed + same count = same event stream.
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


def make_trip_events(count: int, seed: int) -> list[TripEvent]:
    rng = random.Random(seed)
    return [make_trip_event(index, rng) for index in range(count)]


def event_key(event: TripEvent) -> bytes:
    # trip_id keeps one trip lifecycle on the same partition in real Kafka.
    return event.trip_id.encode("utf-8")


def serialize_event(event: TripEvent) -> bytes:
    # Kafka values are bytes. Pydantic v2 model_dump_json gives a JSON string.
    return event.model_dump_json(exclude_none=True).encode("utf-8")


def event_dict(event: TripEvent) -> dict[str, Any]:
    return event.model_dump(exclude_none=True)


def write_json_report(run_id: str, demo_name: str, report: dict[str, Any]) -> Path:
    output_dir = Path("outputs") / "runs" / run_id / demo_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "report.json"
    output_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return output_file


def safe_config_report(config: dict[str, str]) -> dict[str, Any]:
    return {
        "bootstrap_host": config["bootstrap.servers"].split("://")[-1],
        "security_protocol": config["security.protocol"],
        "sasl_mechanisms": config["sasl.mechanisms"],
        "has_username": bool(config["sasl.username"]),
        "has_password": bool(config["sasl.password"]),
    }
