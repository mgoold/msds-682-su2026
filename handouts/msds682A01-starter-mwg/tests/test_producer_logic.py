"""Credential-free checks for deterministic logic and producer control flow."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from producer_async import run_async  # noqa: E402
from analyze_results import load_and_validate_rows, plot_rows  # noqa: E402
from producer_common import (  # noqa: E402
    event_key,
    make_trip_events,
    safe_config_report,
    serialize_event,
)
from producer_compare import run_strategy, validate_benchmark_arguments, write_csv  # noqa: E402
from producer_serialization import run_serialization_demo  # noqa: E402
from producer_sync import run_sync_style  # noqa: E402


class FakeMessage:
    """Minimal delivery-message interface used by DeliveryTracker."""

    def __init__(self, topic: str, key: bytes, offset: int) -> None:
        self._topic = topic
        self._key = key
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeProducer:
    """In-memory producer double that completes callbacks on poll or flush."""

    def __init__(self) -> None:
        self.pending: list[tuple[Any, FakeMessage]] = []
        self.produced_values: list[bytes] = []
        self.poll_calls = 0
        self.flush_calls = 0

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        callback: Any,
    ) -> None:
        self.produced_values.append(value)
        message = FakeMessage(topic, key, len(self.produced_values) - 1)
        self.pending.append((callback, message))

    def poll(self, timeout: float) -> int:
        self.poll_calls += 1
        if self.pending:
            callback, message = self.pending.pop(0)
            callback(None, message)
            return 1
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_calls += 1
        while self.pending:
            callback, message = self.pending.pop(0)
            callback(None, message)
        return 0


def test_same_seed_replays_same_serialized_events() -> None:
    """The benchmark comparison must use reproducible logical payloads."""

    first = [serialize_event(event) for event in make_trip_events(12, 682)]
    second = [serialize_event(event) for event in make_trip_events(12, 682)]
    assert first == second
    assert event_key(make_trip_events(1, 682)[0]).startswith(b"trip_")
    assert json.loads(first[0])["event_type"] == "trip_requested"


def test_sync_and_async_have_expected_flush_patterns() -> None:
    """Sync flushes per message while async flushes once after polling."""

    events = make_trip_events(4, 682)
    sync_producer = FakeProducer()
    sync_report = run_sync_style(sync_producer, "test-topic", events, 1.0)
    assert sync_report["delivered"] == 4
    assert sync_producer.flush_calls == 4

    async_producer = FakeProducer()
    async_report = run_async(async_producer, "test-topic", events, 1.0)
    assert async_report["delivered"] == 4
    assert async_producer.poll_calls == 4
    assert async_producer.flush_calls == 1


def test_serialization_demo_produces_utf8_json_bytes() -> None:
    """Demo 02D must send bytes created from validated event models."""

    producer = FakeProducer()
    report = run_serialization_demo(producer, "test-topic", make_trip_events(4, 682), 1.0)
    assert report["delivered"] == 4
    assert report["serialized_type"] == "UTF-8 JSON bytes"
    assert all(isinstance(value, bytes) for value in producer.produced_values)


def test_benchmark_records_one_completed_row_per_batch() -> None:
    """The reusable runner records correct callback deltas at batch boundaries."""

    events = make_trip_events(1_000, 682)
    for strategy in ("async", "sync_style"):
        producer = FakeProducer()
        rows = run_strategy(producer, "test-topic", events, strategy, 500, 1.0, "test-run")
        assert len(rows) == 2
        assert all(row["batch_delivered"] == 500 for row in rows)
        assert all(row["batch_failed"] == 0 for row in rows)
        assert all(row["remaining_after_flush"] == 0 for row in rows)


def test_base_benchmark_arguments_are_enforced() -> None:
    """CLI validation protects the 2,000-message and 500-message requirements."""

    validate_benchmark_arguments(2_000, 500)
    with pytest.raises(ValueError):
        validate_benchmark_arguments(1_999, 500)
    with pytest.raises(ValueError):
        validate_benchmark_arguments(2_000, 250)


def test_safe_config_report_excludes_credentials() -> None:
    """Configuration evidence may confirm presence but must not reveal secrets."""

    config = {
        "bootstrap.servers": "pkc.example.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "api-key-value",
        "sasl.password": "api-secret-value",
    }
    serialized = json.dumps(safe_config_report(config, "test-topic"))
    assert "api-key-value" not in serialized
    assert "api-secret-value" not in serialized
    assert '"has_username": true' in serialized
    assert '"has_password": true' in serialized


def test_analyzer_validates_and_plots_complete_evidence(tmp_path: Path) -> None:
    """A complete 80-row benchmark can be validated and plotted offline."""

    rows = []
    for strategy in ("async", "sync_style"):
        for batch_index in range(1, 5):
            rows.append(
                {
                    "run_id": "test-run",
                    "strategy": strategy,
                    "batch_index": batch_index,
                    "batch_message_count": 500,
                    "total_messages_so_far": batch_index * 500,
                    "elapsed_seconds": 1.0,
                    "messages_per_second": 500.0,
                    "batch_delivered": 500,
                    "batch_failed": 0,
                    "remaining_after_flush": 0,
                }
            )
    csv_path = write_csv(tmp_path / "benchmark.csv", rows)
    validated = load_and_validate_rows(csv_path)
    plot_path = plot_rows(validated, tmp_path / "benchmark.png")
    assert len(validated) == 8
    assert plot_path.exists()
