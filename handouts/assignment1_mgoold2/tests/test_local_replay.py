"""Extra credit +1: credential-free replay tests.

These prove the assignment's event contract is reproducible and that the local
replay harness reproduces the same produce/poll/flush control flow as the real
producers -- without credentials, network access, or a Confluent account.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for folder in ("src", "extra_credit"):
    path = str(PROJECT_ROOT / folder)
    if path not in sys.path:
        sys.path.insert(0, path)

from local_replay import (  # noqa: E402
    LocalSinkProducer,
    logical_fingerprint,
    replay_strategy,
)
from producer_common import event_key, make_trip_events, serialize_event  # noqa: E402


def test_same_seed_replays_identical_logical_sequence() -> None:
    """The required proof: one seed always yields one logical event stream."""

    first = make_trip_events(500, 682)
    second = make_trip_events(500, 682)

    assert logical_fingerprint(first) == logical_fingerprint(second)
    # Field-level equality, not just the digest, so a hash collision cannot pass.
    assert [serialize_event(e) for e in first] == [serialize_event(e) for e in second]
    assert [event_key(e) for e in first] == [event_key(e) for e in second]


def test_different_seed_produces_a_different_sequence() -> None:
    """Guards against a vacuous pass if the generator ignored its seed."""

    assert logical_fingerprint(make_trip_events(500, 682)) != logical_fingerprint(
        make_trip_events(500, 683)
    )


def test_replay_is_stable_across_repeated_runs() -> None:
    """Replaying the same events twice delivers the same count and partitions."""

    events = make_trip_events(1_000, 682)
    first = replay_strategy(events, "async", 500)
    second = replay_strategy(events, "async", 500)

    assert first["delivered"] == second["delivered"] == 1_000
    assert first["failed"] == second["failed"] == 0
    assert first["partitions_used"] == second["partitions_used"]


@pytest.mark.parametrize(
    ("strategy", "expected_flushes", "expected_polls"),
    [("async", 2, 1_000), ("sync_style", 1_000, 0)],
)
def test_replay_matches_each_strategy_control_flow(
    strategy: str, expected_flushes: int, expected_polls: int
) -> None:
    """Async flushes once per batch; sync_style flushes once per message."""

    events = make_trip_events(1_000, 682)
    result = replay_strategy(events, strategy, 500)

    assert result["delivered"] == 1_000
    assert result["flush_calls"] == expected_flushes
    assert result["poll_calls"] == expected_polls


def test_same_key_always_lands_on_one_partition() -> None:
    """Keying by trip_id keeps a trip's whole lifecycle on a single partition."""

    producer = LocalSinkProducer()
    events = make_trip_events(200, 682)
    seen: dict[bytes, set[int]] = {}

    for event in events:
        key = event_key(event)
        producer.produce(
            "local-replay.trip-events.v1",
            key=key,
            value=serialize_event(event),
            callback=lambda err, msg: None,
        )
        _, message = producer.pending[-1]
        seen.setdefault(key, set()).add(message.partition())

    assert all(len(partitions) == 1 for partitions in seen.values())
