"""Extra credit +1: credential-free deterministic local replay (dry run).

This harness reuses the assignment's event contract (`TripEvent`,
`make_trip_events`, `event_key`, `serialize_event`) and the same produce /
poll(0) / flush control flow as `producer_compare.py`, but sends every message
to an in-memory sink instead of Confluent Cloud.

WHAT THIS IS FOR
    Proving that the event generator is deterministic and that the producer
    control flow is correct, without credentials, network access, or cost.

WHAT THIS IS *NOT*
    ============================================================================
    THESE NUMBERS ARE A HARNESS CHECK, NOT KAFKA PERFORMANCE.
    No broker, no TLS handshake, no network round trip is involved. The
    throughput reported here is the speed of Python plus an in-memory list, and
    it is therefore orders of magnitude faster than the real Confluent Cloud
    benchmark in `results/producer_benchmark.csv`. Never compare the two.
    ============================================================================
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from producer_common import (  # noqa: E402
    DEFAULT_SEED,
    DeliveryTracker,
    TripEvent,
    event_key,
    make_trip_events,
    serialize_event,
    write_json_file,
)

LOCAL_TOPIC = "local-replay.trip-events.v1"


class LocalSinkProducer:
    """In-memory stand-in for confluent_kafka.Producer.

    Mimics the parts of the real client the assignment code touches: produce()
    queues a message, poll(0) serves already-queued callbacks without blocking,
    and flush() drains everything. Offsets increase per partition exactly as a
    real broker would assign them.
    """

    def __init__(self, partitions: int = 3) -> None:
        self.partitions = partitions
        self.pending: list[tuple[Any, "LocalMessage"]] = []
        self.log: dict[int, list[bytes]] = {p: [] for p in range(partitions)}
        self.produced: list[tuple[bytes, bytes]] = []
        self.poll_calls = 0
        self.flush_calls = 0

    def produce(self, topic: str, key: bytes, value: bytes, callback: Any) -> None:
        # Same default partitioner rule the notes describe: hash(key) % N.
        partition = int(hashlib.sha256(key).hexdigest(), 16) % self.partitions
        offset = len(self.log[partition])
        self.log[partition].append(value)
        self.produced.append((key, value))
        self.pending.append((callback, LocalMessage(topic, key, partition, offset)))

    def poll(self, timeout: float) -> int:
        self.poll_calls += 1
        if not self.pending:
            return 0
        callback, message = self.pending.pop(0)
        callback(None, message)
        return 1

    def flush(self, timeout: float) -> int:
        self.flush_calls += 1
        while self.pending:
            callback, message = self.pending.pop(0)
            callback(None, message)
        return 0


class LocalMessage:
    """Minimal message object matching the delivery-callback interface."""

    def __init__(self, topic: str, key: bytes, partition: int, offset: int) -> None:
        self._topic, self._key, self._partition, self._offset = topic, key, partition, offset

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def logical_fingerprint(events: list[TripEvent]) -> str:
    """Return a stable SHA-256 over the serialized event sequence.

    Two runs with the same seed must produce the same fingerprint. This is the
    single value that proves reproducibility.
    """

    digest = hashlib.sha256()
    for event in events:
        digest.update(event_key(event))
        digest.update(b"\x00")
        digest.update(serialize_event(event))
        digest.update(b"\n")
    return digest.hexdigest()


def replay_strategy(events: list[TripEvent], strategy: str, batch_size: int) -> dict[str, Any]:
    """Run one strategy against the local sink using the assignment's control flow."""

    producer = LocalSinkProducer()
    tracker = DeliveryTracker()
    start = time.perf_counter()

    for index in range(0, len(events), batch_size):
        batch = events[index : index + batch_size]
        if strategy == "async":
            for event in batch:
                producer.produce(
                    LOCAL_TOPIC,
                    key=event_key(event),
                    value=serialize_event(event),
                    callback=tracker.callback,
                )
                producer.poll(0)
            producer.flush(30.0)
        else:  # sync_style
            for event in batch:
                producer.produce(
                    LOCAL_TOPIC,
                    key=event_key(event),
                    value=serialize_event(event),
                    callback=tracker.callback,
                )
                producer.flush(30.0)

    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "strategy": strategy,
        "attempted": len(events),
        "delivered": tracker.delivered_count,
        "failed": tracker.failed_count,
        "poll_calls": producer.poll_calls,
        "flush_calls": producer.flush_calls,
        "partitions_used": sorted(p for p, log in producer.log.items() if log),
        "harness_elapsed_seconds": round(elapsed, 6),
    }


def main() -> dict[str, Any]:
    """Replay the event stream twice locally and prove determinism."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="xc-local-replay")
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    # Two independent generations from the same seed.
    first = make_trip_events(args.messages, args.seed)
    second = make_trip_events(args.messages, args.seed)
    fingerprint_a = logical_fingerprint(first)
    fingerprint_b = logical_fingerprint(second)

    # A different seed must produce a different sequence, or the "determinism"
    # result would be vacuous (e.g. if the generator ignored the seed entirely).
    other = make_trip_events(args.messages, args.seed + 1)
    fingerprint_other = logical_fingerprint(other)

    report: dict[str, Any] = {
        "extra_credit_item": "+1 deterministic local replay",
        "mode": "LOCAL DRY RUN - NO KAFKA, NO CREDENTIALS, NO NETWORK",
        "warning": (
            "Timings below are a harness check of Python and an in-memory sink. "
            "They are NOT Kafka performance and must not be compared with "
            "results/producer_benchmark.csv."
        ),
        "run_id": args.run_id,
        "seed": args.seed,
        "messages": args.messages,
        "batch_size": args.batch_size,
        "determinism": {
            "fingerprint_run_1": fingerprint_a,
            "fingerprint_run_2": fingerprint_b,
            "same_seed_reproducible": fingerprint_a == fingerprint_b,
            "fingerprint_different_seed": fingerprint_other,
            "different_seed_differs": fingerprint_a != fingerprint_other,
        },
        "sample_event": json.loads(serialize_event(first[0]).decode("utf-8")),
        "strategies": [
            replay_strategy(first, "async", args.batch_size),
            replay_strategy(first, "sync_style", args.batch_size),
        ],
    }

    output = write_json_file(Path("evidence/xc_local_replay_report.json"), report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")

    if not report["determinism"]["same_seed_reproducible"]:
        raise SystemExit("Local replay was not deterministic for a fixed seed.")
    if not report["determinism"]["different_seed_differs"]:
        raise SystemExit("A different seed unexpectedly produced an identical sequence.")
    return report


if __name__ == "__main__":
    main()
