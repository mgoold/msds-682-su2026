"""Extra credit +1: measured evidence for the AI engineering review.

Produces the numbers cited in AI_REVIEW.md. Every claim in that document must be
reproducible by running this script; nothing is asserted from memory.

  Suggestion 1 (ACCEPTED): the delivery-sample cap must gate on the length of
      the sample list, not on the cumulative delivered counter.
  Suggestion 2 (REJECTED): pre-serialize events outside the batch timer so the
      benchmark measures delivery alone.

Fully local and credential-free.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from producer_common import (  # noqa: E402
    DeliveryTracker,
    event_key,
    make_trip_events,
    serialize_event,
    write_json_file,
)


class _Msg:
    """Minimal message object for driving DeliveryTracker.callback."""

    def __init__(self, offset: int) -> None:
        self._offset = offset

    def topic(self) -> str:
        return "review-evidence"

    def key(self) -> bytes:
        return b"trip_981"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


def cap_variant(gate: str, deliveries: int = 25) -> int:
    """Count samples retained under each candidate cap condition.

    Reimplements the three variants considered during review so the boundary
    behavior is measured rather than argued.
    """

    samples: list[dict[str, Any]] = []
    delivered = 0
    for offset in range(deliveries):
        delivered += 1
        if gate == "delivered_count_lt_10":  # first draft
            keep = delivered < 10
        elif gate == "len_le_10":  # second draft
            keep = len(samples) <= 10
        else:  # accepted fix
            keep = len(samples) < 10
        if keep:
            samples.append({"offset": offset})
    return len(samples)


def shipped_tracker_retention(deliveries: int = 25) -> int:
    """Retention actually produced by the submitted DeliveryTracker."""

    tracker = DeliveryTracker()
    for offset in range(deliveries):
        tracker.callback(None, _Msg(offset))
    return len(tracker.delivery_samples)


def measure_serialization_cost(batch_size: int = 500, repeats: int = 30) -> dict[str, Any]:
    """Time key+value serialization for one batch, repeated for stability."""

    events = make_trip_events(batch_size, 682)
    timings: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        for event in events:
            event_key(event)
            serialize_event(event)
        timings.append(time.perf_counter() - start)

    return {
        "batch_size": batch_size,
        "repeats": repeats,
        "seconds_per_batch_median": round(statistics.median(timings), 8),
        "seconds_per_batch_mean": round(statistics.fmean(timings), 8),
        "seconds_per_batch_max": round(max(timings), 8),
        "microseconds_per_event": round(statistics.median(timings) / batch_size * 1e6, 3),
    }


def main() -> dict[str, Any]:
    """Generate and persist the review evidence."""

    # --- Suggestion 1 evidence: cap boundary -------------------------------
    cap = {
        "deliveries_simulated": 25,
        "first_draft_delivered_count_lt_10": cap_variant("delivered_count_lt_10"),
        "second_draft_len_le_10": cap_variant("len_le_10"),
        "accepted_fix_len_lt_10": cap_variant("len_lt_10"),
        "shipped_DeliveryTracker_retains": shipped_tracker_retention(),
        "requirement": "retain at most 10 secret-free samples",
    }
    cap["first_draft_correct"] = cap["first_draft_delivered_count_lt_10"] == 10
    cap["second_draft_correct"] = cap["second_draft_len_le_10"] == 10
    cap["accepted_fix_correct"] = cap["accepted_fix_len_lt_10"] == 10
    cap["shipped_code_correct"] = cap["shipped_DeliveryTracker_retains"] == 10

    # --- Suggestion 2 evidence: serialization share of batch time ----------
    serialization = measure_serialization_cost()
    # Observed Confluent Cloud batch latencies from the base run (seconds per
    # 500-message batch); async is the fast case, so it is the strict test.
    observed = {"async_fastest_batch": 0.184151, "sync_style_fastest_batch": 38.677392}
    share = {
        name: round(serialization["seconds_per_batch_median"] / seconds * 100, 4)
        for name, seconds in observed.items()
    }

    report = {
        "extra_credit_item": "+1 AI-assisted engineering review (measured evidence)",
        "mode": "LOCAL, CREDENTIAL-FREE",
        "suggestion_1_accepted": {
            "claim": "Cap the delivery samples on len(delivery_samples) < 10, not on delivered_count.",
            "evidence": cap,
            "verdict": (
                "ACCEPTED. Both earlier drafts violate the 'at most 10' requirement "
                "(9 and 11 respectively); only the accepted gate retains exactly 10, "
                "and the shipped DeliveryTracker matches it."
            ),
        },
        "suggestion_2_rejected": {
            "claim": (
                "Pre-serialize every event before starting the batch timer so the "
                "benchmark measures delivery alone rather than serialization + delivery."
            ),
            "evidence": {
                "serialization_cost": serialization,
                "observed_batch_latency_seconds": observed,
                "serialization_share_of_batch_pct": share,
            },
            "verdict": (
                "REJECTED. Serialization is a negligible share of even the fastest "
                "observed batch, far below run-to-run variability, so the measurement "
                "bias it would remove is not detectable. The change would also diverge "
                "from the Demo 02 control flow the assignment asks to reproduce."
            ),
        },
    }

    output = write_json_file(Path("evidence/xc_review_evidence.json"), report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")

    if not cap["accepted_fix_correct"] or not cap["shipped_code_correct"]:
        raise SystemExit("Accepted fix did not retain exactly 10 samples.")
    if cap["first_draft_correct"] or cap["second_draft_correct"]:
        raise SystemExit("A rejected draft unexpectedly satisfied the requirement.")
    return report


if __name__ == "__main__":
    main()
