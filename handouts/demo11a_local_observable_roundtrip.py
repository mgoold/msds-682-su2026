"""Demo 11A: observe accepted, pending, completed, retry, and conflict locally."""

from __future__ import annotations

import argparse
import json
import zlib
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi.testclient import TestClient

from confluent_demo_common import validate_run_id, write_json_report
from demo05_app import LocalTripPublisher
from demo05_common import deterministic_requests
from demo11_app import create_local_observable_app
from demo11_common import SQLiteTripStatusRepository, complete_trip_event


def run_local_observable_roundtrip(*, run_id: str) -> dict[str, Any]:
    """Run the complete credential-free classroom story."""

    seed_offset = zlib.crc32(run_id.encode("utf-8")) % 350
    payload = deterministic_requests(1, seed_offset=seed_offset)[0]
    base_time = datetime(2026, 8, 10, 17, 30, tzinfo=UTC)
    clock_rows = iter((base_time, base_time + timedelta(seconds=3)))
    clock = lambda: next(clock_rows)
    repository = SQLiteTripStatusRepository()
    app = create_local_observable_app(repository=repository, clock=clock)

    with TestClient(app) as client:
        submitted = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        pending = client.get(f"/trip-requests/{payload.request_id}")

        publisher = app.state.publisher
        if not isinstance(publisher, LocalTripPublisher):
            raise TypeError("Demo 11A requires the local teaching publisher")
        if len(publisher.events) != 1:
            raise RuntimeError("Expected exactly one accepted local event")
        completed_by_worker = complete_trip_event(
            repository,
            publisher.events[0],
            compute_owner="python",
            clock=clock,
        )
        completed = client.get(f"/trip-requests/{payload.request_id}")

        retry = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        conflicting_payload = payload.model_dump(mode="json")
        conflicting_payload["zone"] = (
            "south" if conflicting_payload["zone"] != "south" else "north"
        )
        conflict = client.post("/trip-requests", json=conflicting_payload)
        unknown = client.get("/trip-requests/request_9999")
        published_event_count = len(publisher.events)

    report = {
        "demo": "demo11a_local_observable_roundtrip",
        "environment": (
            "fully local teaching double; no Kafka delivery guarantee claimed"
        ),
        "continuation": "Demo 05 HTTP command and TripEventV1 contract",
        "request_id": payload.request_id,
        "first_post_status": submitted.status_code,
        "first_post": submitted.json(),
        "pending_get": pending.json(),
        "worker_result": completed_by_worker.model_dump(mode="json"),
        "completed_get": completed.json(),
        "identical_retry_status": retry.status_code,
        "identical_retry": retry.json(),
        "conflicting_retry_status": conflict.status_code,
        "unknown_status": unknown.status_code,
        "published_event_count": published_event_count,
        "contract_passed": (
            submitted.status_code == 202
            and pending.json()["status"] == "pending"
            and completed.json()["status"] == "completed"
            and retry.status_code == 200
            and retry.json()["created"] is False
            and conflict.status_code == 409
            and unknown.status_code == 404
            and published_event_count == 1
        ),
    }
    repository.close()
    output = write_json_report(run_id, "demo11a_local_observable_roundtrip", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if not report["contract_passed"]:
        raise SystemExit("Demo 11A observable API contract did not pass")
    return report


def main() -> dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec11-demo11a")
    args = parser.parse_args()
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))
    return run_local_observable_roundtrip(run_id=args.run_id)


if __name__ == "__main__":
    main()

