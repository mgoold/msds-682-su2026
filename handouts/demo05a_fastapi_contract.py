"""Demo 05A: learn the FastAPI contract locally with no Cloud credentials."""

from __future__ import annotations

import argparse
import json

from fastapi.testclient import TestClient

from confluent_demo_common import validate_run_id, write_json_report
from demo05_app import create_local_app
from demo05_common import deterministic_requests, request_input_report


def run_local_contract(*, run_id: str, count: int, seed_offset: int) -> dict:
    """Exercise valid input, invalid input, health, and generated OpenAPI."""

    requests = deterministic_requests(count, seed_offset=seed_offset)
    app = create_local_app()
    accepted: list[dict] = []
    accepted_status_codes: list[int] = []
    with TestClient(app) as client:
        health = client.get("/health")
        health.raise_for_status()
        for payload in requests:
            response = client.post(
                "/trip-requests",
                json=payload.model_dump(mode="json"),
            )
            accepted_status_codes.append(response.status_code)
            if response.status_code != 202:
                raise RuntimeError(f"Expected 202, received {response.status_code}")
            accepted.append(response.json())

        invalid_payload = requests[0].model_dump(mode="json")
        invalid_payload["zone"] = "unknown"
        invalid_payload["unexpected"] = "forbidden"
        invalid = client.post("/trip-requests", json=invalid_payload)
        openapi = client.get("/openapi.json")
        openapi.raise_for_status()

    report = {
        "demo": "demo05a_fastapi_contract",
        "environment": "fully local; no Kafka or Schema Registry required",
        "fastapi_concepts": [
            "application",
            "path operation",
            "request model",
            "response model",
            "status code",
            "lifespan",
            "OpenAPI",
            "TestClient",
        ],
        "input": request_input_report(requests, seed_offset=seed_offset),
        "health": health.json(),
        "accepted_status_codes": accepted_status_codes,
        "accepted": accepted,
        "invalid_status_code": invalid.status_code,
        "invalid_error_locations": [
            row.get("loc", []) for row in invalid.json().get("detail", [])
        ],
        "openapi_has_trip_route": "/trip-requests" in openapi.json()["paths"],
    }
    output = write_json_report(run_id, "demo05a_fastapi_contract", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if invalid.status_code != 422 or not report["openapi_has_trip_route"]:
        raise SystemExit("Demo 05A contract checks did not pass")
    return report


def main() -> dict:
    """Run the bounded local FastAPI introduction."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec5-demo05a")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--seed-offset", type=int, default=0)
    args = parser.parse_args()
    try:
        args.run_id = validate_run_id(args.run_id)
        deterministic_requests(args.count, seed_offset=args.seed_offset)
    except ValueError as exc:
        parser.error(str(exc))
    return run_local_contract(
        run_id=args.run_id,
        count=args.count,
        seed_offset=args.seed_offset,
    )


if __name__ == "__main__":
    main()
