"""
================================================================================
DEMO 05A - THE LOCAL FASTAPI CONTRACT  (annotated tutorial copy)
================================================================================

READ demo05_common.py AND demo05_app.py FIRST. Everything imported below is
defined and explained there.

WHAT THIS DEMO TEACHES
    A fully local, credential-free tour of every FastAPI concept the course
    cares about, run through FastAPI's own TestClient rather than a live
    server: application, path operation, request model, response model,
    status code, lifespan, and OpenAPI. No Kafka cluster, no Schema Registry,
    no .env file is touched anywhere in this script.

WHY LOCAL FIRST
    Demo 05C (the real Confluent round trip) has a lot of new machinery -
    async producers, Schema Registry, a background consumer thread. Debugging
    a FastAPI mistake AND a Cloud connectivity mistake at the same time is
    needlessly hard. 05A isolates the FastAPI half completely, so any problem
    encountered here is guaranteed to be about the API contract, not about
    credentials or network access.

WHAT THIS SCRIPT DOES END TO END
    1. build the credential-free local app (create_local_app())
    2. use TestClient to call /health, expecting 200
    3. post several valid deterministic requests, expecting 202
    4. post one deliberately invalid request, expecting 422
    5. fetch /openapi.json and confirm the trip-request route is documented
    6. write a secret-free JSON report and exit non-zero if any check failed
================================================================================
"""

from __future__ import annotations

import argparse
import json

# FastAPI's own TestClient wraps an app so it can be called with plain Python
# function calls instead of real HTTP sockets. It still runs the ENTIRE
# request pipeline - routing, validation, your handler code, lifespan - just
# without opening a port. Using it as a `with` block (below) is what triggers
# the lifespan startup/shutdown hooks from demo05_app.py.
from fastapi.testclient import TestClient

from confluent_demo_common import validate_run_id, write_json_report
from demo05_app import create_local_app
from demo05_common import deterministic_requests, request_input_report


def run_local_contract(*, run_id: str, count: int, seed_offset: int) -> dict:
    """Exercise valid input, invalid input, health, and generated OpenAPI."""

    # Generate the deterministic requests UP FRONT, exactly as Demo 02's sync
    # producer generated all its events before timing started - here there is
    # no timing concern, but the same "prepare input, then exercise the
    # system" separation keeps the script easy to read top to bottom.
    requests = deterministic_requests(count, seed_offset=seed_offset)
    app = create_local_app()
    accepted: list[dict] = []
    accepted_status_codes: list[int] = []

    # ENTERING THE `with` BLOCK is what runs create_app()'s lifespan startup
    # (constructing the LocalTripPublisher); EXITING it runs lifespan
    # shutdown (publisher.close()). Every request below happens between those
    # two moments, which is exactly how a real running server would behave.
    with TestClient(app) as client:
        health = client.get("/health")
        health.raise_for_status()

        for payload in requests:
            # payload is a CreateTripRequest (a pydantic model); model_dump
            # turns it back into a plain JSON-compatible dict so TestClient can
            # send it as the request body, the same way a real HTTP client
            # would send JSON text over the wire.
            response = client.post(
                "/trip-requests",
                json=payload.model_dump(mode="json"),
            )
            accepted_status_codes.append(response.status_code)
            if response.status_code != 202:
                raise RuntimeError(f"Expected 202, received {response.status_code}")
            accepted.append(response.json())

        # THE DELIBERATE FAILURE CASE. Take one otherwise-valid payload and
        # break it in two ways at once: an out-of-range zone value, and an
        # extra field the model does not declare. CreateTripRequest's
        # extra="forbid" and its zone: ServiceZone Literal type mean FastAPI
        # rejects this automatically, before create_trip() ever runs.
        invalid_payload = requests[0].model_dump(mode="json")
        invalid_payload["zone"] = "unknown"
        invalid_payload["unexpected"] = "forbidden"
        invalid = client.post("/trip-requests", json=invalid_payload)

        # THE GENERATED DOCUMENTATION. /openapi.json is FastAPI's machine
        # readable schema of every route, request model, and response model -
        # built automatically from the type hints in demo05_app.py. This is
        # the same document Swagger UI (Demo 05B's /docs page) renders visually.
        openapi = client.get("/openapi.json")
        openapi.raise_for_status()

    report = {
        "demo": "demo05a_fastapi_contract",
        "environment": "fully local; no Kafka or Schema Registry required",
        # Naming the seven concepts explicitly in the evidence file, matching
        # demo05.md's stated learning goals for this exercise.
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
        # Pydantic's 422 error body lists WHICH field(s) failed under "loc"
        # (e.g. ["body", "zone"]). Pulling just those locations into the
        # report shows a reader that the failure was specific and expected,
        # not a generic server error.
        "invalid_error_locations": [
            row.get("loc", []) for row in invalid.json().get("detail", [])
        ],
        "openapi_has_trip_route": "/trip-requests" in openapi.json()["paths"],
    }
    output = write_json_report(run_id, "demo05a_fastapi_contract", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")

    # FAIL LOUDLY if either proof this script exists to demonstrate did not
    # hold: the invalid payload must have been rejected, and the route must
    # be visible in OpenAPI. A silently passing script that actually proved
    # nothing would be worse than a crash.
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
        # Validate BOTH the run-id (path-safety) and the requested count
        # (deterministic_requests' own bounds check) before doing any real
        # work, so bad CLI input fails in milliseconds with a clear message.
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
