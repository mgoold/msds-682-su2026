"""
================================================================================
DEMO 05B - THE LOCAL LIVE SERVICE  (annotated tutorial copy)
================================================================================

READ demo05_common.py, demo05_app.py, AND demo05a_fastapi_contract.py FIRST.
This demo reuses the exact same create_local_app() as 05A - the only
difference is HOW it is run.

WHAT THIS DEMO TEACHES
    Demo 05A proved the FastAPI contract using TestClient, which calls the app
    through Python function calls - useful for automated checks, but it never
    shows you the actual web experience. Demo 05B runs the SAME app as a real
    HTTP server via Uvicorn, so you can open a browser, look at the
    auto-generated Swagger UI at /docs, and submit a request by hand.

WHY THIS SCRIPT IS INTENTIONALLY INTERACTIVE
    Unlike every other demo in this course, this one does not print a JSON
    report and exit - it BLOCKS, serving requests, until you press Ctrl+C.
    That is the nature of a running service: it exists to keep serving, not to
    finish. demo05.md is explicit that 05B (and 05D) remain the intentionally
    interactive demonstrations, while 05A and 05C stay the bounded, automated
    proofs a grader can run unattended.

WHAT YOU SHOULD SEE
    Opening http://127.0.0.1:8001/docs shows Swagger UI listing GET /health
    and POST /trip-requests, each with its request/response schema rendered
    from the same pydantic models demo05a_fastapi_contract.py exercised
    programmatically. Submitting a trip request there returns HTTP 202 with
    "delivery": "local" - proof this is the credential-free boundary, not a
    Kafka delivery.
================================================================================
"""

from __future__ import annotations

import argparse

import uvicorn

from demo05_app import create_local_app

# THE APP OBJECT IS CREATED AT MODULE LEVEL, not inside main(). This matters
# for a subtle reason: Uvicorn can be pointed at either a live object (as
# uvicorn.run(app, ...) does below) or at an IMPORT STRING like
# "demo05b_fastapi_local_service:app" (used for features like --reload). Both
# styles need a module-level `app` name to find, so it is created here rather
# than nested inside a function.
app = create_local_app()


def main() -> None:
    """Run the intentionally interactive local service until Ctrl+C."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    # Printed BEFORE the blocking call below, so the URL is visible the
    # instant the process starts rather than buried after Uvicorn's own
    # startup logging.
    print(f"Swagger UI: http://{args.host}:{args.port}/docs")

    # THE BLOCKING CALL. uvicorn.run() does not return until the process is
    # interrupted (Ctrl+C) or otherwise stopped. Everything demo05_app.py
    # wired up - the lifespan hook, both routes - now runs for real, over a
    # real TCP socket, exactly as it would in production, just on localhost.
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
