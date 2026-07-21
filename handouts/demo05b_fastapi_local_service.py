"""Demo 05B: run the local FastAPI service and inspect its Swagger UI."""

from __future__ import annotations

import argparse

import uvicorn

from demo05_app import create_local_app

app = create_local_app()


def main() -> None:
    """Run the intentionally interactive local service until Ctrl+C."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    print(f"Swagger UI: http://{args.host}:{args.port}/docs")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
