"""
================================================================================
DEMO 05D - THE OPTIONAL LIVE CONFLUENT SERVICE  (annotated tutorial copy)
================================================================================

READ demo05_common.py, demo05_app.py, demo05_kafka.py, AND
demo05c_confluent_fastapi_roundtrip.py FIRST. This demo is to Demo 05C exactly
what Demo 05B was to Demo 05A: the same underlying wiring, run as a real,
interactive server instead of a bounded automated script.

WHAT THIS DEMO TEACHES
    Demo 05C proved the round trip programmatically, with a background
    consumer verifying every event. Demo 05D drops the verification consumer
    entirely and instead lets you open Swagger UI and submit a real request
    by hand against real Confluent Cloud infrastructure - so you can SEE the
    "broker_acknowledged" delivery mode in an actual HTTP response, not just
    in a JSON report file.

WHY THIS SCRIPT IS OPTIONAL AND INTERACTIVE
    demo05.md marks 05D as the optional extension, with 05A/05B/05C forming
    the required 60-minute route. It is interactive for the same reason 05B
    was: a running service is meant to keep serving, not to finish and exit,
    so this script blocks on uvicorn.run() until you stop it with Ctrl+C.

WHAT IS DIFFERENT FROM DEMO 05C, STRUCTURALLY
    Demo 05C needed a background consumer thread AND careful ordering
    (assignment before publishing) because it had to PROVE delivery
    end-to-end automatically. This script has no such consumer - a human
    checking Swagger UI and the Confluent Cloud console IS the verification -
    so build_cloud_app() below is much shorter than 05C's main(): validate
    configuration, ensure the topic exists, build the app, and serve it.
================================================================================
"""

from __future__ import annotations

import argparse

import uvicorn
from confluent_kafka.admin import AdminClient

from confluent_demo_common import (
    ConnectionConfigError,
    TopicSetupError,
    ensure_topic,
    kafka_config,
    schema_registry_config,
)
from demo05_app import create_app
from demo05_common import topic_name
from demo05_kafka import AsyncAvroTripPublisher


def build_cloud_app(*, delivery_timeout: float = 15.0):
    """Build the live app after validating its Cloud configuration.

    Loads the topic name and both credential sets (Kafka, Schema Registry)
    eagerly, at build time, rather than waiting for the first request to
    discover a missing .env value. This mirrors demo05_app.py's comment about
    topic creation being a startup operation, not a per-request one.
    """

    topic = topic_name()
    producer_config = kafka_config(client_id="msds682-demo05-live-aio-producer")
    registry_config = schema_registry_config()

    async def publisher_factory() -> AsyncAvroTripPublisher:
        # Exactly the same AsyncAvroTripPublisher.create() call Demo 05C
        # used - this is the same publisher_factory abstraction from
        # demo05_app.py's create_app(), just supplied by a different script.
        return await AsyncAvroTripPublisher.create(
            topic=topic,
            producer_config=producer_config,
            registry_config=registry_config,
            delivery_timeout=delivery_timeout,
        )

    return create_app(
        publisher_factory,
        mode="confluent",
        app_title="MSDS 682 Demo 05 Live Confluent API",
    )


def main() -> None:
    """Start the interactive Cloud service after an explicit topic check."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if min(args.partitions, args.replication_factor) < 1:
        parser.error("partitions and replication factor must be positive")
    if args.delivery_timeout <= 0:
        parser.error("--delivery-timeout must be positive")

    try:
        # Load Kafka's admin config now, and separately confirm Schema
        # Registry's credentials are present (its return value is discarded
        # here - build_cloud_app() below will load it again when actually
        # constructing the publisher). Failing here, before ensure_topic ever
        # runs, gives a clean one-line message instead of a confusing error
        # from deep inside a Kafka client call.
        admin_config = kafka_config(client_id="msds682-demo05-live-admin")
        schema_registry_config()
    except ConnectionConfigError as exc:
        raise SystemExit(str(exc)) from exc
    topic = topic_name()

    # ========================================================================
    # IMPORTANT NOTE
    # Topic creation is a startup operation. Request handlers only validate,
    # map, publish, and return; they never administer Kafka resources.
    # ========================================================================
    try:
        ensure_topic(
            AdminClient(admin_config),
            topic=topic,
            create=args.create_topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        )
    except TopicSetupError as exc:
        raise SystemExit(f"Demo 05D topic setup failed: {exc}") from None

    app = build_cloud_app(delivery_timeout=args.delivery_timeout)
    print(f"Swagger UI: http://{args.host}:{args.port}/docs")
    print("Stop the interactive service with Ctrl+C when the exercise is complete.")

    # THE BLOCKING CALL, exactly as in Demo 05B - this does not return until
    # you interrupt it. Submitting a request at /docs now produces HTTP 202
    # with "delivery": "broker_acknowledged", proving Kafka truly
    # acknowledged the record - not merely that the local teaching boundary
    # accepted it, as Demo 05B's response would show.
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
