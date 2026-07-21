"""Demo 05D: optional live Confluent FastAPI service with Swagger UI."""

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
    """Build the live app after validating its Cloud configuration."""

    topic = topic_name()
    producer_config = kafka_config(client_id="msds682-demo05-live-aio-producer")
    registry_config = schema_registry_config()

    async def publisher_factory() -> AsyncAvroTripPublisher:
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
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
