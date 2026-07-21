"""Demo 05C: FastAPI to Confluent Avro to bounded consumer round trip."""

from __future__ import annotations

import argparse
import json
import zlib
from typing import Any

from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient

from confluent_demo_common import (
    ConnectionConfigError,
    TopicSetupError,
    consumer_group_id,
    ensure_topic,
    kafka_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    schema_registry_config,
    validate_run_id,
    write_json_report,
)
from demo05_app import create_app
from demo05_common import (
    deterministic_requests,
    request_input_report,
    request_to_event,
    topic_name,
)
from demo05_kafka import AsyncAvroTripPublisher, BoundedTripConsumer
from trip_event_contract import event_key, value_subject


def main() -> dict[str, Any]:
    """Run a bounded real-Cloud HTTP producer and independent consumer."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec5-demo05c")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--consumer-timeout", type=float, default=20.0)
    args = parser.parse_args()
    try:
        args.run_id = validate_run_id(args.run_id)
        if args.partitions < 1 or args.replication_factor < 1:
            raise ValueError("partitions and replication factor must be positive")
        if min(
            args.assignment_timeout,
            args.delivery_timeout,
            args.consumer_timeout,
        ) <= 0:
            raise ValueError("all timeout values must be positive")
    except ValueError as exc:
        parser.error(str(exc))

    seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 350
    try:
        requests = deterministic_requests(args.count, seed_offset=seed_offset)
        producer_config = kafka_config(client_id="msds682-demo05-aio-producer")
        admin_config = kafka_config(client_id="msds682-demo05-admin")
        registry_config = schema_registry_config()
    except (ValueError, ConnectionConfigError) as exc:
        raise SystemExit(str(exc)) from exc

    topic = topic_name()
    try:
        topic_status = ensure_topic(
            AdminClient(admin_config),
            topic=topic,
            create=args.create_topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        )
    except TopicSetupError as exc:
        raise SystemExit(f"Demo 05C topic setup failed: {exc}") from None
    expected_keys = frozenset(
        event_key(request_to_event(item)) for item in requests
    )
    group_id = consumer_group_id("demo05c-fastapi", args.run_id)
    worker = BoundedTripConsumer(
        topic=topic,
        group_id=group_id,
        expected_keys=expected_keys,
        registry_config=registry_config,
        assignment_timeout=args.assignment_timeout,
        consumer_timeout=args.consumer_timeout,
    )
    holder: dict[str, AsyncAvroTripPublisher] = {}

    async def publisher_factory() -> AsyncAvroTripPublisher:
        publisher = await AsyncAvroTripPublisher.create(
            topic=topic,
            producer_config=producer_config,
            registry_config=registry_config,
            delivery_timeout=args.delivery_timeout,
        )
        holder["publisher"] = publisher
        return publisher

    app = create_app(publisher_factory, mode="confluent")
    responses: list[dict[str, Any]] = []
    http_statuses: list[int] = []
    # ========================================================================
    # STUDENT CHECKPOINT
    # Why must this latest-offset consumer receive a real assignment before
    # the API starts producing? What race would a fixed sleep leave behind?
    # ========================================================================
    worker.start()
    try:
        worker.wait_until_ready()
        with TestClient(app) as client:
            for payload in requests:
                response = client.post(
                    "/trip-requests",
                    json=payload.model_dump(mode="json"),
                )
                http_statuses.append(response.status_code)
                if response.status_code != 202:
                    raise RuntimeError(
                        f"Expected HTTP 202, received {response.status_code}: "
                        f"{response.text}"
                    )
                responses.append(response.json())
            worker.mark_publishing_complete()
            consumed = worker.join()
    except BaseException:
        worker.stop()
        try:
            worker.join()
        except BaseException:
            pass
        raise

    receipts = [
        receipt.model_dump(mode="json")
        for receipt in holder["publisher"].receipts
    ]
    report = {
        "demo": "demo05c_confluent_fastapi_roundtrip",
        "topic": topic,
        "topic_status": topic_status,
        "subject": value_subject(topic),
        "input": request_input_report(requests, seed_offset=seed_offset),
        "group_id": group_id,
        "requested": len(requests),
        "http_202": sum(code == 202 for code in http_statuses),
        "broker_acknowledged": len(receipts),
        "consumed": len(consumed),
        "http_responses": responses,
        "delivery_receipts": receipts,
        "consumed_records": consumed,
        "partition_assignments": worker.assignments,
        "skipped_records_from_other_runs": worker.skipped,
        "producer_connection": safe_kafka_config_report(producer_config),
        "schema_registry": safe_registry_config_report(registry_config),
        "application_lifecycle": "one AIO producer per FastAPI lifespan",
        "commit_rule": (
            "deserialize Avro -> validate TripEventV1 -> process -> "
            "synchronous commit"
        ),
    }
    output = write_json_report(
        args.run_id,
        "demo05c_confluent_fastapi_roundtrip",
        report,
    )
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if not (
        report["http_202"]
        == report["broker_acknowledged"]
        == report["consumed"]
        == len(requests)
    ):
        raise SystemExit("Demo 05C did not complete every pipeline stage")
    return report


if __name__ == "__main__":
    main()
