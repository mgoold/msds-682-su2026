"""Demo 11B: real Kafka acknowledgement, bounded processing, and status API."""

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
from demo05_common import (
    deterministic_requests,
    request_input_report,
    request_to_event,
    topic_name,
)
from demo05_kafka import AsyncAvroTripPublisher, BoundedTripConsumer
from demo11_app import create_observable_app
from demo11_common import SQLiteTripStatusRepository, complete_trip_event
from trip_event_contract import event_key, value_subject


def main() -> dict[str, Any]:
    """Run one bounded real-Cloud request from acceptance to completion."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec11-demo11b")
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
    payload = deterministic_requests(1, seed_offset=seed_offset)[0]
    event = request_to_event(payload)
    try:
        producer_config = kafka_config(client_id="msds682-demo11-aio-producer")
        admin_config = kafka_config(client_id="msds682-demo11-admin")
        registry_config = schema_registry_config()
    except ConnectionConfigError as exc:
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
        raise SystemExit(f"Demo 11B topic setup failed: {exc}") from None

    group_id = consumer_group_id("demo11b-observable-api", args.run_id)
    worker = BoundedTripConsumer(
        topic=topic,
        group_id=group_id,
        expected_keys=frozenset({event_key(event)}),
        registry_config=registry_config,
        assignment_timeout=args.assignment_timeout,
        consumer_timeout=args.consumer_timeout,
    )
    repository = SQLiteTripStatusRepository()
    publisher_holder: dict[str, AsyncAvroTripPublisher] = {}

    async def publisher_factory() -> AsyncAvroTripPublisher:
        publisher = await AsyncAvroTripPublisher.create(
            topic=topic,
            producer_config=producer_config,
            registry_config=registry_config,
            delivery_timeout=args.delivery_timeout,
        )
        publisher_holder["publisher"] = publisher
        return publisher

    app = create_observable_app(
        publisher_factory,
        repository=repository,
        mode="confluent",
    )
    worker.start()
    try:
        worker.wait_until_ready()
        with TestClient(app) as client:
            submitted = client.post(
                "/trip-requests",
                json=payload.model_dump(mode="json"),
            )
            pending = client.get(f"/trip-requests/{payload.request_id}")
            worker.mark_publishing_complete()
            consumed = worker.join()
            completed_by_worker = complete_trip_event(
                repository,
                event,
                compute_owner="python",
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
            receipts = [
                row.model_dump(mode="json")
                for row in publisher_holder["publisher"].receipts
            ]
    except BaseException:
        worker.stop()
        try:
            worker.join()
        except BaseException:
            pass
        repository.close()
        raise

    report = {
        "demo": "demo11b_confluent_observable_roundtrip",
        "continuation": "Demo 05 topic, TripEventV1, publisher, and consumer",
        "topic": topic,
        "topic_status": topic_status,
        "subject": value_subject(topic),
        "input": request_input_report([payload], seed_offset=seed_offset),
        "group_id": group_id,
        "first_post_status": submitted.status_code,
        "first_post": submitted.json(),
        "pending_get": pending.json(),
        "consumed": consumed,
        "worker_result": completed_by_worker.model_dump(mode="json"),
        "completed_get": completed.json(),
        "identical_retry_status": retry.status_code,
        "conflicting_retry_status": conflict.status_code,
        "delivery_receipts": receipts,
        "producer_connection": safe_kafka_config_report(producer_config),
        "schema_registry": safe_registry_config_report(registry_config),
        "contract_passed": (
            submitted.status_code == 202
            and submitted.json()["delivery"] == "broker_acknowledged"
            and pending.json()["status"] == "pending"
            and len(consumed) == 1
            and completed.json()["status"] == "completed"
            and retry.status_code == 200
            and conflict.status_code == 409
            and len(receipts) == 1
        ),
    }
    repository.close()
    output = write_json_report(
        args.run_id,
        "demo11b_confluent_observable_roundtrip",
        report,
    )
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if not report["contract_passed"]:
        raise SystemExit("Demo 11B observable API contract did not pass")
    return report


if __name__ == "__main__":
    main()
