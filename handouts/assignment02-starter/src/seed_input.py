"""Seed exactly 12 independent Avro events through the FastAPI boundary."""

from __future__ import annotations

import argparse
import json
from typing import Any

from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient

from app import create_app
from cloud import AsyncAvroPublisher, ensure_topic
from config import (
    ConnectionConfigError,
    kafka_config,
    registry_config,
    safe_kafka_config_report,
    safe_registry_config_report,
    topic_name,
    validate_run_id,
    write_json_report,
)
from contracts import deterministic_requests, value_subject


def main() -> dict[str, Any]:
    """Run the bounded API seeder and write secret-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="assignment2")
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    args = parser.parse_args()
    try:
        run_id = validate_run_id(args.run_id)
        requests = deterministic_requests(run_id, args.count)
        if args.partitions < 1 or args.replication_factor < 1:
            raise ValueError("partitions and replication factor must be positive")
        if args.delivery_timeout <= 0:
            raise ValueError("delivery-timeout must be positive")
        producer_config = kafka_config(client_id="msds682-assignment2-api")
        registry_configuration = registry_config()
    except (ValueError, ConnectionConfigError) as exc:
        parser.error(str(exc))

    topic = topic_name()
    status = ensure_topic(
        AdminClient(producer_config),
        topic=topic,
        create=args.create_topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    holder: dict[str, AsyncAvroPublisher] = {}

    async def publisher_factory() -> AsyncAvroPublisher:
        publisher = await AsyncAvroPublisher.create(
            topic=topic,
            producer_config=producer_config,
            registry_config=registry_configuration,
            delivery_timeout=args.delivery_timeout,
        )
        holder["publisher"] = publisher
        return publisher

    responses: list[dict[str, Any]] = []
    statuses: list[int] = []
    with TestClient(create_app(publisher_factory)) as client:
        for payload in requests:
            response = client.post(
                "/trip-requests",
                json=payload.model_dump(mode="json"),
            )
            statuses.append(response.status_code)
            if response.status_code != 202:
                raise RuntimeError(
                    f"Expected HTTP 202, received {response.status_code}: "
                    f"{response.text}"
                )
            responses.append(response.json())

    receipts = [
        receipt.model_dump(mode="json")
        for receipt in holder["publisher"].receipts
    ]
    report = {
        "assignment": "assignment02",
        "phase": "api_seed",
        "run_id": run_id,
        "topic": topic,
        "topic_status": status,
        "schema_subject": value_subject(topic),
        "requested": len(requests),
        "http_202": sum(code == 202 for code in statuses),
        "broker_acknowledged": len(receipts),
        "sequence_numbers": [item.sequence_number for item in requests],
        "trip_ids": [response["trip_id"] for response in responses],
        "delivery_receipts": receipts,
        "kafka_connection": safe_kafka_config_report(producer_config),
        "schema_registry": safe_registry_config_report(registry_configuration),
        "prior_assignment_data_required": False,
    }
    output = write_json_report("api_seed_report.json", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output}")
    if not (
        report["requested"]
        == report["http_202"]
        == report["broker_acknowledged"]
        == 12
    ):
        raise SystemExit("The API seeder did not acknowledge all 12 records")
    return report


if __name__ == "__main__":
    main()
