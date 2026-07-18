"""Demo 04B: serialize and deserialize Avro with a mock Schema Registry.

Student focus: the schema is registered once, each payload carries a schema ID
rather than the full schema, Avro binary is not JSON text, and a version-2
reader can read version-1 data when the schema change is backward compatible.

This demo is fully local and deterministic. ``mock://`` is provided by the
Confluent Python client and does not make a network request.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

from demo04_common import (
    TripEventV1,
    avro_dict_to_event,
    avro_dict_to_event_v2,
    avro_subject,
    deserializer_conf,
    deterministic_events,
    event_to_avro_dict,
    parse_confluent_wire_header,
    schema_v1_str,
    schema_v2_str,
    serializer_conf,
    synthetic_data_report,
    validate_run_id,
    validation_error_summary,
    write_json_report,
)

LOCAL_TOPIC = "msds682.demo04.local-avro.v1"


def run_demo(
    registry: SchemaRegistryClient,
    *,
    run_id: str,
    count: int,
) -> dict[str, Any]:
    """Perform the bounded local Avro exercise with an open Registry client."""

    context = SerializationContext(LOCAL_TOPIC, MessageField.VALUE)
    writer_schema = schema_v1_str()
    reader_schema_v2 = schema_v2_str()

    serializer = AvroSerializer(
        registry,
        writer_schema,
        to_dict=event_to_avro_dict,
        conf=serializer_conf(),
    )
    deserializer_v1 = AvroDeserializer(
        registry,
        writer_schema,
        from_dict=avro_dict_to_event,
        conf=deserializer_conf(),
    )
    deserializer_v2 = AvroDeserializer(
        registry,
        reader_schema_v2,
        from_dict=avro_dict_to_event_v2,
        conf=deserializer_conf(),
    )

    seed_offset = 4
    events = deterministic_events(count, seed_offset=seed_offset)
    roundtrips: list[dict[str, Any]] = []
    for event in events:
        payload = serializer(event, context)
        if payload is None:
            raise RuntimeError("Avro serializer unexpectedly returned None")
        decoded_v1 = deserializer_v1(payload, context)
        decoded_v2 = deserializer_v2(payload, context)
        if not isinstance(decoded_v1, TripEventV1):
            raise TypeError("Expected the version-1 deserializer to return TripEventV1")

        roundtrips.append(
            {
                "wire": parse_confluent_wire_header(payload),
                "input": event.report_dict(),
                "decoded_v1": decoded_v1.report_dict(),
                "decoded_v2": decoded_v2.report_dict(),
                "v1_equal": event == decoded_v1,
                "v2_default_vehicle_type": decoded_v2.vehicle_type,
            }
        )

    subject = avro_subject(LOCAL_TOPIC)
    # ========================================================================
    # IMPORTANT NOTE
    # mock:// proves local registration, framing, and reader/writer resolution.
    # It does not prove Cloud permissions or Registry compatibility endpoints.
    # Demo 04C exercises those real services.
    # ========================================================================
    v2_is_backward_compatible = all(
        row["v2_default_vehicle_type"] is None for row in roundtrips
    )
    latest_v1 = registry.get_latest_version(subject)

    # ========================================================================
    # KEY CONCEPT
    # Avro validates wire structure. Pydantic validates application meaning;
    # therefore Avro can encode a double that the fare rule correctly rejects.
    # ========================================================================
    structurally_valid_but_business_invalid = {
        "trip_id": "trip_4999",
        "event_type": "trip_completed",
        "rider_id": "rider_499",
        "event_time": deterministic_events(1)[0].event_time,
        "zone": "north",
        "driver_id": "driver_499",
        "fare": -10.0,
    }
    raw_serializer = AvroSerializer(
        registry,
        writer_schema,
        conf=serializer_conf(),
    )
    structurally_encoded = raw_serializer(structurally_valid_but_business_invalid, context)
    try:
        TripEventV1.model_validate(structurally_valid_but_business_invalid)
        business_validation_errors: list[dict[str, Any]] = []
        business_validation_passed = True
    except ValidationError as exc:
        business_validation_errors = validation_error_summary(exc)
        business_validation_passed = False

    first_payload = serializer(deterministic_events(1, seed_offset=8)[0], context)
    if first_payload is None:
        raise RuntimeError("Avro serializer unexpectedly returned None")
    try:
        json.loads(first_payload.decode("utf-8"))
        json_mismatch_error = None
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        json_mismatch_error = {"type": type(exc).__name__, "message": str(exc)}

    # ====================================================================
    # STUDENT CHECKPOINT
    # For each result below, identify the owner: Pydantic business rules,
    # Avro wire structure, or Schema Registry lookup and evolution.
    # ====================================================================
    report = {
        "demo": "demo04b_local_avro_roundtrip",
        "registry": "mock://msds682-demo04",
        "topic": LOCAL_TOPIC,
        "subject": subject,
        "registered_subjects": registry.get_subjects(),
        "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),
        "writer_schema_id": latest_v1.schema_id,
        "writer_schema_version": latest_v1.version,
        "compatibility_evidence": "V1 writer payloads resolved with the V2 reader schema",
        "v2_backward_compatible": v2_is_backward_compatible,
        "roundtrips": roundtrips,
        "structural_vs_business_validation": {
            "avro_encoded": structurally_encoded is not None,
            "business_validation_passed": business_validation_passed,
            "business_validation_errors": business_validation_errors,
        },
        "serializer_mismatch": {
            "attempt": "interpret Confluent-framed Avro bytes as UTF-8 JSON",
            "error": json_mismatch_error,
        },
        "key_points": [
            "The Kafka payload contains Confluent framing plus Avro binary; Registry resolves the schema by ID.",
            "The schema ID is in the payload header; the full schema lives in Schema Registry.",
            "A backward-compatible reader schema can add a field with an appropriate default.",
            "Application validation remains necessary for business rules that Avro types do not express.",
        ],
    }
    output_file = write_json_report(run_id, "demo04b_local_avro_roundtrip", report)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")

    if not all(row["v1_equal"] for row in roundtrips):
        raise SystemExit("At least one Avro round trip changed the application event.")
    if not v2_is_backward_compatible:
        raise SystemExit("The supplied version-2 reader schema was expected to be backward compatible.")
    if business_validation_passed:
        raise SystemExit("The negative fare was expected to fail application validation.")
    if json_mismatch_error is None:
        raise SystemExit("Avro bytes were unexpectedly accepted as UTF-8 JSON.")
    return report


def main() -> dict[str, Any]:
    """Manage the mock Registry lifecycle and write local evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04b")
    parser.add_argument("--count", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    with SchemaRegistryClient.new_client({"url": "mock://msds682-demo04"}) as registry:
        return run_demo(registry, run_id=args.run_id, count=args.count)


if __name__ == "__main__":
    main()
