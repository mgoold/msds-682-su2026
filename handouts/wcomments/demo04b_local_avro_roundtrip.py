"""
================================================================================
DEMO 04B - AVRO + SCHEMA REGISTRY, LOCALLY  (annotated tutorial copy)
================================================================================

READ demo04_common.py AND demo04a FIRST.

WHAT THIS DEMO TEACHES
    LAYER 2 of the schema story: the Avro WIRE format and Schema Registry -
    still with no cloud account, no credentials, and no network.

    It uses "mock://", a real SchemaRegistryClient backed by an in-memory
    registry. Registration, schema IDs, and reader/writer resolution all behave
    genuinely; only the network is absent.

THE FOUR THINGS IT DEMONSTRATES, each with an assertion at the bottom:

    1. ROUND TRIP - an event encoded to Avro and decoded back is unchanged.
    2. SCHEMA EVOLUTION - bytes written with schema V1 can be read with schema
       V2, because the added field has a default. This is "backward compatible"
       demonstrated rather than asserted.
    3. AVRO IS NOT JSON - decoding the payload as UTF-8 JSON raises. Avro binary
       is genuinely binary.
    4. THE TWO LAYERS ARE INDEPENDENT - a negative fare encodes to valid Avro
       and is still rejected by the business rules. This is the demo's thesis,
       proven in code.

WHAT mock:// PROVES AND WHAT IT DOES NOT
    The IMPORTANT NOTE in the body says it directly: mock:// proves local
    registration, wire framing, and reader/writer resolution. It does NOT prove
    Cloud permissions, network reachability, or the Registry's compatibility
    ENFORCEMENT endpoints. Demo 04C exercises those against the real service.

    That is a healthy habit generally: know which parts of a system your test
    double actually covers.
================================================================================
"""

from __future__ import annotations

import argparse
import json
from typing import Any

# --- Schema Registry and Avro imports -----------------------------------
#   SchemaRegistryClient - talks to the registry (here, an in-memory mock)
#   AvroSerializer       - application object -> Confluent-framed Avro bytes
#   AvroDeserializer     - Confluent-framed Avro bytes -> application object
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

# SerializationContext tells the serializer WHICH topic and WHICH part of the
# message (key or value) it is working on, because the subject name - and
# therefore the schema - depends on both.
from confluent_kafka.serialization import MessageField, SerializationContext

from pydantic import ValidationError

from demo04_common import (
    TripEventV1,
    avro_dict_to_event,         # Avro dict -> validated V1 model
    avro_dict_to_event_v2,      # Avro dict -> validated V2 model
    avro_subject,               # topic -> "<topic>-value"
    deserializer_conf,
    deterministic_events,
    event_to_avro_dict,         # V1 model -> Avro dict
    parse_confluent_wire_header,
    schema_v1_str,
    schema_v2_str,
    serializer_conf,
    synthetic_data_report,
    validate_run_id,
    validation_error_summary,
    write_json_report,
)

# A LOCAL-ONLY topic name. Nothing is ever sent to Kafka in this demo; the name
# exists solely because the subject-name strategy derives the registry subject
# from a topic ("<topic>-value").
LOCAL_TOPIC = "msds682.demo04.local-avro.v1"


def run_demo(
    registry: SchemaRegistryClient,
    *,
    run_id: str,
    count: int,
) -> dict[str, Any]:
    """Perform the bounded local Avro exercise with an open Registry client."""

    # THE SERIALIZATION CONTEXT: (topic, key-or-value). Passed to every
    # serializer/deserializer call so the library can compute the subject name.
    context = SerializationContext(LOCAL_TOPIC, MessageField.VALUE)

    writer_schema = schema_v1_str()      # what we ENCODE with
    reader_schema_v2 = schema_v2_str()   # an evolved schema we DECODE with

    # ---- THE SERIALIZER -----------------------------------------------
    # Three ingredients:
    #   registry      - where schemas are registered/looked up
    #   writer_schema - the Avro schema text to encode against
    #   to_dict       - a function converting YOUR object into a plain dict
    #                   whose keys match the schema's field names
    #
    # to_dict is the seam between Layer 1 (pydantic) and Layer 2 (Avro): the
    # library knows nothing about pydantic, so you supply the bridge.
    serializer = AvroSerializer(
        registry,
        writer_schema,
        to_dict=event_to_avro_dict,
        conf=serializer_conf(),
    )

    # ---- TWO DESERIALIZERS, ONE FOR EACH READER SCHEMA ----------------
    # The evolution demonstration hinges on decoding the SAME bytes twice.
    deserializer_v1 = AvroDeserializer(
        registry,
        writer_schema,
        from_dict=avro_dict_to_event,
        conf=deserializer_conf(),
    )
    deserializer_v2 = AvroDeserializer(
        registry,
        reader_schema_v2,          # the EVOLVED schema, with vehicle_type
        from_dict=avro_dict_to_event_v2,
        conf=deserializer_conf(),
    )

    seed_offset = 4
    events = deterministic_events(count, seed_offset=seed_offset)
    roundtrips: list[dict[str, Any]] = []

    for event in events:
        # ---- ENCODE ---------------------------------------------------
        # On the FIRST call the serializer registers writer_schema with the
        # registry (auto.register.schemas=True) and remembers the returned
        # schema ID; afterwards it reuses the cached ID.
        #
        # `payload` is: 1 magic byte + 4-byte schema ID + Avro binary body.
        payload = serializer(event, context)
        if payload is None:
            raise RuntimeError("Avro serializer unexpectedly returned None")

        # ---- DECODE THE SAME BYTES TWO WAYS ---------------------------
        # Both deserializers read the schema ID from the header, fetch the
        # WRITER schema from the registry, then resolve it against their own
        # READER schema. That writer/reader resolution is Avro's evolution
        # mechanism.
        decoded_v1 = deserializer_v1(payload, context)
        decoded_v2 = deserializer_v2(payload, context)

        if not isinstance(decoded_v1, TripEventV1):
            raise TypeError("Expected the version-1 deserializer to return TripEventV1")

        roundtrips.append(
            {
                # The parsed 5-byte header: magic byte, schema ID, and the size
                # split between framing and Avro body.
                "wire": parse_confluent_wire_header(payload),

                "input": event.report_dict(),
                "decoded_v1": decoded_v1.report_dict(),
                "decoded_v2": decoded_v2.report_dict(),

                # PROOF OF A LOSSLESS ROUND TRIP. Pydantic models compare by
                # field values, so this is a genuine content comparison.
                "v1_equal": event == decoded_v1,

                # THE EVOLUTION EVIDENCE. The V1 bytes contain no vehicle_type,
                # so the V2 reader supplies its default. Expected: None.
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

    # Backward compatibility holds only if EVERY V1 record read through the V2
    # reader got the default. all(...) over the round trips checks that.
    v2_is_backward_compatible = all(
        row["v2_default_vehicle_type"] is None for row in roundtrips
    )

    # Ask the registry what it now knows: the schema's ID and version number.
    # Proof that registration actually happened during the loop above.
    latest_v1 = registry.get_latest_version(subject)

    # ========================================================================
    # KEY CONCEPT
    # Avro validates wire structure. Pydantic validates application meaning;
    # therefore Avro can encode a double that the fare rule correctly rejects.
    # ========================================================================
    #
    # THE CENTRAL EXPERIMENT OF DEMO 04. This payload is structurally perfect -
    # every field present, every type correct - but fare is -10.0, which is
    # meaningless for a completed trip.
    structurally_valid_but_business_invalid = {
        "trip_id": "trip_4999",
        "event_type": "trip_completed",
        "rider_id": "rider_499",
        "event_time": deterministic_events(1)[0].event_time,
        "zone": "north",
        "driver_id": "driver_499",
        "fare": -10.0,
    }

    # A serializer WITHOUT a to_dict function, so it accepts a raw dict and
    # bypasses the pydantic model entirely. That bypass is the point: it lets us
    # ask what Avro alone thinks.
    raw_serializer = AvroSerializer(
        registry,
        writer_schema,
        conf=serializer_conf(),
    )

    # AVRO ACCEPTS IT. -10.0 is a valid double; Avro has no concept of "a fare
    # cannot be negative". Encoding succeeds.
    structurally_encoded = raw_serializer(structurally_valid_but_business_invalid, context)

    # PYDANTIC REJECTS IT. The exact same data, run through the application
    # model, fails on the ge=0 constraint.
    try:
        TripEventV1.model_validate(structurally_valid_but_business_invalid)
        business_validation_errors: list[dict[str, Any]] = []
        business_validation_passed = True
    except ValidationError as exc:
        business_validation_errors = validation_error_summary(exc)
        business_validation_passed = False
    # Together those two results are the demo's thesis, executed rather than
    # asserted: STRUCTURE and MEANING are different contracts, enforced by
    # different tools, and you need both.

    # ---- DEMONSTRATION: AVRO BYTES ARE NOT JSON TEXT ------------------
    # Deliberately attempt the wrong thing and record the failure.
    first_payload = serializer(deterministic_events(1, seed_offset=8)[0], context)
    if first_payload is None:
        raise RuntimeError("Avro serializer unexpectedly returned None")
    try:
        json.loads(first_payload.decode("utf-8"))
        json_mismatch_error = None       # would mean Avro somehow WAS text
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        # The expected outcome. Two possible errors:
        #   UnicodeDecodeError - the bytes are not valid UTF-8 at all
        #   JSONDecodeError    - they decoded as text but are not JSON
        # Either way: you cannot read Avro with a JSON consumer. Point a Demo 02
        # style consumer at an Avro topic and this is the error you get.
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

        # Everything the registry holds - here just our one subject. Proof that
        # registration is real even in mock mode.
        "registered_subjects": registry.get_subjects(),

        "synthetic_data": synthetic_data_report(events, seed_offset=seed_offset),

        # THE SCHEMA ID that every payload header carries, and the version
        # number within the subject. First registration is version 1.
        "writer_schema_id": latest_v1.schema_id,
        "writer_schema_version": latest_v1.version,

        "compatibility_evidence": "V1 writer payloads resolved with the V2 reader schema",
        "v2_backward_compatible": v2_is_backward_compatible,
        "roundtrips": roundtrips,

        # The two-layers experiment, side by side in one object.
        "structural_vs_business_validation": {
            "avro_encoded": structurally_encoded is not None,      # expected True
            "business_validation_passed": business_validation_passed,  # expected False
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

    # default=str lets json.dumps fall back to str() for any exotic object in
    # the registry metadata.
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {output_file}")

    # ---- FOUR ASSERTIONS: EACH DEMONSTRATION MUST HOLD ----------------
    # These turn the script into a self-verifying document. If a library upgrade
    # or a schema edit broke any claim above, the run fails loudly instead of
    # printing a confident but false report.

    # 1. The round trip must be lossless.
    if not all(row["v1_equal"] for row in roundtrips):
        raise SystemExit("At least one Avro round trip changed the application event.")

    # 2. Evolution must actually be backward compatible.
    if not v2_is_backward_compatible:
        raise SystemExit("The supplied version-2 reader schema was expected to be backward compatible.")

    # 3. NOTE THE INVERSION: this fails if validation PASSED. The negative fare
    #    is SUPPOSED to be rejected; acceptance would mean the business rules
    #    had been weakened.
    if business_validation_passed:
        raise SystemExit("The negative fare was expected to fail application validation.")

    # 4. Likewise inverted: Avro bytes must NOT parse as JSON.
    if json_mismatch_error is None:
        raise SystemExit("Avro bytes were unexpectedly accepted as UTF-8 JSON.")

    return report


def main() -> dict[str, Any]:
    """Manage the mock Registry lifecycle and write local evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec4-demo04b")
    parser.add_argument("--count", type=int, default=4)
    args = parser.parse_args()

    # Bound the count: this is a teaching demo, not a load test.
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    try:
        args.run_id = validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))

    # ---- THE MOCK REGISTRY --------------------------------------------
    # "mock://" is provided by the Confluent client itself. It is a real
    # SchemaRegistryClient whose backend is an in-memory store: no network, no
    # credentials, no account. Schemas registered here vanish when the process
    # ends.
    #
    # The `with` block ensures the client is closed even if run_demo raises.
    with SchemaRegistryClient.new_client({"url": "mock://msds682-demo04"}) as registry:
        return run_demo(registry, run_id=args.run_id, count=args.count)


if __name__ == "__main__":
    main()
