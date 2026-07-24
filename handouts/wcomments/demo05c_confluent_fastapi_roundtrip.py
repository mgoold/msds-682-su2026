"""
================================================================================
DEMO 05C - THE REAL CONFLUENT FASTAPI ROUND TRIP  (annotated tutorial copy)
================================================================================

READ demo05_common.py, demo05_app.py, AND demo05_kafka.py FIRST. This script
wires all three together against real Confluent Cloud infrastructure, plus
the independent BoundedTripConsumer from demo05_kafka.py.

WHAT THIS DEMO TEACHES
    One complete, bounded, automated proof of the full path stated in
    demo05.md:

        HTTP JSON -> CreateTripRequest -> request_to_event(...)
          -> strict TripEventV1 -> AsyncAvroSerializer -> AIOProducer -> Kafka
          -> bounded standard Consumer -> AvroDeserializer -> strict TripEventV1
          -> process -> commit

    Four numbers appear in the report - requested, http_202,
    broker_acknowledged, consumed - and the run only succeeds if all four are
    equal. That equality IS the proof: every request became an accepted HTTP
    call, every accepted call was truly acknowledged by the broker, and every
    acknowledged record was independently read back and validated.

THE ORCHESTRATION PROBLEM THIS SCRIPT SOLVES
    The independent consumer (BoundedTripConsumer) starts at "latest" - it
    will only see records produced AFTER it owns its partitions. If the API
    started accepting requests before that assignment was confirmed, the
    earliest published events could vanish from this run's view forever
    (Demo 04C's wait_for_assignment note explains the same hazard for a
    single blocking consumer; here it applies to a background thread
    instead). The sequence below is therefore very deliberately ordered:

        1. start the consumer thread
        2. WAIT for its real partition assignment
        3. only then start posting HTTP requests
        4. mark publishing complete once the last response returns
        5. join the consumer (bounded by --consumer-timeout from that mark)
================================================================================
"""

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

    # -------------------------------------------------------------------
    # STEP 1: COMMAND-LINE ARGUMENTS AND VALIDATION
    # -------------------------------------------------------------------
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec5-demo05c")
    parser.add_argument("--count", type=int, default=3)
    # Opt-in topic creation, same safe default as Demo 04C: never auto-create
    # a topic with settings you did not explicitly request.
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    # THE DOWNSTREAM COMPLETION BUDGET - see the module docstring and
    # demo05.md section 9: this clock starts only after the final HTTP
    # request finishes, via worker.mark_publishing_complete() below.
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

    # A PER-RUN DATA OFFSET, exactly Demo 04C's technique: crc32(run_id) is a
    # stable integer, so the same --run-id always generates the same requests
    # (reproducible), while different students/runs get different request_ids
    # (so concurrent runs on a shared topic do not collide).
    seed_offset = zlib.crc32(args.run_id.encode("utf-8")) % 350
    try:
        requests = deterministic_requests(args.count, seed_offset=seed_offset)
        producer_config = kafka_config(client_id="msds682-demo05-aio-producer")
        admin_config = kafka_config(client_id="msds682-demo05-admin")
        registry_config = schema_registry_config()
    except (ValueError, ConnectionConfigError) as exc:
        # A setup problem (missing .env values, bad --count), not a bug -
        # converted to a clean one-line SystemExit rather than a traceback.
        raise SystemExit(str(exc)) from exc

    # -------------------------------------------------------------------
    # STEP 2: TOPIC AND EXPECTED-KEY SETUP
    # -------------------------------------------------------------------
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

    # THE CONSUMER'S FINITE STOPPING CONDITION. Compute, in advance, exactly
    # which Kafka keys this run's HTTP requests WILL produce once posted -
    # by running each request through the very same request_to_event() +
    # event_key() functions the API itself uses internally. The consumer can
    # then know it is "done" the moment it has seen every one of these keys,
    # rather than guessing from a timeout alone.
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

    # `holder` is a one-item dict rather than a plain variable purely so the
    # nested publisher_factory() closure below can WRITE to an outer-scope
    # name (Python closures can read an enclosing variable but cannot rebind
    # it without `nonlocal`; mutating a dict sidesteps that restriction and
    # lets main() read back the publisher after the `with` block exits).
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

    # THE SAME create_app() FROM demo05_app.py Demo 05A used, now handed a
    # Cloud-backed factory instead of the local one - no route code changes.
    app = create_app(publisher_factory, mode="confluent")
    responses: list[dict[str, Any]] = []
    http_statuses: list[int] = []

    # -------------------------------------------------------------------
    # STEP 3: THE CAREFULLY ORDERED ROUND TRIP
    # -------------------------------------------------------------------
    # ========================================================================
    # STUDENT CHECKPOINT
    # Why must this latest-offset consumer receive a real assignment before
    # the API starts producing? What race would a fixed sleep leave behind?
    # ========================================================================
    #
    # (Answer, for this annotated copy: with auto.offset.reset="latest", the
    #  consumer only sees records appended AFTER its partitions are assigned.
    #  If HTTP posting began even slightly before that assignment completed,
    #  the earliest published events would fall before the consumer's
    #  starting point and be silently missed - the run would then show
    #  broker_acknowledged == 3 but consumed < 3, a confusing partial
    #  failure. A fixed sleep only guesses how long a rebalance takes: too
    #  short and this fails intermittently on a slow network, too long and
    #  every run wastes classroom time. wait_until_ready() blocks on the
    #  actual on_assign callback firing, which is deterministic.)
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

            # Only now does the consumer's completion budget begin - after
            # every request has already received its 202, including whatever
            # broker-acknowledgement wait that response implied.
            worker.mark_publishing_complete()
            consumed = worker.join()
    except BaseException:
        # If anything above failed - an HTTP error, an assignment timeout -
        # make sure the background consumer thread is told to stop and is
        # joined before this script exits, rather than leaving an orphaned
        # thread and an open Kafka connection behind.
        worker.stop()
        try:
            worker.join()
        except BaseException:
            pass
        raise

    # -------------------------------------------------------------------
    # STEP 4: BUILD THE EVIDENCE REPORT
    # -------------------------------------------------------------------
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
        # THE FOUR NUMBERS. On a fully successful run these are all equal to
        # len(requests) - see the module docstring for what each one proves.
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

    # THE ROUND-TRIP ASSERTION. All four counts must agree with the number of
    # requests sent - this is what turns the script into a genuine automated
    # test rather than a demonstration that merely "ran without crashing".
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
