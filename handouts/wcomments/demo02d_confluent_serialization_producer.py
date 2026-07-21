"""
================================================================================
DEMO 02D - VALIDATION AND EXPLICIT SERIALIZATION  (annotated tutorial copy)
================================================================================

READ demo02b FIRST. The control flow here is IDENTICAL to Demo 02B - produce,
poll(0), one final flush. Nothing about the delivery strategy is new.

WHAT THIS DEMO TEACHES
    Not speed. This demo is about the BOUNDARY between your Python objects and
    the bytes Kafka actually stores, and about the fact that VALIDATION and
    SERIALIZATION are two different steps that are easy to conflate.

        validated Python object   <- pydantic guarantees it is MEANINGFUL
                  |
                  v
              JSON string          <- a textual representation
                  |
                  v
              UTF-8 bytes          <- what Kafka actually stores
                  |
                  v
          producer.produce()

    Step 1 answers "is this event correct?" (fare not negative, event_type one
    of four allowed values). Steps 2-3 answer "how does it travel?".

WHY THIS DESERVES ITS OWN DEMO
    Because Kafka stores OPAQUE BYTES. A broker never parses your payload and
    will happily accept total garbage. Nothing downstream will warn you. So the
    only thing standing between a malformed event and your consumers is the
    validation you perform BEFORE producing - which is why this demo makes that
    step visible rather than hiding it inside a helper call.

    In Demo 02B the conversion was inlined:
        value=serialize_event(event)
    Here it is hoisted onto its own line:
        value_bytes = serialize_event(event)
        ... value=value_bytes ...

    Functionally identical - the same bytes, the same speed. The point is
    pedagogical: the object-to-bytes step becomes something you can point at,
    log, and inspect. (In production the explicit form earns its keep once
    serialization is non-trivial - an Avro serializer that may call out to a
    Schema Registry and can fail on its own. See Demo 04.)

WHERE VALIDATION ACTUALLY HAPPENS IN THIS SCRIPT
    Not in this file! It happens inside make_trip_events() -> TripEvent(...),
    in demo02_producer_common.py. Constructing a TripEvent with fare=-5 raises
    a pydantic ValidationError immediately, so an invalid event can never reach
    the loop below. By the time an event exists, it is already valid.
================================================================================
"""

from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer

from demo02_producer_common import (
    TOPIC_NAME,
    DeliveryTracker,
    event_key,
    event_dict,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)


def main() -> dict:
    """Produce validated, explicitly-serialized events and report the byte form."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02d")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    args = parser.parse_args()

    config = require_producer_config()
    producer = Producer(config)
    tracker = DeliveryTracker()

    # VALIDATION HAPPENS HERE, invisibly. Every TripEvent inside this list has
    # already passed pydantic's checks - correct types, allowed event_type,
    # non-negative fare. Had any been invalid, this line would have raised and
    # the program would have stopped before touching Kafka at all.
    events = make_trip_events(args.count, args.seed)

    # A HUMAN-READABLE PREVIEW OF THE WIRE FORMAT.
    # serialize_event() gives bytes; .decode("utf-8") turns those bytes back
    # into a str purely so they can be embedded in the JSON report and read.
    #
    # This is the single most instructive line in the demo: it lets you SEE
    # exactly what Kafka is storing, e.g.
    #   {"trip_id":"trip_981","event_type":"trip_requested","rider_id":"rider-981",...}
    # Note there is no driver_id and no fare - a trip_requested event has
    # neither, and exclude_none=True dropped them.
    #
    # The `if events else ""` guard avoids an IndexError when count is 0.
    serialized_preview = serialize_event(events[0]).decode("utf-8") if events else ""

    start = time.perf_counter()

    for event in events:
        # ---- THE EXPLICIT SERIALIZATION STEP --------------------------
        # This is the whole reason Demo 02D exists. Object -> JSON -> UTF-8
        # bytes, on its own named line rather than buried in the produce() call.
        #
        # `value_bytes` is genuinely of type `bytes`, not `str`. If you tried to
        # pass a str here, confluent-kafka would either reject it or silently
        # encode it - and relying on that is how encoding bugs start.
        value_bytes = serialize_event(event)

        producer.produce(
            topic=TOPIC_NAME,

            # The key is serialized too - trip_id.encode("utf-8"). Kafka keys
            # are bytes just like values. Here it stays inline, since the demo's
            # focus is the payload.
            key=event_key(event),

            # The named bytes from above.
            value=value_bytes,

            callback=tracker.callback,
        )

        # Same async pattern as Demo 02B: non-blocking callback service.
        producer.poll(0)

    # The one mandatory wait, exactly once, outside the loop.
    remaining = producer.flush(args.flush_timeout)
    elapsed = max(time.perf_counter() - start, 0.000001)

    # -------------------------------------------------------------------
    # THE REPORT - note the three serialization-specific fields
    # -------------------------------------------------------------------
    report = {
        "demo": "demo02d_confluent_serialization_producer",
        "topic": TOPIC_NAME,
        "attempted": len(events),
        "delivered": len(tracker.delivered),
        "failed": tracker.failed,
        "remaining_after_flush": remaining,
        "elapsed_seconds": round(elapsed, 6),
        "connection": safe_config_report(config),

        # THE BEFORE-AND-AFTER PAIR. These two fields exist so you can compare
        # the two representations of the SAME event side by side in the report:
        #
        #   sample_python_object -> a nested Python dict, as your code sees it
        #   sample_serialized_value -> the compact JSON string that becomes the
        #                              bytes Kafka stores
        #
        # Same information, two forms. That is the boundary this demo is about.
        "sample_python_object": event_dict(events[0]) if events else {},
        "sample_serialized_value": serialized_preview,

        # A literal label naming the wire format. Trivial-looking, but it is the
        # kind of metadata that saves a consumer author from guessing - and it
        # is exactly what Schema Registry formalizes in Demo 04.
        "serialized_type": "UTF-8 JSON bytes",
    }

    output_file = write_json_report(args.run_id, "demo02d_confluent_serialization_producer", report)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")

    if tracker.failed or remaining:
        raise SystemExit("Some messages were not delivered.")

    return report


if __name__ == "__main__":
    main()
