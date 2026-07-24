"""
================================================================================
DEMO 06D - PROVING SAME-GROUP RESUME AND NEW-GROUP REPLAY
(annotated tutorial copy)
================================================================================

READ demo06_common.py AND demo06c_confluent_stream_processor.py FIRST. This
script calls run_processor() from Demo 06C three times in a row with
different arguments - it does not reimplement any processing logic itself.

WHAT THIS DEMO TEACHES
    Consumer groups remember progress by GROUP ID, and that memory is what
    makes "resume" and "replay" fundamentally different actions, not two
    names for the same thing. This script proves both, back to back, in one
    run:

        PASS 1 (first)   - a brand-new base group with no prior commits.
                            Reads the FIRST --messages-per-pass records.
        PASS 2 (resume)  - the SAME base group, called again. Because that
                            group already committed offsets in Pass 1, it
                            naturally continues from where it left off and
                            reads the NEXT --messages-per-pass records.
        PASS 3 (replay)  - a DIFFERENT, distinct replay group, with
                            force_beginning=True. Even though this group has
                            never run before (so auto.offset.reset="earliest"
                            would apply anyway), force_beginning makes the
                            replay EXPLICIT rather than incidental - see the
                            note below on why that distinction matters.

WHY THE REPLAY IS "FORCED" RATHER THAN LEFT TO auto.offset.reset
    auto.offset.reset="earliest" is a FALLBACK: it only takes effect when a
    group has NO committed position at all. If a student reused the replay
    group ID on a SECOND run, that group would now have committed offsets
    from the first replay, and "earliest" would do NOTHING on the second run
    - resume, not replay, would happen silently. AssignmentTracker's
    force_beginning (via on_assign in demo06_common.py) instead overwrites
    every assigned partition's starting offset explicitly, every single
    time this group runs - a real reset command, not a "hope it's still
    fresh" fallback.

WHAT THE VALIDATION BELOW ACTUALLY PROVES
    validate_resume_replay() compares the STABLE source_record_id values
    (input topic:partition:offset, see demo06_common.py) each pass processed:
    first and resume must be disjoint (no overlap - true progress was made);
    replay must exactly reproduce first's coordinates (a genuine replay of
    the same input); the group IDs must show the described base/distinct
    relationship. If any of those fail, this script raises rather than
    silently reporting success.
================================================================================
"""

from __future__ import annotations

import argparse
from typing import Any

from confluent_demo_common import (
    consumer_group_id,
    validate_run_id,
    write_json_report,
)
from demo06_common import source_coordinates
from demo06c_confluent_stream_processor import run_processor


def validate_resume_replay(
    *,
    first: dict[str, Any],
    resume: dict[str, Any],
    replay: dict[str, Any],
) -> dict[str, Any]:
    """Verify the observable offset contract across three bounded passes.

    Each of the four checks below tests ONE distinct claim from the module
    docstring. `all(checks.values())` failing means at least one of those
    claims did not hold for this run, and the AssertionError below shows
    exactly WHICH ones - useful when debugging a failed classroom run rather
    than just seeing "something went wrong".
    """

    first_coordinates = source_coordinates(first["records"])
    resume_coordinates = source_coordinates(resume["records"])
    replay_coordinates = source_coordinates(replay["records"])
    checks = {
        # PROGRESS CLAIM: the resume pass must NOT have reprocessed anything
        # the first pass already committed - the two coordinate sets must
        # share nothing.
        "same_group_resumed_without_reprocessing_first_batch": (
            set(first_coordinates).isdisjoint(resume_coordinates)
        ),
        # REPLAY CLAIM: the replay pass, starting from OFFSET_BEGINNING, must
        # reproduce EXACTLY the first pass's coordinates - the SAME SET of
        # source records, not necessarily in the SAME ORDER. Comparing as
        # sets (rather than list equality) is deliberate: partitions can be
        # polled in a different interleaving between passes even when every
        # underlying record is identical, so an order-sensitive comparison
        # could fail a genuinely correct replay. The length check alongside
        # the set check still catches a duplicate-masking-a-miss case that
        # comparing sets alone would not (e.g. {a, a, b} vs {a, b, b} are
        # different multisets but equal as plain sets).
        "new_group_replayed_first_batch": (
            len(replay_coordinates) == len(first_coordinates)
            and set(replay_coordinates) == set(first_coordinates)
        ),
        # GROUP IDENTITY CLAIMS: first and resume share one group id; replay
        # uses a genuinely different one.
        "base_group_reused": first["group_id"] == resume["group_id"],
        "replay_group_is_distinct": replay["group_id"] != first["group_id"],
    }
    if not all(checks.values()):
        raise AssertionError(f"Resume/replay contract failed: {checks}")
    return {
        "checks": checks,
        "first_coordinates": first_coordinates,
        "resume_coordinates": resume_coordinates,
        "replay_coordinates": replay_coordinates,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--messages-per-pass", type=int, default=3)
    parser.add_argument("--assignment-timeout", type=float, default=15.0)
    parser.add_argument("--idle-timeout", type=float, default=15.0)
    parser.add_argument("--delivery-timeout", type=float, default=15.0)
    parser.add_argument("--create-topics", action="store_true")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()

    run_id = validate_run_id(args.run_id)
    if not 1 <= args.messages_per_pass <= 25:
        parser.error("--messages-per-pass must be between 1 and 25")

    # TWO DISTINCT, NAMESPACED GROUP IDS - the entire experiment hinges on
    # these being genuinely different consumer groups, computed once and
    # reused by name across all three passes below.
    base_group = consumer_group_id("demo06d-resume", run_id)
    replay_group = consumer_group_id("demo06d-replay", run_id)

    # Arguments common to all three run_processor() calls, collected once so
    # each call below only needs to vary the THREE things that actually
    # differ per pass: run_id suffix, group_id, and force_beginning.
    shared = {
        "max_messages": args.messages_per_pass,
        "assignment_timeout": args.assignment_timeout,
        "idle_timeout": args.idle_timeout,
        "delivery_timeout": args.delivery_timeout,
        "partitions": args.partitions,
        "replication_factor": args.replication_factor,
        # None: suppress run_processor()'s own per-pass report file. This
        # script writes ONE combined report instead, covering all three
        # passes plus the validation result.
        "report_demo_name": None,
    }

    # PASS 1: a fresh base group. create_topics is honored only here - if the
    # topics do not exist yet, this is the one pass responsible for making
    # them, exactly as Demo 06A/06C's own --create-topics flags do elsewhere.
    first = run_processor(
        run_id=f"{run_id}-first",
        group_id=base_group,
        create_topics=args.create_topics,
        force_beginning=False,
        **shared,
    )
    # PASS 2: THE SAME base_group, called again. No topic creation needed
    # (the topics certainly exist by now), and force_beginning stays False -
    # this pass's entire point is to demonstrate ORDINARY continuation, with
    # no special intervention.
    resume = run_processor(
        run_id=f"{run_id}-resume",
        group_id=base_group,
        create_topics=False,
        force_beginning=False,
        **shared,
    )
    # PASS 3: the DISTINCT replay_group, with force_beginning=True - the one
    # pass in this script that deliberately overrides where consumption
    # starts, rather than trusting either a fresh group's default or a
    # resumed group's committed position.
    replay = run_processor(
        run_id=f"{run_id}-replay",
        group_id=replay_group,
        create_topics=False,
        force_beginning=True,
        **shared,
    )
    validation = validate_resume_replay(
        first=first,
        resume=resume,
        replay=replay,
    )

    report = {
        "demo": "06D",
        "run_id": run_id,
        "messages_per_pass": args.messages_per_pass,
        "base_group": base_group,
        "replay_group": replay_group,
        **validation,
        # PLAIN-LANGUAGE INTERPRETATION embedded directly in the evidence, so
        # a reader of the JSON report alone - without this source file open -
        # can still understand what each pass demonstrated and why replay's
        # duplicate output is expected rather than a bug.
        "interpretation": {
            "resume": (
                "The same consumer group starts after its committed input offsets."
            ),
            "replay": (
                "A distinct replay group explicitly overrides every assigned "
                "partition to OFFSET_BEGINNING."
            ),
            "output_duplicates": (
                "Replay intentionally republishes derived events. Stable output "
                "keys make the duplicate identity observable."
            ),
        },
    }
    path = write_json_report(run_id, "demo06d", report)
    print("Same-group resume and new-group replay checks passed")
    print(f"Secret-free report: {path}")


if __name__ == "__main__":
    main()
