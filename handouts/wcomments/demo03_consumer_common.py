"""
================================================================================
DEMO 03 - SHARED CONSUMER MODULE  (annotated tutorial copy)
================================================================================

WHAT THIS FILE IS
    The consumer counterpart to demo02_producer_common.py. All four Demo 03
    scripts import from here. It defines, once:

        1. CONSUMER CONFIG   - the extra settings a consumer needs beyond a producer
        2. GROUP IDs         - how a consumer identifies which "reader" it is
        3. THE POLL LOOP     - consume_records(), the heart of every consumer
        4. DECODING          - bytes back into a validated TripEvent
        5. TRACKERS          - recording commits and partition assignments

    Note it IMPORTS from demo02_producer_common: the same topic, the same
    TripEvent model, the same connection loader. That reuse is the point - the
    producer and consumer must agree on the data contract, and the cleanest way
    to guarantee that is to share the literal same class.

THE BIG SHIFT FROM DEMO 02: READING IS HARDER THAN WRITING
    Producing is basically "hand over bytes and confirm they landed." Consuming
    introduces genuinely new concepts, all of which appear in this file:

    - CONSUMER GROUPS: consumers are organized into named groups. Within a
      group, each partition is read by exactly ONE member, which is how you
      scale out. Different groups are independent and each sees ALL messages.

    - COMMITTED OFFSETS: Kafka remembers, per group, "how far have you read?"
      Reading does NOT consume or delete the message - the log stays put. Your
      position is just a bookmark, and you control when it advances.

      * Note: a "commit" in this context is just your consumer saving the last offset of its read.  So when you commit, you "save your place, which is the offset you were at in that partition at that moment.

    - REBALANCING: when members join or leave a group, Kafka reassigns
      partitions among them. Your code can observe this via callbacks.

    - AT-LEAST-ONCE DELIVERY: because "process" and "commit" are two separate
      steps, a crash between them causes a message to be reprocessed. Kafka's
      default guarantee is therefore at-least-once, and your processing should
      be idempotent (safe to run twice).

WHY EVERY LOOP HERE IS BOUNDED
    A real consumer runs forever. These demos must terminate, so
    consume_records() stops on explicit limits (max messages, idle time, wall
    clock). That is a teaching device, not production practice.
================================================================================
"""

from __future__ import annotations

import json
import os
import re      # for sanitizing group-ID strings
import time
from dataclasses import dataclass, field
from typing import Any, Literal

# --- Kafka imports ------------------------------------------------------
#   KafkaError      - error codes attached to messages/operations
#   KafkaException  - the exception type raised for real failures
#   OFFSET_BEGINNING- a sentinel meaning "the very start of the partition",
#                     used to force a replay (see AssignmentTracker below)
from confluent_kafka import KafkaError, KafkaException, OFFSET_BEGINNING

# REUSING THE PRODUCER'S CONTRACT. This is deliberate and important: the
# consumer validates incoming data with the SAME TripEvent class the producer
# used to create it. If these two ever drift apart, messages that were valid on
# write become invalid on read - the classic schema-drift failure.
from demo02_producer_common import (
    TOPIC_NAME,               # the same topic Demo 02 wrote to
    TripEvent,                # the same validation model
    load_dotenv_for_demo,     # the same .env loader
    require_producer_config,  # the same credential check (connection settings
                              # are identical for producers and consumers)
    safe_config_report,       # the same secret-free reporting helper
)


# --- Type aliases -------------------------------------------------------
# `Literal` restricts a value to an exact set of strings. These give the
# functions below self-documenting parameters and catch typos in a type checker.

# How (and whether) the consumer advances its saved position:
#   "none"  - never commit (each run re-reads the same messages)
#   "sync"  - commit and BLOCK until the broker confirms
#   "async" - commit and continue; confirmation arrives via callback
CommitMode = Literal["none", "sync", "async"]

# Where to start reading WHEN THE GROUP HAS NO SAVED POSITION:
#   "earliest" - the oldest retained message
#   "latest"   - only messages produced from now on
#   "error"    - refuse to start
OffsetReset = Literal["earliest", "latest", "error"]


def normalize_identifier(value: str) -> str:
    """Make an arbitrary string safe to use inside a Kafka group ID.

    Kafka group IDs tolerate a limited character set. This replaces any run of
    disallowed characters with a single hyphen, trims stray hyphens from the
    ends, and falls back to "demo" if nothing usable remains.

    Example: "My Run #3!" -> "My-Run-3"
    """
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip())
    # `or "demo"` catches the case where the input reduced to an empty string.
    return normalized.strip("-") or "demo"


def default_group_id(demo_name: str, run_id: str | None = None) -> str:
    """Build a predictable consumer-group ID like "msds682-su2026-demo03a-basic".

    WHAT A GROUP ID IS - this is the most important consumer concept.

    The group ID is the IDENTITY UNDER WHICH KAFKA REMEMBERS YOUR PROGRESS.
    Kafka stores committed offsets per (group, topic, partition). So:

      - Run twice with the SAME group ID -> the second run RESUMES where the
        first stopped. This is what makes a consumer restartable.
      - Run with a DIFFERENT group ID -> Kafka has no saved position for it, so
        auto.offset.reset applies and you may re-read from the beginning.
      - Run two processes with the same group ID SIMULTANEOUSLY -> they SHARE
        the partitions between them (each partition to exactly one member).
      - Run two processes with DIFFERENT group IDs -> each independently reads
        EVERY message. This is how one topic feeds many applications.

    So changing the group ID is not cosmetic; it changes which history you
    inherit. Keeping it stable and predictable (as here) is what makes "run it
    again and watch it resume" a demonstrable behavior.

    The optional prefix comes from CONSUMER_GROUP_ID_PREFIX in .env, so students
    sharing one cluster do not collide with each other's progress.
    """
    load_dotenv_for_demo()
    prefix = normalize_identifier(os.getenv("CONSUMER_GROUP_ID_PREFIX", "msds682-su2026"))
    parts = [prefix, normalize_identifier(demo_name)]
    if run_id:
        parts.append(normalize_identifier(run_id))
    return "-".join(parts)


def build_consumer_config(
    kafka_config: dict[str, str],
    *,                       # everything after this must be passed BY NAME,
                             # which prevents mixing up same-typed arguments
    group_id: str,
    auto_offset_reset: OffsetReset,
    enable_auto_commit: bool,
    client_id: str,
    on_commit: Any | None = None,
) -> dict[str, Any]:
    """Add consumer-only settings to the shared connection config.

    The CONNECTION settings (bootstrap.servers, security.protocol, SASL) are
    identical for producers and consumers - they describe the cluster, not the
    role. What follows are the four settings unique to reading.
    """
    config: dict[str, Any] = {
        # Spread the shared connection settings in first.
        **kafka_config,

        # ---- 1. GROUP ID: who am I? ----------------------------------
        # Required for any consumer that wants Kafka to track its progress.
        "group.id": group_id,

        # ---- 2. CLIENT ID: which process am I? ------------------------
        # Purely a label, used in broker logs and metrics. Multiple members of
        # the SAME group have different client IDs. It has no effect on
        # behavior; it exists to make debugging possible.
        "client.id": client_id,

        # ---- 3. WHERE TO START, IF THERE IS NO SAVED POSITION ---------
        # THE SINGLE MOST MISUNDERSTOOD CONSUMER SETTING.
        #
        # This applies ONLY when the group has no committed offset for a
        # partition - a brand-new group, or one whose saved offsets expired.
        #
        #   "earliest" -> start at the oldest retained message
        #   "latest"   -> start at the end; only see NEW messages
        #
        # If the group DOES have a committed offset, this setting is IGNORED
        # entirely and the consumer resumes from the saved position. It is a
        # fallback, not a "seek to here" instruction. People routinely set
        # earliest, see nothing re-read, and conclude it is broken - when in
        # fact the group simply had saved progress.
        "auto.offset.reset": auto_offset_reset,

        # ---- 4. WHO DECIDES WHEN PROGRESS IS SAVED --------------------
        #   True  -> the client commits periodically in the background (easy,
        #            but it may commit a message you have not finished
        #            processing, risking data loss on a crash)
        #   False -> YOUR CODE commits explicitly, after processing succeeds
        "enable.auto.commit": enable_auto_commit,
    }

    if not enable_auto_commit:
        # A SECOND, FINER SWITCH that only matters in manual mode.
        #
        # The client keeps an internal "stored offset" that commits draw from.
        # By default, merely RECEIVING a message from poll() stores its offset -
        # meaning "I have read this", which is not the same as "I have
        # successfully processed this".
        #
        # Setting this False means nothing is stored automatically: the
        # application decides what counts as progress. That is what makes
        # "process first, then commit" actually enforceable.
        config["enable.auto.offset.store"] = False

    if on_commit is not None:
        # A callback invoked when an ASYNCHRONOUS commit completes. Async
        # commits return immediately without telling you the outcome, so
        # without this callback a failed commit is silent. Same pattern as the
        # producer's delivery callback in Demo 02.
        config["on_commit"] = on_commit

    return config


def require_consumer_config(
    *,
    group_id: str,
    auto_offset_reset: OffsetReset = "earliest",
    enable_auto_commit: bool = True,
    client_id: str,
    on_commit: Any | None = None,
) -> dict[str, Any]:
    """Load credentials AND build the full consumer config, or exit.

    Thin wrapper: require_producer_config() supplies the connection half (and
    exits if credentials are missing); build_consumer_config() adds the
    consumer half.
    """
    return build_consumer_config(
        require_producer_config(),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        client_id=client_id,
        on_commit=on_commit,
    )


def safe_consumer_config_report(config: dict[str, Any]) -> dict[str, Any]:
    """Secret-free summary of the consumer's configuration, for the report.

    Extends the producer's version with the four consumer-only settings. None
    of these are secret - and recording them is genuinely useful, because
    "which group ID did this run use?" is the first question you ask when a
    consumer does not see the messages you expected.
    """
    connection = safe_config_report(config)
    return {
        **connection,
        "group_id": config["group.id"],
        "client_id": config["client.id"],
        "auto_offset_reset": config["auto.offset.reset"],

        # bool(...) normalizes to real booleans for clean JSON.
        "enable_auto_commit": bool(config["enable.auto.commit"]),

        # .get(..., True) because this key is only present in manual-commit
        # mode; when absent, the library default (True) applies.
        "enable_auto_offset_store": bool(config.get("enable.auto.offset.store", True)),
    }


def decode_utf8(value: bytes | None, field_name: str) -> str | None:
    """Turn Kafka bytes back into a str, with a useful error if they are not text.

    THE MIRROR IMAGE OF THE PRODUCER'S .encode("utf-8"). The producer turned a
    str into bytes; the consumer must turn bytes back into a str. Kafka stored
    and returned them unchanged and unexamined.

    The try/except matters because Kafka CANNOT guarantee the bytes are UTF-8.
    Someone could produce Avro, protobuf, or random binary to this topic and
    the broker would accept it happily. Catching UnicodeDecodeError and
    re-raising with a clear message turns a cryptic failure into a diagnosable
    one. (`raise ... from exc` preserves the original traceback.)
    """
    if value is None:
        # Legitimately possible: Kafka permits null keys and null values. A null
        # value is in fact meaningful - it is a "tombstone" that marks a key as
        # deleted in a compacted topic.
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Kafka {field_name} is not valid UTF-8") from exc


def message_to_record(message: Any) -> dict[str, Any]:
    """Convert one raw Kafka message into a validated, JSON-safe dict.

    THIS IS THE FULL INBOUND PIPELINE, and it is the exact reverse of what the
    producer did:

        producer:  TripEvent -> JSON str -> UTF-8 bytes -> Kafka
        consumer:  Kafka -> UTF-8 bytes -> JSON str -> TripEvent

    Note that VALIDATION HAPPENS AGAIN HERE, on the way in. That is not
    redundant paranoia:
      - the broker validated nothing;
      - the producer that wrote this message might have been an older version
        of your code, or someone else's program entirely.
    Re-validating on read is how you find out immediately, rather than three
    functions later via a confusing AttributeError.
    """
    raw_value = decode_utf8(message.value(), "value")
    if raw_value is None:
        raise ValueError("Kafka message value is missing")

    try:
        # model_validate_json parses the JSON *and* enforces the model rules in
        # one step: types, the four allowed event_type values, fare >= 0.
        event = TripEvent.model_validate_json(raw_value)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Kafka value is not a valid Demo 02 TripEvent JSON document") from exc

    # ---- MESSAGE METADATA ---------------------------------------------
    # Every Kafka message carries a timestamp alongside the payload.
    # message.timestamp() returns a (type, milliseconds) tuple, where type says
    # whether the time came from the producer (CreateTime) or the broker
    # (LogAppendTime). The hasattr guard makes this tolerant of test doubles
    # that do not implement timestamp().
    timestamp_type: int | None = None
    timestamp_ms: int | None = None
    if hasattr(message, "timestamp"):
        timestamp_type, timestamp_ms = message.timestamp()

    return {
        # THE COORDINATES OF THIS RECORD. (topic, partition, offset) is its
        # unique, permanent address in the cluster - the closest thing Kafka
        # has to a primary key, and exactly what you would store if you needed
        # to deduplicate reprocessed messages.
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),

        "timestamp_type": timestamp_type,
        "timestamp_ms": timestamp_ms,

        # The key, decoded (the trip_id the producer used for partitioning).
        "key": decode_utf8(message.key(), "key"),

        # The validated payload as a plain dict, ready for json.dumps().
        "event": event.model_dump(exclude_none=True),
    }


def topic_partition_records(partitions: Any | None) -> list[dict[str, int | str]]:
    """Convert Kafka TopicPartition objects into plain dicts for JSON output.

    TopicPartition is a small library object (topic, partition, offset) that
    appears in assignment callbacks and commit results. It is not JSON
    serializable, so this flattens it for the report.
    """
    if not partitions:
        return []
    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,

            # In this context the offset usually means "the position", e.g. the
            # offset that was just committed, or where reading will resume.
            "offset": partition.offset,
        }
        for partition in partitions
    ]


@dataclass
class CommitTracker:
    """Records the outcome of ASYNCHRONOUS offset commits.

    @dataclass generates __init__ and friends automatically.
    field(default_factory=list) gives each instance its OWN empty list - using
    a bare `= []` default would share one list across all instances, a classic
    Python bug.

    WHY THIS EXISTS: an async commit returns immediately without telling you
    whether it worked. The only way to find out is a callback - the same
    deferred-result pattern as the producer's delivery callback in Demo 02.
    A silently failed commit means the next run re-reads messages you thought
    were done.
    """

    acknowledged: list[list[dict[str, int | str]]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def callback(self, error: Any, partitions: Any) -> None:
        """Invoked by the client when an async commit completes or fails."""
        if error is not None:
            self.failed.append(str(error))
            return
        self.acknowledged.append(topic_partition_records(partitions))


@dataclass
class AssignmentTracker:
    """Observes REBALANCES, and optionally forces a replay from the beginning.

    WHAT A REBALANCE IS
        Partitions are distributed among the members of a consumer group. When
        membership changes - a member starts, stops, or crashes - Kafka
        REBALANCES: it revokes current assignments and hands out new ones so
        every partition has exactly one owner.

        You can hook this via two callbacks passed to subscribe():
            on_assign - "you now own these partitions"
            on_revoke - "you are losing these partitions"

        These are the right places to initialize per-partition state, or to
        flush/commit work in progress before losing a partition.
    """

    # When True, on_assign overrides the assigned positions to force a replay.
    force_beginning: bool = False
    assigned: list[list[dict[str, int | str]]] = field(default_factory=list)
    revoked: list[list[dict[str, int | str]]] = field(default_factory=list)

    def on_assign(self, consumer: Any, partitions: Any) -> None:
        """Called when this member is given partitions to read."""
        if self.force_beginning:
            # ---- EXPLICIT REPLAY --------------------------------------
            # THIS is how you deliberately re-read a topic from the start.
            #
            # Note it is NOT done with auto.offset.reset - that only applies
            # when a group has no saved position at all. To override an
            # EXISTING position you must set each partition's offset yourself
            # and re-assign:
            #
            #   OFFSET_BEGINNING is a sentinel constant meaning "the oldest
            #   available message in this partition".
            #
            # Kafka makes replay require an explicit override precisely so it
            # cannot happen by accident - reprocessing an entire topic is
            # expensive and, if your processing is not idempotent, harmful.
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

        rows = topic_partition_records(partitions)
        self.assigned.append(rows)

        # Printing makes the rebalance visible while the demo runs - useful when
        # you start a second member and watch partitions move between them.
        print(f"Assigned partitions: {rows}")

    def on_revoke(self, _consumer: Any, partitions: Any) -> None:
        """Called just BEFORE partitions are taken away from this member.

        In a production consumer this is where you would commit offsets for
        work already finished, so the member taking over does not redo it.
        The leading underscore in `_consumer` signals the argument is required
        by the callback signature but unused here.
        """
        rows = topic_partition_records(partitions)
        self.revoked.append(rows)
        print(f"Revoked partitions: {rows}")


@dataclass
class ConsumeResult:
    """The bundled outcome of one bounded consume loop."""

    records: list[dict[str, Any]]     # the validated messages we read
    stop_reason: str                  # WHY the loop ended (see consume_records)
    commit_requests: int              # how many commits we asked for
    synchronous_commit_results: list[list[dict[str, int | str]]]


def consume_records(
    consumer: Any,
    *,
    max_messages: int,
    poll_timeout: float,
    idle_timeout: float,
    run_timeout: float | None = None,
    commit_mode: CommitMode = "none",
) -> ConsumeResult:
    """THE POLL LOOP - the heart of every Kafka consumer.

    Every consumer ever written has this shape:

        while (still running):
            message = consumer.poll(timeout)
            if message is None:   continue          # nothing arrived; normal
            if message.error():   handle it
            process(message)
            maybe commit

    A production consumer would loop forever. This one stops on three explicit
    limits so the demos terminate:

        max_messages - stop after N successfully processed messages
        idle_timeout - stop after N seconds with NOTHING arriving (the topic is
                       drained, or we are caught up)
        run_timeout  - stop after N seconds total, regardless of activity

    `stop_reason` records which limit fired, which turns "it stopped" into
    diagnosable information: "idle_timeout" means you are caught up, whereas
    "max_messages" means there may be more waiting.
    """
    # ---- ARGUMENT VALIDATION ------------------------------------------
    # Fail immediately on nonsense input rather than behaving strangely later.
    if max_messages < 1:
        raise ValueError("max_messages must be at least 1")
    if poll_timeout <= 0 or idle_timeout <= 0:
        raise ValueError("poll_timeout and idle_timeout must be positive")
    if run_timeout is not None and run_timeout <= 0:
        raise ValueError("run_timeout must be positive when provided")

    # time.monotonic() never goes backwards, even if the system clock is
    # adjusted mid-run. Always use it for timeouts.
    started = time.monotonic()
    last_message_at = started

    records: list[dict[str, Any]] = []
    commit_requests = 0
    synchronous_results: list[list[dict[str, int | str]]] = []

    # Default assumption; overwritten if a timeout fires first.
    stop_reason = "max_messages"

    while len(records) < max_messages:
        now = time.monotonic()

        # ---- BOUND 1: total wall-clock time ---------------------------
        if run_timeout is not None and now - started >= run_timeout:
            stop_reason = "run_timeout"
            break

        # ---- BOUND 2: silence ------------------------------------------
        # Measured since the last MESSAGE, not since loop start - so a busy
        # consumer keeps running while an idle one exits promptly.
        if now - last_message_at >= idle_timeout:
            stop_reason = "idle_timeout"
            break

        # ---- FETCH ONE MESSAGE ----------------------------------------
        # CONSUMER poll() IS COMPLETELY DIFFERENT FROM PRODUCER poll().
        #   producer.poll() -> services outbound delivery callbacks
        #   consumer.poll() -> FETCHES a record from the broker
        # Same name, opposite direction. This one returns a Message or None.
        #
        # The timeout is how long to wait for data before giving up and
        # returning None.
        message = consumer.poll(poll_timeout)

        # ---- None IS NORMAL, NOT AN ERROR -----------------------------
        # It simply means "nothing arrived within the timeout". Beginners often
        # treat this as a failure; it is the ordinary state of a caught-up
        # consumer. Loop around and poll again (the idle check above is what
        # eventually ends it).
        if message is None:
            continue

        # ---- CHECK FOR ERRORS BEFORE USING THE MESSAGE ----------------
        # A returned Message may carry an error instead of data. You must check
        # before touching .value().
        if message.error():
            # _PARTITION_EOF is INFORMATIONAL, not a failure: "you have reached
            # the current end of this partition." Perfectly normal for a
            # caught-up consumer, so skip it rather than raising. (It is only
            # emitted when explicitly enabled, but handling it costs nothing.)
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue

            # Anything else is real: authorization failure, unknown topic, a
            # broken broker. Raise and let the caller deal with it.
            raise KafkaException(message.error())

        # ---- PROCESS, *THEN* COMMIT -----------------------------------
        # ORDER IS EVERYTHING HERE, and it is the source of Kafka's delivery
        # guarantees:
        #
        #   process then commit (this code):
        #       crash in between -> the message is reprocessed next run.
        #       Guarantee: AT-LEAST-ONCE. Duplicates possible, loss is not.
        #
        #   commit then process (the tempting inversion):
        #       crash in between -> the message is marked done but was never
        #       handled. Guarantee: at-most-once. DATA LOSS.
        #
        # At-least-once is almost always the right default, and it is why
        # consumer processing should be IDEMPOTENT - safe to run twice for the
        # same message (upsert by trip_id rather than increment a counter).
        record = message_to_record(message)     # decode + validate ("process")
        records.append(record)
        last_message_at = time.monotonic()      # reset the idle clock

        print(
            f"Consumed {record['topic']}[{record['partition']}] "
            f"offset={record['offset']} key={record['key']}"
        )

        if commit_mode != "none":
            asynchronous = commit_mode == "async"

            # COMMIT: tell Kafka "this group has finished through this offset."
            #
            #   asynchronous=False (sync)  - blocks until the broker confirms,
            #       and RETURNS the committed partitions. Safest; slowest.
            #   asynchronous=True  (async) - returns immediately; the outcome
            #       arrives via the on_commit callback (see CommitTracker).
            #
            # Note we commit per message here for teaching clarity. Production
            # consumers usually commit every N messages or every few seconds,
            # because a commit is a network round trip.
            committed = consumer.commit(message=message, asynchronous=asynchronous)
            commit_requests += 1

            if not asynchronous:
                # Only the sync form returns results directly; the async form's
                # results arrive later through the callback.
                synchronous_results.append(topic_partition_records(committed))

    return ConsumeResult(
        records=records,
        stop_reason=stop_reason,
        commit_requests=commit_requests,
        synchronous_commit_results=synchronous_results,
    )


def assert_expected_topic(records: list[dict[str, Any]]) -> None:
    """Safety check: every record must have come from the expected topic.

    A consumer can subscribe to several topics at once, and a copy-paste error
    could point a demo at the wrong one. This makes that mistake loud instead of
    producing a plausible-looking but wrong report.

    The set comprehension collects distinct unexpected topic names; sorted()
    makes the error message deterministic.
    """
    unexpected = sorted({record["topic"] for record in records if record["topic"] != TOPIC_NAME})
    if unexpected:
        raise RuntimeError(f"Unexpected topic(s): {', '.join(unexpected)}")
