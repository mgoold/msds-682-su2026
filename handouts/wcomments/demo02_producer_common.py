"""
================================================================================
DEMO 02 - SHARED PRODUCER MODULE  (annotated tutorial copy)
================================================================================

WHAT THIS FILE IS
    The shared "library" that all four Demo 02 producer scripts (02A, 02B, 02C,
    02D) import. It contains no main() and produces nothing by itself. Its job
    is to define, exactly once:

        1. the EVENT MODEL      - what a trip event looks like (TripEvent)
        2. the CONFIG LOADER    - how to connect to Confluent Cloud safely
        3. the EVENT GENERATOR  - deterministic fake data (make_trip_events)
        4. the SERIALIZERS      - how a Python object becomes Kafka bytes
        5. the DELIVERY TRACKER - how we record what happened to each message
        6. the REPORT WRITERS   - how we save secret-free evidence

WHY A SHARED MODULE AT ALL
    The four demos differ ONLY in *when they wait for delivery*. If each script
    defined its own event model and serialization, you could never tell whether
    a speed difference came from the delivery strategy or from the data. Keeping
    everything else identical is what makes the comparison in Demo 02C fair.

READ THIS FILE FIRST. The other four demos will make almost no sense until you
understand the pieces defined here.

KAFKA BACKGROUND YOU NEED FOR THIS FILE
    - Kafka stores MESSAGES (a.k.a. records) in TOPICS.
    - A message is essentially a KEY (bytes) + a VALUE (bytes).
    - Kafka does NOT understand your data. It stores opaque bytes. Turning a
      Python object into bytes is 100% your application's job - that is what
      event_key() and serialize_event() below do.
    - A PRODUCER is any program that writes messages to a topic.
================================================================================
"""

# `from __future__ import annotations` makes all type hints in this file lazy
# (stored as strings, not evaluated at import time). Practical effect: you can
# write modern syntax like `str | None` and `dict[str, str]` even on older
# Python versions. Nothing Kafka-specific here.
from __future__ import annotations

import json      # to write the JSON evidence reports at the end
import os        # to read environment variables (where credentials live)
import random    # for the DETERMINISTIC fake-data generator (seeded, see below)
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

# --- Third-party imports -------------------------------------------------
# python-dotenv: reads a ".env" text file and loads its KEY=VALUE pairs into
# the process environment, so os.getenv() can see them. This is the standard
# way to keep secrets OUT of source code.
from dotenv import load_dotenv

# pydantic: a data-validation library. A "BaseModel" is a class where every
# field has a declared type, and pydantic ENFORCES those types at construction
# time. If you try to build an invalid object, it raises instead of silently
# accepting bad data. This is our schema-on-write layer.
from pydantic import BaseModel, Field


# =============================================================================
# MODULE-LEVEL CONSTANTS
# =============================================================================

# The Kafka TOPIC every Demo 02 script writes to.
#
# NAMING CONVENTION: <org>.<context>.<entity>.<version>
#   msds682      - the course/org namespace
#   demo01       - which demo created it
#   trip-events  - what the data is
#   v1           - schema version
#
# Why the version matters: a topic's messages should all share one shape. If
# you make an incompatible change to the event structure, the convention is to
# create a NEW topic (...v2) rather than start writing a different shape into
# the existing one, because consumers of v1 would break.
#
# IMPORTANT: all four demos deliberately use this SAME topic. You are comparing
# producer BEHAVIOR, not creating four different topics.
TOPIC_NAME = "msds682.demo01.trip-events.v1"

# A fixed starting timestamp for generated events. Because it is a hard-coded
# constant (not datetime.now()), re-running the generator produces the exact
# same timestamps every time - part of what makes the data reproducible.
#
# tzinfo=timezone.utc makes this "timezone-aware". A naive datetime (no tzinfo)
# is ambiguous - 10:00 in which part of the world? Always use aware datetimes
# for event data that may cross machines or regions.
BASE_EVENT_TIME = datetime(2026, 7, 4, 10, 0, tzinfo=timezone.utc)


# =============================================================================
# 1. THE EVENT MODEL  - what one message's VALUE looks like
# =============================================================================
class TripEvent(BaseModel):
    """One ride-hailing trip event: the business meaning of a Kafka message.

    THIS CLASS IS THE CONTRACT between producer and consumer.

    Critically: Kafka knows NOTHING about this class. It is pure application
    code living in your repo. It is never uploaded to the broker, never
    registered anywhere, and the broker will happily accept bytes that violate
    it. The contract holds only because BOTH your producer and your consumer
    choose to import and apply this same class. (Demo 04 introduces Schema
    Registry, which is the tool that fixes this "trust me" problem.)

    Pydantic enforces the rules below at OBJECT CONSTRUCTION time - i.e. before
    produce() is ever called - so malformed events cannot reach the topic.
    """

    # A stable identifier for the whole trip. This is also used as the Kafka
    # MESSAGE KEY (see event_key() below), which is what keeps all events for
    # one trip on the same partition and therefore in order.
    trip_id: str

    # `Literal[...]` restricts this field to EXACTLY these four strings.
    # Passing anything else (say "trip_cancelled") raises a ValidationError.
    #
    # Notice these four values describe the LIFECYCLE of a single trip:
    #   trip_requested -> driver_matched -> trip_started -> trip_completed
    # This models Kafka's core idea: records are IMMUTABLE. You never "update"
    # a trip's status; you APPEND a new event with the next event_type. The
    # current state of a trip is simply its most recent event.
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]

    # Who requested the trip.
    rider_id: str

    # When the event happened, as an ISO-8601 string (e.g. "2026-07-04T10:00:00Z").
    # Stored as `str` rather than `datetime` because JSON has no native datetime
    # type; keeping it a string makes serialization trivial and unambiguous.
    event_time: str

    # Pickup zone - a simple categorical field ("north" / "south" / "west").
    zone: str

    # `str | None = None` means OPTIONAL: may be absent, defaults to None.
    # Business reason: a trip that has only just been REQUESTED has no driver
    # assigned yet, so driver_id is legitimately missing for those events.
    driver_id: str | None = None

    # Optional too, but with an extra CONSTRAINT.
    #   Field(default=None, ge=0)
    #     default=None -> optional, absent unless supplied
    #     ge=0         -> "greater than or equal to zero"
    #
    # This is a BUSINESS RULE, not merely a type rule. A negative fare is a
    # perfectly valid floating-point number, so a type system alone would
    # accept it; only this constraint rejects it. Worth remembering for Demo 04:
    # Avro can enforce "this field is a double" but CANNOT enforce "and it must
    # not be negative". That is why application-level validation never goes away.
    fare: float | None = Field(default=None, ge=0)


# =============================================================================
# 2. THE DELIVERY TRACKER  - recording what happened to each message
# =============================================================================
class DeliveryTracker:
    """Collects the outcome of every message, via Kafka delivery callbacks.

    WHY THIS EXISTS - the single most important idea about Kafka producers:

        producer.produce(...) DOES NOT SEND THE MESSAGE.

    It validates your arguments, works out the target partition, appends the
    record to an in-memory queue, and returns - usually in microseconds. A
    BACKGROUND THREAD (inside the underlying C library, librdkafka) does the
    actual network I/O later.

    So `produce()` returning successfully tells you NOTHING about whether the
    message reached Kafka. The only way you ever find out is that the library
    calls a function you supplied - a DELIVERY CALLBACK - once the broker has
    acknowledged the message, or once it has definitively failed.

    That callback is `self.callback` below. This class simply accumulates the
    results so the script can report them at the end.
    """

    def __init__(self) -> None:
        # Successful deliveries. Each entry is a small dict of ROUTING METADATA
        # (see callback() for exactly what). Note it stores metadata, never
        # credentials and never the message payload - everything this program
        # writes to disk is meant to be safe to commit and submit.
        self.delivered: list[dict[str, Any]] = []

        # Failed deliveries, stored as error strings.
        self.failed: list[str] = []

    def callback(self, err, msg) -> None:
        """Called by the Kafka client ONCE PER MESSAGE, after its fate is known.

        You never call this function yourself. You hand it to produce() as the
        `callback=` argument, and the library invokes it later.

        IMPORTANT TIMING DETAIL: this does not run spontaneously. Delivery
        results queue up inside the client, and they are only handed to your
        Python code when you call producer.poll() or producer.flush(). If you
        never call either, your callbacks NEVER FIRE. That is why every demo
        below calls one or the other.

        Parameters
        ----------
        err : KafkaError or None
            None     -> the message was delivered successfully.
            not None -> delivery failed; this describes why.
        msg : Message
            The message object. AFTER delivery it knows its final partition and
            offset, which the broker assigned. Those two fields simply do not
            exist yet at produce() time - which is precisely why you need a
            callback to learn them.
        """
        # ---- FAILURE PATH ----------------------------------------------
        if err is not None:
            # Record the error text and return early. In production you might
            # instead retry, or write the record to a "dead letter" topic.
            self.failed.append(str(err))
            return

        # ---- SUCCESS PATH ----------------------------------------------
        # msg.key() returns the raw KEY BYTES we supplied at produce() time.
        # We decode back to a str purely so the JSON report is human-readable.
        # The `if msg.key() else ""` guard handles messages produced with no
        # key at all (allowed in Kafka; not used in these demos).
        key = msg.key().decode("utf-8") if msg.key() else ""

        self.delivered.append(
            {
                # Which topic it landed in. Useful as a sanity check that all
                # four demos really did write to the same topic.
                "topic": msg.topic(),

                # WHICH PARTITION the message went to. A topic is split into N
                # partitions; each is an independent, ordered, append-only log.
                # The partition was chosen by the CLIENT (see event_key notes).
                "partition": msg.partition(),

                # THE OFFSET: this message's position within that partition's
                # log - 0, 1, 2, 3, ... The BROKER assigns this at the moment it
                # appends the record, so it is the one field nobody upstream can
                # know in advance. A record's full address is the triple
                # (topic, partition, offset).
                "offset": msg.offset(),

                # The key we sent, decoded for readability.
                "key": key,
            }
        )


# =============================================================================
# 3. CONFIGURATION AND CREDENTIALS
# =============================================================================
def load_dotenv_for_demo() -> None:
    """Locate and load the .env file holding your Confluent credentials.

    Tries the directory you RAN the command from first, then falls back to the
    directory this script lives in. That flexibility matters because you might
    run `python handouts/demo02a_....py` from the repo root or from inside
    handouts/, and both should work.

    A `.env` file is just lines of KEY=VALUE. load_dotenv() reads them into the
    process environment so os.getenv() can find them. The file is git-ignored,
    which is the whole point: SECRETS NEVER ENTER SOURCE CONTROL.
    """
    cwd_env = Path.cwd() / ".env"                          # where you ran the command
    script_env = Path(__file__).resolve().parent / ".env"  # next to this file
    load_dotenv(cwd_env if cwd_env.exists() else script_env)


def load_producer_config() -> dict[str, str]:
    """Build the configuration dictionary the Kafka client needs to connect.

    The confluent-kafka library is configured with a plain dict whose keys are
    the standard librdkafka property names (note: dots, not underscores).

    These five keys are the minimum for Confluent Cloud.
    """
    load_dotenv_for_demo()
    return {
        # WHERE THE CLUSTER IS: "host:port".
        #
        # "bootstrap" is meaningful - this is only a STARTING POINT, not "the
        # server". The client connects here once to download CLUSTER METADATA
        # (which brokers exist, which broker leads which partition), then talks
        # DIRECTLY to the right broker for each partition afterwards.
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", ""),

        # HOW TO SECURE THE CONNECTION.
        #   SASL_SSL = authenticate with SASL, encrypt everything with TLS.
        # Confluent Cloud requires this. (A local dev broker might use
        # PLAINTEXT, i.e. no auth and no encryption - never do that publicly.)
        "security.protocol": os.getenv("SECURITY_PROTOCOL", "SASL_SSL"),

        # WHICH AUTHENTICATION MECHANISM.
        #   PLAIN = username/password style. On Confluent Cloud, the "username"
        #   is your API key and the "password" is your API secret.
        #   ("PLAIN" refers to the mechanism, NOT to sending it unencrypted -
        #   TLS from the line above still encrypts the whole connection.)
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS", "PLAIN"),

        # THE CREDENTIALS THEMSELVES.
        #
        # Note the pattern: os.getenv(...) with an empty-string default. The
        # values are NEVER written literally in code. If they were, they would
        # end up in git history, in screenshots, and in submitted archives -
        # and a leaked Kafka API key lets anyone write to (or read) your cluster.
        #
        # The empty-string default is deliberate: it lets require_producer_config()
        # below detect "missing" and exit with a helpful message, rather than
        # handing the broker a blank password and getting a cryptic auth error.
        "sasl.username": os.getenv("SASL_USERNAME", ""),
        "sasl.password": os.getenv("SASL_PASSWORD", ""),
    }


def missing_config(config: dict[str, str]) -> list[str]:
    """Return the ENV VAR NAMES of any config values that came back empty.

    Note it returns the environment-variable names (BOOTSTRAP_SERVERS) rather
    than the client keys (bootstrap.servers), because the env var name is what
    the user has to go fix in their .env file. Small detail, big usability win.
    """
    env_by_client_key = {
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "security.protocol": "SECURITY_PROTOCOL",
        "sasl.mechanisms": "SASL_MECHANISMS",
        "sasl.username": "SASL_USERNAME",
        "sasl.password": "SASL_PASSWORD",
    }
    # Keeps any key whose value is falsy (empty string).
    return [env_by_client_key[key] for key, value in config.items() if not value]


def require_producer_config() -> dict[str, str]:
    """Load config, or stop the program with a clear message listing what's missing.

    FAIL FAST. Without this, a missing password produces a confusing
    authentication error thirty seconds later, after a connection timeout.

    SystemExit with a string prints that string and exits non-zero.

    Security detail worth copying: the error names the missing VARIABLES but
    never prints their values.
    """
    config = load_producer_config()
    missing = missing_config(config)
    if missing:
        raise SystemExit(f"Missing required .env values: {', '.join(missing)}")
    return config


# =============================================================================
# 4. THE DETERMINISTIC EVENT GENERATOR
# =============================================================================
def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    """Build ONE trip event from its position in the stream.

    "DETERMINISTIC" IS THE WHOLE POINT. Given the same `index` and the same
    seeded `rng`, this returns the same event every single time you run it.

    Why that matters: Demo 02C benchmarks two delivery strategies against each
    other. If the two strategies sent different data, a timing difference might
    just mean one of them happened to send smaller messages (message size
    affects throughput). Identical payloads make the comparison FAIR - the
    delivery strategy becomes the only variable, exactly like fixing a random
    seed before comparing two ML models.

    Note how little is actually random: event_type, trip_id, zone, and
    event_time are all pure functions of `index`. Only driver_id and fare draw
    from the RNG. This is a structured simulation with a bit of jitter, not noise.

    Parameters
    ----------
    index : int
        Position in the stream: 0, 1, 2, 3, ...
    rng : random.Random
        A SEEDED random generator, created once by make_trip_events() and
        passed in. Sharing one generator across all events is what makes the
        whole SEQUENCE reproducible, not just individual events.
    """
    # Cycle through the four lifecycle stages: index 0 -> requested,
    # 1 -> matched, 2 -> started, 3 -> completed, 4 -> requested again...
    event_types = ["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    event_type = event_types[index % len(event_types)]

    # Integer division by 4 means every GROUP of four consecutive events shares
    # one trip number. So events 0-3 are all trip_981's lifecycle, events 4-7
    # are trip_982's, and so on. This is what makes the data realistic: the
    # stream contains complete trip lifecycles, not disconnected events.
    trip_number = 981 + (index // len(event_types))

    # Each event is one second after the previous one, counting from the fixed
    # BASE_EVENT_TIME constant. Deterministic, and it produces a sensible
    # increasing timeline.
    #
    # .isoformat() gives "2026-07-04T10:00:00+00:00"; the .replace() converts
    # the UTC offset to the shorter, more conventional "Z" suffix:
    # "2026-07-04T10:00:00Z".
    event_time = (BASE_EVENT_TIME + timedelta(seconds=index)).isoformat().replace("+00:00", "Z")

    return TripEvent(
        trip_id=f"trip_{trip_number}",
        event_type=event_type,
        rider_id=f"rider-{trip_number}",

        # BUSINESS RULE IN ACTION: a trip that has only been REQUESTED has no
        # driver yet, so driver_id stays None. Any later stage gets one.
        # `:03d` zero-pads to three digits -> "driver-007".
        driver_id=None if event_type == "trip_requested" else f"driver-{rng.randint(1, 8):03d}",

        # Likewise, only a COMPLETED trip has a fare.
        fare=round(rng.uniform(10.0, 90.0), 2) if event_type == "trip_completed" else None,

        zone=["north", "south", "west"][index % 3],
        event_time=event_time,
    )
    # SUBTLE BUT CRITICAL: Python evaluates keyword arguments left to right, so
    # rng.randint (in driver_id) always runs BEFORE rng.uniform (in fare). If
    # you reordered these two arguments, the random sequence would shift and the
    # "same seed = same data" guarantee would silently break.


def make_trip_events(count: int, seed: int) -> list[TripEvent]:
    """Build a reproducible LIST of events.

    random.Random(seed) creates an INDEPENDENT generator seeded with `seed`.
    Using a private instance (rather than the global random.seed()) means this
    function cannot be perturbed by, or perturb, random number use elsewhere in
    the program - a good habit in any reproducibility-sensitive code.

    Same seed + same count => byte-for-byte identical event stream, always.
    """
    rng = random.Random(seed)
    return [make_trip_event(index, rng) for index in range(count)]


# =============================================================================
# 5. SERIALIZATION  - turning Python objects into Kafka bytes
# =============================================================================
def event_key(event: TripEvent) -> bytes:
    """Produce the KEY BYTES for one message.

    WHAT A KEY IS FOR - this is one of Kafka's most important ideas.

    A topic is split into N partitions. When you produce a message with a key,
    the client picks the partition like this:

            partition = hash(key) % number_of_partitions

    Because that is a pure function, THE SAME KEY ALWAYS GOES TO THE SAME
    PARTITION. And because each partition is an ordered append-only log, all
    messages sharing a key are stored IN ORDER relative to each other.

    Here the key is trip_id, so every event for trip_981 - requested, matched,
    started, completed - lands on one partition, in sequence. A consumer reading
    that partition sees the trip's lifecycle in the correct order.

    Kafka does NOT provide global ordering across a whole topic; ordering is
    per-partition. Choosing your key IS choosing what must stay ordered together.

    (Side effect worth knowing: because the formula divides by the partition
    count, changing that count later reshuffles which partition a key maps to.
    That is why partition count is treated as semi-static.)

    .encode("utf-8") converts the Python str to bytes, because Kafka keys and
    values are ALWAYS bytes.
    """
    return event.trip_id.encode("utf-8")


def serialize_event(event: TripEvent) -> bytes:
    """Produce the VALUE BYTES for one message - the actual payload.

    Three transformations happen in this one line:

      1. model_dump_json()      pydantic object  ->  JSON string
      2. exclude_none=True      drop fields that are None
      3. .encode("utf-8")       JSON string      ->  bytes

    On (2): a trip_requested event has no driver_id and no fare. Without
    exclude_none they would be transmitted as `"driver_id": null`, wasting bytes
    and forcing consumers to handle explicit nulls. Omitting them is cleaner.

    Result for a requested event:
        {"trip_id":"trip_981","event_type":"trip_requested",...}
    Compact - no spaces - because those bytes travel over the network for every
    single message.

    WHY BYTES AT ALL? Because Kafka brokers are deliberately payload-agnostic.
    A broker never parses your data; it appends and serves opaque byte arrays.
    That is what makes it fast and what lets a Java producer, a Python consumer,
    and a Go stream processor all share one topic. The cost is that MEANING is
    entirely your responsibility, at both ends.

    JSON is used here because it is human-readable and needs no extra infra.
    Demo 04 replaces it with Avro, which is compact and schema-checked.
    """
    return event.model_dump_json(exclude_none=True).encode("utf-8")


def event_dict(event: TripEvent) -> dict[str, Any]:
    """Plain-Python view of an event, for embedding in the JSON report.

    Same as serialize_event() but stops one step earlier: a dict rather than
    bytes, because json.dumps() in the report writer wants a dict.
    """
    return event.model_dump(exclude_none=True)


# =============================================================================
# 6. EVIDENCE WRITING  - saving results without leaking secrets
# =============================================================================
def write_json_report(run_id: str, demo_name: str, report: dict[str, Any]) -> Path:
    """Write the run's results to outputs/runs/<run_id>/<demo_name>/report.json.

    The run_id in the path lets you keep results from separate runs side by side
    instead of overwriting them.

    mkdir(parents=True, exist_ok=True):
        parents=True    create intermediate directories as needed
        exist_ok=True   do not raise if it already exists (re-runnable)

    indent=2 keeps the JSON readable and diff-friendly.
    """
    output_dir = Path("outputs") / "runs" / run_id / demo_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "report.json"
    output_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return output_file


def safe_config_report(config: dict[str, str]) -> dict[str, Any]:
    """Summarize the connection WITHOUT exposing any credential.

    This is a small function with a big idea behind it. You want evidence that
    you really connected to a real cluster with real credentials - but the
    report file gets committed to git and submitted for grading.

    The resolution: report the SHAPE of the configuration, never its secrets.

      - bootstrap_host: the hostname. Not secret (it is a public endpoint), and
        useful for proving which cluster you used.
      - security_protocol / sasl_mechanisms: how you connected. Not secret.
      - has_username / has_password: BOOLEANS, not values. They prove the
        credentials were present without revealing a single character of them.

    bool("") is False and bool("abc") is True, so bool(...) collapses any
    secret to a harmless True/False.

    .split("://")[-1] tolerates a value written either as "host:9092" or
    "sasl_ssl://host:9092" - taking the last piece gives the host either way.
    """
    return {
        "bootstrap_host": config["bootstrap.servers"].split("://")[-1],
        "security_protocol": config["security.protocol"],
        "sasl_mechanisms": config["sasl.mechanisms"],
        "has_username": bool(config["sasl.username"]),
        "has_password": bool(config["sasl.password"]),
    }
