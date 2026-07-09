# Demo 02: Kafka Producer with Trip Events

**This handout completes the Lec 2 producer path after Demo 01 topic creation.**

Important: the local benchmark is a warm-up. The Confluent scripts send real messages to the Kafka topic created in Demo 01.

Demo 01 created the real Confluent topic:

```text
msds682.demo01.trip-events.v1
```

All producer scripts use that same topic name.

> Key classroom point: Demo 01 creates an empty topic. Producer demos create messages and append them to that topic. The real Kafka producer is asynchronous by default.

## Demo Goal

| Question | Answer |
|---|---|
| What are we producing? | Trip lifecycle event messages |
| Which topic name? | `msds682.demo01.trip-events.v1` |
| Does this hit Confluent Cloud? | Local benchmark: no. Confluent scripts: yes. |
| Why keep local first? | Students can inspect message shape before touching cloud Kafka. |
| What comes next? | Demo 03 reads the same topic name and replays by offset. |

## The Four Lec 2 Demos

| Demo | Script | What students should learn |
|---|---|---|
| Demo 01: Create Topic | `demo01_create_topic.py` | A Kafka topic can exist before any messages exist |
| Demo 02B: Async Producer | `demo02b_confluent_async_producer.py` | `produce(...)` is asynchronous; delivery comes back by callback |
| Demo 02C: Async vs Sync-Style | `demo02c_confluent_async_sync_compare.py` | Async queues many messages; sync-style waits after each message and is a teaching simplification |
| Demo 02D: Serialization Producer | `demo02d_confluent_serialization_producer.py` | A Python object must become bytes before Kafka stores it |

The local script `demo02_producer_benchmark.py` is a no-cloud warm-up. It is useful, but it is not the full Confluent producer demo.

## What You Should See at the End

Local warm-up:

Run:

```bash
python demo02_producer_benchmark.py --run-id lec2-demo02 --count 1000 --batch-size 100
```

Expected terminal output includes:

```json
{
  "topic": "msds682.demo01.trip-events.v1",
  "rows": [
    {
      "strategy": "sync_style",
      "message_count": 1000
    },
    {
      "strategy": "batched",
      "message_count": 1000
    }
  ]
}
```

The script also writes:

```text
outputs/runs/lec2-demo02/topics/msds682_demo01_trip-events_v1.jsonl
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.csv
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.png
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark_report.json
```

The `.jsonl` file is the local topic log. Each line is one produced message.

Real Confluent async producer:

```bash
python demo02b_confluent_async_producer.py --run-id lec2-demo02b --count 4
```

Expected output includes:

```json
{
  "producer_mode": "async",
  "topic": "msds682.demo01.trip-events.v1",
  "attempted": 4,
  "delivered": 4,
  "remaining_after_flush": 0
}
```

Async vs sync-style comparison:

```bash
python demo02c_confluent_async_sync_compare.py --run-id lec2-demo02c --count 4
```

Expected output includes two rows:

```json
[
  {
    "strategy": "async",
    "delivered": 4
  },
  {
    "strategy": "sync_style_flush_each_message",
    "delivered": 4
  }
]
```

Serialization producer:

```bash
python demo02d_confluent_serialization_producer.py --run-id lec2-demo02d --count 4
```

Expected output includes:

```json
{
  "serialized_type": "UTF-8 JSON bytes",
  "delivered": 4
}
```

## Verified Run Results

These are example results from a local run with the instructor `.env`.

| Script | Command shape | Result |
|---|---|---|
| Local warm-up | `python demo02_producer_benchmark.py --count 8 --batch-size 4` | wrote 8 local JSONL messages, CSV, PNG |
| Async producer | `python demo02b_confluent_async_producer.py --count 4` | attempted 4, delivered 4, remaining after flush 0 |
| Async vs sync-style | `python demo02c_confluent_async_sync_compare.py --count 4` | async delivered 4; sync-style delivered 4 |
| Serialization producer | `python demo02d_confluent_serialization_producer.py --count 4` | serialized JSON bytes and delivered 4 |

The Confluent reports look like this:

```json
{
  "topic": "msds682.demo01.trip-events.v1",
  "attempted": 4,
  "delivered": 4,
  "failed": [],
  "remaining_after_flush": 0
}
```

The async/sync comparison report looks like this:

```json
{
  "rows": [
    {
      "strategy": "async",
      "attempted": 4,
      "delivered": 4
    },
    {
      "strategy": "sync_style_flush_each_message",
      "attempted": 4,
      "delivered": 4
    }
  ],
  "note": "sync_style is a teaching simplification; confluent-kafka produce() is asynchronous by default."
}
```

Do not copy the exact timing numbers. They depend on network and machine state. The important check is delivered count equals attempted count.

## One Message Looks Like This

```json
{
  "trip_id": "trip_981",
  "event_type": "driver_matched",
  "rider_id": "rider-981",
  "driver_id": "driver-003",
  "zone": "north",
  "event_time": "2026-07-04T10:01:00Z"
}
```

This is the message value. Kafka itself would store bytes; our local demo stores readable JSON so students can inspect it.

## Relation to Demo 01 and Demo 03

| Demo | What it does | Topic name |
|---|---|---|
| Demo 01 | Creates the empty Confluent topic | `msds682.demo01.trip-events.v1` |
| Demo 02 | Produces trip event messages locally first, then to Confluent | `msds682.demo01.trip-events.v1` |
| Demo 03 | Consumes/replays trip event messages from the same topic thread | `msds682.demo01.trip-events.v1` |

The topic name is shared on purpose. The storage backend is different:

| Storage | Used by | Meaning |
|---|---|---|
| Confluent Kafka | Demo 01 topic creation | Real cloud topic exists |
| Confluent Kafka | Demo 02B/02C/02D producer scripts | Real messages are appended to the cloud topic |
| Local JSONL file | Demo 02 warm-up and local replay checks | Local append-only log for reproducible class runs |

## High-Value Details to Notice

| Detail | Why it matters |
|---|---|
| `TOPIC_NAME = "msds682.demo01.trip-events.v1"` | Keeps topic creation, producer, and consumer demos aligned |
| `TripEvent` model | Defines the local message value shape for this producer |
| `event_type` | Shows that one topic can carry related lifecycle event types |
| `model_dump(exclude_none=True)` | Avoids writing fields like `driver_id: null` when the event does not have a driver yet |
| `produce(...)` | Real Kafka: queues one message asynchronously |
| `callback=delivery_report` | Tells whether Kafka accepted the message |
| `producer.poll(0)` | Lets delivery callbacks run while the script continues |
| `producer.flush()` | Explicit wait point before the script exits |
| Benchmark CSV/PNG | Shows a simple way to compare producer strategies |
| JSONL file | Lets students open the topic log and inspect messages directly |
| Clean topic per strategy | Each strategy starts from an empty local topic so timing comparisons are isolated |

## Producer Configuration in This Demo

Producer configuration has two layers:

1. Connection config: how Python connects to Confluent Cloud.
2. Per-message producer choices: which topic/key/value/callback to use.

**Core point: `topic`, `key`, and `value` are producer choices, not secrets. Confluent credentials stay in `.env`.**

Connection config from `.env`:

```python
producer_config = {
    # Confluent Cloud Kafka cluster endpoint.
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    # Encrypted SASL connection to Confluent Cloud.
    "security.protocol": "SASL_SSL",
    # API-key/API-secret authentication.
    "sasl.mechanisms": "PLAIN",
    # Kafka API key.
    "sasl.username": os.getenv("SASL_USERNAME"),
    # Kafka API secret.
    "sasl.password": os.getenv("SASL_PASSWORD"),
}
```

What each config field does:

| Config field | Meaning |
|---|---|
| `bootstrap.servers` | Kafka broker endpoint from Confluent Cloud |
| `security.protocol` | Uses encrypted/authenticated connection; for this class: `SASL_SSL` |
| `sasl.mechanisms` | Auth mechanism; for API key/secret: `PLAIN`; librdkafka also accepts `sasl.mechanism` as an alias |
| `sasl.username` | Kafka API key |
| `sasl.password` | Kafka API secret |

Per-message choices in the producer call:

```python
producer.produce(
    topic=TOPIC_NAME,
    key=event_key(event),
    value=serialize_event(event),
    callback=tracker.callback,
)
```

| Config item | In this demo | Why it matters |
|---|---|---|
| Topic | `msds682.demo01.trip-events.v1` | Same topic thread as Demo 01 and Demo 03 |
| Key | `trip_id` in Confluent scripts | Same trip lifecycle goes to the same partition while partition count is stable |
| Value | `TripEvent` serialized to UTF-8 JSON bytes | This is the message payload Kafka stores |
| Serialization | `TripEvent -> JSON string -> UTF-8 bytes` | Kafka stores bytes |
| Producer mode | Async by default in Confluent scripts | Higher-throughput producer pattern |
| Delivery report | `callback=tracker.callback` | Records success/failure without printing secrets |

That config is required for the Confluent producer scripts. It is not required for the local warm-up script.

## Batch Means Two Different Things Here

**Important: `--batch-size` in the local warm-up is not Kafka's real `batch.size` producer config.**

| Term | Meaning in this course demo |
|---|---|
| Local `--batch-size` | How many fake trip events the JSONL warm-up holds before writing them together |
| Kafka internal batching | What `confluent-kafka`/librdkafka does after `produce(...)` queues messages |
| Real Kafka `linger.ms` / `queue.buffering.max.ms` | How long the client may wait so more messages can accumulate into a batch |
| Real Kafka `batch.num.messages` | Max number of messages in one internal batch |
| Real Kafka `batch.size` | Max byte size of one internal batch |

We do not tune Kafka's production batching settings in Demo 02. That is intentional. For Lec 2, students only need the producer flow: configure client, choose topic/key/value, serialize, call `produce(...)`, serve callback, `flush()` before exit.

## Poll, Flush, And Callback

Direct rules:

| Code | Meaning |
|---|---|
| `producer.produce(...)` | Queues a message asynchronously and returns quickly |
| `callback=tracker.callback` | Registers the delivery report handler |
| `producer.poll(0)` | Gives the client a chance to run callbacks without blocking |
| `producer.flush()` | Waits for queued/in-flight messages before the script exits |

**Do not call `flush()` after every message in normal producer code.** Demo 02C does that only to show a sync-style teaching simplification.

## Async Producer Is Not Python asyncio

In this handout, async means:

```text
produce(...) queues the message now; delivery result arrives later by callback.
```

It does not mean:

```text
async def ...
await ...
asyncio.run(...)
```

The current Confluent Python client also has AsyncIO APIs. We are not using them in Lec 2 because this demo is about Kafka producer fundamentals, not Python `asyncio`.

## Serialization And Pydantic

Kafka stores message keys and values as bytes. A Python object cannot be stored directly.

Demo 02D uses this path:

```text
TripEvent Pydantic model
-> model_dump_json(exclude_none=True)
-> JSON string
-> encode("utf-8")
-> bytes sent to Kafka
```

Code:

```python
def serialize_event(event: TripEvent) -> bytes:
    # Kafka values are bytes. Pydantic v2 model_dump_json gives a JSON string.
    return event.model_dump_json(exclude_none=True).encode("utf-8")
```

What Pydantic gives us here:

| Benefit | Meaning |
|---|---|
| Validation before produce | Bad sample data fails before it reaches Kafka |
| Consistent fields | Every trip event follows the same local Python model |
| Clean JSON output | `exclude_none=True` avoids writing unused fields like `driver_id: null` |
| Readable demo payload | Students can inspect JSON in Confluent and local JSONL |

**Pydantic is not Schema Registry.** It validates inside this Python demo. Cross-language schema governance would use Avro/JSON Schema/Protobuf plus Schema Registry later.

## What To Expect In Confluent Cloud

After Demo 01:

| Confluent UI area | Expected result |
|---|---|
| Topics page | Topic `msds682.demo01.trip-events.v1` exists |
| Partitions column | `3` partitions |
| Messages | No messages yet, unless a producer has already run |

After Demo 02B/02C/02D:

| Confluent UI area | Expected result |
|---|---|
| Topic name | Still `msds682.demo01.trip-events.v1` |
| Messages viewer | Trip event messages may appear when searching/consuming from the topic |
| Key | UTF-8 bytes for `trip_id`, for example `trip_981` |
| Value | UTF-8 JSON bytes, displayed as a JSON trip event if the UI decodes it |
| Partition/offset | Kafka assigns the partition from the key and returns partition/offset in delivery reports |
| Metrics | Production counters may lag; the script's delivered count is the immediate proof |

## Sync vs Async Producer

Kafka producers are normally asynchronous.

Direct meaning:

| Term | Meaning |
|---|---|
| Async producer | `produce(...)` queues the message and returns quickly; delivery result arrives later by callback/poll/flush |
| Sync-style producer | Code waits after each send or handles one message at a time; simpler to explain, usually slower |
| Batch producer | Conceptually groups multiple messages; real Kafka batching is handled inside the client |

In the local warm-up:

| Strategy | What it really is |
|---|---|
| `sync_style` | Teaching simplification: one local append per message |
| `batched` | Teaching simplification: collect several local messages, then append them together |

In the Confluent compare script:

| Strategy | What it really is |
|---|---|
| `async` | Queue all messages, then `flush()` once |
| `sync_style_flush_each_message` | Produce one message, then `flush()` immediately |

In real `confluent-kafka`, the default producer pattern is async:

```python
producer.produce(
    topic="msds682.demo01.trip-events.v1",
    key=event.trip_id,
    value=json.dumps(event.model_dump(exclude_none=True)).encode("utf-8"),
    callback=delivery_report,
)

# Serve delivery callbacks and wait for queued messages before exiting.
producer.flush()
```

So yes: `sync_style` is simplified. The real Kafka producer is async by default; `flush()` is the explicit wait point.

## Read the Core Code First

Shared event model and serialization:

```python
TOPIC_NAME = "msds682.demo01.trip-events.v1"


class TripEvent(BaseModel):
    # Local message value model for the ridesharing topic.
    # Kafka itself stores bytes; this model keeps our sample payloads consistent.
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    rider_id: str
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0)


def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    # Deterministic fake events: same seed + same count = same local topic log.
    event_types = ["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    event_type = event_types[index % len(event_types)]
    trip_number = 981 + (index // len(event_types))
    return TripEvent(
        trip_id=f"trip_{trip_number}",
        event_type=event_type,
        rider_id=f"rider-{trip_number}",
        driver_id=None if event_type == "trip_requested" else f"driver-{rng.randint(1, 8):03d}",
        fare=round(rng.uniform(10.0, 90.0), 2) if event_type == "trip_completed" else None,
        zone=["north", "south", "west"][index % 3],
        event_time=f"2026-07-04T10:{index % 60:02d}:00Z",
    )
```

Confluent async producer core:

```python
producer = Producer(producer_config)
tracker = DeliveryTracker()

for event in events:
    producer.produce(
        topic=TOPIC_NAME,
        key=event.trip_id.encode("utf-8"),
        value=event.model_dump_json(exclude_none=True).encode("utf-8"),
        callback=tracker.callback,
    )
    producer.poll(0)

producer.flush()
```

Sync-style comparison core:

```python
for event in events:
    producer.produce(
        topic=TOPIC_NAME,
        key=event.trip_id.encode("utf-8"),
        value=event.model_dump_json(exclude_none=True).encode("utf-8"),
        callback=tracker.callback,
    )
    producer.flush()  # teaching simplification: wait after each message
```

Local warm-up append:

```python
def produce(self, topic: str, payload: dict) -> None:
    path = self.topic_path(topic)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
```

The second block is the local version of producer behavior: append one serialized message to the topic log.

The benchmark runs two strategies. Each strategy starts from a clean local topic so the timing comparison is isolated. The final `.jsonl` file shows the messages from the last strategy run; the CSV and PNG are the comparison artifacts.

## What Each Piece Does

| Piece | What it does | Why students need it |
|---|---|---|
| `TripEvent` | Defines the local Python shape of message values | Makes producer output predictable |
| `make_trip_event(...)` | Generates deterministic sample trip events | Same seed means reproducible messages |
| `demo02_producer_common.py` | Shared topic, config, event model, serializer, callback | Keeps producer scripts DRY |
| `Producer` | Confluent Kafka producer client | Sends messages to Kafka |
| `DeliveryTracker` | Callback result collector | Shows delivered/failed counts |
| `JsonlTransport` | Local append-only topic storage | Warm-up only; no cloud credentials |
| `producer_benchmark.csv` | Structured benchmark result | Easy to inspect or grade |
| `producer_benchmark.png` | Bar chart of messages/sec | Quick visual comparison |

## What This Demo Does Not Do

| Not included yet | Why |
|---|---|
| Create the topic in Confluent | Demo 01 already covers topic creation |
| Consume messages | Demo 03 covers offset/replay behavior |
| Schema Registry | Pydantic validates locally; Schema Registry is a later/optional concept |
| Prove distributed Kafka performance | This is a local teaching benchmark, not a cloud load test |

## Step 1: Create a Working Folder

```bash
mkdir -p msds682-demos
cd msds682-demos
```

## Step 2: Create and Activate Python

Recommended:

```bash
uv python install 3.11
uv venv --python 3.11 .venv
source .venv/bin/activate
```

Fallback:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

## Step 3: Install Packages

```bash
python -m pip install --upgrade pip
python -m pip install confluent-kafka python-dotenv pydantic pandas matplotlib
```

The Confluent producer scripts use the same `.env` shape from Demo 01.

## Step 4: Download the Script

Download:

[demo02_producer_common.py](handouts/demo02_producer_common.py)

[demo02_producer_benchmark.py](handouts/demo02_producer_benchmark.py)

[demo02b_confluent_async_producer.py](handouts/demo02b_confluent_async_producer.py)

[demo02c_confluent_async_sync_compare.py](handouts/demo02c_confluent_async_sync_compare.py)

[demo02d_confluent_serialization_producer.py](handouts/demo02d_confluent_serialization_producer.py)

Put it in your working folder.

Your folder should look like:

```text
msds682-demos/
  .env
  demo02_producer_common.py
  demo02_producer_benchmark.py
  demo02b_confluent_async_producer.py
  demo02c_confluent_async_sync_compare.py
  demo02d_confluent_serialization_producer.py
  .venv/
```

`.env` is required only for the Confluent producer scripts. The local warm-up can run without `.env`.

## Step 5: Run Demo 02

```bash
python demo02_producer_benchmark.py --run-id lec2-demo02 --count 1000 --batch-size 100
```

For a shorter classroom check:

```bash
python demo02_producer_benchmark.py --run-id quick --count 20 --batch-size 5
```

Run the real async producer:

```bash
python demo02b_confluent_async_producer.py --run-id lec2-demo02b --count 4
```

Run the async vs sync-style comparison:

```bash
python demo02c_confluent_async_sync_compare.py --run-id lec2-demo02c --count 4
```

Run the serialization producer:

```bash
python demo02d_confluent_serialization_producer.py --run-id lec2-demo02d --count 4
```

## Step 6: Inspect the Local Topic Log

Open:

```text
outputs/runs/lec2-demo02/topics/msds682_demo01_trip-events_v1.jsonl
```

You should see one JSON object per line. Each line is one produced message.

Important: this local file is standing in for Kafka's append-only topic log. In real Kafka, messages are stored in partition logs on brokers, not in a plain `.jsonl` file.

## Step 7: Inspect the Benchmark Artifacts

Open:

```text
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.csv
outputs/runs/lec2-demo02/demo02_producer_benchmark/producer_benchmark.png
```

The CSV records:

| Column | Meaning |
|---|---|
| `strategy` | Producer style being tested |
| `message_count` | Number of messages attempted |
| `batch_size` | Local warm-up grouping size; not Kafka producer `batch.size` |
| `elapsed_seconds` | Local wall-clock time |
| `throughput_msg_per_sec` | Messages per second in this local run |

Do not over-interpret the absolute speed. The point is to compare patterns, not to benchmark Confluent Cloud.

## Common Problems

| Error or symptom | Most likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: pydantic` | Packages not installed in the active environment | Activate `.venv`, then reinstall packages |
| `Missing required .env values` | Running Confluent script without Demo 01 `.env` | Put `.env` in the same folder or current working directory |
| `Local: Message timed out` | Topic missing, bad credentials, or network issue | Run Demo 01 first and verify topic exists |
| No output files | Running from a different folder than expected | Check current directory with `pwd` |
| Chart file missing | `matplotlib` was not installed correctly | Reinstall `matplotlib` in the active environment |
| Topic file looks duplicated | You reused the same run folder and reran after editing | Use a new `--run-id` |
| Confused why Confluent metrics do not change after local warm-up | `demo02_producer_benchmark.py` is local JSONL only | Run `demo02b`, `demo02c`, or `demo02d` to send real cloud messages |
| Messages not visible immediately in Confluent UI | UI search window or metrics lag | Trust the script delivery report first, then search the topic messages |

## What Students Need to Master

Keep this short:

1. A topic can exist empty. Demo 01 creates it.
2. A producer chooses topic, key, and value.
3. Kafka stores bytes, so value serialization matters.
4. `confluent-kafka` producer is async by default.
5. Delivery success/failure comes from callback/poll/flush.
6. `sync_style` means "wait after each message" and is a teaching simplification.
7. Use `trip_id` as key when you want one trip lifecycle to stay ordered in one partition.
8. Local `--batch-size` is not Kafka's production `batch.size` setting.

Do not over-focus on performance numbers. The point is the producer flow.
