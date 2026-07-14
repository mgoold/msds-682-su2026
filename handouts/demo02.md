# Demo 02: Kafka Producer with Trip Events

**Goal:** produce trip lifecycle event messages into the topic created in Demo 01.

Topic used by every Lec 2 producer script:

```text
msds682.demo01.trip-events.v1
```

**Core classroom point:** Demo 01 creates one empty Kafka topic. Demo 02A/02B/02C/02D send real messages to that same Confluent Cloud topic.

All Demo 02 scripts in this handout use `confluent-kafka` and write to Confluent Cloud.

## Table of Contents

1. [Demo 01 Plus The Four Producer Demos](#1-demo-01-plus-the-four-producer-demos)
2. [Topic, Key, And Message Shape](#2-topic-key-and-message-shape)
3. [Producer Configuration And Core Code](#3-producer-configuration-and-core-code)
4. [Run The Demo](#4-run-the-demo)
5. [Expected Results](#5-expected-results)
6. [Troubleshooting And Takeaways](#6-troubleshooting-and-takeaways)

## 1. Demo 01 Plus The Four Producer Demos

Read this table first. It is the roadmap for the page.

**Count:** Demo 02 has four producer demos: 02A, 02B, 02C, and 02D. They all use one Kafka topic: `msds682.demo01.trip-events.v1`.

| Step | Demo | Script | Runs against | What students should learn |
|---:|---|---|---|---|
| 1 | Demo 01: create topic | `demo01_create_topic.py` | Confluent Cloud | A Kafka topic can exist before any messages exist |
| 2 | Demo 02A: sync-style producer | `demo02a_confluent_sync_style_producer.py` | Confluent Cloud | Wait after each message; easiest mental model |
| 3 | Demo 02B: async producer | `demo02b_confluent_async_producer.py` | Confluent Cloud | `produce(...)` queues messages; delivery comes back by callback |
| 4 | Demo 02C: async vs sync-style | `demo02c_confluent_async_sync_compare.py` | Confluent Cloud | Async queues many messages; sync-style waits after each message |
| 5 | Demo 02D: serialization producer | `demo02d_confluent_serialization_producer.py` | Confluent Cloud | Python objects must become bytes before Kafka stores them |

These match the old Fall 2023 producer notebooks more directly:

| Fall 2023 notebook style | 2026 script | What changed |
|---|---|---|
| Sync Producer | `demo02a_confluent_sync_style_producer.py` | Still waits after each message, but prints a JSON report instead of notebook cell lines |
| Asynchronous Producer | `demo02b_confluent_async_producer.py` | Same callback idea; now uses the shared trip topic and shared helper functions |
| Compare Async and Sync Producers | `demo02c_confluent_async_sync_compare.py` | Same comparison, with secret-free structured output |
| Asynchronous Producer w/ Serialization | `demo02d_confluent_serialization_producer.py` | Same concept, now uses `TripEvent -> JSON string -> UTF-8 bytes` |

The main lecture path is:

```text
create topic -> sync-style producer -> async producer -> compare sync/async -> serialize explicitly
```

## 2. Topic, Key, And Message Shape

### 2.1 Topic Relationship

| Demo | What it does | Topic name |
|---|---|---|
| Demo 01 | Creates the empty Confluent topic | `msds682.demo01.trip-events.v1` |
| Demo 02A/02B/02C/02D | Produce trip event messages in Confluent | `msds682.demo01.trip-events.v1` |
| [Demo 03A–03D](#/handouts/demo03) | Consume, commit, rebalance, replay, and use native asyncio on Confluent | `msds682.demo01.trip-events.v1` |

The topic name is shared on purpose:

| Storage | Used by | Meaning |
|---|---|---|
| Confluent Kafka | Demo 01, Demo 02A–02D, and Demo 03A–03D | Real Kafka topic, real messages, and independent consumer-group progress |

### 2.2 Message Value Example

One produced message value looks like this:

```json
{
  "trip_id": "trip_981",
  "event_type": "driver_matched",
  "rider_id": "rider-981",
  "driver_id": "driver-004",
  "zone": "south",
  "event_time": "2026-07-04T10:00:01Z"
}
```

Important:

| Piece | In this demo | Why it matters |
|---|---|---|
| Topic | `msds682.demo01.trip-events.v1` | Destination log |
| Key | `trip_id`, for example `trip_981` | Keeps one trip lifecycle on the same partition while partition count is stable |
| Value | Trip event JSON bytes | Actual payload Kafka stores |
| Timestamp | Kafka write time plus the sample `event_time` field | Shows event time versus Kafka storage time |

Kafka stores message keys and values as bytes. The readable JSON is our chosen serialization format.

## 3. Producer Configuration And Core Code

### 3.1 Connection Config From `.env`

The Confluent producer scripts need Kafka credentials. Keep them in `.env`, not in Python source code.

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

| Config field | Meaning |
|---|---|
| `bootstrap.servers` | Kafka broker endpoint from Confluent Cloud |
| `security.protocol` | Encrypted/authenticated connection; for this class: `SASL_SSL` |
| `sasl.mechanisms` | API-key/API-secret auth mechanism: `PLAIN` |
| `sasl.username` | Kafka API key |
| `sasl.password` | Kafka API secret |

All Demo 02 scripts require this `.env` config because they write to Confluent Cloud.

### 3.2 Producer Call

This is the core pattern used by the real Confluent producer scripts:

```python
producer.produce(
    topic=TOPIC_NAME,
    key=event_key(event),
    value=serialize_event(event),
    callback=tracker.callback,
)
producer.poll(0)
producer.flush()
```

| Code | Meaning |
|---|---|
| `topic=TOPIC_NAME` | Write to `msds682.demo01.trip-events.v1` |
| `key=event_key(event)` | Use `trip_id` as the message key |
| `value=serialize_event(event)` | Convert `TripEvent` to UTF-8 JSON bytes |
| `callback=tracker.callback` | Record success/failure delivery reports |
| `producer.poll(0)` | Let callbacks run while the script continues |
| `producer.flush()` | Wait for queued/in-flight messages before exit |

### 3.3 Async And Sync-Style

Direct definitions:

| Term | Meaning in this handout |
|---|---|
| Async producer | `produce(...)` queues a message and returns quickly; delivery result arrives later |
| Sync-style producer | Teaching simplification: produce one message, then `flush()` immediately |

The visible difference is the waiting pattern:

```text
Demo 02A sync-style:
produce -> flush
produce -> flush
produce -> flush
produce -> flush

Demo 02B async:
produce
produce
produce
produce
flush
```

Both write real messages to the same topic: `msds682.demo01.trip-events.v1`.

**Sync-style feels simpler because it waits after each message. Async can feel less direct because delivery reports come back later, but it is the normal Kafka producer pattern.**

**Do not call `flush()` after every message in normal producer code.** Demo 02C does it only to make sync-style behavior easy to see.

With only 4 messages, async may not look much faster. With many messages, async is usually faster because the producer can queue messages and let network delivery overlap.

Yes, `wait until delivered` is slower when you do it after every message. Async also waits, but it waits once at the end with `flush()`.

Demo 02A uses this sync-style loop:

```python
for event in events:
    producer.produce(
        topic=TOPIC_NAME,
        key=event_key(event),
        value=serialize_event(event),
        callback=tracker.callback,
    )
    producer.flush()  # teaching simplification: wait after each message
```

### 3.4 Serialization And Pydantic

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

Pydantic helps us validate and format the Python object before produce. It is not Schema Registry. Cross-language schema governance would use Avro/JSON Schema/Protobuf plus Schema Registry later.

### 3.5 Async Does Not Mean Python `asyncio`

In this page, async means Kafka producer queuing plus callback delivery reports.

It does not mean:

```text
async def ...
await ...
asyncio.run(...)
```

Confluent has AsyncIO APIs, but Lec 2 does not use them. This demo stays focused on producer fundamentals.

## 4. Run The Demo

### 4.1 Setup

```bash
mkdir -p msds682-demos
cd msds682-demos

uv python install 3.11.14
uv venv --python 3.11.14 .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Use the published [Summer 2026 requirements file](handouts/requirements.txt),
which pins `confluent-kafka[avro,schemaregistry]==2.15.0` and the rest of the
course environment.

Download these files into the same folder:

[demo02_producer_common.py](handouts/demo02_producer_common.py)

[demo02a_confluent_sync_style_producer.py](handouts/demo02a_confluent_sync_style_producer.py)

[demo02b_confluent_async_producer.py](handouts/demo02b_confluent_async_producer.py)

[demo02c_confluent_async_sync_compare.py](handouts/demo02c_confluent_async_sync_compare.py)

[demo02d_confluent_serialization_producer.py](handouts/demo02d_confluent_serialization_producer.py)

For the Confluent scripts, also use the same `.env` format from Demo 01.

### 4.2 Run Commands In Order

| Step | Command | Expected proof |
|---:|---|---|
| 2 | `python demo02a_confluent_sync_style_producer.py --run-id lec2-demo02a --count 4` | `producer_mode: sync_style_flush_each_message`, `delivered: 4` |
| 3 | `python demo02b_confluent_async_producer.py --run-id lec2-demo02b --count 4` | `attempted: 4`, `delivered: 4`, `remaining_after_flush: 0` |
| 4 | `python demo02c_confluent_async_sync_compare.py --run-id lec2-demo02c --count 4` | Two rows: `async` and `sync_style_flush_each_message` |
| 5 | `python demo02d_confluent_serialization_producer.py --run-id lec2-demo02d --count 4` | `serialized_type: UTF-8 JSON bytes`, `delivered: 4` |

For a real speed comparison, run Demo 02C with a larger count:

```bash
python demo02c_confluent_async_sync_compare.py --run-id lec2-demo02c-speed --count 2000
```

This sends 2,000 async messages and 2,000 sync-style messages to the same topic. Total new topic messages: 4,000.

Demo 02C intentionally writes the same deterministic logical events twice: once through async and once through sync-style. This is for producer behavior comparison, not a production pattern.

### 4.3 Output Files

Confluent scripts write secret-free JSON reports under:

```text
outputs/runs/<run-id>/<demo-name>/report.json
```

## 5. Expected Results

Use this table to understand what each run proves.

| Demo | What you see in output | Meaning |
|---|---|---|
| Demo 02A | `producer_mode: sync_style_flush_each_message`, `delivered: 4` | Four messages were sent; script waited after each message |
| Demo 02B | `producer_mode: async`, `delivered: 4`, `remaining_after_flush: 0` | Four messages were queued first, then the script waited at the end |
| Demo 02C | Two result rows with `elapsed_seconds` and `messages_per_sec` | Same message idea, different waiting strategy and speed |
| Demo 02D | `serialized_type: UTF-8 JSON bytes` plus delivery counts | A `TripEvent` Python object was validated and converted to bytes before Kafka stored it |

All four demos use one topic. They create different messages and reports, not different topics.

Real Confluent Cloud run checked on 2026-07-09:

| Script | Count | Delivered | Elapsed seconds | Messages/sec | Result |
|---|---:|---:|---:|---:|---|
| Demo 01 create topic | 1 topic | n/a | n/a | n/a | `already_exists`; topic has 3 partitions |
| Demo 02A sync-style | 4 | 4 | 1.144073 | n/a | Passed |
| Demo 02B async | 4 | 4 | 1.063295 | n/a | Passed |
| Demo 02C async row | 2000 | 2000 | 1.056219 | 1893.55 | Passed |
| Demo 02C sync-style row | 2000 | 2000 | 88.750945 | 22.53 | Passed |
| Demo 02D serialization | 4 | 4 | 1.056037 | n/a | Passed |

Demo 02C produced 4,000 total messages in that run: 2,000 async messages plus 2,000 sync-style messages.
Those two batches use the same seed, so they intentionally contain duplicate logical trip events.

### 5.1 Sync-Style Producer

Demo 02A is the closest match to the old sync producer notebook. The old notebook printed lines such as "All messages delivered successfully". The 2026 script keeps the same meaning but prints structured JSON:

```json
{
  "topic": "msds682.demo01.trip-events.v1",
  "producer_mode": "sync_style_flush_each_message",
  "attempted": 4,
  "delivered": 4,
  "remaining_after_flush": 0,
  "delivered_messages": [
    {
      "topic": "msds682.demo01.trip-events.v1",
      "partition": 2,
      "offset": 26,
      "key": "trip_981"
    }
  ]
}
```

Offsets will differ every run. The important check is `delivered == attempted`.

`delivered_messages` lists at most the first 10 deliveries, so reports stay readable even with a large `--count`.

### 5.2 Async, Compare, Serialization

Demo 02B, 02C, and 02D use the same delivery-report idea. The old notebook printed callback lines such as `Message delivered to topic_example_v1 [2] at offset 526`; the 2026 scripts store the same information in `delivered_messages` or comparison rows.

Demo 02B async producer:

```json
{
  "topic": "msds682.demo01.trip-events.v1",
  "producer_mode": "async",
  "attempted": 4,
  "delivered": 4,
  "failed": [],
  "remaining_after_flush": 0
}
```

Demo 02C comparison:

```json
{
  "rows": [
    {
      "strategy": "async",
      "attempted": 2000,
      "delivered": 2000,
      "elapsed_seconds": 1.056219,
      "messages_per_sec": 1893.55
    },
    {
      "strategy": "sync_style_flush_each_message",
      "attempted": 2000,
      "delivered": 2000,
      "elapsed_seconds": 88.750945,
      "messages_per_sec": 22.53
    }
  ]
}
```

The exact numbers depend on network and Confluent Cloud, but the expected pattern is direct:

```text
async messages_per_sec > sync_style_flush_each_message messages_per_sec
async elapsed_seconds < sync_style_flush_each_message elapsed_seconds
```

Why: sync-style waits for network delivery after every message. Async queues many messages first, then waits once at the end.

Demo 02D serialization:

```json
{
  "sample_python_object": {"trip_id": "trip_981", "event_type": "trip_requested"},
  "sample_serialized_value": "{\"trip_id\":\"trip_981\",\"event_type\":\"trip_requested\",\"rider_id\":\"rider-981\",\"event_time\":\"2026-07-04T10:00:00Z\",\"zone\":\"north\"}",
  "serialized_type": "UTF-8 JSON bytes",
  "delivered": 4
}
```

The important delivery check is:

```text
delivered == attempted
```

### 5.3 Confluent UI

After Demo 01:

| UI area | Expected result |
|---|---|
| Topics page | Topic `msds682.demo01.trip-events.v1` exists |
| Partitions column | `3` partitions |
| Messages | Empty unless a producer has already run |

After Demo 02A/02B/02C/02D:

| UI area | Expected result |
|---|---|
| Messages viewer | Trip event messages may appear when searching/consuming from the topic |
| Key | UTF-8 bytes for `trip_id`, for example `trip_981` |
| Value | UTF-8 JSON bytes, displayed as a JSON trip event if the UI decodes it |
| Partition/offset | Kafka returns partition/offset in delivery reports |
| Metrics | Production counters may lag; the script delivery report is the immediate proof |

## 6. Troubleshooting And Takeaways

### 6.1 Common Problems

| Error or symptom | Most likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: pydantic` | Packages not installed in the active environment | Activate `.venv`, then reinstall packages |
| `Missing required .env values` | Running Confluent script without Demo 01 `.env` | Put `.env` in the same folder or current working directory |
| `SASL authentication error` mentioning `Global API key` | The key is for Confluent Cloud management, not Kafka broker auth | Create a Kafka API key scoped to the cluster and put that key/secret in `.env` |
| `Local: Message timed out` | Topic missing, bad credentials, or network issue | Run Demo 01 first and verify topic exists |
| No output files | Running from a different folder than expected | Check current directory with `pwd` |
| Messages not visible immediately in Confluent UI | UI search window or metrics lag | Trust the script delivery report first, then search the topic messages |

### 6.2 What This Demo Does Not Cover Yet

| Not included yet | Why |
|---|---|
| Creating the topic | Demo 01 covers topic creation |
| Consuming messages | [Demo 03](#/handouts/demo03) covers poll loops, commits, groups, replay, and native asyncio |
| Schema Registry | Pydantic validation is local to Python; Schema Registry is later/optional |
| Distributed Kafka performance testing | This is a concept demo, not a load test |
| Python `asyncio` producer API | Lec 2 focuses on Kafka producer basics |

### 6.3 What Students Need To Master

1. A topic can exist empty. Demo 01 creates it.
2. A producer chooses topic, key, and value.
3. Kafka stores bytes, so value serialization matters.
4. `confluent-kafka` producer is async by default.
5. Delivery success/failure comes from callback, `poll()`, and `flush()`.
6. `sync_style` means "wait after each message" and is a teaching simplification.
7. Use `trip_id` as key when one trip lifecycle should stay ordered in one partition.
