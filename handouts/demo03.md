# Demo 03: Kafka Consumers on Confluent Cloud

**Lecture:** Lecture 3 — Apache Kafka Part 2: Consumers

**Python:** 3.11.14

**Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`

**Kafka platform:** the course Confluent Cloud Kafka cluster

**Shared topic:** `msds682.demo01.trip-events.v1`

> **Core classroom point:** Demo 01 creates one real topic, Demo 02 writes trip
> events to it, and Demo 03 reads those same events. Consumers do not remove
> Kafka records. Their group offsets record progress independently.

## 1. What Demo 03 contains

| Step | Demo | Main question | Real Confluent behavior |
|---:|---|---|---|
| 1 | Demo 03A: basic consumer | How does a consumer subscribe, poll, decode, and close? | Reads bounded records and reports topic, partition, offset, key, timestamp, and validated value. |
| 2 | Demo 03B: offset commit and resume | Where does the same consumer group continue after restart? | Processes first, then commits synchronously or asynchronously. |
| 3 | Demo 03C: groups and replay | How do consumers share partitions, and how is replay different from resume? | Shows assignment/revocation events and makes forced replay explicit. |
| 4 | Demo 03D: native asyncio | When does Python `asyncio` add value? | Runs `AIOProducer` and `AIOConsumer` concurrently without blocking the event loop. |

FastAPI is intentionally **not** part of Demo 03. It begins in Lecture 4.
Demo 03A–03C are the consumer core; Demo 03D is the short asyncio extension
promised in Lecture 3. Confluent removed the AsyncIO clients' experimental
designation in version 2.13, so the pinned 2.15.0 API is not experimental.

## 2. One continuous Demo 01 → Demo 02 → Demo 03 story

```text
Demo 01
create msds682.demo01.trip-events.v1
        |
        v
Demo 02A–02D
serialize and produce TripEvent values
        |
        v
Demo 03A–03D
poll bytes -> decode UTF-8 JSON -> validate TripEvent -> process -> commit
```

All producer and consumer scripts reuse the same topic and Pydantic `TripEvent`
contract from `demo02_producer_common.py`. Demo 03 does not introduce a second
topic constant or a duplicate event schema.

This shared topic can already contain thousands of retained Demo 02 benchmark
records. Keep the provided message/time limits: Demo 03 teaches consumer
behavior with bounded samples rather than printing the entire retained log.

## 3. Fall 2023 → Summer 2026 updates

The teaching intent is preserved, but the implementation has been modernized.

| Fall 2023 behavior | Summer 2026 update | Why |
|---|---|---|
| Credentials appeared in `config.ini` and slide/code examples. | Credentials exist only in ignored `.env`; reports contain booleans and the broker host, never API keys or secrets. | Published credentials must be treated as compromised. |
| The consumer and producer ran in infinite loops. | Every demo has `--max-messages`, `--idle-timeout`, or `--run-seconds`. | Classroom runs finish predictably and produce inspectable evidence. |
| The normal assignment callback forced every partition to `OFFSET_BEGINNING`. | Normal mode respects committed group offsets; only explicit replay mode requests `OFFSET_BEGINNING`. | Otherwise “resume” silently becomes “replay.” |
| `Consumer.poll(1.0)` was placed inside `async def`, even though it blocked the event loop. | Demo 03A–03C use the standard synchronous `Consumer`; Demo 03D uses native `AIOConsumer`/`AIOProducer`. | `async def` alone does not make a blocking call nonblocking. |
| Producer and consumer tasks were both infinite and awaited sequentially. | Demo 03D uses finite tasks and `asyncio.gather()`. | Concurrency and shutdown are visible and testable. |
| The AsyncIO client originally carried an experimental designation. | Confluent removed that designation in 2.13; this course pins 2.15.0. | Students use the current native API while treating asyncio as a focused Lecture 3 extension. |
| Consumer cleanup was incomplete. | Every consumer closes in `finally`; native async clients are awaited during shutdown. | Closing releases sockets and leaves the group promptly, triggering faster rebalance. |
| Message values were printed as raw bytes. | UTF-8 JSON is decoded and validated against the same Pydantic v2 `TripEvent` used by Demo 02. | Serialization and deserialization become one explicit contract. |
| Package versions floated. | Python 3.11.14 and the Summer 2026 requirements file are pinned, including `confluent-kafka==2.15.0`. | Instructor, TA, and student behavior stays reproducible. |

## 4. Consumer concepts before running code

### 4.1 Topic, partition, and offset

An offset identifies a record's position **inside one partition**. It is not a
global topic-wide sequence number.

```text
topic: msds682.demo01.trip-events.v1

partition 0: offset 0 -> offset 1 -> offset 2
partition 1: offset 0 -> offset 1
partition 2: offset 0 -> offset 1 -> offset 2 -> offset 3
```

The technical address of one Kafka record is:

```text
(topic, partition, offset)
```

### 4.2 Consumer group

A `group.id` identifies one logical consuming application. Within a group, a
partition is assigned to at most one active consumer at a time. A second group
can read the same topic independently.

```text
topic with 3 partitions

group analytics
  member A <- partitions 0, 2
  member B <- partition 1

group audit
  member C <- partitions 0, 1, 2
```

### 4.3 `auto.offset.reset` is not a rewind button

`earliest` or `latest` matters only when the group has no valid committed
offset for that partition.

- same group + committed offset: resume from the committed position;
- new group + `earliest`: begin with the oldest retained records;
- new group + `latest`: wait for records produced after the group starts;
- explicit forced beginning: ignore the normal resume position for this replay.

### 4.4 Process before committing

The safe teaching sequence is:

```text
poll -> decode -> validate -> process successfully -> commit progress
```

Committing before successful processing can skip work after a failure.

### 4.5 `close()` is not producer `flush()`

`Consumer.close()` releases resources, leaves the group, and allows Kafka to
rebalance promptly. It does not “send pending consumer messages.” Producers use
`flush()` because producers have outgoing delivery queues.

Offset behavior depends on configuration:

- Demo 03A has `enable.auto.commit=true`, so `close()` also commits its latest
  stored offsets before leaving the group.
- Demo 03B has `enable.auto.commit=false`; its two-run resume works because the
  script explicitly calls `commit()` after successful decode and validation.
  `close()` does not replace that manual commit.

## 5. Setup

### 5.1 Prerequisites

Complete these first:

- [Demo 00: Environment Setup](#/handouts/demo00)
- [Demo 01: Create a Kafka Topic](#/handouts/demo01)
- [Demo 02: Kafka Producer](#/handouts/demo02)

Demo 03 expects the topic to exist and contain Demo 02 trip events.

### 5.2 Download the exact environment

Download [requirements.txt](handouts/requirements.txt). It pins the Summer 2026
environment, including:

```text
Python 3.11.14
confluent-kafka[avro,schemaregistry]==2.15.0
pydantic==2.13.4
python-dotenv==1.2.2
pytest==9.1.1
```

Create and activate the environment:

```bash
uv python install 3.11.14
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

Expected Kafka client version:

```text
2.15.0
```

The `venv + pip` fallback is also supported:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 5.3 Download the code

Keep these files in the same folder:

- [demo02_producer_common.py](handouts/demo02_producer_common.py) — topic, event model, serialization, and Kafka connection SSOT
- [demo02b_confluent_async_producer.py](handouts/demo02b_confluent_async_producer.py) — produces small live batches used by the consumer demos
- [demo03_consumer_common.py](handouts/demo03_consumer_common.py) — consumer config, decode, commit, replay, and evidence helpers
- [demo03a_confluent_basic_consumer.py](handouts/demo03a_confluent_basic_consumer.py)
- [demo03b_confluent_offsets_commit.py](handouts/demo03b_confluent_offsets_commit.py)
- [demo03c_confluent_groups_replay.py](handouts/demo03c_confluent_groups_replay.py)
- [demo03d_confluent_asyncio_produce_consume.py](handouts/demo03d_confluent_asyncio_produce_consume.py)

### 5.4 Reuse the Demo 01 `.env`

Use the same Kafka cluster credentials as Demo 01 and Demo 02. Add only one
consumer naming prefix:

```dotenv
BOOTSTRAP_SERVERS=
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISMS=PLAIN
SASL_USERNAME=
SASL_PASSWORD=
DEMO01_TOPIC_NAME=msds682.demo01.trip-events.v1
CONSUMER_GROUP_ID_PREFIX=msds682-su2026
```

Never submit or publish `.env`. `CONSUMER_GROUP_ID_PREFIX` is not secret; API
keys and API secrets are.

### 5.5 Put a small live batch on the topic

If needed, produce 12 fresh events before Demo 03A:

```bash
python demo02b_confluent_async_producer.py --run-id lec3-prerequisite --count 12
```

## 6. Demo 03A: basic consumer

Run:

```bash
python demo03a_confluent_basic_consumer.py \
  --run-id lec3-demo03a \
  --max-messages 8
```

The code performs this bounded flow:

```text
Consumer(config)
-> subscribe([topic])
-> poll()
-> inspect errors
-> decode key/value bytes
-> validate TripEvent
-> record topic/partition/offset
-> close()
```

The report is written to:

```text
outputs/runs/lec3-demo03a/demo03a_confluent_basic_consumer/report.json
```

Look for:

- the same topic name used by Demo 01 and Demo 02;
- `partition` and `offset` on every record;
- a decoded UTF-8 key;
- a validated `TripEvent` value;
- the partition assignment callback;
- no credentials in the report.

## 7. Demo 03B: commit and resume

Choose one stable group ID and use it for both runs. Replace
`<usf_username>` below.

### First run

```bash
python demo03b_confluent_offsets_commit.py \
  --run-id lec3-demo03b-first \
  --group-id msds682-demo03b-<usf_username> \
  --commit-mode sync \
  --max-messages 4
```

### Second run with the same group

```bash
python demo03b_confluent_offsets_commit.py \
  --run-id lec3-demo03b-second \
  --group-id msds682-demo03b-<usf_username> \
  --commit-mode sync \
  --max-messages 4
```

Compare the offsets. The second run should continue after the offsets committed
by the first run. `auto.offset.reset=earliest` does not override an existing
valid committed position.

To demonstrate asynchronous commit requests, use a new group ID:

```bash
python demo03b_confluent_offsets_commit.py \
  --run-id lec3-demo03b-async \
  --group-id msds682-demo03b-async-<usf_username> \
  --commit-mode async \
  --max-messages 4
```

The script records commit requests and asynchronous commit acknowledgements.
Synchronous commit waits for Kafka's response; asynchronous commit returns
before the acknowledgement and reports completion through the callback.

## 8. Demo 03C: consumer groups and replay

### 8.1 Two members of one group

Open two terminals. Start these commands close together.

Terminal A:

```bash
python demo03c_confluent_groups_replay.py \
  --run-id lec3-demo03c-a \
  --group-id msds682-demo03c-<usf_username> \
  --member-id member-a \
  --run-seconds 20
```

Terminal B:

```bash
python demo03c_confluent_groups_replay.py \
  --run-id lec3-demo03c-b \
  --group-id msds682-demo03c-<usf_username> \
  --member-id member-b \
  --run-seconds 20
```

While both are running, produce a fresh batch in a third terminal:

```bash
python demo02b_confluent_async_producer.py --run-id lec3-group-input --count 12
```

Expected result:

- both consumers share one `group.id`;
- Kafka assigns different partitions to active members;
- one partition is never processed by two members of the same group at once;
- joining or leaving produces assignment/revocation evidence;
- if consumers outnumber partitions, an extra member is idle.

### 8.2 Explicit replay

Replay eight retained events from the beginning:

```bash
python demo03c_confluent_groups_replay.py \
  --run-id lec3-demo03c-replay \
  --group-id msds682-demo03c-replay-<usf_username> \
  --member-id replay \
  --force-beginning \
  --max-messages 8 \
  --run-seconds 10
```

Forced replay is deliberately visible in the command and report. Normal
consumer-group mode does not silently reset partitions to the beginning.

## 9. Demo 03D: native asyncio producer and consumer

Run:

```bash
python demo03d_confluent_asyncio_produce_consume.py \
  --run-id lec3-demo03d \
  --count 6
```

The script uses the native clients available in the pinned 2.15.0 environment:

```python
from confluent_kafka.aio import AIOConsumer, AIOProducer
```

Its structure is:

```text
asyncio event loop
|-- AIOConsumer task: await subscribe() and await poll()
`-- AIOProducer task: await produce(), delivery futures, flush(), close()

await asyncio.gather(producer_task, consumer_task)
```

This is the appropriate time to use Kafka's asyncio clients: the application
already has an event loop and needs Kafka work to coexist with other async I/O.
For a normal command-line consumer, Demo 03A–03C's synchronous `Consumer` is
simpler and appropriate.

The Demo 03D report must show the requested count delivered and consumed:

```text
outputs/runs/lec3-demo03d/demo03d_confluent_asyncio_produce_consume/report.json
```

## 10. Expected results and evidence

| Demo | Expected result | Secret-free evidence |
|---|---|---|
| 03A | Stops after the requested sample or idle timeout; every value validates as a `TripEvent`. | Decoded records, assignments, offsets, and safe consumer config. |
| 03B | The second sync run with the same group resumes after the first run's committed offsets; async mode records acknowledgements with no failures. | Records, commit mode, commit requests/results, and resume group ID. |
| 03C | Concurrent members receive non-overlapping partition assignments; forced replay reads a bounded sample from retained history. | Assignment/revocation history, member ID, group ID, and explicit replay mode. |
| 03D | The requested count is both delivered and consumed before the timeout. | Delivered Kafka metadata, consumed validated records, and safe producer/consumer config. |

The reports may include group IDs, client IDs, topic names, partitions, and
offsets. They must never include `sasl.username`, `sasl.password`, `.env`, or
credential values.

## 11. Common errors

| Symptom | Likely cause | Fix |
|---|---|---|
| `Missing required .env values` | Demo 01 configuration is absent | Put the same ignored `.env` beside the scripts or run from its folder. |
| Authentication failure | Wrong or revoked Kafka cluster API key | Create a Kafka API key scoped to the cluster; do not use the Confluent website password. |
| Demo 03A consumes zero records | Topic is empty or this group already reached the end | Run Demo 02B again or use a fresh group ID. |
| Demo 03B second run starts from the beginning | A different group ID was used, or the first run did not commit | Use exactly the same group ID and check `commit_requests`. |
| Demo 03C members do not split work | The commands did not overlap, or the topic has only one partition | Run both for 20 seconds and confirm Demo 01 created three partitions. |
| Replay reads old data | Expected behavior | `--force-beginning` explicitly requests retained history. |
| `No module named confluent_kafka.aio` | Client version is older than the course pin | Install `requirements.txt` and verify version `2.15.0`. |
| Consumer appears to hang | A consumer normally waits for new records | These demos stop using visible time/message limits; wait for the printed report. |

## 12. Cost, cleanup, and safe shutdown

These demos use the existing course topic and small message counts. Monitor
Confluent Cloud credits and stop command-line consumers after the bounded run.
Do not delete the shared topic while Assignment 1 or later consumer exercises
still depend on it.

Every consumer uses `close()` in a `finally` block. Demo 03D awaits async client
shutdown. This is part of the lesson, not optional cleanup boilerplate.

## 13. Lecture 3 summary

1. Consumers read records; they do not remove them.
2. Ordering and offsets are per partition.
3. A consumer group owns progress independently from other groups.
4. `auto.offset.reset` applies only when a valid committed position is absent.
5. Process successfully before committing progress.
6. Normal resume and forced replay are different operations.
7. Always close a consumer so resources and group membership are released.
8. Use the standard Consumer for ordinary scripts; use native asyncio clients
   when Kafka must cooperate with an existing async application.

## Official references

- [Confluent Python client overview and consumer examples](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Confluent Python client changelog](https://docs.confluent.io/kafka-clients/python/current/changelog.html)
- [Confluent Kafka Python client repository](https://github.com/confluentinc/confluent-kafka-python)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
