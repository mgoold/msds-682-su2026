# Kafka Notes

A complete, reader-friendly tutorial on Kafka — **ordered by conceptual
dependency**, then by hands-on skill. Every section points at concrete lines in
the course demo scripts.

The notes are in three parts:

| Part | What it covers | Demos |
|---|---|---|
| **I. Concepts** (§1–9) | What Kafka *is*: brokers, topics, partitions, offsets, replication | — |
| **II. Skills** (§10–17) | How to *use* it from Python: connect, create, produce, consume, evolve | Demo 00–04 |
| **III. Reference** (§18–20) | File map, command cheat sheet, troubleshooting | all |

> **How to read this:** Start at the top. Each section assumes only the ones
> above it, with two deliberate exceptions, both flagged inline where they occur:
> §4 names *partition* and *offset* as a map before §5 and §6 zoom in on them,
> and the schema layer (§16) is introduced late, matching the demo order, even
> though §12 already uses a validated model. Part I is theory with no code you
> run; Part II is where you write and run things. A companion
> [`glossary.md`](glossary.md) defines every term precisely.

---
---

# Part I — Concepts

## 1. The big picture: producers, consumers, and the broker

Kafka itself is only the thing in the **middle** — the **brokers** that receive,
durably store, and serve streams of records organized into **topics**. Kafka does
**not** produce or consume your business data.

```
   YOUR CODE                    KAFKA                      YOUR CODE
┌──────────────┐        ┌──────────────────┐        ┌──────────────┐
│  Producer    │ write  │  Broker(s)       │  read  │  Consumer    │
│ (an app you  │───────▶│  - topics        │───────▶│ (an app you  │
│  write)      │        │  - partitions    │        │  write)      │
└──────────────┘        │  - stores logs   │        └──────────────┘
                        └──────────────────┘
```

- **Producer** = *any* application that **writes** messages to a topic.
- **Consumer** = *any* application that **reads** messages from a topic.
- Neither is a "Kafka asset" — both are **external client applications you write**,
  distinguished only by the **direction of data flow**. The same app can be both
  (consume from topic A, produce to topic B) — see
  [`demo03d`](../handouts/demo03d_confluent_asyncio_produce_consume.py), which runs
  both at once.

**What makes a script a producer is the client object it creates, not its genre.**

- Not a producer — an *admin* client that only manages the cluster:
  [`demo01_create_topic.py:8`](../msds682-demos/demo1/demo01_create_topic.py#L8) imports
  `AdminClient`; [`:125`](../msds682-demos/demo1/demo01_create_topic.py#L125) instantiates it.
  It creates topics but never sends a message.
- A producer — the moment this line runs:
  [`demo02a...:31`](../handouts/demo02a_confluent_sync_style_producer.py#L31)
  `producer = Producer(config)`. The role is realized in
  [`:39-44`](../handouts/demo02a_confluent_sync_style_producer.py#L39-L44)
  where `producer.produce(...)` writes into the topic.

> **"Producer" vs "producer client":** same object, two zoom levels. Broadly a
> *producer* is the whole app that writes to Kafka; the *producer client* is the
> library object (`Producer(config)`) embedded **inside** that app that actually
> speaks the Kafka protocol. It is **not** a separate network tier between you and
> the broker.

---

## 2. Topics: a logical name, not a storage object

A **topic** is a **logical name** you route records to (e.g.
`msds682.demo01.trip-events.v1`). It is *not* paired to a single storage item like
an S3 bucket, and it is *not* pure namespace either — it is a name over **N
partitions**, each of which is real physical storage.

QUESTION: is it correct to say that a topic *is* a log file?  Is that what it is materially?

A topic is the level at which you set **policy** and **route**:

- Partition count, replication factor, and cleanup policy are chosen at creation:
  [`demo01_create_topic.py:74-79`](../msds682-demos/demo1/demo01_create_topic.py#L74-L79).
- Producers route by topic name:
  [`demo02a...:39`](../handouts/demo02a_confluent_sync_style_producer.py#L39)
  (`topic=TOPIC_NAME`).

---

## 3. Partitions: the real storage unit

A **partition** is the unit that actually stores data. Each partition is an
**append-only log**, kept as **segment files on a broker's local disk** (and
replicated to other brokers — see §8).

Key facts:

- A topic owns **many** partitions; a partition belongs to **exactly one** topic
  and is never shared. Its identity is the pair `(topic, partition_index)`, and the
  topic name is even part of its on-disk log directory
  (e.g. `msds682.demo01.trip-events.v1-0`). Topics reuse partition *numbers*
  (every topic has a partition 0), but those are distinct logs.
- Partition count is a **deliberate, semi-static** choice
  ([`demo01_create_topic.py:114`](../msds682-demos/demo1/demo01_create_topic.py#L114),
  `--partitions` default 3) — **not** an auto-scaling knob. Kafka scales by
  spreading existing partitions across more brokers, not by minting new ones.
- A partition is a **first-class storage unit**, but **not the same *kind*** as an
  S3 bucket. A bucket is an unordered, mutable, random-access object store; a
  partition is an **ordered, append-only, immutable log** read sequentially.
  Closest mapping: **topic ≈ bucket-like container, segment file ≈ object** (and
  under tiered storage, old segments literally become objects in a bucket).

QUESTION: explain in more detail what a segment is.

**Buckets store *things you fetch*; partitions store *a sequence you replay*.**

---

## 4. Producing a record: who fills in which field

The unit written to Kafka is a **record** (a.k.a. **message**; your business
meaning of it is an **event**). A `produce()` call does **not** send a finished
record — it hands the **producer client** the fields you own; the client assembles
the record, batches it, and ships it; the **broker** finalizes the rest.

```
YOUR produce() call     →   PRODUCER CLIENT          →   BROKER
(the parts you own)         (assembles + batches)        (finalizes)
 topic                       computes PARTITION           assigns OFFSET
 key   (→ bytes)             stamps a TIMESTAMP           may overwrite
 value (→ bytes)             bundles into a RECORD BATCH  TIMESTAMP
 [headers, partition]        sends
 callback (stays LOCAL)
```

**Only `key` and `value` are serialized to bytes by your code.** See
[`demo02_producer_common.py:114-121`](../handouts/demo02_producer_common.py#L114-L121):
`event_key()` and `serialize_event()` both `.encode()` to bytes. Kafka treats those
bytes as **opaque** — giving them meaning (JSON, Avro) is the application's job
(§16).

| Field | Who sets it | When |
|---|---|---|
| `key`, `value`, `headers` | **You** → bytes | at `produce()` |
| `topic` | You (routing) | at `produce()` |
| **partition** | **Producer client** (partitioner), unless you specify | client-side |
| **timestamp** | Producer client stamps; broker may overwrite | client-side / append |
| **offset** | **Broker only** | at log append |
| `callback` | You, but **local-only**, never transmitted | fires after ack |

The callback can only report `partition` and `offset` *after* delivery
([`demo02_producer_common.py:35-47`](../handouts/demo02_producer_common.py#L35-L47))
— precisely because those are finalized downstream of `produce()`.

---

## 5. Partitioning: how a record picks a partition

The producer **client** chooses the partition before sending, using recently
refreshed **broker metadata** (it asks the cluster how many partitions the topic
has). The default partitioner:

```
partition = hash(key) % num_partitions        # when a key is present
```

Keying keeps related records together and ordered:
[`demo02_producer_common.py:114-116`](../handouts/demo02_producer_common.py#L114-L116)
keys on `trip_id` so *one trip's whole lifecycle stays on the same partition*.

This is exactly **why partition count is kept stable**: if `num_partitions`
changed, `hash(key) % N` would send the same key to a *different* partition,
breaking per-key ordering. So Kafka only lets you **increase** partitions (a
deliberate admin act), never shuffle them dynamically.

QUESTION: re: " producer client chooses the partition before sending, using recently refreshed broker metadata" -- does the client every change the partition it sends to dynamically? If it does, what causes that change?

## 6. Offsets: address *and* bookmark

An **offset** is a **monotonically increasing integer identifying a record's
position within a partition** — the record's sequence number in that partition's
log.

- **Per-partition, not global.** A record's true address is the triple
  `(topic, partition, offset)`.
- **Assigned by the broker** at append time (the one field nobody upstream knows).
- **Never resets or reuses** — even after retention deletes old records, the
  counter keeps climbing.

Offsets do **two distinct jobs** (a common point of confusion):

1. **The record's address** — a fixed coordinate, the same for everyone. This is
   what the delivery callback records:
   [`demo02_producer_common.py:39-47`](../handouts/demo02_producer_common.py#L39-L47).
2. **A consumer's bookmark** (its *position* / *committed offset*) — a per-consumer
   cursor holding *which offset it will read next*. This is what §14.3 is about.

> **The book analogy:** page numbers (record offsets) are printed once and shared
> by all readers; each reader's bookmark (consumer offset) sits on a different
> page. There is only **one** numbering; consumers differ only in *where they are*
> in it. Kafka needs per-consumer bookmarks because **reading does not consume the
> record** — the immutable log stays put, so each consumer tracks its own place.
> This is what lets multiple consumers independently read and **replay** the same
> partition.

---

## 7. Immutability and how "updates" work

Records in a partition are **immutable** — once appended at an offset, their bytes
and offset never change. So Kafka reframes "update": you **append a new record**,
you never mutate an old one.

The demo models this exactly. The `event_type` field
([`demo02_producer_common.py:21`](../handouts/demo02_producer_common.py#L21))
lists the life stages of one trip — `trip_requested → driver_matched →
trip_started → trip_completed`. Each stage is a **new immutable record keyed on the
same `trip_id`**, kept in order on the same partition. The "current state" is the
**latest record for that key**.

Two ways to handle updates, chosen via `cleanup.policy`
([`demo01_create_topic.py:78`](../msds682-demos/demo1/demo01_create_topic.py#L78)):

| | `delete` (log / history) | `compact` (table / latest) |
|---|---|---|
| An "update" is… | a new appended event | a new record superseding the prior one for that key |
| Old versions | retained until retention expires | garbage-collected by compaction |
| Mental model | event stream (event sourcing) | key → latest-value table |

**Deletes** are just **tombstone** records (a key with a `null` value). Kafka only
ever *appends*.

---

## 8. Replication: leaders and followers

For fault tolerance, each partition is copied `replication_factor` times across
**different** brokers
([`demo01_create_topic.py:115`](../msds682-demos/demo1/demo01_create_topic.py#L115),
default 3). Among the replicas of one partition:

- **Leader** — exactly one replica. Handles **all reads and writes** for that
  partition and **assigns offsets**. Producers/consumers talk only to the leader
  (which is why the client fetches metadata to learn *which broker leads which
  partition*).
- **Followers** — the other replicas. They continuously **fetch** from the leader
  to stay in sync and stand by. They do not serve clients by default.

**Leadership is per-partition, not per-broker.** Each broker is leader for some
partitions and follower for others, so write load spreads across the cluster.

**ISR (in-sync replicas)** is the set of replicas currently caught up with the
leader. With the producer setting **`acks=all`**, the leader acknowledges a write
only once **all in-sync replicas** have it — so a confirmed write survives broker
loss.

### How many brokers do you need?

**At least** `replication_factor` — never fewer. Each of a partition's replicas
must sit on a **distinct** broker, so `RF=3` requires ≥3 brokers (topic creation
fails otherwise). You usually have **many more**: any one partition uses *exactly*
RF brokers, but different partitions land on different broker subsets, so a topic
spreads across the whole cluster. Adding brokers scales capacity; RF stays fixed
as your durability setting.

### When a leader's broker dies

1. Followers where it led → the controller **promotes an in-sync follower** to
   leader (brief unavailability, **no acknowledged data lost** because the ISR was
   caught up).
2. Partitions where it was a follower → go **under-replicated** (ISR shrinks;
   leader unaffected).
3. Whether producing continues depends on **`min.insync.replicas`**: if the
   shrunken ISR still meets it, `acks=all` writes succeed; if not, those partitions
   go **read-only** to protect durability. This is the **durability-vs-availability
   tradeoff**.
4. On return, the broker **catches up from the current leaders, rejoins the ISR**,
   and **preferred-leader election** eventually restores the original leadership
   balance.

---

## 9. How the pieces relate (cardinality summary)

```
Cluster
 ├── has many Brokers ────────────────┐
 │                                     │ (a broker hosts partition-replicas
 └── has many Topics                   │  from MANY topics — shared infra)
      └── each Topic has many Partitions (count set per topic)
           └── each Partition has RF Replicas
                └── each Replica lives on a DISTINCT broker
                     └── exactly one Replica is LEADER, the rest FOLLOWERS
```

| Relationship | Cardinality |
|---|---|
| Topic → Partition | one → many (count per topic) |
| Partition → Topic | many → **one** (never shared) |
| Partition → Replica | one → RF |
| Replica → Broker | each on a **distinct** broker |
| Broker → Replica | one → many (mixed topics) |
| Replica → role | exactly **one leader** per partition, rest followers |

---
---

# Part II — Skills

Everything above is what Kafka *is*. The rest is how you *use* it from Python
with `confluent-kafka`. The demos build one continuous story on **one topic**:

```
  demo00        demo01         demo02          demo03         demo04
 ┌───────┐    ┌────────┐    ┌──────────┐    ┌──────────┐   ┌──────────┐
 │ env   │───▶│ create │───▶│ produce  │───▶│ consume  │──▶│ schemas  │
 │ check │    │ topic  │    │ (write)  │    │ (read)   │   │ + Avro   │
 └───────┘    └────────┘    └──────────┘    └──────────┘   └──────────┘
  connect      AdminClient    Producer        Consumer     SchemaRegistry
```

## 10. Connecting: configuration and credentials

Every client — admin, producer, consumer — is constructed from the **same
connection dictionary**. Five keys
([`demo02_producer_common.py:57-70`](../handouts/demo02_producer_common.py#L57-L70)):

| Key | Meaning | Typical value |
|---|---|---|
| `bootstrap.servers` | cluster endpoint (`host:port`) | `pkc-xxxxx.region.provider.confluent.cloud:9092` |
| `security.protocol` | encrypted + authenticated | `SASL_SSL` |
| `sasl.mechanisms` | auth style | `PLAIN` (API key/secret) |
| `sasl.username` | your **API key** | from Confluent |
| `sasl.password` | your **API secret** | from Confluent |

`bootstrap.servers` is only a **starting point**, not "the server." The client
connects there once to fetch **cluster metadata** (which brokers exist, which
leads which partition — §8), then talks directly to the right broker per
partition.

### The credential rule

**Credentials always come from environment variables, never source code.**

```python
"sasl.password": os.getenv("SASL_PASSWORD", "")     # correct
"sasl.password": "abc123..."                        # never
```

The pattern used throughout: `.env` file → `load_dotenv()` → `os.getenv()`
([`demo02_producer_common.py:50-70`](../handouts/demo02_producer_common.py#L50-L70)).
`.env` is **git-ignored**; `.env.example` (blank values) is committed as the
template.

Two habits that come with this:

- **Fail loudly on missing config.** `require_producer_config()`
  ([`:84-89`](../handouts/demo02_producer_common.py#L84-L89)) exits with the *names*
  of the missing variables — never their values.
- **Prove connection without leaking it.** `safe_config_report()`
  ([`:136-143`](../handouts/demo02_producer_common.py#L136-L143)) emits the host and
  booleans `has_username` / `has_password`. Every report file in every demo is
  built to be safe to commit and submit.

**Demo 00** ([`demo00.md`](../handouts/demo00.md),
[`demo00_environment_check.py`](../msds682-demos/demo0/demo00_environment_check.py))
is just this step in isolation: confirm Python version, packages, and that your
`.env` loads — before any Kafka concept is involved.

---

## 11. Creating a topic: the admin client

[`demo01_create_topic.py`](../msds682-demos/demo1/demo01_create_topic.py) is the
only demo that uses **`AdminClient`** rather than a producer or consumer. It
manages cluster *metadata*; it never sends a message.

```python
from confluent_kafka.admin import AdminClient, NewTopic     # :8

topic = NewTopic(                                            # :74-79
    topic_name,
    num_partitions=partitions,          # default 3
    replication_factor=replication_factor,   # default 3
    config={"cleanup.policy": cleanup_policy},
)
admin_client = AdminClient(config)                           # :125
```

Those three settings are exactly the three policy decisions from Part I:

- **`num_partitions`** → parallelism and ordering granularity (§3, §5). Semi-static;
  raising it later breaks `hash(key) % N` ordering.
- **`replication_factor`** → durability (§8). Requires ≥ RF brokers.
- **`cleanup.policy`** → `delete` (event history) vs `compact` (latest-per-key
  table) (§7).

Topic creation is **asynchronous**: `create_topics()` returns a dict of futures,
one per topic, which you must resolve to learn the outcome. Creating a topic that
already exists raises `TOPIC_ALREADY_EXISTS` — which the demo treats as success,
making the script safely re-runnable.

* QUESTION: using code snippet examples from our demo0* files if possible, explain in more detail the user of  `create_topics()`, and a "dict of futures, one per topic, which you must resolve to learn the outcome.".  Also: are these actions performed on the client, the broker, or somewhere else?

**Naming convention** used all course: `msds682.demo01.trip-events.v1` —
`<org>.<context>.<entity>.<version>`. The trailing `v1` matters: an
incompatible schema change usually means a **new topic**, not a mutated one.

---

## 12. Producing: the anatomy of one send

All four producer demos share one shape. The important thing to internalize:

> **`produce()` does not send.** It validates arguments, computes the partition,
> appends the record to an in-memory queue, and returns — typically in
> microseconds. A background thread does the actual network I/O. You learn a
> message's fate only later, through a **callback**.

Everything else about producing follows from that one fact.

### 12.1 The four arguments

```python
producer.produce(
    topic=TOPIC_NAME,           # routing (§2)
    key=event_key(event),       # bytes → partition + ordering (§5)
    value=serialize_event(event),  # bytes → the payload (§4)
    callback=tracker.callback,  # local-only; fires after ack (§4)
)
```

### 12.2 Object → bytes: the serialization boundary

Kafka stores **bytes**. Turning a Python object into bytes is your job, and the
demos make it two explicit one-liners
([`demo02_producer_common.py:114-121`](../handouts/demo02_producer_common.py#L114-L121)):

```python
def event_key(event):        return event.trip_id.encode("utf-8")
def serialize_event(event):  return event.model_dump_json(exclude_none=True).encode("utf-8")
```

Three things happen in that second line: the validated Pydantic model becomes a
**JSON string**, `exclude_none=True` **drops empty optional fields**, and
`.encode("utf-8")` produces **bytes**. A real payload looks like:

```json
{"trip_id":"trip_981","event_type":"trip_requested","rider_id":"rider-981","event_time":"2026-07-04T10:00:00Z","zone":"north"}
```

Note the absent `driver_id` and `fare` — a `trip_requested` event has neither.

The **model** is the contract
([`:18-26`](../handouts/demo02_producer_common.py#L18-L26)): a Pydantic
`BaseModel` where `event_type` is a `Literal` of four allowed strings and `fare`
carries `ge=0`. This is **schema-on-write** — malformed events are rejected
*before* they can reach the topic.

* QUESTION: remind me from where the pydantic base model is obtained and where it is applied.  Is it pulled from the broker, and applied on the client side?

> **Forward pointer:** this validation layer is the subject of §16 — treat the
> model here as "a contract that rejects bad events" and read §16.1 when you want
> the full rules, plus §16.2 for how Avro adds a second, structural layer beneath
> it. Nothing in §12–15 requires having read §16 first.

### 12.3 The delivery callback

The callback is the **only** place your program learns what happened to an
individual message
([`demo02_producer_common.py:35-47`](../handouts/demo02_producer_common.py#L35-L47)):

```python
def callback(self, err, msg):
    if err is not None:
        self.failed.append(str(err))     # failure: record the error
        return
    self.delivered.append({              # success: routing metadata only
        "topic": msg.topic(), "partition": msg.partition(),
        "offset": msg.offset(), "key": msg.key().decode("utf-8"),
    })
```

Two design points worth copying:

- It records **only routing metadata** — topic, partition, offset, key. Never
  credentials, never full payloads.
- Real runs **cap the samples kept** (e.g. first 10). At 2,000 messages, storing
  every delivery would bloat the report for no added evidence.

`partition` and `offset` are available here *because* delivery has completed —
they are assigned downstream of `produce()` (§4, §6).

### 12.4 `poll()` vs `flush()` — the whole ballgame

| | `poll(0)` | `flush(timeout)` |
|---|---|---|
| Blocks? | **No** (`0` = don't wait) | **Yes**, until queue empty |
| Does what | serves **already-completed** callbacks | drains **everything** in flight |
| Returns | count of events handled | count of messages **still undelivered** |
| Use it | inside the produce loop | once, before exit / at a batch boundary |

`poll(0)` keeps the callback queue drained so memory stays bounded during a long
run. `flush()` is the **explicit wait point**. Where you put it *is* the delivery
strategy.

* QUESTION: explain in detail what poll() and flush() are and do.

### 12.5 Three delivery patterns

**A. Sync-style — flush after every message**
([`demo02a`](../handouts/demo02a_confluent_sync_style_producer.py#L37-L45))

```python
for event in events:
    producer.produce(topic, key=..., value=..., callback=tracker.callback)
    remaining = producer.flush(flush_timeout)     # inside the loop
```

Strictly sequential: at most one message in flight. Easy to reason about, easy to
debug — and **slow**, because every message pays a full network round trip.

**B. Async — poll while producing, flush once**
([`demo02b`](../handouts/demo02b_confluent_async_producer.py#L36-L48))

```python
for event in events:
    producer.produce(topic, key=..., value=..., callback=tracker.callback)
    producer.poll(0)                              # non-blocking, inside loop
remaining = producer.flush(flush_timeout)         # exactly once, after loop
```
* QUESTION: explain in detail where "remaining" comes from as its orgin is not shown in this code.  Explain what it does.  Does it take any messages that were not successfully processed?

The idiomatic high-throughput pattern. Messages pipeline over the network instead
of going one at a time.

> **The single most common bug:** forgetting the final `flush()`. `poll(0)` never
> waits, so messages still in flight at exit are simply **lost**, and your counts
> silently under-report. The final `flush()` is mandatory.

**C. Explicit serialization**
([`demo02d`](../handouts/demo02d_confluent_serialization_producer.py#L37-L47))

Structurally identical to B, but lifts the byte conversion onto its own line to
foreground the boundary:

```python
for event in events:
    value_bytes = serialize_event(event)      # object → bytes, explicitly
    producer.produce(topic, key=event_key(event), value=value_bytes, callback=...)
    producer.poll(0)
remaining = producer.flush(flush_timeout)
```
* QUESTION: what are the use cases for pattern B vs pattern C? When is each one best to do?

The lesson: **validation and serialization are distinct steps.** Pydantic
guarantees the object is *correct*; serialization decides how it travels *on the
wire*.

---

## 13. Measuring: async vs sync, with evidence

[`demo02c`](../handouts/demo02c_confluent_async_sync_compare.py) runs both
strategies over the **same deterministic events** and compares them. Two ideas
make the comparison trustworthy.

### 13.1 Determinism is what makes it fair

`make_trip_events(count, seed)`
([`demo02_producer_common.py:92-111`](../handouts/demo02_producer_common.py#L92-L111))
seeds one `random.Random` and generates events from `index` alone, so the same
seed always yields the same stream. Both strategies send **identical payloads** —
any speed difference is therefore attributable to the strategy, not the data.

This is why the `rng` call order inside `make_trip_event` must never change: it
would desynchronize the sequence.

* QUESTION: I don't understand the point of using a seed for ML work in order to generate reproduceable results, but I don't understand the point of sending anything random via kafka... is this purely for testing purposes?

### 13.2 Measure through *completed delivery*

Time each batch from before the first `produce()` until **after** its `flush()`
returns. Stopping the clock earlier would measure *queueing*, not delivery.

Per-batch bookkeeping uses **deltas**, not totals: snapshot the tracker's counts
before the batch, subtract after. A running total would report 1000 delivered on
batch 2 instead of 500.

### 13.3 What the numbers actually look like

A real 2,000-message-per-strategy run at batch size 500:

| Strategy | Throughput | Time per 500-msg batch |
|---|---|---|
| **async** | ~2,660 msg/sec | ~0.19 s |
| **sync_style** | ~12.7 msg/sec | ~39 s |

**~205× difference.** Sync pays ~78 ms per message in round-trip latency; async
pipelines the batch and waits once.

Two details worth reading off such a run:

- **Async batch 1 is much slower than batches 2–4** (~305 vs ~2,660 msg/s). That
  is **connection warm-up** — TLS handshake, SASL auth, metadata fetch — amortized
  after the first batch. Sync shows no such spike because that fixed cost is
  trivial next to 500 sequential round trips.
- **This is not a Kafka capacity claim.** It measures one client, one network
  path, default producer tuning, small JSON messages. The valid conclusion is the
  *relative* one — flushing per message serializes on latency — not the absolute
  numbers.

### 13.4 Plotting it

Because sync is ~200× slower, a **linear y-axis flattens it to zero**, implying
it delivered nothing. Use a **log y-axis** so both series stay legible, and force
**integer x-ticks** (there is no "batch 1.5"):

```python
plt.yscale("log")
plt.xticks(sorted({row["batch_index"] for row in rows}))
plt.grid(True, which="both", alpha=0.3)   # "both" = minor gridlines on log scale
```

---

## 14. Consuming: the poll loop

Reading is where the **offset-as-bookmark** idea (§6) becomes operational.

### 14.1 Consumer configuration adds four keys

On top of the same five connection keys (§10), a consumer needs
([`demo03_consumer_common.py:50-74`](../handouts/demo03_consumer_common.py#L50-L74)):

| Key | Meaning |
|---|---|
| `group.id` | **which consumer group** this member belongs to — the identity that owns committed offsets |
| `client.id` | a label for this individual process (diagnostics) |
| `auto.offset.reset` | where to start **only when the group has no committed offset**: `earliest` or `latest` |
| `enable.auto.commit` | whether the client commits offsets for you on a timer |

> **`auto.offset.reset` is a fallback, not a seek.** It applies *only* to a group
> with no stored position. Once a group has committed offsets, it always resumes
> from them and this setting is ignored. Getting this backwards is the most common
> consumer confusion.

### 14.2 Subscribe, poll, decode, close

[`demo03a`](../handouts/demo03a_confluent_basic_consumer.py) is the minimal shape:

```python
consumer = Consumer(config)
try:
    consumer.subscribe([TOPIC_NAME], on_assign=..., on_revoke=...)
    while ...:
        message = consumer.poll(timeout)      # None if nothing arrived
        if message is None: continue
        if message.error(): ...               # see below
        record = message_to_record(message)   # decode + validate
finally:
    consumer.close()                          # ALWAYS
```

Four rules this encodes
([`demo03_consumer_common.py:236-267`](../handouts/demo03_consumer_common.py#L236-L267)):

1. **`poll()` returns `None` constantly.** That is normal — it means "no message
   within the timeout," not an error. Loop again.
2. **Check `message.error()` before using the message.** `_PARTITION_EOF` is
   informational (you reached the end of a partition) and should be skipped, not
   raised.
3. **Decode defensively.** The value arrives as bytes; the demo decodes UTF-8 and
   re-validates it through the *same* Pydantic model the producer used —
   `TripEvent.model_validate_json(...)`. The schema is a contract enforced at both
   ends, because Kafka enforces nothing (§4).
4. **`close()` in a `finally`.** It commits final offsets (when auto-commit is on)
   and leaves the group promptly, so a rebalance doesn't wait for a session
   timeout.

**Bound your loops.** Every demo stops on explicit limits — max messages, idle
timeout, wall-clock timeout — so no script runs forever.

### 14.3 Committing offsets: at-least-once delivery

[`demo03b`](../handouts/demo03b_confluent_offsets_commit.py) turns auto-commit
**off** and commits manually. The ordering rule is the entire point:

```
poll → decode → validate → process → THEN commit
```

Committing *before* processing risks **losing** a record (marked done, then you
crash). Committing *after* risks **reprocessing** it (done, crash before commit) —
which is why Kafka's default guarantee is **at-least-once**, and why consumer
processing should be **idempotent**.

* QUESTION: remind me what idempotent means in this context.  I'm used to it meaning AB = BA in linear algebra.

With `enable.auto.commit=False`, the demo also sets
`enable.auto.offset.store=False` so *the application* decides what counts as
progress.

| Commit mode | Call | Behavior |
|---|---|---|
| **sync** | `commit(message=msg, asynchronous=False)` | blocks, returns committed partitions; safest |
| **async** | `commit(message=msg, asynchronous=True)` | returns immediately; result arrives via `on_commit` callback |

Async commits need the **`on_commit` callback** to detect failure — the same
callback pattern as producer delivery
([`demo03_consumer_common.py:164-175`](../handouts/demo03_consumer_common.py#L164-L175)).

**Proof it works:** run the script twice with the same `group.id`. The second run
resumes *after* the offsets the first one committed.

### 14.4 Groups, rebalancing, and replay

[`demo03c`](../handouts/demo03c_confluent_groups_replay.py) demonstrates the
**consumer group** — the mechanism for parallel consumption.

```
Topic (3 partitions)          Group "analytics"
 ┌───────────┐
 │ partition 0│ ──────────────▶ member A
 │ partition 1│ ──────────────▶ member A
 │ partition 2│ ──────────────▶ member B
 └───────────┘
```

Rules:

- **Within one group, each partition goes to exactly one member.** Adding members
  spreads partitions; you cannot usefully run more members than partitions —
  extras sit idle.
- **Different groups are independent.** Each has its own committed offsets and
  each sees *every* record. This is how one topic feeds many applications (§6:
  reading does not consume).
- **Rebalancing** happens when membership changes. The `on_assign` / `on_revoke`
  callbacks fire so you can observe it
  ([`demo03_consumer_common.py:178-198`](../handouts/demo03_consumer_common.py#L178-L198)).

**Replay** is a separate contract from normal group consumption. To reread from
the start you must *explicitly override* the assigned offsets:

```python
def on_assign(consumer, partitions):
    for p in partitions:
        p.offset = OFFSET_BEGINNING      # explicit replay
    consumer.assign(partitions)
```

This is deliberate: replay is an override, never something that happens by
accident. (Changing `group.id` also gives you a fresh history — a new group has
no committed offsets, so `auto.offset.reset` applies.)

---

## 15. Concurrency: asyncio clients

[`demo03d`](../handouts/demo03d_confluent_asyncio_produce_consume.py) uses the
native `AIOProducer` / `AIOConsumer` so Kafka work shares an event loop with other
async work.

Differences from the blocking clients:

- Calls are **awaited**: `await producer.produce(...)`, `await consumer.poll(...)`,
  `await producer.flush()`.
- `await producer.produce(...)` returns a **future per message**; gather them to
  collect delivery results — the async analogue of the delivery callback.
- Producer and consumer run as concurrent **tasks** via `asyncio.gather`.

The genuinely instructive part is **coordination**. A consumer starting at
`latest` will miss messages produced before it is assigned partitions. The wrong
fix is `sleep(2)` and hope. The right fix is an explicit signal:

```python
assignment_ready = asyncio.Event()

async def on_assign(consumer, partitions):     # in the consumer
    await consumer.assign(partitions)
    assignment_ready.set()                      # signal: I'm ready

# in the producer, before producing anything:
await asyncio.wait_for(assignment_ready.wait(), timeout=assignment_timeout)
```

**A signal is deterministic; a sleep is a guess.** The demo also cancels and
awaits the sibling task on failure, so every client's `finally` still closes its
sockets.

---

## 16. Schemas: validation, Avro, and the Registry

Demo 04 answers a question Part I left open: Kafka stores **opaque bytes** (§4),
so *what stops a producer from writing garbage a consumer can't read?* Nothing —
unless you add a schema layer. There are **two distinct layers**, and confusing
them is the central lesson.

| Layer | Enforces | Tool | Example it catches |
|---|---|---|---|
| **Application / domain** | *meaning* — business rules | **Pydantic** | `fare = -10` is invalid |
| **Wire / structure** | *shape* — field names & types | **Avro + Schema Registry** | `fare` must be a `double` |

**Neither replaces the other.** Avro will happily encode `fare = -10.0` — it is a
valid `double`. Only the business rule rejects it
([`demo04b:109-135`](../handouts/demo04b_local_avro_roundtrip.py#L109-L135)
demonstrates exactly this).

### 16.1 Application validation (Demo 04A)

[`demo04a`](../handouts/demo04a_schema_validation.py) is fully local — no Kafka,
no Registry. It runs pass/fail cases through the JSON → Pydantic boundary an
application actually uses (`model_validate_json`, not a plain dict). The strict
contract rejects:

- **naive timestamps** (no timezone) — ambiguous across regions
- **extra fields** — silent schema drift
- **`"27.50"` as a string** where a float is required — type coercion hides bugs
- **negative fare** — a domain rule
- **cross-field rules**: `trip_completed` *requires* a fare; `trip_requested`
  *forbids* a `driver_id`

That last category is the one **Avro cannot express at all**. Avro types a field;
it cannot say "this field is required only when `event_type` is X."

### 16.2 Avro + Schema Registry (Demo 04B, local)

[`demo04b`](../handouts/demo04b_local_avro_roundtrip.py) runs against
`mock://` — a real Registry client with no network.

The mechanism: **the schema is registered once and the payload carries only its
ID.**

```
Confluent wire format:
┌──────────┬───────────────────┬─────────────────────────────┐
│ magic (1)│  schema ID (4, BE)│   Avro binary body          │
│  = 0     │  → Registry lookup│   (no field names!)         │
└──────────┴───────────────────┴─────────────────────────────┘
     ^ 5-byte header
```

([`demo04_common.py:421-432`](../handouts/demo04_common.py#L421-L432) parses
exactly this.) Consequences:

- **Avro binary is not JSON.** Trying `json.loads(payload.decode("utf-8"))` raises
  — the demo asserts that it does.
- **Compact.** Field names live in the schema, not in every message.
- **The Registry is the source of truth**, keyed by *subject*, conventionally
  `<topic>-value`.

**Schema evolution** is the payoff. A **backward-compatible** change lets a *new*
reader read *old* data — most commonly by adding a field **with a default**:

```
v1 writer payload  ──read by──▶  v2 reader schema  ──▶  vehicle_type = None (default)
```

The demo proves this by deserializing the same v1 bytes with both a v1 and a v2
reader. Adding a field *without* a default, or removing/renaming one, breaks
compatibility — and the Registry can enforce the rule before a bad schema ships.

### 16.3 The real cloud round trip (Demo 04C/04D)

[`demo04c`](../handouts/demo04c_confluent_avro_roundtrip.py) does the same thing
against real Confluent Cloud, which adds one thing worth noting: **Schema Registry
credentials are separate from Kafka credentials** (`SCHEMA_REGISTRY_URL`,
`SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET` in `.env.example`). Two
services, two credential pairs.

Its consumer path restates §14.3's ordering rule with schemas added:

```
poll → deserialize (Avro) → validate (Pydantic) → process → commit
```

[`demo04d`](../handouts/demo04d_asyncio_avro_roundtrip.py) combines this with the
asyncio clients from §15.

---

## 17. The complete data path

One event, end to end, with every concept in place:

```
  make_trip_event(index, rng)            deterministic generation      §13.1
        │
        ▼
  TripEvent (Pydantic)                   application validation        §16.1
        │
        ▼
  event_key()  +  serialize_event()      object → bytes                §12.2
        │        (or Avro serializer + schema ID)                      §16.2
        ▼
  producer.produce(topic, key, value, callback)
        │                                queued, returns instantly     §12
        ▼
  poll(0)  /  flush()                    drain + fire callbacks        §12.4
        │
        ▼
  ── partitioner picks partition ──▶ broker appends ──▶ assigns OFFSET  §5, §6
        │
        ▼
  DeliveryTracker.callback(err, msg)     success/failure evidence      §12.3
        │
   ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ the log persists ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
        ▼
  consumer.poll()                        one member of a group         §14.4
        │
        ▼
  decode → TripEvent.model_validate_json()   re-validate on read       §14.2
        │
        ▼
  process → commit()                     at-least-once progress        §14.3
```

Everything above is the shape of every real Kafka application you will write.

---
---

# Part III — Reference

## 18. Demo file map

| File | Role in this story | Sections |
|---|---|---|
| [`demo00.md`](../handouts/demo00.md) / [`demo00_environment_check.py`](../msds682-demos/demo0/demo00_environment_check.py) | Environment & connectivity check | §10 |
| [`demo01.md`](../handouts/demo01.md) / [`demo01_create_topic.py`](../msds682-demos/demo1/demo01_create_topic.py) | **Admin client** — creates a topic (partitions, RF, cleanup policy). *Not* a producer. | §2, §3, §7, §11 |
| [`demo02.md`](../handouts/demo02.md) | Producer lecture notes | §12–13 |
| [`demo02_producer_common.py`](../handouts/demo02_producer_common.py) | Shared producer helpers: config, `TripEvent`, key/value serialization, delivery tracking | §4, §5, §10, §12 |
| [`demo02a_confluent_sync_style_producer.py`](../handouts/demo02a_confluent_sync_style_producer.py) | Producer — **sync-style** (flush per message) | §12.5A |
| [`demo02b_confluent_async_producer.py`](../handouts/demo02b_confluent_async_producer.py) | Producer — **async** (poll + one flush) | §12.5B |
| [`demo02c_confluent_async_sync_compare.py`](../handouts/demo02c_confluent_async_sync_compare.py) | Producer — **benchmark**, async vs sync | §13 |
| [`demo02d_confluent_serialization_producer.py`](../handouts/demo02d_confluent_serialization_producer.py) | Producer — **explicit serialization** | §12.5C |
| [`demo03.md`](../handouts/demo03.md) | Consumer lecture notes | §14–15 |
| [`demo03_consumer_common.py`](../handouts/demo03_consumer_common.py) | Shared consumer helpers: group config, poll loop, commit & assignment trackers | §14 |
| [`demo03a_confluent_basic_consumer.py`](../handouts/demo03a_confluent_basic_consumer.py) | Consumer — subscribe, poll, decode, close | §14.2 |
| [`demo03b_confluent_offsets_commit.py`](../handouts/demo03b_confluent_offsets_commit.py) | Consumer — **manual offset commits**, resume proof | §14.3 |
| [`demo03c_confluent_groups_replay.py`](../handouts/demo03c_confluent_groups_replay.py) | Consumer — **groups, rebalancing, replay** | §14.4 |
| [`demo03d_confluent_asyncio_produce_consume.py`](../handouts/demo03d_confluent_asyncio_produce_consume.py) | **asyncio** producer + consumer together | §15 |
| [`demo04.md`](../handouts/demo04.md) | Schema/Avro lecture notes | §16 |
| [`demo04_common.py`](../handouts/demo04_common.py) | Shared schema helpers: `TripEventV1`, Avro conversion, wire-header parsing | §16 |
| [`demo04a_schema_validation.py`](../handouts/demo04a_schema_validation.py) | **Pydantic** application/domain validation (local) | §16.1 |
| [`demo04b_local_avro_roundtrip.py`](../handouts/demo04b_local_avro_roundtrip.py) | **Avro + mock Registry**, schema evolution (local) | §16.2 |
| [`demo04c_confluent_avro_roundtrip.py`](../handouts/demo04c_confluent_avro_roundtrip.py) | Avro round trip on **real Confluent Cloud** | §16.3 |
| [`demo04d_asyncio_avro_roundtrip.py`](../handouts/demo04d_asyncio_avro_roundtrip.py) | Avro + **asyncio** round trip | §15, §16.3 |

---

## 19. Command cheat sheet

```bash
# Setup (once)
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env          # then fill in YOUR credentials; never commit

# 0. Environment check
python demo00_environment_check.py

# 1. Create the topic (safe to re-run)
python demo01_create_topic.py --partitions 3 --replication-factor 3

# 2. Produce
python demo02a_confluent_sync_style_producer.py --count 4
python demo02b_confluent_async_producer.py      --count 4
python demo02c_confluent_async_sync_compare.py  --count 2000
python demo02d_confluent_serialization_producer.py --count 4

# 3. Consume  (run 03b twice with the same group to see resume)
python demo03a_confluent_basic_consumer.py --max-messages 8
python demo03b_confluent_offsets_commit.py --commit-mode sync
python demo03c_confluent_groups_replay.py  --member-id member-a
python demo03c_confluent_groups_replay.py  --force-beginning   # explicit replay
python demo03d_confluent_asyncio_produce_consume.py --count 6

# 4. Schemas  (04a/04b need no cloud account)
python demo04a_schema_validation.py
python demo04b_local_avro_roundtrip.py
python demo04c_confluent_avro_roundtrip.py
```

Common flags: `--run-id` (names the evidence folder), `--count`, `--seed 682`
(reproducibility), `--max-messages` / `--idle-timeout` (bound consumer loops).

---

## 20. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Missing required .env values: ...` | `.env` absent or not found | Confirm `.env` is in the directory you run from (§10) |
| Authentication / SASL errors | wrong API key or secret; Registry keys used for Kafka | Kafka and Schema Registry have **separate** credentials (§16.3) |
| `TOPIC_ALREADY_EXISTS` | topic exists | Not an error — the demo treats it as success (§11) |
| Producer reports fewer delivered than attempted | missing final `flush()` | Add `remaining = producer.flush(timeout)` after the loop (§12.5B) |
| `remaining_after_flush > 0` | flush timed out | Raise `--flush-timeout`; check connectivity |
| Consumer prints nothing, exits on idle | group already committed past the data, or `auto.offset.reset=latest` on a fresh group with no new writes | Use a new `group.id`, or `--force-beginning` to replay (§14.4) |
| Consumer re-reads the same records every run | offsets never committed | Enable auto-commit or commit manually (§14.3) |
| `_PARTITION_EOF` raised as an error | treating EOF as fatal | Skip it — it is informational (§14.2) |
| `UnicodeDecodeError` / `JSONDecodeError` on consume | reading **Avro** bytes as JSON | Use the Avro deserializer; Avro is not text (§16.2) |
| Sync line looks flat at zero on the chart | linear y-axis with a ~200× gap | Use `plt.yscale("log")` (§13.4) |

---

See [`glossary.md`](glossary.md) for precise definitions of every term above.
