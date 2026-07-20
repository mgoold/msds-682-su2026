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

### Is a topic "a log file"?

**No — and the gap between that intuition and reality is worth pinning down,
because it explains most of Kafka's behavior.**

A topic is **not** a file. Materially, a topic with 3 partitions is:

```
msds682.demo01.trip-events.v1          ← the topic: a NAME + POLICY. No bytes of its own.
├── partition 0  →  a directory on some broker   ← real storage
│   ├── 00000000000000000000.log                 ← a SEGMENT file (actual records)
│   ├── 00000000000000000000.index
│   └── 00000000000004096.log                    ← the next segment
├── partition 1  →  a directory on some (possibly different) broker
└── partition 2  →  a directory on some (possibly different) broker
```

So "topic = log file" is wrong twice over:

1. **A topic is not one log — it is N logs.** Each partition is an independent
   append-only log with its own offset counter starting at 0. There is no
   file, anywhere, containing "the topic."
2. **Even a partition is not one file.** It is a *directory* of **segment**
   files that Kafka rolls over as they fill (see §3 below).

And the partitions of one topic usually live on **different brokers**, so a
topic's bytes are physically scattered across several machines.

**What a topic materially *is*:** an entry in cluster metadata — a name, a
partition count, a replication factor, and a config bag (`cleanup.policy`,
retention, etc.). It is closer to a **table name in a database** than to a file:
the name plus the rules, while the actual rows live in physical structures
underneath.

> **If you want a one-liner:** a topic is a *named, partitioned, policy-carrying
> stream*. The only thing that is literally a log file is a **segment**.

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

### Segments: the files a partition is actually made of

A partition is a **directory**; a **segment** is a **file inside it**. Kafka
never writes one giant ever-growing file — it writes a series of bounded ones.

```
msds682.demo01.trip-events.v1-0/          ← partition 0's directory
├── 00000000000000000000.log      ← CLOSED segment: records for offsets 0…4095
├── 00000000000000000000.index    ← offset → byte-position lookup
├── 00000000000000000000.timeindex← timestamp → offset lookup
├── 00000000000000004096.log      ← ACTIVE segment: currently being appended
├── 00000000000000004096.index
└── 00000000000000004096.timeindex
```

**The filename is the base offset.** `00000000000000004096.log` holds records
starting at offset 4096. That naming is what makes a read seek fast: to find
offset 5000, Kafka binary-searches the filenames to pick the segment, then uses
that segment's `.index` to jump to roughly the right byte, then scans forward a
little. No database, no central lookup table.

**Only one segment per partition is "active."** Writes always append to the
active segment. When it hits a limit, Kafka **closes** it and opens a new one —
a *roll*. Two settings trigger a roll:

| Setting | Default | Meaning |
|---|---|---|
| `segment.bytes` | 1 GB | roll when the active segment reaches this size |
| `segment.ms` | 7 days | roll after this much time, even if small |

**Why this design matters — three consequences you can feel:**

1. **Deletion is cheap and coarse.** Retention (`cleanup.policy=delete`) does not
   remove individual records — it deletes **whole closed segment files** whose
   newest record has aged out. This is why you cannot delete one record by
   offset: the unit of forgetting is a file, not a row. (Compaction is different:
   it rewrites segments, keeping the latest record per key.)
2. **The active segment is never deleted or compacted.** Data you just wrote
   sticks around until the segment rolls, regardless of retention settings —
   a common surprise when testing short retention.
3. **Writes are sequential appends to one open file**, which is why Kafka is so
   fast. It rides the OS page cache and sequential disk I/O rather than doing
   random writes, and can hand bytes straight to the network.

**Tiered storage** (§3's last bullet) operates at this granularity too: closed
segments get shipped to object storage while the active one stays on local disk.
That is the sense in which "segment ≈ object" — a segment really does become an
S3 object.

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

### Does the client ever change partitions dynamically?

**It depends entirely on whether your record has a key — and the two cases are
opposites by design.**

#### With a key (what this course does): no, it is deterministic

`hash(key) % num_partitions` is a pure function. The same `trip_id` goes to the
same partition on every call, from every producer instance, in every process, for
as long as the partition count is unchanged. That determinism *is* the ordering
guarantee — it is not an implementation detail you should hope holds.

You can see it in the assignment evidence: all four messages keyed `trip_981`
landed on partition 2 at consecutive offsets 20–23.

#### Without a key: yes, deliberately and constantly

If `key=None`, there is nothing to hash, so the client is free to load-balance —
and modern clients use a **sticky partitioner**:

```
pick a partition → fill an entire batch with records → send it
                 → pick a DIFFERENT partition → fill the next batch → …
```

Not round-robin per record (that would scatter each record into a different
half-empty batch and destroy batching efficiency), but **sticky per batch**. So a
keyless producer changes partition roughly every time a batch is dispatched.

#### Four things that *can* shift the target, even with a key

| Cause | Effect |
|---|---|
| **Partition count increases** | `% N` changes, so existing keys remap to different partitions. Old records stay put; new ones may land elsewhere — this is exactly why §5 says the count is semi-static and why Kafka only allows *increases*, never decreases or reshuffles. |
| **Metadata refresh** | The client periodically re-fetches topic metadata (`topic.metadata.refresh.interval.ms`, default 5 min). It learns of new partitions or new leaders here — the count it divides by comes from this cache, not from a live query per record. |
| **Leader change / unavailable partition** | If a partition's leader broker dies, the client re-fetches metadata and sends to the new leader. The *partition* is unchanged; only the broker it talks to moves. Some clients additionally avoid partitions with no available leader when keyless. |
| **You pass `partition=` explicitly** | `produce()` accepts an explicit partition, which bypasses the partitioner entirely. Rare, and it puts the ordering burden on you. |

**The practical takeaway:** keying is how you *buy* ordering, and the price is
that partition count becomes hard to change later. A keyless producer gets easy,
even load distribution and gives up per-key ordering. Choose the key based on
what must stay ordered together — here, one trip's lifecycle.

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

### `create_topics()` in detail — and where the work actually happens

Here is the whole thing from
[`demo01_create_topic.py:74-83`](../msds682-demos/demo1/demo01_create_topic.py#L74-L83):

```python
topic = NewTopic(
    topic_name,
    num_partitions=partitions,
    replication_factor=replication_factor,
    config={"cleanup.policy": cleanup_policy},
)
# create_topics() is async: it returns {topic_name: Future}.
# result() blocks until the broker confirms, or raises on failure.
futures = admin_client.create_topics([topic])
futures[topic_name].result(timeout=30)
```

Three lines, three distinct ideas.

#### 1. `NewTopic(...)` is a plain local object

It creates *nothing*. It is a request description — a dataclass-like bundle of
"here is what I want" — sitting in your process's memory. No network traffic yet.

#### 2. `create_topics([topic])` takes a **list** and returns a **dict of futures**

Note the API shape: you hand it a **list** of `NewTopic` objects, and get back a
`dict` keyed by topic name:

```python
futures = admin_client.create_topics([topic_a, topic_b, topic_c])
# → {"topic_a": Future, "topic_b": Future, "topic_c": Future}
```

**Why a dict of futures rather than a single result?** Because the operations
succeed or fail *independently*. In one call, `topic_a` might be created,
`topic_b` might already exist, and `topic_c` might be rejected for requesting
more replicas than you have brokers. A single return value could not express
three different outcomes, so each topic gets its own future you resolve
separately.

This is the same `concurrent.futures.Future` you may know from Python's
`ThreadPoolExecutor` — a handle to a result that isn't ready yet.

#### 3. `.result(timeout=30)` is where you actually find out

`create_topics()` returns **immediately**, before the cluster has done anything.
Calling `.result()` **blocks** your thread until the answer arrives, then either
returns normally (success) or **raises** `KafkaException`. That raise is how the
demo detects a topic that already exists — it catches the exception, inspects the
error code for `TOPIC_ALREADY_EXISTS`, and treats it as success, which is what
makes the script safely re-runnable.

If you never call `.result()`, your script can exit having learned nothing — and
possibly before the request was even sent.

#### So where does the work happen?

All three places, in sequence:

```
YOUR PROCESS (client)          NETWORK            KAFKA CLUSTER
─────────────────────          ───────            ─────────────
NewTopic(...)                                     
  builds a local request                          
                                                  
admin_client.create_topics()                      
  ├─ hands request to the ──── CreateTopics ────▶  CONTROLLER broker
  │  background I/O thread      request            ├─ validates the request
  ├─ creates Future objects                        ├─ picks which brokers host
  └─ RETURNS IMMEDIATELY ◀──┐                      │  which partition replicas
                            │                      ├─ writes to cluster metadata
future.result(timeout=30)   │                      └─ creates log directories
  └─ BLOCKS your thread     │                         on the chosen brokers
                            └──── response ─────────┘
     background thread resolves the Future
     → .result() returns, or raises
```

- **On the client:** building the request, managing futures, and blocking in
  `.result()`. The client also runs a **background thread** that owns the socket
  and resolves futures when responses land — the same architecture as the
  producer's background thread in §12.
- **On the broker:** all the real work. Specifically the **controller** — one
  elected broker responsible for cluster metadata. It decides replica placement,
  updates the cluster's metadata, and instructs brokers to create the partition
  directories from §3.
- **Nowhere else.** There is no separate coordination service in modern Kafka
  (older versions used ZooKeeper for this; KRaft mode keeps it inside the brokers).

**The pattern generalizes:** this "call returns a future, `.result()` blocks and
raises" shape is how nearly all admin operations work (`delete_topics`,
`create_partitions`, `describe_configs`). And it is the same *asynchronous
client, background I/O thread, deferred outcome* model you meet again in
`produce()` + callbacks (§12) — Kafka clients are asynchronous almost everywhere,
and your job is deciding **where to block**.

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

### Where does the Pydantic model come from, and where is it applied?

**It is your own source code, and it runs entirely on the client. The broker has
no idea it exists.** This is worth stating plainly because it is the single most
common misconception about Kafka schemas.

#### Where it comes from: a file in your repo

```python
# demo02_producer_common.py:18-26 — you wrote this; nothing fetched it
class TripEvent(BaseModel):
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    rider_id: str
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0)
```

Nothing is pulled from the broker. Nothing is registered anywhere. It is an
ordinary Python class that both your producer and your consumer `import`.

#### Where it is applied: both ends, independently

```
PRODUCER (your process)                          CONSUMER (your process)
────────────────────────                         ────────────────────────
TripEvent(...)          ← validates HERE         raw bytes off the wire
   │                                                │
   ▼                                                ▼
serialize_event()       → JSON → bytes           TripEvent.model_validate_json()
   │                                                │  ← validates HERE, again
   ▼                                                ▲
producer.produce() ─────▶ BROKER ───────────────────┘
                          (stores opaque bytes;
                           validates NOTHING)
```

- **On write:** validation happens when you *construct* the `TripEvent`, before
  `produce()` is ever called. A negative `fare` raises a `ValidationError` in
  your code — the message never reaches the network.
- **On read:** the consumer decodes bytes and calls
  `TripEvent.model_validate_json(...)`
  ([`demo03_consumer_common.py:129`](../handouts/demo03_consumer_common.py#L129)),
  applying the *same class* to data that arrives.

#### Why it is validated twice

Because **the broker will not do it for you.** Kafka stores opaque bytes (§4). It
cannot reject a malformed record, because it has no notion of what your bytes
mean. The schema is therefore a **social contract between two programs**, and it
holds only as long as both programs independently choose to honor it.

That has a real failure mode: if someone deploys a producer with a changed model
and the consumer still has the old one, nothing warns you. The mismatch surfaces
at 3 a.m. as a `ValidationError` on the consumer — or worse, as silently
misinterpreted data.

#### What *is* fetched from a server: Schema Registry (§16)

This is exactly the gap Schema Registry fills, and it is the one schema component
that genuinely lives elsewhere:

| | Pydantic model | Avro + Schema Registry |
|---|---|---|
| **Lives where** | your source code | a **separate registry service** (not the broker) |
| **Fetched at runtime?** | no — imported | **yes** — client fetches a schema by ID |
| **Enforces** | business meaning (`fare >= 0`, cross-field rules) | wire structure (field names, types) |
| **Can reject a bad deploy?** | no | yes — compatibility checks before a schema is accepted |

Even then, note the registry is a **third service** alongside the Kafka brokers,
with its **own credentials** (§16.3) — and it still does not validate business
rules. `fare = -10` is a perfectly valid Avro `double`. Only your Pydantic model
rejects it, client-side, as it always did.

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

### `poll()` and `flush()` in detail

To understand both, you need one fact about how the client is built:
**`confluent-kafka` is a thin Python wrapper over a C library (librdkafka) that
runs its own background thread.** That thread — not your Python thread — owns the
socket and does all network I/O.

```
YOUR PYTHON THREAD                    BACKGROUND THREAD (C)
──────────────────                    ─────────────────────
produce()  ──appends to──▶  [ send queue ]
  returns instantly                        │ picks up records,
                                           │ batches them, sends,
                                           │ waits for broker acks
                                           ▼
                                    [ callback queue ]  ← results land here
poll()  ──drains──────────────────────────┘
  runs YOUR callbacks
  on YOUR thread
```

The two queues are the key. `produce()` fills the first; the background thread
moves work into the second; **`poll()` is the only thing that empties the
second.**

#### `poll(timeout)` — "run my pending callbacks now"

```python
n = producer.poll(0)     # non-blocking: serve whatever is ready, return count
n = producer.poll(1.0)   # wait up to 1 second for something to become ready
```

- **What it does:** executes queued delivery callbacks **on your calling thread**,
  and returns how many events it served.
- **`timeout=0`** means *do not wait* — serve what's ready and return
  immediately, possibly having done nothing.
- **Why callbacks need it:** your Python callback cannot safely be invoked from
  the C background thread, so results are queued until you call `poll()`. **A
  callback never fires on its own.**

**Two things go wrong if you never call it:**

1. **Memory grows.** Completed results accumulate in the callback queue forever.
2. **Producing eventually fails.** The send queue is bounded
   (`queue.buffering.max.messages`, default ~100,000). Once full, `produce()`
   raises `BufferError` instead of enqueuing. Calling `poll(0)` in the loop keeps
   the pipeline moving.

#### `flush(timeout)` — "block until everything is resolved"

```python
remaining = producer.flush(30.0)
```

Conceptually, `flush()` is just **`poll()` in a loop until the queues are empty**:

```python
# what flush(timeout) is doing, in spirit
deadline = now() + timeout
while messages_in_flight() > 0 and now() < deadline:
    poll(small_interval)
return messages_in_flight()      # 0 if everything resolved
```

- **It blocks** until every produced message has reached a **terminal state** —
  either delivered (callback with `err=None`) or failed (callback with an error).
- **It is not itself a network call.** It sends nothing; it *waits* for the
  background thread to finish what it already has, and serves the resulting
  callbacks along the way.
- **It returns a count**, not a status: how many messages are *still* unresolved
  when it gave up. `0` means done. See the next section for what that number does
  and does not include.

#### The comparison that actually matters

| | `poll(0)` | `flush(timeout)` |
|---|---|---|
| Blocks? | no | yes, until empty or timeout |
| Serves callbacks? | yes, whatever is ready | yes, continuously while waiting |
| Waits for in-flight messages? | **no** | **yes** — that is its whole job |
| Returns | number of events served | number of messages **still unresolved** |
| Typical placement | inside the produce loop | once before exit, or per batch |

**The one-sentence version:** `poll()` is *"process results that already
arrived"*; `flush()` is *"wait until there are no more results coming."* Every
producer must call `flush()` at least once before exiting, or messages still in
the queue are silently discarded when the process dies.

> **Consumers have a `poll()` too, and it is a different animal** — it *fetches
> records from the broker* and returns one message (§14.2). Same name, opposite
> direction: producer `poll` drains outbound results; consumer `poll` pulls
> inbound data.

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
> **Where does `remaining` come from?** It has no prior definition — that line
> *creates* it. `remaining` is simply the **return value of `flush()`**:
> the number of messages still sitting unresolved in the producer's queue when
> `flush()` returned. On a healthy run it is `0`.
>
> **Does it include messages that failed?** **No — and this is the important
> subtlety.** Every produced message ends in exactly one of three states:
>
> | State | How you learn about it | Counted in `remaining`? |
> |---|---|---|
> | **Delivered** | callback fires with `err = None` → `delivered_count += 1` | no |
> | **Failed** | callback fires with an error → appended to `failed_messages` | **no** |
> | **Unresolved** | callback never fired; still queued when the timeout expired | **yes** |
>
> A *failed* message is a **resolved** message — Kafka reached a verdict and told
> you via the callback, so it leaves the queue and does not count as remaining.
> `remaining > 0` means something different and worse: **you don't know what
> happened.** Those messages might still be delivered after your process exits,
> or might not be. The usual cause is a `flush()` timeout that was too short, or
> a broker you cannot reach.
>
> That is exactly why the reports check **both** conditions:
>
> ```python
> if report["failed"] or report["remaining_after_flush"]:
>     raise SystemExit("Some messages were not delivered.")
> ```
>
> `failed` catches *known* problems; `remaining` catches *unknown* ones. A run is
> only trustworthy when both are zero — which is also why the benchmark's CSV
> contract requires `batch_delivered == 500`, `batch_failed == 0`, **and**
> `remaining_after_flush == 0` on every row.

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
### When to use pattern B vs pattern C

**Straight answer: they are the same pattern.** C is B with one expression pulled
onto its own line. The produce/poll/flush control flow is identical, the bytes on
the wire are identical, and the performance is identical — measurably so:
serialization is ~0.88 µs per event, about 0.24 % of a 500-message async batch.

So the choice is not architectural. It is about **when the serialization step
deserves to be visible**:

| Use the inline form (B) when… | Use the explicit form (C) when… |
|---|---|
| Serialization is a one-liner that can't fail interestingly | You need to **inspect, log, or measure** the bytes (e.g. record a `sample_serialized_value`, as Demo 02D does) |
| The payload is simple JSON | You use a **serializer object** — Avro, Protobuf — that needs a `SerializationContext` and may hit Schema Registry (§16.2) |
| You want the shortest readable loop | Serialization can **raise**, and you want to `try/except` around just that step to skip or dead-letter a bad record without aborting the batch |
| | You're **teaching or reviewing** the code and want the object → bytes boundary impossible to miss |

**Why Demo 02D exists at all:** its purpose is pedagogical. The assignment wants
you to see that **validation and serialization are two different steps** —
Pydantic guarantees the object is *meaningful*, serialization decides how it
*travels*. Inlining the call hides that boundary; naming `value_bytes` makes it a
thing you can point at.

**In production**, the explicit form tends to win as soon as serialization stops
being trivial — which happens the moment you adopt Avro:

```python
# with Avro, "serialization" is a stateful object that may call out to a registry
value_bytes = avro_serializer(event, SerializationContext(topic, MessageField.VALUE))
producer.produce(topic, key=event_key(event), value=value_bytes, callback=cb)
```

Here the inline form would be genuinely unpleasant, and error handling around
just the serializer would be impossible. **Pick C when serialization is a step
with its own failure modes; pick B when it is punctuation.**

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

### Why send *random* data through Kafka at all?

**Short answer: yes, this is a test harness — but "random" is doing something
more specific than it sounds, and your ML seeding intuition transfers exactly.**

#### The data isn't random because randomness is useful — it's *synthetic*

There is no real ride-hailing company feeding this course a live event stream. To
exercise a producer you need *some* payload, so the demos fabricate plausible
trip events. Synthetic data is the norm for this because it is realistic in
shape, free, and carries no privacy or PII concerns. In production, this
generator is exactly the piece you delete — real events arrive from your
application, and nothing about the producer code changes.

Note also how *unrandom* the "random" data actually is
([`demo02_producer_common.py:92-106`](../handouts/demo02_producer_common.py#L92-L106)):
`event_type` cycles deterministically through the four lifecycle stages,
`trip_id` increments every four events, `zone` cycles north/south/west, and
timestamps advance one second per index. **Only two fields draw from the RNG at
all** — `driver_id` and `fare`. The stream is a structured simulation with a
little jitter, not noise.

#### The seed is doing the same job it does in ML — controlling a nuisance variable

Your ML instinct is precisely right, just pointed at a different experiment:

| | ML training | This benchmark |
|---|---|---|
| **What you're measuring** | does architecture A beat B? | does async beat sync-style? |
| **Nuisance variable** | weight init, shuffling, dropout | the payload itself |
| **Seed's job** | so a score difference reflects the *architecture*, not luckier initialization | so a timing difference reflects the *strategy*, not different data |

Without the seed, the two strategies would send different bytes. Message size
directly affects throughput — a batch of `trip_completed` events (which carry a
`fare`) is larger than a batch of `trip_requested` events (which don't). If async
happened to draw more compact payloads, part of its advantage would be an
artifact of the data. **Seeding makes the payload a constant so the strategy is
the only variable.**

This is also why §13.1 warns that the `rng` call order must never change: reorder
the two draws and the sequences diverge, silently breaking the comparison.

#### Where the reproducibility genuinely pays off

1. **Fair A/B comparison** — the benchmark's whole claim depends on it.
2. **Credential-free testing** — `test_producer_logic.py` asserts that the same
   seed produces byte-identical serialized events, so control-flow bugs are
   caught with no cluster and no cost.
3. **Debuggability** — "the failure happens at message 1,447" is only a
   meaningful statement if message 1,447 is the same message every run.

**In production you would not seed anything** — you'd send real events as they
occur. Determinism is a property of the *measurement harness*, not of Kafka or of
production producers. It belongs to the same family of habits as fixing a random
seed before reporting a model score.

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

### What "idempotent" means here

**Your algebra instinct is nearly right — one substitution away.** `AB = BA` is
*commutativity*. **Idempotence** in linear algebra is:

$$A^2 = A$$

A projection matrix is the classic example: project a vector onto a plane, and
projecting *again* changes nothing. **Applying the operation twice equals
applying it once.** That is exactly — not merely analogously — the Kafka meaning:

$$f(f(x)) = f(x)$$

**Processing the same message twice leaves the system in the same state as
processing it once.**

#### Why you are forced to care

Because §14.3's ordering rule guarantees duplicates will eventually happen:

```
poll → decode → validate → process → commit
                                  ↑
                          crash HERE and the message
                          was processed but never committed
                          → next run redelivers it
```

That is the meaning of **at-least-once**: every message arrives *at least* once,
possibly more. You cannot engineer the duplicates away — the crash window between
"processed" and "committed" is irreducible. So you make duplicates *harmless*.

#### The distinction in practice

| Idempotent ✓ | Not idempotent ✗ |
|---|---|
| `UPSERT INTO trips VALUES (trip_id, …)` — same row, same result | `UPDATE trips SET count = count + 1` — runs twice, counts twice |
| `SET status = 'completed'` | `INSERT INTO events …` without a unique key — duplicate rows |
| `redis.SADD(key, trip_id)` — sets ignore re-adds | `list.append(record)` — grows every time |
| Writing a file at a deterministic path | Sending a confirmation email; charging a card |

The pattern: **assignment and set-insertion are idempotent; accumulation and
side-effects are not.**

#### Three ways to get it

1. **Key your writes by a natural ID.** Upserting on `trip_id` is idempotent by
   construction — the most common and cheapest fix.
2. **Deduplicate on `(topic, partition, offset)`.** That triple is unique
   forever (§6), so a "processed offsets" table lets you skip replays. Useful
   when the work genuinely cannot be made idempotent.
3. **Make the side-effect conditional.** Check "did I already send this?" before
   sending — effectively option 2 for external systems.

> **Related but different — the producer's `enable.idempotence` setting.** That
> config solves the *other* end: it stops the **producer** from writing the same
> record twice when it retries after an ambiguous network failure, by tagging
> records with a producer ID and sequence number the broker can deduplicate.
> Useful, but it protects the write path only. It does nothing about a **consumer**
> reprocessing a record after a crash — that remains your application's problem,
> and idempotent processing is the answer.

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
