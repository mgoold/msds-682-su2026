# Kafka Notes

A reader-friendly walkthrough of the core Kafka concepts, **ordered by conceptual
dependency** — each section builds on the previous one. Where possible, sections
point to concrete lines in the demo scripts under
[`../msds682-demos/`](../msds682-demos/).

> **How to read this:** Start at the top. Each concept assumes only the ones
> above it. A companion [`glossary.md`](glossary.md) defines every term precisely.

---

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
  (consume from topic A, produce to topic B).

**What makes a script a producer is the client object it creates, not its genre.**

- Not a producer — an *admin* client that only manages the cluster:
  [`demo01_create_topic.py:8`](../msds682-demos/demo01_create_topic.py#L8) imports
  `AdminClient`; [`:125`](../msds682-demos/demo01_create_topic.py#L125) instantiates it.
  It creates topics but never sends a message.
- A producer — the moment this line runs:
  [`demo02a_confluent_sync_style_producer.py:31`](../msds682-demos/demo02a_confluent_sync_style_producer.py#L31)
  `producer = Producer(config)`. The role is realized in
  [`:39-44`](../msds682-demos/demo02a_confluent_sync_style_producer.py#L39-L44)
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

A topic is the level at which you set **policy** and **route**:

- Partition count, replication factor, and cleanup policy are chosen at creation:
  [`demo01_create_topic.py:74-79`](../msds682-demos/demo01_create_topic.py#L74-L79).
- Producers route by topic name:
  [`demo02a_...py:39`](../msds682-demos/demo02a_confluent_sync_style_producer.py#L39)
  (`topic=TOPIC_NAME`).

---

## 3. Partitions: the real storage unit

A **partition** is the unit that actually stores data. Each partition is an
**append-only log**, kept as **segment files on a broker's local disk** (and
replicated to other brokers — see §7).

Key facts:

- A topic owns **many** partitions; a partition belongs to **exactly one** topic
  and is never shared. Its identity is the pair `(topic, partition_index)`, and the
  topic name is even part of its on-disk log directory
  (e.g. `msds682.demo01.trip-events.v1-0`). Topics reuse partition *numbers*
  (every topic has a partition 0), but those are distinct logs.
- Partition count is a **deliberate, semi-static** choice
  ([`demo01_create_topic.py:114`](../msds682-demos/demo01_create_topic.py#L114),
  `--partitions` default 3) — **not** an auto-scaling knob. Kafka scales by
  spreading existing partitions across more brokers, not by minting new ones.
- A partition is a **first-class storage unit**, but **not the same *kind*** as an
  S3 bucket. A bucket is an unordered, mutable, random-access object store; a
  partition is an **ordered, append-only, immutable log** read sequentially.
  Closest mapping: **topic ≈ bucket-like container, segment file ≈ object** (and
  under tiered storage, old segments literally become objects in a bucket).

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
[`demo02_producer_common.py:114-121`](../msds682-demos/demo02_producer_common.py#L114-L121):
`event_key()` and `serialize_event()` both `.encode()` to bytes. Kafka treats those
bytes as **opaque** — giving them meaning (JSON, Avro) is the application's job.

| Field | Who sets it | When |
|---|---|---|
| `key`, `value`, `headers` | **You** → bytes | at `produce()` |
| `topic` | You (routing) | at `produce()` |
| **partition** | **Producer client** (partitioner), unless you specify | client-side |
| **timestamp** | Producer client stamps; broker may overwrite | client-side / append |
| **offset** | **Broker only** | at log append |
| `callback` | You, but **local-only**, never transmitted | fires after ack |

The callback can only report `partition` and `offset` *after* delivery
([`demo02_producer_common.py:35-47`](../msds682-demos/demo02_producer_common.py#L35-L47))
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
[`demo02_producer_common.py:114-116`](../msds682-demos/demo02_producer_common.py#L114-L116)
keys on `trip_id` so *one trip's whole lifecycle stays on the same partition*.

This is exactly **why partition count is kept stable**: if `num_partitions`
changed, `hash(key) % N` would send the same key to a *different* partition,
breaking per-key ordering. So Kafka only lets you **increase** partitions (a
deliberate admin act), never shuffle them dynamically.

---

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
   [`demo02_producer_common.py:39-47`](../msds682-demos/demo02_producer_common.py#L39-L47).
2. **A consumer's bookmark** (its *position* / *committed offset*) — a per-consumer
   cursor holding *which offset it will read next*.

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

Your demo models this perfectly. The `event_type` field
([`demo02_producer_common.py:21`](../msds682-demos/demo02_producer_common.py#L21))
lists the life stages of one trip — `trip_requested → driver_matched →
trip_started → trip_completed`. Each stage is a **new immutable record keyed on the
same `trip_id`**, kept in order on the same partition. The "current state" is the
**latest record for that key**.

Two ways to handle updates, chosen via `cleanup.policy`
([`demo01_create_topic.py:78`](../msds682-demos/demo01_create_topic.py#L78)):

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
([`demo01_create_topic.py:115`](../msds682-demos/demo01_create_topic.py#L115),
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

## Demo file map

| File | Role in this story |
|---|---|
| [`demo00_environment_check.py`](../msds682-demos/demo00_environment_check.py) | Environment / connectivity check |
| [`demo01_create_topic.py`](../msds682-demos/demo01_create_topic.py) | **Admin client** — creates a topic (partitions, RF, cleanup policy). *Not* a producer. |
| [`demo01_environment_check.py`](../msds682-demos/demo01_environment_check.py) | Environment check variant |
| [`demo02_producer_common.py`](../msds682-demos/demo02_producer_common.py) | Shared producer helpers: config, `TripEvent` model, key/value serialization, delivery tracking |
| [`demo02a_confluent_sync_style_producer.py`](../msds682-demos/demo02a_confluent_sync_style_producer.py) | **Producer** — sync-style (flush after each message) |
| [`demo02b_confluent_async_producer.py`](../msds682-demos/demo02b_confluent_async_producer.py) | Producer — async style |
| [`demo02c_confluent_async_sync_compare.py`](../msds682-demos/demo02c_confluent_async_sync_compare.py) | Producer — sync vs async comparison |
| [`demo02d_confluent_serialization_producer.py`](../msds682-demos/demo02d_confluent_serialization_producer.py) | Producer — serialization focus |

See [`glossary.md`](glossary.md) for precise definitions of every term above.
