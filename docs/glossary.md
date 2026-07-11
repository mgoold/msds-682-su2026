# Kafka Glossary

Definitions of the Kafka terms and concepts used in this session and the
[`../msds682-demos/`](../msds682-demos/) code. Terms **explicitly asked about in
this session** are marked ⭐. Cross-references link related entries. For the
narrative version, see [`kafka_notes.md`](kafka_notes.md).

---

### `acks` (acknowledgements)
A **producer** setting controlling how many replicas must confirm a write before
it's considered delivered. `acks=all` (a.k.a. `acks=-1`) waits for **all in-sync
replicas** → strongest durability. Pairs with [min.insync.replicas](#mininsyncreplicas)
to define the durability-vs-availability tradeoff. See [ISR](#isr-in-sync-replicas),
[acks & durability](#durability).

### Admin client
A Kafka client (`AdminClient`) used for **cluster management** — creating topics,
listing metadata — **not** for producing or consuming. Example:
[`demo01_create_topic.py:125`](../msds682-demos/demo01_create_topic.py#L125).
Contrast with [Producer](#producer-), [Consumer](#consumer-).

### Append-only log
The data structure a [partition](#partition-) is. Records are only ever **added to
the end**; existing records are never edited or reordered. Basis of
[immutability](#immutability-) and [offsets](#offset-).

### Batch → see [Record batch](#record-batch)

### Bootstrap servers
The initial broker address(es) a client connects to in order to discover the rest
of the cluster and its metadata. Configured via `BOOTSTRAP_SERVERS`:
[`demo02_producer_common.py:61`](../msds682-demos/demo02_producer_common.py#L61).

### Broker ⭐
A Kafka **server**. Brokers receive, durably store, and serve records. They host
[partition](#partition-) replicas — a single broker holds replicas from **many
partitions of many topics** (shared infrastructure), leading some and following
others. You need **at least [replication_factor](#replication-factor-) brokers**
(never fewer). See [Cluster](#cluster), [Leader](#leader--follower-).

### Callback (delivery callback / delivery report)
A **local** function the producer client invokes on your machine *after* the broker
acknowledges (or fails) a record. It is **never transmitted** to Kafka. Reports the
final `partition` and `offset`. Example:
[`demo02_producer_common.py:35-47`](../msds682-demos/demo02_producer_common.py#L35-L47).

### `cleanup.policy` ⭐
A per-topic setting for how a partition's log is reclaimed:
- **`delete`** — expire old [segments](#segment) by retention (time/size). Keeps
  full history (event-sourcing style).
- **`compact`** — keep only the **latest record per key**; garbage-collect older
  ones (table/changelog style).

Set at [`demo01_create_topic.py:78`](../msds682-demos/demo01_create_topic.py#L78).
See [Immutability](#immutability-), [Tombstone](#tombstone), [Compaction](#compaction).

### Cluster
The full set of [brokers](#broker-) working together, plus their coordination
(KRaft controller quorum, or ZooKeeper in older versions). Holds many topics.

### Compaction
The background process behind `cleanup.policy=compact`: rewrites [segments](#segment)
to keep only the most recent record per key, physically dropping superseded
records. Still append-only — it never edits a record in place.

### Committed offset → see [Consumer offset](#consumer-offset-position--committed-offset-)

### Consumer ⭐
An application that **reads** records from a topic. Not a "Kafka asset" — it's an
external client app you write, defined by direction of data flow (reads *out of*
Kafka). Tracks its own [consumer offset](#consumer-offset-position--committed-offset-).
Contrast with [Producer](#producer-).

### Consumer offset (position / committed offset) ⭐
A **per-consumer bookmark**: the offset value a consumer will read **next** in a
given partition. **Not** a separate numbering of records — it's a *cursor into* the
single shared [offset](#offset-) coordinate system. Stored by Kafka (internal
`__consumer_offsets` topic) when the consumer **commits**. Enables independent
reading, resume-after-failure, and [replay](#replay). *Book analogy: the bookmark,
vs. the printed page numbers (record offsets).*

### Controller
The cluster role that manages metadata and coordinates failure handling — detects a
dead broker and triggers [leader election](#leader-election). Contrast with
per-partition [leaders](#leader--follower-).

### Durability
The guarantee that acknowledged records survive failures. Governed by three knobs
together: [replication_factor](#replication-factor-) (how many copies),
[min.insync.replicas](#mininsyncreplicas) (how many must be caught up), and
[`acks`](#acks-acknowledgements) (how many must confirm). `RF=3` + `acks=all` means
a confirmed write exists on multiple brokers and survives a broker loss.

### Event ⭐
The **business meaning** of a record — your domain-level fact. In the demos, a
`TripEvent` ([`demo02_producer_common.py:18-26`](../msds682-demos/demo02_producer_common.py#L18-L26))
becomes the [value](#value-) of a [record](#record--message-). Same physical thing
as "message"/"record", viewed at the application altitude. See
[Message vs Record vs Event](#message-vs-record-vs-event-).

### Follower → see [Leader / follower](#leader--follower-)

### Headers
Optional key→bytes metadata pairs attached to a record. Serialized like key/value
if used. The demos don't set any. See [Record](#record--message-).

### Immutability ⭐
Records in a partition **cannot be changed** once written — bytes and
[offset](#offset-) are fixed. "Updates" are handled by **appending new records**
(see [cleanup.policy](#cleanuppolicy-), [Tombstone](#tombstone)). Immutability is
what lets multiple consumers independently read and [replay](#replay) the same log.

### ISR (in-sync replicas) ⭐
The set of a partition's [replicas](#replica) currently **caught up** with the
[leader](#leader--follower-). A follower that falls behind is dropped from the ISR
until it catches up. `acks=all` waits for all ISR members;
[min.insync.replicas](#mininsyncreplicas) sets the floor below which writes are
refused. Shrinks when a broker dies; recovers when the broker catches up and
rejoins.

### Key ⭐
One of the two fields **you serialize to bytes** in a record. Used by the
[partitioner](#partitioner) to choose a partition (`hash(key) % num_partitions`), so
all records with the same key land on the **same partition, in order**. Example:
[`demo02_producer_common.py:114-116`](../msds682-demos/demo02_producer_common.py#L114-L116)
keys on `trip_id`. See [Value](#value-), [Partitioning](#partitioner).

### KRaft / ZooKeeper
The cluster coordination layer. **KRaft** (modern, ZooKeeper-free) or **ZooKeeper**
(older) provides the [controller](#controller) and metadata consensus.

### Leader / follower ⭐
The two roles a partition's [replicas](#replica) take:
- **Leader** — the single replica that handles **all reads and writes** for that
  partition and **assigns [offsets](#offset-)**. Clients talk only to it.
- **Follower** — a replica that continuously **fetches** from the leader to stay in
  sync and stand by; doesn't serve clients by default.

Leadership is **per-partition** (a broker leads some, follows others), which
balances load. See [ISR](#isr-in-sync-replicas), [Leader election](#leader-election).

### Leader election
The [controller](#controller) promoting an **in-sync [follower](#leader--follower-)**
to leader when the current leader's broker fails. No acknowledged data is lost
because ISR members were caught up. **Preferred-leader election** later restores the
original (balanced) leadership assignment.

### Log
Synonym for the [append-only](#append-only-log) sequence of records that *is* a
[partition](#partition-), stored physically as [segment](#segment) files.

### Message → see [Message vs Record vs Event](#message-vs-record-vs-event-)

### Message vs Record vs Event ⭐
Three names, same physical unit, different altitude:
- **Record** — the precise/technical term (the API `ProducerRecord`/`ConsumerRecord`).
- **Message** — the informal/conversational term.
- **Event** — the domain/business meaning (a `TripEvent`).

Only the [key](#key-) and [value](#value-) are serialized; the record is **assembled
by the producer client**, not built by the broker — the broker only finalizes the
[offset](#offset-) (and possibly [timestamp](#timestamp)).

### `min.insync.replicas`
The minimum [ISR](#isr-in-sync-replicas) size required for an `acks=all` write to
succeed. If the ISR shrinks below it (e.g. after a broker dies), affected partitions
go **read-only** to protect [durability](#durability). The crux of the
**durability-vs-availability** tradeoff.

### Offset ⭐
A **monotonically increasing integer identifying a record's position within a
partition** — its sequence number in the partition's log. **Per-partition**
(a record's true address is `(topic, partition, offset)`), **broker-assigned** at
append time, **never resets or reuses**. Plays two roles: the record's fixed
**address**, and (as a [consumer offset](#consumer-offset-position--committed-offset-))
a per-consumer **bookmark**. Reported in
[`demo02_producer_common.py:39-47`](../msds682-demos/demo02_producer_common.py#L39-L47).

### Partition ⭐
The **real storage unit** of Kafka: an [append-only log](#append-only-log) of
records, stored as [segment](#segment) files on broker disk and
[replicated](#replica). Belongs to **exactly one** [topic](#topic-) (identity =
`(topic, partition_index)`; never shared). The unit of **ordering** and
**parallelism**. Count is a deliberate, semi-static choice
([`demo01_create_topic.py:114`](../msds682-demos/demo01_create_topic.py#L114)), not
auto-scaling. See [Offset](#offset-), [Partitioner](#partitioner), [Leader](#leader--follower-).

### Partitioner
The producer-client logic that picks a partition for a record. Default:
`hash(key) % num_partitions` when a [key](#key-) is present; a sticky/round-robin
strategy when the key is absent. Relies on recently refreshed broker
[metadata](#metadata) for the partition count.

### Producer ⭐
An application that **writes** records to a topic. Not a "Kafka asset" — an external
client app you write, defined by direction (writes *into* Kafka). A script *becomes*
a producer when it creates a `Producer` object
([`demo02a_...py:31`](../msds682-demos/demo02a_confluent_sync_style_producer.py#L31))
and calls `produce()`.

### Producer client ⭐
The **library object** (`Producer(config)`) embedded **inside** a producer app that
implements the Kafka protocol — computes the partition, stamps a timestamp, batches
records, manages the socket, retries, fires the [callback](#callback-delivery-callback--delivery-report).
Same thing as "the producer" at a narrower zoom; **not** a separate network tier
between your app and the broker. In `confluent-kafka` it wraps the C library
**librdkafka**.

### `produce()` ⭐
The producer method that hands the client the fields you own
(`topic`, `key`, `value`, optional `headers`/`partition`/`callback`). It does **not**
send a finished record — the client assembles and batches it. Example:
[`demo02a_...py:39-44`](../msds682-demos/demo02a_confluent_sync_style_producer.py#L39-L44).

### Record → see [Message vs Record vs Event](#message-vs-record-vs-event-)

### Record batch
A group of records the producer client bundles together for efficient
transmission/storage. On disk, a batch stores a base offset and per-record deltas.

### Replica
One physical copy of a [partition](#partition-)'s log on a [broker](#broker-). There
are [replication_factor](#replication-factor-) replicas per partition, each on a
**distinct** broker, one being the [leader](#leader--follower-) and the rest
followers.

### `replication_factor` ⭐
How many [replicas](#replica) each partition has — the **durability** setting. Set
at [`demo01_create_topic.py:115`](../msds682-demos/demo01_create_topic.py#L115)
(default 3). Requires **at least that many brokers** (each replica on a distinct
broker). Fixed per topic; you scale capacity by adding brokers, not by raising RF.

### Replay
Re-reading records a consumer has already processed by resetting its
[consumer offset](#consumer-offset-position--committed-offset-) backward (e.g. to 0).
Possible because records are [immutable](#immutability-) and not deleted on read.

### Retention
The policy (time/size) governing how long a partition keeps records under
`cleanup.policy=delete` before old [segments](#segment) are expired. Note: offsets
still don't reset when old records expire.

### Segment
A file on disk that stores a slice of a [partition](#partition-)'s log (with `.log`,
`.index`, `.timeindex` companions). The [active segment](#segment) is the one
currently being appended; it "rolls over" to a new segment by size/time. The unit of
[retention](#retention)/[compaction](#compaction). Under **tiered storage**, old
segments can be offloaded to object storage (S3/GCS) as objects.

### Serialization ⭐
Turning a value into the **bytes** Kafka stores. **Only the [key](#key-) and
[value](#value-)** are serialized by your code; Kafka treats the result as opaque.
Example: [`demo02_producer_common.py:119-121`](../msds682-demos/demo02_producer_common.py#L119-L121)
(`model_dump_json().encode()`). Schema-aware serialization (e.g. Avro) is explored in
[`demo02d_confluent_serialization_producer.py`](../msds682-demos/demo02d_confluent_serialization_producer.py).

### Tiered storage
An optional storage mode (KIP-405; used by managed clouds like Confluent Cloud)
where recent [segments](#segment) stay on broker disk and **older segments are
offloaded to object storage** (S3/GCS). Transparent — you still produce to a topic,
not a bucket. This is the only place an S3 bucket genuinely enters the picture, and
even then the mapping is **segment → object**, not topic → bucket.

### Timestamp
A record field set by the producer client at `produce()` time (`CreateTime`), or
overwritten by the broker on append (`LogAppendTime`), depending on the topic's
`message.timestamp.type`. Positional ordering still comes from the
[offset](#offset-), not the timestamp.

### Tombstone
A record with a key and a **`null` value**, signaling **deletion** of that key under
[compaction](#compaction). Deletion in Kafka is itself an appended fact —
consistent with [immutability](#immutability-).

### Topic ⭐
A **logical name** you route records to (e.g. `msds682.demo01.trip-events.v1`).
**Not** a single storage object (not an S3 bucket) and **not** pure namespace — a
name over **N [partitions](#partition-)**. The level at which you set policy
(partition count, RF, cleanup) and route (`produce(topic=...)`). Owns many
partitions; each partition belongs to exactly one topic.

### Value ⭐
The record field carrying the **payload** — one of the two things **you serialize to
bytes**. Kafka treats it as opaque bytes; the application defines its meaning
(JSON, Avro, …). Example:
[`demo02_producer_common.py:119-121`](../msds682-demos/demo02_producer_common.py#L119-L121).
See [Key](#key-), [Serialization](#serialization-).

### Under-replicated
A partition whose [ISR](#isr-in-sync-replicas) is smaller than its
[replication_factor](#replication-factor-) — typically after a broker fails. Still
available (has a leader) but with a reduced safety margin until the missing
[replica](#replica) catches up.
