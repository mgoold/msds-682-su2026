# Kafka Glossary

Definitions of the Kafka terms and concepts used in this session and the
[`../msds682-demos/`](../msds682-demos/) and [`../handouts/`](../handouts/) code
(Demo 00 through Demo 06). Terms **explicitly asked about in this session** are
marked ⭐. Cross-references link related entries. For the narrative version, see
[`kafka_notes.md`](kafka_notes.md).

---

### `acks` (acknowledgements)
A **producer** setting controlling how many replicas must confirm a write before
it's considered delivered. `acks=all` (a.k.a. `acks=-1`) waits for **all in-sync
replicas** → strongest durability. Pairs with [min.insync.replicas](#mininsyncreplicas)
to define the durability-vs-availability tradeoff. See [ISR](#isr-in-sync-replicas),
[acks & durability](#durability).

### `AIOProducer` / `AIOConsumer` / native async clients
The **asyncio-native** confluent-kafka clients: `await`-able instead of
callback-based. `await producer.produce(...)` returns a delivery **future**
you `await` yourself — the async analogue of the [callback](#callback-delivery-callback--delivery-report).
Used standalone in
[`demo03d_confluent_asyncio_produce_consume.py`](../handouts/demo03d_confluent_asyncio_produce_consume.py)
and [`demo04d_asyncio_avro_roundtrip.py`](../handouts/demo04d_asyncio_avro_roundtrip.py),
and inside a FastAPI [lifespan](#lifespan-fastapi) in
[`demo05_kafka.py`](../handouts/demo05_kafka.py), where it protects FastAPI's
own shared event loop from being blocked by a plain [Producer](#producer-client-).
Paired with `AsyncSchemaRegistryClient`/`AsyncAvroSerializer`, the awaitable
counterparts of [Schema Registry](#schema-registry)'s ordinary Avro client
from Demo 04. See [Kafka Notes §19](kafka_notes.md#19-native-async-kafka-clients-inside-a-running-service-demo-05c05d).

### Admin client
A Kafka client (`AdminClient`) used for **cluster management** — creating topics,
listing metadata — **not** for producing or consuming. Example:
[`demo01_create_topic.py:125`](../msds682-demos/demo01_create_topic.py#L125).
Contrast with [Producer](#producer-), [Consumer](#consumer-).

### Append-only log
The data structure a [partition](#partition-) is. Records are only ever **added to
the end**; existing records are never edited or reordered. Basis of
[immutability](#immutability-) and [offsets](#offset-).

### At-least-once / idempotent processing
Kafka's default consumer guarantee: every record is processed **at least
once**, possibly more. It follows directly from the `poll → process → commit`
ordering ([Consumer offset](#consumer-offset-position--committed-offset-)) — a
crash between "processed" and "committed" causes redelivery. Because
duplicates cannot be engineered away, processing should be **idempotent**:
applying it twice leaves the system in the same state as applying it once
(`f(f(x)) = f(x)`) — e.g. an upsert keyed by ID, not an `UPDATE ... +1`. Demo 06's
[stream processor](#stream-processor-consume--validate--derive--produce--output-ack--commit)
extends this across **two** topics: it bakes the input's own
`(topic, partition, offset)` into the derived event's key, so a duplicate
caused by a crash between the output acknowledgement and the input commit is
at least *observable and traceable*, even though it is not prevented. Contrast
with the **producer**-side `enable.idempotence`, which solves a different
problem (duplicate writes from a retried send), not consumer reprocessing.

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

### Connector → see [Kafka Connect](#kafka-connect-connector--worker--task--converter)

### Consumer ⭐
An application that **reads** records from a topic. Not a "Kafka asset" — it's an
external client app you write, defined by direction of data flow (reads *out of*
Kafka). Tracks its own [consumer offset](#consumer-offset-position--committed-offset-).
Contrast with [Producer](#producer-).

### Consumer group
The identity (`group.id`) that owns one set of [consumer offsets](#consumer-offset-position--committed-offset-)
for a topic. Within one group, each partition is read by **exactly one**
member — adding members spreads partitions, but extras beyond the partition
count sit idle. **Different groups are fully independent**: each has its own
committed offsets and each sees *every* record, which is how one topic feeds
several applications at once. Demo 06 uses this independence as a **safety
boundary**, not just for parallelism: the inspection consumer
([`demo06b`](../handouts/demo06b_confluent_source_consumer.py)), the real
processor ([`demo06c`](../handouts/demo06c_confluent_stream_processor.py)),
and the forced-replay pass ([`demo06d`](../handouts/demo06d_confluent_resume_replay.py))
each use a **different** group ID precisely so one can never disturb another's
progress. See [Replay](#replay), [`OFFSET_BEGINNING`](#offset_beginning-forced-replay).

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

### Converter → see [Kafka Connect](#kafka-connect-connector--worker--task--converter)

### Delivery mode (`local` / `broker_acknowledged`)
Demo 05's own labels for **how far** an event actually got before an HTTP
response was returned — deliberately distinct from HTTP acceptance:
`"local"` means the credential-free teaching publisher accepted it in memory,
no Kafka involved at all; `"broker_acknowledged"` means the Kafka
[producer](#producer-client-)'s delivery future genuinely resolved — the same
[`acks`](#acks-acknowledgements) guarantee from §8, now surfaced in an API
response body. Neither claims that a downstream consumer has finished
processing the event — see [HTTP status code](#http-status-code-fastapi).

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

### FastAPI (application, path operation, request/response models)
The web framework Demo 05 puts **in front of** a Kafka producer. An
**application** is the one `FastAPI(...)` object; a **path operation** is a
Python function decorated `@app.get(...)`/`@app.post(...)` handling one URL +
method; a **request model** is the Pydantic model FastAPI parses and validates
an incoming body against (`CreateTripRequest`); a **response model** is the
model FastAPI validates the *return value* against and documents. FastAPI
never talks to Kafka itself — it validates HTTP input, then hands a plain
Python object to whichever [producer](#producer-client-) the route was given.
See [`demo05_app.py`](../handouts/demo05_app.py),
[Lifespan (FastAPI)](#lifespan-fastapi),
[OpenAPI / Swagger UI / TestClient](#openapi--swagger-ui--testclient),
[HTTP status code](#http-status-code-fastapi).

### Follower → see [Leader / follower](#leader--follower-)

### `group.protocol` (classic vs. incremental)
A [consumer](#consumer-) config key naming which **rebalance protocol** the
client uses. `"classic"` is used throughout this course whenever a script
supplies its own `on_assign` callback and calls `consumer.assign(partitions)`
directly (e.g. to observe or override an assignment, as in
[Replay](#replay)) — the newer **incremental** rebalance protocol (KIP-848)
completes assignment on the broker side and expects
`consumer.incremental_assign()` instead, so it is incompatible with this
course's custom-`on_assign` pattern. First appears in
[`demo03d`](../handouts/demo03d_confluent_asyncio_produce_consume.py) and is
set explicitly in every consumer from Demo 04 onward, including
[`demo06b`](../handouts/demo06b_confluent_source_consumer.py) and
[`demo06c`](../handouts/demo06c_confluent_stream_processor.py).

### Headers
Optional key→bytes metadata pairs attached to a record. Serialized like key/value
if used. Demo 04C tags records with a run-id header to filter a shared topic;
Demo 06C tags each derived record with its source `(topic, partition, offset)`
as human-inspectable evidence, alongside — not instead of — encoding that same
coordinate in the record's own [key](#key-). See [Record](#record--message-).

### HTTP status code (FastAPI)
What a [FastAPI](#fastapi-application-path-operation-requestresponse-models)
route returns to distinguish **acceptance** from **acknowledgement** from
**processing**, three different claims Demo 05 is careful never to conflate:
`202 Accepted` means the request was accepted by the configured
[delivery mode](#delivery-mode-local--broker_acknowledged) — not that any
downstream consumer has finished; `422` means FastAPI's own
[request model](#fastapi-application-path-operation-requestresponse-models)
rejected the body before any application code ran; `503` means the
[producer](#producer-client-) or Schema Registry could not confirm acceptance
in time. Only an independent consumer's own report — never the HTTP response —
proves processing completed.

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

### Kafka Connect (connector / worker / task / converter)
A managed integration runtime that moves data between an external system and a
Kafka topic **without a custom producer**. Four vocabulary words: a
**connector** is a configured job ("read from X, write Avro to topic Y"); a
**worker** is the running process that executes connectors; a **task** is one
parallel unit of work a connector splits into (`tasks.max`); a **converter**
translates the external system's data into a Kafka record (here: Avro, via
[Schema Registry](#schema-registry)). A source connector still produces an
ordinary `(topic, partition, offset, key, value)` — only *who* calls the
equivalent of [`produce()`](#produce-) differs (the worker, not your Python
process). Demo 06A/06B use the managed **Datagen Source** connector (`ORDERS`
quickstart); [`demo06_seed_source.py`](../handouts/demo06_seed_source.py) is a
Python fallback for accounts that cannot create one, honestly self-labeled as
`"not Kafka Connect"` in its own report. See
[Kafka Notes §20](kafka_notes.md#20-kafka-connect-getting-data-in-without-a-producer-demo-06a06b).

### `KafkaException`
The confluent-kafka library's general-purpose error type, carrying a Kafka
error code. Raised by a blocked `.result()` on a failed admin operation
([`create_topics()`](#admin-client), §11), and raised explicitly by Demo 06C's
processor after inspecting a [commit](#consumer-offset-position--committed-offset-)'s
returned partition list for a per-partition error — because a synchronous
commit can partially fail across multiple partitions **without raising on its
own**, so silence is not proof of success. See
[Kafka Notes §21.4](kafka_notes.md#214-verifying-a-commit-actually-landed--not-just-that-it-didnt-raise).

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

### Lifespan (FastAPI)
An `async` context manager wrapping the entire time a
[FastAPI](#fastapi-application-path-operation-requestresponse-models)
application serves requests: code before its `yield` runs once at process
startup, code after runs once at shutdown. Demo 05 constructs its **one**
[producer](#producer-client-) here — never inside a route — so the expensive
setup cost (TLS handshake, SASL auth, a metadata fetch) is paid once per
process, matching the client's lifetime to the application's, the same way a
plain script constructs its producer once before its produce loop. See
[`demo05_app.py`](../handouts/demo05_app.py).

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

### `OFFSET_BEGINNING` (forced replay)
A sentinel value assigned to a `TopicPartition.offset` inside an `on_assign`
callback, then passed to `consumer.assign(partitions)`, to **explicitly**
override where consumption starts — a real reset command, not a fallback.
Contrast with [`auto.offset.reset`](#consumer-offset-position--committed-offset-),
which only applies when a [consumer group](#consumer-group) has **no**
committed position at all; a group with prior commits ignores it entirely, so
reusing a group ID would silently turn an intended replay into an ordinary
resume. Demo 06D's `AssignmentTracker.force_beginning` rewrites every assigned
partition's offset this way on **every** run of its replay group, regardless
of what that group has or has not committed before. See
[Replay](#replay), [Kafka Notes §22.2](kafka_notes.md#222-autooffsetreset-is-a-fallback-offset_beginning-is-a-command).

### OpenAPI / Swagger UI / `TestClient`
Three ways of looking at the same
[FastAPI](#fastapi-application-path-operation-requestresponse-models)
application. **OpenAPI** is the machine-readable schema FastAPI generates
automatically from a route's request/response models, served at
`/openapi.json`. **Swagger UI** (`/docs`) renders that same document as an
interactive page a human can browse and submit requests from — used in Demo
05B/05D's intentionally interactive services. **`TestClient`** runs requests
through the identical pipeline — routing, validation, [lifespan](#lifespan-fastapi) —
via plain function calls instead of a real socket, which is how Demo 05A/05C
exercise the app as a bounded, automated script rather than an interactive one.

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

### Schema Registry
A **separate service** from the Kafka brokers — its own URL and its own
credentials (`SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY/SECRET`) — that
stores Avro schema versions under named **subjects** (conventionally
`<topic>-value`) and enforces **compatibility** rules between versions. An
Avro-serialized [value](#value-) carries only a 5-byte header (a magic byte
plus a 32-bit schema ID) — the Registry, not the message, is where the actual
field names and types live. Demo 04 introduces the synchronous client
(`SchemaRegistryClient`/`AvroSerializer`); Demo 05 adds the awaitable
`AsyncSchemaRegistryClient`/`AsyncAvroSerializer` for use inside a
[FastAPI lifespan](#lifespan-fastapi) — see
[`AIOProducer` / native async clients](#aioproducer--aioconsumer--native-async-clients).

### Stream processor (consume → validate → derive → produce → output ack → commit)
A consumer and a producer combined into one loop: each input record causes a
**new**, computed fact to be published to a second topic, before the input's
own progress is recorded. Demo 06C's exact sequence:
`poll input → Avro deserialize → Pydantic validate → derive a new event →
Avro serialize → produce to the derived topic → wait for output broker
acknowledgement → commit the input offset`. The output-then-commit ordering is
the same "process, then commit" idea from
[Consumer offset](#consumer-offset-position--committed-offset-)/
[At-least-once processing](#at-least-once--idempotent-processing), just with
"process" expanded to include a second topic's own delivery guarantee before
the first topic's progress may advance. See
[`demo06c_confluent_stream_processor.py`](../handouts/demo06c_confluent_stream_processor.py),
[Kafka Notes §21](kafka_notes.md#21-stream-processing-deriving-a-new-event-demo-06c).

### Task → see [Kafka Connect](#kafka-connect-connector--worker--task--converter)

### Threading (background worker thread)
A `threading.Thread` + `threading.Event` pattern for coordinating a **plain,
blocking** [consumer](#consumer-) that runs independently of an async
application's event loop — used by Demo 05C's verification consumer, which
has no event loop of its own to protect and so gains nothing from the async
clients ([`AIOProducer`](#aioproducer--aioconsumer--native-async-clients))
FastAPI's own routes use. It is the **same** "signal, not sleep" discipline
as the `asyncio.Event` coordination in Demo 03D/05C's producer side, just
expressed with a different concurrency primitive because the consumer here is
a thread, not a coroutine: `ready.wait()` blocks the main thread until real
partition assignment is confirmed, exactly as `await assignment_ready.wait()`
does for two cooperating asyncio tasks.

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

### Uvicorn
The **ASGI server** that actually accepts TCP connections and speaks HTTP for
a [FastAPI](#fastapi-application-path-operation-requestresponse-models) app —
FastAPI itself only defines routes and models; Uvicorn is what makes it a
real, reachable process. `demo05b`/`demo05d`'s interactive services block on
`uvicorn.run(...)` until interrupted; `demo05a`/`demo05c` never invoke it at
all, running the same app through
[`TestClient`](#openapi--swagger-ui--testclient) instead.

### Worker → see [Kafka Connect](#kafka-connect-connector--worker--task--converter)
