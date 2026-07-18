# Lecture 4 Supplement: Consumer Groups, Data Contracts, and a Ridesharing Trip Streaming Example

- **Course:** MSDS 682 — Data Stream Processing
- **Placement:** Use after Lecture 3 consumer concepts and before or during Lecture 4 data contracts
- **Purpose:** Clarify partitions, consumers, consumer groups, offsets, validation, Avro, and Schema Registry through one end-to-end example.

> **Scope:** This is an optional architecture case study, not an implementation
> contract for Demo 04A–04D. The four demos use `TripEventV1`; the `ridesharing.*`
> topics below are conceptual designs for classroom discussion. Flink, Kafka
> Streams, stateful joins, windows, watermarks, and checkpoints are advanced
> extensions and are not part of the Summer 2026 implementation baseline.

---

## 1. How Lecture 3 connects to Lecture 4

Lecture 3 answers:

> **Who reads each Kafka record, and where does that application resume after a restart?**

Lecture 4 answers:

> **How do independently deployed producers and consumers agree on what the record means?**

The complete processing path is:

```text
Producer application
    -> validate application data
    -> serialize the event into bytes
    -> write the bytes to Kafka

Kafka
    -> store the record in one partition
    -> assign an offset
    -> retain and deliver the bytes

Consumer application
    -> receive the bytes
    -> deserialize the event
    -> validate application meaning
    -> process successfully
    -> commit the next offset
```

Lecture 3 focuses on the final two ideas: **partition assignment and progress**. Lecture 4 adds the missing contract: **schema, serialization, compatibility, and validation**.

---

## 2. The four Kafka objects students must separate

### Topic

A topic is a named event stream.

```text
ridesharing.trip-created.v1
```

### Partition

A partition is one ordered, append-only slice of a topic. Ordering and offsets exist inside a partition.

```text
ridesharing.trip-created.v1
    partition 0
    partition 1
    ...
    partition 49
```

### Consumer

A consumer is one running application instance that reads records.

A consumer can own:

- one partition;
- several partitions; or
- no partition temporarily.

A consumer is **not limited to one partition**.

### Consumer group

A consumer group is one logical consuming application identified by a shared `group.id`.

The group provides four real functions:

1. **Partition assignment** — Kafka distributes subscribed partitions across active group members.
2. **Load balancing** — members share the processing work.
3. **Progress ownership** — committed offsets belong to the group.
4. **Failover** — if one member stops, Kafka reassigns its partitions to surviving members.

A consumer group is much more than a label.

---

## 3. The most important consumer-group clarification

Suppose a topic has **1,000 partitions** and one consumer group has **10 consumers**.

Kafka does not assign only 10 partitions and ignore the remaining 990. It assigns all 1,000 partitions across the 10 consumers.

A simplified assignment might be:

```text
pricing group
    consumer 1  -> partitions   0-99
    consumer 2  -> partitions 100-199
    consumer 3  -> partitions 200-299
    ...
    consumer 10 -> partitions 900-999
```

Each consumer owns approximately 100 partitions. All 1,000 partitions are covered by the group.

The group processes the complete topic collectively:

```text
one consumer sees a subset
all consumers in the group together see the full stream
```

### Capacity rule

```text
non-idle consumers = min(group members, topic partitions)
maximum partition-level parallelism <= number of partitions
```

A group may have more active members than partitions, but the excess members
receive no partition assignment and remain idle. The partition count limits
useful parallelism; it does not reject extra group members.

If a topic has 50 partitions:

- 1 consumer can own all 50 partitions;
- 10 consumers can own about 5 partitions each;
- 50 consumers can own about 1 partition each;
- 60 consumers leave about 10 consumers idle.

More consumers increase parallelism only until the group reaches the partition count.

---

## 4. Consumers normally subscribe to a topic, not manually to one partition

The normal application code is:

```python
consumer.subscribe(["ridesharing.trip-created.v1"])
```

Kafka then assigns partitions to the members of that consumer group.

The application normally does not say:

```text
consumer A always reads partition 0
consumer B always reads partition 1
```

Kafka owns the assignment and may change it during a rebalance.

Direct partition assignment exists, but it is a specialized mode used for explicit control, replay utilities, migrations, or unusual processing designs. It is not the normal consumer-group pattern taught first.

---

## 5. One logical application should usually be one consumer group

A consumer group should represent one logical application whose members run the same code.

The three calculations can be modeled in two valid ways.

### Case A: one fare-estimation application

If duration and distance exist only to calculate fare, one group can run the complete pipeline:

```text
Consumer group: ridesharing-fare-estimation-v1

Every member runs:
    estimate duration
    -> estimate distance
    -> estimate fare
```

Kafka divides trips across the members. It does not divide the three business steps across different members.

### Case B: independently reusable calculations

If duration, distance, and pricing are separate products used by other systems, use separate groups:

```text
Topic: ridesharing.trip-created.v1

Consumer group: ridesharing-trip-duration-v1
    identical duration consumers

Consumer group: ridesharing-trip-distance-v1
    identical distance consumers

Consumer group: ridesharing-trip-pricing-v1
    identical pricing consumers
```

Each group receives the complete source stream independently, while members inside a group divide its partitions.

### Incorrect design

This design is incorrect:

```text
one group
    consumer A runs duration logic
    consumer B runs distance logic
    consumer C runs pricing logic
```

Kafka would give different partitions to the three members. Each calculation would process only part of the trip stream.

The direct rule is:

> **One group = one logical application. Every active member runs the same processing contract.**

---

## 6. Consumer-group progress and offsets

Kafka stores progress per:

```text
(group.id, topic, partition)
```

There is no single offset for the entire group.

Example:

```text
group.id = trip-pricing-v1

topic: ridesharing.trip-created.v1
    partition 0 -> committed offset 1250
    partition 1 -> committed offset 981
    partition 2 -> committed offset 1433
```

The committed offset means the **next record to read**.

If the group commits offset 1250 for partition 0, offsets 0 through 1249 are considered completed for that group.

### Failure and reassignment

Suppose pricing consumer 3 owns partitions 10 through 14 and then crashes.

Kafka rebalances the group:

```text
before failure
    consumer 3 -> partitions 10-14

after failure
    consumer 1 -> partitions 10-11
    consumer 2 -> partitions 12-14
```

The new owners resume from the pricing group’s committed offsets for those partitions.

The progress belongs to the group, not permanently to the failed consumer instance.

---

## 7. Auto commit and manual commit

### Auto commit

With automatic offset management, the client periodically commits stored positions.

It is simple, but the stored position can move ahead of slow application processing. A crash can cause unfinished work to be skipped from the application’s perspective.

### Manual commit

The safer teaching sequence is:

```text
poll
-> deserialize
-> validate
-> process successfully
-> commit the next offset
```

This supports at-least-once processing:

- committing too early can lose unfinished work;
- committing after success can repeat work after a crash;
- idempotent processing handles the possible duplicate.

For Lecture 3, this is the key sentence:

> **A fetched record is not completed work. Commit only after the application has completed it successfully.**

---

## 8. The Lecture 4 validation stack

The word **validation** refers to several different layers.

| Layer | Tool or system | Main responsibility |
|---|---|---|
| Application model | Pydantic | Python types, ranges, patterns, timezone awareness, and business rules |
| Wire schema | Avro | Cross-language record fields, wire types, unions, defaults, and writer/reader resolution |
| Schema service | Schema Registry | Schema storage, IDs, versions, subjects, and compatibility checks |
| Transport | Kafka | Key/value bytes, partitions, offsets, retention, and delivery |

### Pydantic

Pydantic answers:

```text
Is this valid data for this Python application?
```

Examples:

- `trip_id` is a nonempty string;
- latitude is within a legal range;
- longitude is within a legal range;
- event time includes a timezone;
- estimated distance is nonnegative;
- a completed trip has the required final fields.

Pydantic is useful before production and after deserialization.

### Avro

Avro answers:

```text
How is this structured record represented as compact bytes?
```

It defines:

- field names;
- field types;
- nullable unions;
- defaults;
- writer/reader schema resolution.

### Schema Registry

Schema Registry answers:

```text
Which schema version produced these bytes, and is the proposed new version compatible?
```

It stores schemas and governance metadata. It does not store the trip records themselves.

### Kafka

Kafka answers:

```text
Where are the bytes stored, who reads them, and where does each group resume?
```

Kafka does not understand the business meaning of `trip_id`, `distance`, or `price`.

---

## 9. End-to-end Ridesharing example: business requirement and assumptions

### Business requirement

A trip-request service receives real-time ride requests across the United States. For each trip, the system wants to estimate:

1. trip duration;
2. trip distance; and
3. trip fare.

A simplified fare formula is:

```text
estimated_fare = base_fare
               + estimated_distance * distance_rate
               + estimated_duration * time_rate
```

The final fare calculation may later use a machine-learning model, but the streaming architecture does not depend on whether the calculation is a formula or a model.

### Source topic

```text
ridesharing.trip-created.v1
```

For the classroom example, the topic has 50 partitions and each partition is described as representing one state.

```text
partition 0  -> Alabama
partition 1  -> Alaska
...
partition 4  -> California
...
partition 49 -> Wyoming
```

This mapping is intentionally simplified. A production system would need an explicit state-to-partition mapping or a custom partitioner. A normal Kafka hash does not automatically map one state to one partition.

### Producer path

```text
Trip request API
-> Pydantic validation
-> Avro serialization
-> Schema Registry schema ID
-> Kafka producer
-> ridesharing.trip-created.v1
```

The producer writes one validated `TripCreated` event for each new trip request.

---

## 10. First decide what the logical application is

The most important design question is:

> **Are duration, distance, and fare one application pipeline, or are they three independently useful applications?**

Both answers can be valid.

### One logical fare-estimation application

If the only business output is the estimated fare, one consumer group can run the complete calculation:

```text
read trip
-> estimate duration
-> estimate distance
-> estimate fare
```

Every member of this group runs the same complete logic for its assigned trips.

### Three independently reusable calculations

If duration and distance estimates must also be published and reused by other systems, they can become independent applications with separate consumer groups and output topics.

The rule remains:

> **Members inside one consumer group run the same logic. Different logical applications use different group IDs.**

---

## 11. Workflow Chart 1: Proposal 1 — one-stage fare-estimation group

**Teaching goal:** explain how one consumer group covers all 50 partitions while every member runs the same complete pipeline.

```text
Trip intake producer
        |
        v
+----------------------------------------------------------------+
| Topic: ridesharing.trip-created.v1                             |
| 50 partitions (simplified classroom model: 50 states)          |
+----------------------------------------------------------------+
        |
        v
+----------------------------------------------------------------+
| Consumer Group: ridesharing-fare-estimation-v1                 |
|                                                                |
| consumer 1  -> assigned subset of state partitions             |
| consumer 2  -> assigned subset of state partitions             |
| ...                                                            |
| consumer 10 -> assigned subset of state partitions             |
|                                                                |
| Every consumer runs the SAME per-trip logic:                   |
| estimate duration -> estimate distance -> estimate fare        |
+----------------------------------------------------------------+
        |
        v
+----------------------------------------------------------------+
| Topic: ridesharing.trip-priced.v1                              |
+----------------------------------------------------------------+
```

### How the group covers the full stream

With 50 partitions and 10 consumers, Kafka may assign about five partitions to each consumer.

```text
consumer 1  -> partitions 0-4
consumer 2  -> partitions 5-9
...
consumer 10 -> partitions 45-49
```

Each consumer sees only its assigned subset. The **group collectively processes all 50 partitions**.

### Strengths

- fewest moving parts;
- lowest end-to-end latency;
- no intermediate topics or joins;
- easiest failure and replay model;
- good for a laptop-level project.

### Weaknesses

- duration, distance, and pricing are tightly coupled;
- duration and distance are not independently reusable;
- changing one calculation may require redeploying the whole application;
- one slow model can slow the complete pipeline.

### Best use

Use this design when the three calculations belong to one product workflow and only the final fare is required.

---

## 12. Workflow Chart 2: Proposal 2 — combined metrics topic, then pricing

**Teaching goal:** introduce the consumer-producer pattern without requiring a multi-topic join.

```text
+------------------------------------------------------------+
| Topic: ridesharing.trip-created.v1                         |
+------------------------------------------------------------+
               |
               v
+------------------------------------------------------------+
| Consumer Group: ridesharing-trip-metrics-v1                |
|                                                            |
| consume TripCreated                                        |
| -> estimate duration                                       |
| -> estimate distance                                       |
| -> produce one combined metrics event                      |
+------------------------------------------------------------+
               |
               v
+------------------------------------------------------------+
| Topic: ridesharing.trip-metrics-estimated.v1               |
| trip_id + duration + distance                              |
+------------------------------------------------------------+
               |
               v
+------------------------------------------------------------+
| Consumer Group: ridesharing-trip-pricing-v1                |
| consume combined metrics                                   |
| -> calculate or predict fare                               |
| -> produce pricing event                                   |
+------------------------------------------------------------+
               |
               v
+------------------------------------------------------------+
| Topic: ridesharing.trip-priced.v1                          |
+------------------------------------------------------------+
```

The metrics service is both:

- a **consumer** of `ridesharing.trip-created.v1`; and
- a **producer** of `ridesharing.trip-metrics-estimated.v1`.

### Strengths

- clearly separates trip metrics from pricing;
- duration and distance are computed once;
- pricing consumes one complete input event;
- avoids a stateful two-topic join;
- intermediate metrics can be replayed, inspected, and reused;
- demonstrates consumer-producer chaining cleanly.

### Weaknesses

- adds an intermediate topic and another deployment stage;
- creates additional storage, schema, monitoring, and operational work;
- pricing cannot begin until the metrics event is produced.

### Best use

This is the **recommended classroom architecture** and a reasonable production design when duration and distance naturally belong to one metrics service.

---

## 13. Workflow Chart 3: Proposal 3 — separate duration and distance topics, then join

**Teaching goal:** show the original larger proposal and explain why the final pricing stage becomes stateful.

```text
+-------------------------------------+
| Topic: ridesharing.trip-created.v1  |
+-------------------------------------+
                  |                                          |
                  v                                          v
+-------------------------------------+   +-------------------------------------+
| Group: ridesharing-duration-v1      |   | Group: ridesharing-distance-v1      |
| estimate duration                   |   | estimate distance                   |
| produce duration event              |   | produce distance event              |
+-------------------------------------+   +-------------------------------------+
                  |                                          |
                  v                                          v
+-------------------------------------+   +-------------------------------------+
| Topic: ridesharing.trip-duration.v1 |   | Topic: ridesharing.trip-distance.v1 |
+-------------------------------------+   +-------------------------------------+
                  \                                          /
                   \                                        /
                    v                                      v
+-----------------------------------------------------------------------+
| Pricing join application                                              |
| Consumer Group: ridesharing-pricing-join-v1                           |
|                                                                       |
| subscribe to BOTH topics                                              |
| -> key records by trip_id                                             |
| -> store partial results                                              |
| -> wait for duration AND distance                                     |
| -> calculate or predict fare                                          |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-------------------------------------+
| Topic: ridesharing.trip-priced.v1   |
+-------------------------------------+
```

### Does the pricing consumer subscribe to both topics?

Yes. A Kafka consumer can subscribe to multiple topics:

```python
consumer.subscribe([
    "ridesharing.trip-duration.v1",
    "ridesharing.trip-distance.v1",
])
```

Subscription is the easy part. The difficult part is correlating the two independent streams. In a multi-instance plain Kafka consumer service, normal partition assignment does not by itself guarantee that the matching duration and distance records for one `trip_id` reach the same process. The application needs deliberate co-partitioning, external shared state, or a framework that repartitions by key.

The pricing application must:

- match events using `trip_id`;
- ensure both streams are keyed compatibly by `trip_id`;
- ensure matching keys reach the same stateful task, often through repartitioning or a stream-processing framework;
- remember a duration event while waiting for distance, or vice versa;
- handle events arriving in either order;
- handle missing, duplicated, late, or revised estimates;
- decide how long to wait;
- recover its join state after failure;
- replay both topics consistently.

This is a **stateful stream-stream join**.

### Strengths

- duration and distance services can scale and deploy independently;
- each estimate can be reused by many downstream applications;
- model ownership and schemas are separated cleanly;
- one model can be upgraded without redeploying the other.

### Weaknesses

- pricing requires durable join state;
- more topics, schemas, services, alerts, and failure modes;
- late or missing results need explicit policies;
- manual implementation is easy to get wrong.

### Best use

Use this design when duration and distance are genuinely independent products, have different teams or scaling requirements, or must support multiple downstream users.

---

## 14. Four proposals and their trade-offs

The four rows are related, but Proposal 4 is different in kind. The first three describe **pipeline topology**. The fourth describes **how to implement and operate a topology**.

| Proposal | Pipeline topology | Main benefit | Main cost | Best fit |
|---|---|---|---|---|
| **1. Single-stage fare group** | One group reads the source and computes duration, distance, and fare in one application | Simplest, lowest latency, easiest replay | Tight coupling; intermediate estimates are not reusable | Small system, course project, one final output |
| **2. Combined metrics, then pricing** | Metrics group writes one event containing duration and distance; pricing reads one topic | Good separation without a join; reusable metrics | One extra topic and stage | **Best balance for this course and many production systems** |
| **3. Separate topics plus join** | Duration and distance are independent topics; pricing joins both by `trip_id` | Maximum service independence and reuse | Stateful join, late data, recovery, higher operations cost | Large systems with independent teams/models |
| **4. Stream-processing framework** | Implement Proposal 2 or 3 with Flink, Kafka Streams, or another stateful engine | Managed state, event time, joins, checkpoints, recovery, scaling | More infrastructure, concepts, and operational expertise | Production pipelines with nontrivial state/time semantics |

### Practical recommendation for the Ridesharing fare example

- **Laptop-level course project:** Proposal 1 or Proposal 2.
- **Best teaching architecture:** Proposal 2.
- **Independent production services with reusable outputs:** Proposal 3.
- **Production Proposal 3 with serious join/time/state requirements:** Proposal 4 implementing Proposal 3.

---

## 15. The exact relationship between Proposal 3 and Proposal 4

> **Optional advanced discussion:** Sections 15–16 explain why a specialized
> stream-processing runtime can help with state and time. Students are not
> expected to install or implement Flink or Kafka Streams for Demo 04.

Proposal 3 and Proposal 4 can have the same business data flow:

```text
duration topic + distance topic -> join by trip_id -> priced topic
```

The difference is the implementation responsibility.

### Proposal 3 implemented manually

A normal Python Kafka service may:

- subscribe to both topics;
- store partial results in Redis, a database, or local state;
- implement timeouts and cleanup;
- coordinate offsets with state writes;
- recover state after restart;
- handle rebalances and duplicate events.

The development team owns all of that logic.

### Proposal 4 implemented with a stream-processing framework

A framework such as Flink or Kafka Streams provides abstractions for:

- keyed state;
- stream-stream joins;
- event-time processing;
- windows;
- watermarks;
- timers;
- checkpoints or changelog-backed recovery;
- state redistribution during scaling;
- backpressure and operator monitoring.

### Machine learning is orthogonal

A machine-learning model can be used in any proposal:

```text
Proposal 1: one application calls duration, distance, and pricing models
Proposal 2: metrics stage calls models; pricing stage calls another model
Proposal 3: independent model services produce separate streams
Proposal 4: a framework manages the streaming state around those model calls
```

Proposal 4 does **not** mean “the calculation uses machine learning.” It means a specialized stream-processing runtime manages state, time, joins, fault tolerance, and scaling.

---

## 16. Basic components of stream processing (optional extension)

These components explain what a framework adds beyond a simple consumer loop.

### 16.1 Sources

Continuous input streams, such as:

```text
ridesharing.trip-duration.v1
ridesharing.trip-distance.v1
```

### 16.2 Operators or transformations

Per-record calculations such as:

- map;
- filter;
- validate;
- enrich;
- aggregate;
- call a prediction function.

### 16.3 Keys and partitioning

Records are grouped by a stable key such as `trip_id` so related events reach the same logical state owner.

```text
keyBy(trip_id)
```

### 16.4 State

The processor remembers information across records.

For the pricing join:

```text
trip_id -> {
    duration: available or missing,
    distance: available or missing
}
```

State may be local for speed but must be recoverable after failure.

### 16.5 Event time

Event time represents when the business event happened, not when the processor received it.

```text
requested_at
calculated_at
```

### 16.6 Windows

An infinite stream may be divided into finite time ranges.

Examples:

- tumbling window: each record belongs to one fixed interval;
- sliding window: intervals overlap;
- session window: events are grouped by periods of activity.

The simple per-trip pricing join may not need a traditional aggregation window, but it still needs a bounded waiting policy.

### 16.7 Watermarks

A watermark is the processor’s estimate that most events before a certain event time have arrived.

It helps decide when late data should be processed, revised, or discarded.

### 16.8 Joins

A join correlates records from multiple streams using a key and usually a time constraint.

```text
DurationEstimate.trip_id == DistanceEstimate.trip_id
```

### 16.9 Timers and timeouts

A timer can close incomplete state:

```text
If distance has not arrived within 30 seconds,
mark the trip estimate incomplete or use a fallback.
```

### 16.10 Checkpointing and recovery

The system periodically records processing state and source positions so a failed task can resume consistently.

### 16.11 Sinks

Processed results are written to a destination such as:

```text
ridesharing.trip-priced.v1
```

### 16.12 Backpressure and scaling

When downstream processing is slower than incoming data, the runtime slows upstream operators, exposes lag, or scales work across more resources.

---

## 17. Data contracts for every topic

The tables below show the complete minimum fields used by this conceptual
pipeline. Each producer owns the contract of its output topic. Use `trip_id` as
the Kafka record key so related records partition consistently and remain
ordered within a partition. A deterministic record key does **not** by itself
deduplicate retries or make business processing idempotent.

The field tables are only one part of each record contract. This case study
uses the following shared governance decisions:

| Topic | Owning producer | Kafka key / partitioning | Value format and default subject | Compatibility | Event-time field |
|---|---|---|---|---|---|
| `ridesharing.trip-created.v1` | trip intake | UTF-8 `trip_id`; hash partitioning | Confluent-framed Avro; `ridesharing.trip-created.v1-value` | `BACKWARD` | `requested_at` |
| `ridesharing.trip-duration.v1` | duration service | UTF-8 `trip_id`; hash partitioning | Confluent-framed Avro; `ridesharing.trip-duration.v1-value` | `BACKWARD` | `requested_at` (`calculated_at` is output time) |
| `ridesharing.trip-distance.v1` | distance service | UTF-8 `trip_id`; hash partitioning | Confluent-framed Avro; `ridesharing.trip-distance.v1-value` | `BACKWARD` | `requested_at` (`calculated_at` is output time) |
| `ridesharing.trip-metrics-estimated.v1` | metrics service | UTF-8 `trip_id`; hash partitioning | Confluent-framed Avro; `ridesharing.trip-metrics-estimated.v1-value` | `BACKWARD` | `requested_at` (`calculated_at` is output time) |
| `ridesharing.trip-priced.v1` | pricing service | UTF-8 `trip_id`; hash partitioning | Confluent-framed Avro; `ridesharing.trip-priced.v1-value` | `BACKWARD` | `requested_at` (`calculated_at` is output time) |

These subject names assume the default topic-name strategy. Changing the
subject naming strategy is a separate governance decision and must be
coordinated across producers and consumers.

### 17.1 Topic 1: `ridesharing.trip-created.v1`

This is the source event written by the trip-intake producer.

| Field | Type | Required? | Purpose |
|---|---|---:|---|
| `trip_id` | UUID string | Yes | trip correlation identifier and recommended Kafka record key |
| `rider_id` | string | Yes | rider requesting the trip |
| `driver_id` | string or null | No | may be unknown before matching |
| `requested_at` | timestamp | Yes | timezone-aware request time |
| `state` | string | Yes | two-letter state code |
| `pickup_lat` | double | Yes | pickup latitude |
| `pickup_lon` | double | Yes | pickup longitude |
| `dropoff_lat` | double | Yes | destination latitude |
| `dropoff_lon` | double | Yes | destination longitude |
| `vehicle_type` | string or null | No | standard, XL, luxury, etc. |
| `surge_code` | string or null | No | optional pricing context |

Example:

```json
{
  "trip_id": "550e8400-e29b-41d4-a716-446655440000",
  "rider_id": "rider_2088",
  "driver_id": null,
  "requested_at": "2026-07-16T20:15:30Z",
  "state": "CA",
  "pickup_lat": 37.7749,
  "pickup_lon": -122.4194,
  "dropoff_lat": 37.3382,
  "dropoff_lon": -121.8863,
  "vehicle_type": "standard",
  "surge_code": null
}
```

Pydantic can enforce:

- valid UUID;
- nonempty rider ID;
- legal state code;
- timezone-aware timestamp;
- latitude from `-90` to `90`;
- longitude from `-180` to `180`.

---

### 17.2 Topic 2A: `ridesharing.trip-duration.v1`

Used in Proposal 3.

| Field | Type | Required? | Purpose |
|---|---|---:|---|
| `trip_id` | UUID string | Yes | correlation key |
| `state` | string | Yes | source context |
| `requested_at` | timestamp | Yes | source event time |
| `estimated_duration_seconds` | double | Yes | duration estimate |
| `duration_model_version` | string | Yes | reproducibility and audit |
| `calculated_at` | timestamp | Yes | output event time |

---

### 17.3 Topic 2B: `ridesharing.trip-distance.v1`

Used in Proposal 3.

| Field | Type | Required? | Purpose |
|---|---|---:|---|
| `trip_id` | UUID string | Yes | correlation key |
| `state` | string | Yes | source context |
| `requested_at` | timestamp | Yes | source event time |
| `estimated_distance_miles` | double | Yes | distance estimate |
| `distance_model_version` | string | Yes | reproducibility and audit |
| `calculated_at` | timestamp | Yes | output event time |

---

### 17.4 Recommended intermediate topic: `ridesharing.trip-metrics-estimated.v1`

Used in Proposal 2.

| Field | Type | Required? | Purpose |
|---|---|---:|---|
| `trip_id` | UUID string | Yes | trip identity |
| `state` | string | Yes | source context |
| `requested_at` | timestamp | Yes | source event time |
| `estimated_duration_seconds` | double | Yes | duration result |
| `estimated_distance_miles` | double | Yes | distance result |
| `duration_model_version` | string | Yes | duration lineage |
| `distance_model_version` | string | Yes | distance lineage |
| `calculated_at` | timestamp | Yes | metrics event time |

Example:

```json
{
  "trip_id": "550e8400-e29b-41d4-a716-446655440000",
  "state": "CA",
  "requested_at": "2026-07-16T20:15:30Z",
  "estimated_duration_seconds": 1840.0,
  "estimated_distance_miles": 42.8,
  "duration_model_version": "duration_v3",
  "distance_model_version": "distance_v2",
  "calculated_at": "2026-07-16T20:15:31Z"
}
```

---

### 17.5 Final topic: `ridesharing.trip-priced.v1`

| Field | Type | Required? | Purpose |
|---|---|---:|---|
| `trip_id` | UUID string | Yes | trip identity |
| `state` | string | Yes | source context |
| `estimated_duration_seconds` | double | Yes | pricing input and audit |
| `estimated_distance_miles` | double | Yes | pricing input and audit |
| `estimated_fare_amount` | double | Yes | final estimated price |
| `currency` | string | Yes | for example `USD` |
| `requested_at` | timestamp | Yes | original source event time |
| `duration_model_version` | string | Yes | duration lineage |
| `distance_model_version` | string | Yes | distance lineage |
| `pricing_model_version` | string | Yes | formula or model version |
| `calculated_at` | timestamp | Yes | pricing event time |

Example:

```json
{
  "trip_id": "550e8400-e29b-41d4-a716-446655440000",
  "state": "CA",
  "estimated_duration_seconds": 1840.0,
  "estimated_distance_miles": 42.8,
  "estimated_fare_amount": 57.40,
  "currency": "USD",
  "requested_at": "2026-07-16T20:15:30Z",
  "duration_model_version": "duration_v3",
  "distance_model_version": "distance_v2",
  "pricing_model_version": "pricing_v5",
  "calculated_at": "2026-07-16T20:15:32Z"
}
```

---

## 18. Pydantic, Avro, and Schema Registry at each stage

Every topic has a separate contract.

```text
TripCreated schema
TripMetricsEstimated schema
TripDurationEstimated schema
TripDistanceEstimated schema
TripPriced schema
```

For each producer stage:

```text
application object
-> Pydantic validation
-> Avro serialization
-> Registry compatibility check / schema ID
-> Kafka record
```

For each consumer stage:

```text
Kafka record
-> schema ID
-> Avro deserialization
-> Pydantic validation
-> business processing
-> produce next event
-> commit source offset after success
```

### Structural versus business validation

- Avro can verify that `estimated_fare_amount` is encoded as a numeric field.
- Pydantic can additionally verify that the amount is nonnegative and the currency is allowed.
- Schema Registry can reject an incompatible schema change.
- Kafka transports the resulting bytes and tracks group progress.

---

## 19. Schema evolution examples

### Safe addition to `TripCreated`

Add an optional field with a default:

```json
{
  "name": "vehicle_type",
  "type": ["null", "string"],
  "default": null
}
```

### Safe addition to `TripPriced`

Add currency with an appropriate default only when the business semantics justify one:

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

A default is not merely a technical trick. It must represent a correct business assumption for older records.

### Dangerous changes

- changing `trip_id` from string to integer;
- deleting a required field without a compatible resolution path;
- changing miles to kilometers without changing the field meaning or name;
- reusing a field while silently changing its semantics.

---

## 20. Partition strategy and the 50-state simplification

The one-state-per-partition design is useful for teaching assignment and coverage, but it can create data skew.

```text
California traffic >> Wyoming traffic
```

One California partition may be overloaded while a Wyoming partition is mostly idle.

### Production alternatives

#### Partition by `trip_id`

```text
key = trip_id
```

Benefits:

- lifecycle events for one trip remain ordered;
- load is usually distributed more evenly;
- state remains a payload field for filtering and aggregation.

#### Partition by state plus bucket

```text
key = state + bucket(trip_id)
```

Example:

```text
CA-0, CA-1, ..., CA-19
WY-0, WY-1
```

This preserves some state locality while giving high-volume states more parallel capacity.

### Teaching rule

Use 50 states and 50 partitions as a conceptual model. Then explicitly tell students that production partitioning must balance:

- ordering requirements;
- data skew;
- throughput;
- future scale;
- key stability.

---

## 21. Processing, commits, and idempotency across stages

A consumer-producer stage should follow this logical sequence:

```text
1. poll source record
2. deserialize with Avro
3. validate with Pydantic
4. perform calculation
5. produce derived event successfully
6. commit the next source offset
```

If the application commits before the derived event is safely produced, a crash may lose the output.

If the output is produced but the source offset is not committed before a crash, the input may be processed again. The derived producer therefore needs idempotency.

Keep three ideas separate:

- `key=trip_id` gives deterministic partitioning and per-partition ordering;
- Kafka producer idempotence prevents duplicate writes from certain producer
  retries within one producer session;
- business idempotency handles replay after consumer crashes, restarts, or
  source reprocessing, usually through a deterministic result identity plus a
  deduplication or upsert rule.

The Kafka record key alone does not enforce uniqueness and Kafka does not
automatically replace an older record with the same key.

A useful result key is:

```text
trip_id + result_type + model_version
```

Possible result identities:

```text
trip_123 + duration + duration_v3
trip_123 + distance + distance_v2
trip_123 + price + pricing_v5
```

The derived event should carry this identity explicitly, or the destination
store should derive and enforce it consistently. The exact storage mechanism is
an implementation decision outside this conceptual case study.

---

## 22. Failure handling and observability

### Invalid records

A rejected record may be routed to a dead-letter topic such as:

```text
ridesharing.trip-invalid.v1
```

Safe error metadata can include:

- source topic;
- partition;
- offset;
- schema ID;
- validation error category;
- nonsecret trace ID.

### Metrics

Monitor:

- consumer lag by group and partition;
- throughput;
- processing latency;
- model inference latency;
- Avro deserialization failures;
- Pydantic validation failures;
- output production failures;
- commit failures;
- rebalance frequency;
- join-state size and timeout count;
- hot partitions.

---

## 23. Recommended classroom teaching sequence

1. **Review Lecture 3.** A group owns partition assignments and committed progress.
2. **Show Proposal 1.** Ten identical fare consumers collectively cover 50 partitions.
3. **Clarify the logic rule.** Each member computes all three numbers for its assigned trips.
4. **Show Proposal 2.** A metrics consumer becomes a producer and writes one combined intermediate topic.
5. **Show Proposal 3.** Separate duration and distance outputs force pricing to perform a stateful join.
6. **Introduce Proposal 4.** A stream-processing framework manages the state, time, join, and recovery complexity of Proposal 2 or 3.
7. **Make ML orthogonal.** A formula or model can run in any operator; it does not define the topology.
8. **Introduce the topic contracts.** Each pipeline boundary has its own schema.
9. **Connect the validation layers.** Pydantic validates the application, Avro defines bytes, Registry governs evolution, and Kafka transports records.
10. **Close with safe processing.** Deserialize, validate, calculate, produce, then commit.

---

## 24. Common misconceptions and corrections

### Misconception 1

> Proposal 1 means three different consumers in the same group run three different calculations.

**Correction:** Every member runs the same complete duration-distance-fare pipeline. Kafka divides trips, not business steps, among members.

### Misconception 2

> Proposal 2 produces separate duration and distance topics.

**Correction:** Proposal 2 writes one combined metrics event containing both values.

### Misconception 3

> Subscribing to two topics automatically joins them.

**Correction:** Subscription only delivers records. The application or framework must key, store, correlate, time out, and recover the join state.

### Misconception 4

> Proposal 4 is Proposal 3 with a machine-learning model.

**Correction:** Proposal 4 is Proposal 2 or 3 implemented with a runtime that manages state, event time, joins, recovery, and scaling. Machine learning is optional and orthogonal.

### Misconception 5

> A stream window is always required for pricing.

**Correction:** A per-trip join needs bounded state and a timeout policy. It may use a time-bounded join, but it does not necessarily use an aggregation window.

### Misconception 6

> A combined metrics topic is always less production-ready than separate topics.

**Correction:** The right boundary follows ownership, reuse, scaling, and failure requirements. Fewer services can be the more reliable production design when the calculations naturally belong together.

---

## 25. Final design recommendation

For this course and this Ridesharing fare example:

### Main implementation

Use **Proposal 2**:

```text
ridesharing.trip-created.v1
-> metrics consumer-producer
-> ridesharing.trip-metrics-estimated.v1
-> pricing consumer-producer
-> ridesharing.trip-priced.v1
```

It demonstrates:

- topics and partitions;
- consumer groups;
- offset progress;
- consumer-producer chaining;
- multiple data contracts;
- Pydantic validation;
- Avro serialization;
- Schema Registry compatibility;
- replay and idempotency;
- clear production trade-offs.

### Discussion extension

Use Proposal 3 to ask students:

- How would you join two topics?
- Where does state live?
- What happens when one result is late?
- How would replay work?

Then explain that Proposal 4 supplies production abstractions for those problems.

### One-sentence mental model

> **The first three proposals choose where the pipeline boundaries are; the fourth chooses a specialized runtime to manage state, time, joins, recovery, and scale across those boundaries.**

---

## 26. Student checkpoint questions

1. In Proposal 1, does one consumer calculate only duration, or all three outputs for its assigned trips?
2. Why is Proposal 2 easier than Proposal 3 for the pricing stage?
3. Can one consumer subscribe to two topics?
4. Why does subscribing to two topics not complete the join automatically?
5. What state must Proposal 3 retain for each `trip_id`?
6. What should happen if distance arrives but duration never arrives?
7. Is Proposal 4 a new business topology or an implementation approach?
8. Name five components commonly provided by a stream-processing framework.
9. Can a machine-learning model be used in Proposal 1?
10. Why might Proposal 2 be more reliable than Proposal 3 even in production?
11. Which schema belongs to each topic boundary?
12. Why should the derived event be produced before committing the source offset?
13. Why can 50 states mapped directly to 50 partitions create skew?
14. What should be the Kafka key for a stateful join?
15. Which design would you choose for a laptop-level final project, and why?
