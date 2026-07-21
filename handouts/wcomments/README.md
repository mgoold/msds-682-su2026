# Annotated Demo Collection — a Kafka Tutorial in Code

Heavily commented copies of every `demo0*` file, written to be **read rather
than run**. Each file explains not just *what* the code does but *why* it is
written that way, assuming Python experience and no Kafka background.

> **These are reading copies.** The logic is byte-for-byte identical to the
> originals — verified by comparing the parsed syntax tree of each annotated
> file against its source, ignoring comments and docstrings. Only explanation
> was added. Run the originals in `handouts/`, not these.

The `.md` lecture notes and `demo04-student.zip` were copied here unchanged;
only the `.py` files are annotated.

---

## Read in this order

Each file assumes the ones above it. The two `*_common.py` modules are the
foundations — read each before the demos that import it.

### 1 · Producers (Lecture 2)

| # | File | What it teaches |
|---|------|-----------------|
| 1 | **`demo02_producer_common.py`** | **Start here.** The event model, credentials, serialization, delivery callbacks. Everything else imports this. |
| 2 | `demo02a_confluent_sync_style_producer.py` | The simplest producer: send one, wait, repeat. Easy to follow, ~200× slower. |
| 3 | `demo02b_confluent_async_producer.py` | The pattern you should actually use. Differs from 02A by *one moved line*. |
| 4 | `demo02c_confluent_async_sync_compare.py` | Benchmarks both — and how to measure fairly. |
| 5 | `demo02d_confluent_serialization_producer.py` | The object → bytes boundary, made explicit. |

### 2 · Consumers (Lecture 3)

| # | File | What it teaches |
|---|------|-----------------|
| 6 | **`demo03_consumer_common.py`** | Consumer config, group IDs, and the poll loop. Read before 03A–03D. |
| 7 | `demo03a_confluent_basic_consumer.py` | Minimal consumer: subscribe, poll, decode, close. |
| 8 | `demo03b_confluent_offsets_commit.py` | Committing offsets, and why order determines whether you lose or duplicate data. |
| 9 | `demo03c_confluent_groups_replay.py` | Consumer groups, rebalancing, and deliberate replay. |
| 10 | `demo03d_confluent_asyncio_produce_consume.py` | asyncio, and coordinating two clients with a signal instead of a sleep. |

### 3 · Schemas and Avro (Lecture 4)

| # | File | What it teaches |
|---|------|-----------------|
| 11 | **`demo04_common.py`** | Strict validation, Avro converters, the Confluent wire format, two credential sets. |
| 12 | `demo04a_schema_validation.py` | Application rules alone — no Kafka, no network. |
| 13 | `demo04b_local_avro_roundtrip.py` | Avro + Schema Registry via `mock://`, and schema evolution. |
| 14 | `demo04c_confluent_avro_roundtrip.py` | The full cycle on real Confluent Cloud. |
| 15 | `demo04d_asyncio_avro_roundtrip.py` | Everything above, on one asyncio event loop. |

### 4 · Tests *(lighter annotation)*

| # | File | What it teaches |
|---|------|-----------------|
| 16 | `demo04-tests/conftest.py` | How pytest finds the modules under test. |
| 17 | `demo04-tests/test_demo04_local.py` | Verifying Kafka code with **no cloud account**: `mock://`, fake clients, `monkeypatch`. |

---

## The five ideas the whole collection is built on

**1 · `produce()` does not send.** It queues the message and returns in
microseconds; a background thread does the network I/O. You learn a message's
fate only later, through a callback. Nearly every producer design decision
follows from this. → `demo02_producer_common.py`

**2 · Where you put `flush()` *is* the delivery strategy.** Inside the loop,
one message is in flight at a time (~12 msg/sec). Outside the loop, hundreds
pipeline concurrently (~2,600 msg/sec). Same code otherwise.
→ compare `demo02a` with `demo02b`

**3 · Kafka stores opaque bytes and validates nothing.** The schema is a
contract between two programs that both *choose* to honor it. This is why
validation happens on write *and* again on read. → `demo04_common.py`

**4 · Reading does not consume.** The log is immutable; a consumer group just
moves a bookmark. That is why many groups can independently read one topic, and
why replay is possible at all. → `demo03b`, `demo03c`

**5 · Process, then commit.** Commit last and a crash costs you a *repeated*
message; commit first and it costs you a *lost* one. Kafka's default guarantee
is at-least-once, so consumer processing should be idempotent. → `demo03b`

---

## Quick concept lookup

| To understand… | Read… |
|---|---|
| Topics, partitions, offsets | `demo02_producer_common.py` → `event_key`, `DeliveryTracker.callback` |
| Why keys matter | `demo02_producer_common.py` → `event_key` |
| Credential safety | `demo02_producer_common.py` → `load_producer_config`, `safe_config_report` |
| `poll()` vs `flush()` | `demo02b` → the produce loop |
| Reproducible test data | `demo02_producer_common.py` → `make_trip_event` |
| Consumer groups & rebalancing | `demo03c` |
| Offset commits, at-least-once | `demo03b` |
| Replay from the beginning | `demo03c` → `AssignmentTracker.on_assign` |
| asyncio coordination | `demo03d` → `assignment_ready` |
| Strict validation, business rules | `demo04_common.py` → `TripEventV1.enforce_lifecycle_rules` |
| The 5-byte Avro wire header | `demo04_common.py` → `parse_confluent_wire_header` |
| Schema evolution | `demo04b` → the V1/V2 reader comparison |
| Message headers | `demo04c` → `headers_as_dict` |
| Error handling that preserves the real error | `demo04c`, `demo04d` → the `finally` blocks |

---

## Notes

- **Two sets of credentials.** Kafka (`BOOTSTRAP_SERVERS`, `SASL_*`) and Schema
  Registry (`SCHEMA_REGISTRY_*`) are *different services*. Demo 04 needs both.
- **Three demos need no cloud account at all** — `demo04a`, `demo04b`, and the
  test suite run entirely locally.
- **Every demo is bounded.** Real consumers loop forever; these stop on explicit
  message, idle, and wall-clock limits so they terminate.
- **Every report is secret-free by construction** — hostnames and
  `has_password`-style booleans, never credential values.
- Companion prose: [`docs/kafka_notes.md`](../../docs/kafka_notes.md) covers the
  same material as narrative, and [`docs/glossary.md`](../../docs/glossary.md)
  defines the terms.
