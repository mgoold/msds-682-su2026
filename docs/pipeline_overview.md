# Assignment 1 Pipeline Overview — A Kafka Producer Tutorial

This document is a guided tour of the Assignment 1 pipeline for someone who has
never written a Kafka producer. It explains *what each piece does*, *why it
exists*, and *how the pieces fit together*, pointing at the exact code that
carries each idea. Read it top to bottom once before you start implementing —
it will make the `CODE START HERE` blocks feel obvious instead of mysterious.

---

## 1. What this pipeline actually is

Apache Kafka is a **distributed append-only log**. You write ("produce")
messages to a named **topic**, and later, other programs ("consumers") read them
back in order. This assignment only builds the **producer** half — the part that
*writes* events into Kafka (here, a hosted Confluent Cloud cluster).

The whole assignment is one small event-streaming system that answers a
practical engineering question:

> **When you send thousands of messages, is it faster to wait for each one to be
> confirmed before sending the next (synchronous), or to fire them all and
> confirm at the end (asynchronous)? And how do you *prove* the answer with
> evidence?**

To answer that, the pipeline does five things:

1. **Manufactures** a reproducible stream of fake ride-hailing "trip events."
2. **Serializes** each event into bytes and a key.
3. **Produces** those bytes to a Kafka topic using two different delivery
   strategies.
4. **Benchmarks** both strategies over 2,000 messages and records the numbers.
5. **Validates and visualizes** the results as a CSV and a chart.

Everything below maps onto those five steps.

---

## 2. The mental model: five Kafka concepts you must hold in your head

Before the code, here are the five Kafka ideas the whole assignment revolves
around. Every file is just an application of these.

| Concept | One-line meaning | Where it lives in the code |
|--------|------------------|-----------------------------|
| **Topic** | The named log you write into | [`get_topic_name()`](src/producer_common.py) — default `msds682.demo01.trip-events.v1` |
| **Message = key + value** | Each record has a routing key and a payload, both **bytes** | [`event_key()`](src/producer_common.py) and [`serialize_event()`](src/producer_common.py) |
| **Producer** | The client object that sends messages | `Producer(config)` created in each program's `main()` |
| **Delivery callback** | Kafka calls *you back* to say "delivered" or "failed" — asynchronously | [`DeliveryTracker.callback()`](src/producer_common.py) |
| **`poll` / `flush`** | How you let the producer *service* those callbacks and *drain* its send queue | the producer loops in `producer_sync.py`, `producer_async.py`, etc. |

The single most important and most counter-intuitive idea for a novice:
**`produce()` does not send anything immediately.** It puts the message in an
in-memory queue and returns instantly. The message is actually transmitted in
the background, and you only *learn its fate* later, when you call `poll()` or
`flush()` and the library invokes your callback. Almost every design decision in
this assignment flows from that one fact.

---

## 3. The dependency map (build order)

```
                    ┌─────────────────────────────────────────────┐
                    │           producer_common.py                │
                    │  TripEvent · DeliveryTracker · config load  │
                    │  make_trip_event · event_key · serialize    │
                    └─────────────────────────────────────────────┘
                        ▲        ▲          ▲            ▲
        imports         │        │          │            │
        everything ─────┼────────┼──────────┼────────────┤
                        │        │          │            │
             producer_sync   producer_async  producer_serialization
                (02A)           (02B)              (02D)
                        │        │          │
                        └────────┴────┐     │
                                      ▼     ▼
                              producer_compare.py  (02C)
                              run_strategy + CSV contract
                                      │
                                      ▼
                              analyze_results.py
                              validate CSV + plot PNG
```

**Two hard dependencies only:**

1. *Everything* imports [`producer_common.py`](src/producer_common.py) — nothing
   runs until its TODOs are real.
2. [`analyze_results.py`](src/analyze_results.py) imports the CSV contract
   (`CSV_COLUMNS`, `MINIMUM_MESSAGES`, `REQUIRED_BATCH_SIZE`) from
   [`producer_compare.py`](src/producer_compare.py) and reads the CSV it writes.

**Recommended implementation order** (foundation first, then increasing
difficulty so each pattern is reused in the next):

```
producer_common.py → sync → async → serialization → compare → analyze_results.py
   (foundation)       02A     02B        02D           02C        (plotting)
```

---

## 4. Step 1 — The foundation: `producer_common.py`

This is the shared library. Every producer strategy calls into it, which is
exactly why the assignment puts the reusable, testable logic here and keeps the
four programs thin. It has five pieces to complete, and there is an internal
order even within the file.

### 4a. `TripEvent` — the data contract *(lines ~21–29)*

```python
class TripEvent(BaseModel):
    # trip_id, event_type, rider_id, event_time, zone,
    # optional driver_id, optional nonnegative fare
```

This is a **Pydantic model** — a typed, self-validating schema. In a streaming
system the schema *is* the contract between producer and consumer: if the
producer sends a `fare` of `-5`, a downstream consumer will choke. Defining it
as a validated model means bad data is rejected **before** it ever reaches
Kafka. Everything downstream (`event_key`, `serialize_event`, `make_trip_event`)
depends on the field names you choose here, so it must be first.

**Key idea:** *Schema-on-write.* Validation happens at produce time.

### 4b. `DeliveryTracker` — the callback bookkeeper *(lines ~32–54)*

```python
class DeliveryTracker:
    def __init__(self): ...            # delivered_count, failed_messages, delivery_samples
    def callback(self, err, msg): ...  # Kafka calls this per message
    @property
    def failed_count(self): ...        # already implemented for you
```

This is where the **asynchronous delivery-callback** concept becomes concrete.
When Kafka finishes (or fails) sending a message, it calls
`callback(err, msg)`. On failure, `err` is set and you record it. On success,
`err` is `None`, you increment the count and stash a *small, secret-free* sample
as evidence.

Notice the deliberate security boundary: the tracker keeps **at most 10 samples**
and stores only non-sensitive fields (topic, key, partition, offset). This is a
recurring theme — **evidence must never contain credentials.** The test
[`test_safe_config_report_excludes_credentials`](tests/test_producer_logic.py)
enforces the same rule for connection metadata.

The `FakeProducer` in the tests
([`tests/test_producer_logic.py`](tests/test_producer_logic.py) lines ~49–82)
shows exactly how this callback gets invoked: its `poll()` pops one pending
message and calls the callback; its `flush()` drains all of them. Reading that
double is the fastest way to understand what real `poll`/`flush` do.

### 4c. `load_producer_config` / `require_producer_config` *(lines ~65–87)*

```python
def load_producer_config() -> dict[str, str]:
    # return the five confluent-kafka keys, from environment variables only
```

This builds the dictionary Confluent needs to connect:
`bootstrap.servers`, `security.protocol`, `sasl.mechanisms`, `sasl.username`,
`sasl.password`. **Credentials come from `.env` via environment variables —
never string literals.** `require_producer_config()` (already written) wraps it
and exits with a friendly message if a required value is missing.

**Key idea:** *Configuration and secrets live outside the code.* The
`.gitignore` keeps `.env` out of git; `safe_config_report()` lets you prove you
connected without leaking how.

### 4d. `make_trip_event` / `make_trip_events` *(lines ~97–113)*

```python
def make_trip_event(index, rng) -> TripEvent: ...        # you implement
def make_trip_events(count, seed=682) -> list[...]: ...   # already implemented
```

This is the **event generator**. The critical property is **determinism**: the
same `index` + the same seeded `random.Random` must always produce the same
logical event. `make_trip_events` seeds one RNG and builds the whole sequence.

Why does determinism matter so much? Because the benchmark
([`producer_compare.py`](src/producer_compare.py)) sends the *exact same 2,000
events* through both strategies. If the payloads differed, any speed difference
could be blamed on the data instead of the strategy. Reproducibility is what
makes the comparison *fair*. The test
[`test_same_seed_replays_same_serialized_events`](tests/test_producer_logic.py)
locks this in.

### 4e. `event_key` and `serialize_event` — turning objects into bytes *(lines ~116–138)*

```python
def event_key(event) -> bytes:       # trip_id encoded as UTF-8
def serialize_event(event) -> bytes: # compact JSON, drop None fields, UTF-8
```

Kafka does not know about Python objects — **it only stores bytes.** These two
functions are the bridge:

- **`event_key`** produces the *message key*. Kafka uses the key to decide the
  **partition**, and all messages with the same key land on the same partition
  *in order*. Keying by `trip_id` means every event for one trip stays ordered.
  This is how Kafka gives you per-entity ordering without global ordering.
- **`serialize_event`** produces the *message value* — the payload. It converts
  the validated model to compact JSON bytes and drops `None` fields so optional
  data (like `driver_id`) doesn't bloat the wire.

`event_dict` and `safe_config_report` (already written) produce human-readable,
secret-free previews for the JSON evidence reports.

**Key idea:** *Serialization is where your typed domain object becomes a
Kafka message: `key: bytes` for routing, `value: bytes` for payload.*

---

## 5. Steps 2–4 — The three producer strategies

All three programs share the same skeleton: parse args → build config → make
events → run a loop → write a secret-free JSON report to `evidence/`. The **only**
thing you implement in each is the produce loop, and the differences between
those loops *are the lesson*.

### 5a. `producer_sync.py` — Demo 02A: synchronous style *(the simplest)*

```python
# For every event: produce(topic, key, value, callback); then flush() INSIDE the loop
```

Here you `flush()` **after every single message**. `flush()` blocks until the
producer's queue is empty and all callbacks have fired — so this turns the
inherently-async producer into a **synchronous** one: send, wait for
confirmation, send the next. It is the easiest to reason about and the slowest,
because you pay a full network round-trip per message.

The test asserts `sync_producer.flush_calls == 4` for 4 messages — one flush
each ([`test_sync_and_async_have_expected_flush_patterns`](tests/test_producer_logic.py)).

### 5b. `producer_async.py` — Demo 02B: asynchronous *(fire, then drain)*

```python
# For every event: produce(...); poll(0) INSIDE the loop
# flush() EXACTLY ONCE after the loop
```

This is the idiomatic high-throughput pattern:

- `produce()` queues the message and returns instantly.
- `poll(0)` services *already-completed* callbacks without blocking (the `0`
  timeout means "don't wait"). This keeps the callback queue from backing up
  and keeps memory bounded while you stream.
- One `flush()` **after** the loop blocks until every remaining in-flight
  message is confirmed.

The payoff: messages pipeline over the network in parallel instead of
one-at-a-time. The test asserts `poll_calls == 4` and `flush_calls == 1`.

> **Implementation note:** the report dict at the bottom references a
> `remaining` variable (e.g. `"remaining_after_flush": remaining`). Your loop
> must define it — set it from the return value of `flush()` (the count of
> messages still undelivered, which should be `0` on success). The same note
> applies to `producer_serialization.py`.

### 5c. `producer_serialization.py` — Demo 02D: explicit serialization

```python
# For each validated TripEvent: build UTF-8 key/value bytes explicitly,
# produce with callback, poll(0); flush() once after the loop.
```

Structurally this is the async pattern again, but it **foregrounds the
serialization boundary**: you explicitly turn each validated model into
`event_key(event)` and `serialize_event(event)` bytes before producing, and the
report records a `sample_serialized_value` and `"serialized_type": "UTF-8 JSON
bytes"`. The lesson is that **validation and serialization are distinct steps**:
Pydantic guarantees the object is *correct*; serialization decides how it
travels *on the wire*. The test checks that every produced value
`isinstance(value, bytes)`.

**Why these three, in this order?** Sync teaches "wait for each." Async teaches
"fire and drain." Serialization isolates "objects → bytes." By the time you
reach the benchmark, you have already written both loop styles and understand
the byte boundary — so the hard file becomes assembly, not invention.

---

## 6. Step 4 — The benchmark: `producer_compare.py` (Demo 02C)

This is the payoff and the hardest file, but it introduces almost no new
concepts — it **combines** the two loops you already wrote.

### The one function you implement: `run_strategy` *(lines ~54–81)*

```python
# Process events in batch_size (500) slices.
#   async:      produce + poll(0) for every event, then flush ONCE per batch.
#   sync_style: flush after EVERY event.
# Time each batch through completed delivery.
# Append one row per batch using every CSV_COLUMNS field + callback-count deltas.
```

The key new idea is **measuring throughput honestly**. You time each 500-message
batch *through completed delivery* (i.e., including the flush), then compute
`messages_per_second`. Because you record the tracker's delivered/failed counts
at each batch boundary and take the **delta**, each CSV row reflects exactly the
work of that batch.

### The CSV contract *(lines ~29–40)*

```python
CSV_COLUMNS = [run_id, strategy, batch_index, batch_message_count,
               total_messages_so_far, elapsed_seconds, messages_per_second,
               batch_delivered, batch_failed, remaining_after_flush]
```

This fixed schema is the **interface between the producer and the analyzer** —
the same discipline as the Kafka message schema, one layer up. `main()` runs
*both* strategies over one shared event list (line ~111 —
`# One event list is intentionally reused so the logical payloads are equal`),
writes the rows via `write_csv` (already implemented), and then **self-checks**:
it refuses to finish unless every batch delivered exactly 500 with zero failures
and zero remaining (lines ~137–149). That guard is what makes the evidence
trustworthy.

Guardrails enforced by `validate_benchmark_arguments` (already written): at
least 2,000 messages, batch size exactly 500, evenly divisible — the base
assignment size. Test:
[`test_base_benchmark_arguments_are_enforced`](tests/test_producer_logic.py).

**Expected result:** async should post dramatically higher `messages_per_second`
than sync_style, because sync pays a round-trip per message while async
pipelines. The chart in the next step makes that visible.

---

## 7. Step 5 — Validate and visualize: `analyze_results.py`

The final stage is **offline** — no Kafka, no credentials — which is why the
tests can exercise it fully. It closes the loop by proving the evidence is
complete and turning it into a picture.

### 7a. `load_and_validate_rows` *(lines ~16–25)*

Reads the CSV with `DictReader`, checks that every `CSV_COLUMNS` field is
present, converts numeric fields, requires **both** `async` and `sync_style`
strategies, and requires at least `MINIMUM_MESSAGES // REQUIRED_BATCH_SIZE`
(= 4) sequential valid rows per strategy with zero failures and zero remaining.
This is a **data-quality gate**: it refuses to plot incomplete or corrupted
evidence, so a passing chart *means* something.

### 7b. `plot_rows` *(lines ~28–35)*

Draws one labeled line per strategy — `batch_index` on x, `messages_per_second`
on y — with title, axis labels, grid, and legend, then saves the PNG. This is
the human-readable answer to the assignment's central question.

The test [`test_analyzer_validates_and_plots_complete_evidence`](tests/test_producer_logic.py)
builds a synthetic 8-row benchmark and confirms both functions work end to end
without a cluster.

---

## 8. How it all runs — the end-to-end command flow

From the README, run from the starter's top-level directory (all four programs
share one topic and one `.env`):

```bash
# 0. Environment (once)
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env         # then fill in YOUR Confluent credentials

# 1. Credential-free logic tests — should pass before you touch the cloud
python -m pytest -q

# 2. The four producer runs (write evidence/ and results/ artifacts)
python src/producer_sync.py           --run-id assignment1
python src/producer_async.py          --run-id assignment1
python src/producer_compare.py        --run-id assignment1 --messages 2000 --batch-size 500 --seed 682
python src/analyze_results.py         --input results/producer_benchmark.csv --output results/producer_benchmark.png
python src/producer_serialization.py  --run-id assignment1
```

**Artifacts produced** (all secret-free — this is deliberate):

| File | Written by | Contains |
|------|-----------|----------|
| `evidence/demo02a_report.json` | `producer_sync.py` | sync run summary |
| `evidence/demo02b_report.json` | `producer_async.py` | async run summary |
| `evidence/demo02d_report.json` | `producer_serialization.py` | serialization run + sample bytes |
| `evidence/demo02c_config.json` | `producer_compare.py` | benchmark config (no secrets) |
| `results/producer_benchmark.csv` | `producer_compare.py` | per-batch throughput rows |
| `results/producer_benchmark.png` | `analyze_results.py` | async-vs-sync throughput chart |

> The sync-style benchmark flushes after every message, so the 2,000-message
> base run can take up to ~20 minutes depending on network latency. Start early.

---

## 9. The big picture — data's journey through the pipeline

Follow one trip event from birth to chart:

```
  make_trip_event(index, rng)          →  a validated TripEvent object (Pydantic)
        │  (deterministic, seed 682)
        ▼
  event_key(event)  +  serialize_event(event)
        │                                →  key: bytes   +   value: bytes
        ▼
  producer.produce(topic, key, value, callback)
        │                                →  queued in memory, returns instantly
        ▼
  poll(0) / flush()                      →  library transmits + invokes callback
        │
        ▼
  DeliveryTracker.callback(err, msg)     →  counts + secret-free samples
        │
        ▼
  JSON report  (evidence/)   and/or   CSV row  (results/)
        │                                →  per-batch elapsed + messages_per_second
        ▼
  load_and_validate_rows()  →  plot_rows()   →   producer_benchmark.png
```

Every concept from the table in §2 appears on that path exactly once: schema
validation, byte serialization + keying, the queue-and-callback producer model,
`poll`/`flush` draining, and finally evidence and visualization. That is the
whole assignment — and, in miniature, the shape of every real Kafka producer you
will ever write.

---

## 10. Quick reference — where each Kafka concept lives

| To understand… | Read… |
|----------------|-------|
| The message schema / validation | `TripEvent` in [`src/producer_common.py`](src/producer_common.py) |
| Keys, partitioning, serialization | `event_key`, `serialize_event` in [`src/producer_common.py`](src/producer_common.py) |
| Async delivery callbacks + security | `DeliveryTracker` in [`src/producer_common.py`](src/producer_common.py) |
| Secrets & configuration handling | `load_producer_config`, `safe_config_report` in [`src/producer_common.py`](src/producer_common.py) |
| Synchronous producing (`flush` each) | `run_sync_style` in [`src/producer_sync.py`](src/producer_sync.py) |
| Asynchronous producing (`poll` + one `flush`) | `run_async` in [`src/producer_async.py`](src/producer_async.py) |
| Explicit object→bytes boundary | `run_serialization_demo` in [`src/producer_serialization.py`](src/producer_serialization.py) |
| Throughput benchmarking + CSV contract | `run_strategy`, `CSV_COLUMNS` in [`src/producer_compare.py`](src/producer_compare.py) |
| Evidence validation + plotting | `load_and_validate_rows`, `plot_rows` in [`src/analyze_results.py`](src/analyze_results.py) |
| How `produce`/`poll`/`flush` behave | `FakeProducer` in [`tests/test_producer_logic.py`](tests/test_producer_logic.py) |
```
