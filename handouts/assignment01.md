# Assignment 1: Confluent Cloud Kafka Producer Performance Analysis

**Due:** July 18, 2026 at 11:59 PM PDT

**Submission:** Upload one ZIP file to [Canvas](https://usfca.instructure.com/courses/1633704).

**Score:** 20 base points plus up to 3 extra-credit points; maximum 23 points. The assignment remains worth 20% of the course grade.

> The required assignment runs on **real Confluent Cloud Kafka**. A local-only benchmark does not satisfy the 20-point base assignment.

## Download the student starter

[Download `assignment01-starter.zip`](handouts/assignment01-starter.zip), unzip
it, and follow its `README.md`. The starter contains the required file layout,
report and AI-assistance templates, credential-free tests, docstrings, comments,
and clearly marked implementation blocks.

Complete every block between these exact markers:

```python
# ==================== CODE START HERE ====================
# Write your implementation here.
# ===================== CODE ENDS HERE =====================
```

Do not remove the markers, docstrings, or explanatory comments. You may add
small helper functions when they make your code clearer, but keep the provided
interfaces so the included tests remain useful.

## Objective

Use the complete Lecture 2 Demo 02 producer sequence to compare Kafka producer behavior and performance:

1. Demo 02A: sync-style producer,
2. Demo 02B: asynchronous producer,
3. Demo 02C: async-versus-sync performance comparison, and
4. Demo 02D: explicit event validation and serialization.

All four parts must use the same Kafka topic in your Confluent Cloud cluster. You are comparing producer behavior, not creating four different topics.

## Prerequisites and course resources

Complete these course materials first:

- [Demo 01: create the Confluent topic](#/handouts/demo01)
- [Demo 02: Kafka Producer](#/handouts/demo02)
- [Demo 02A sync-style producer](handouts/demo02a_confluent_sync_style_producer.py)
- [Demo 02B async producer](handouts/demo02b_confluent_async_producer.py)
- [Demo 02C async-versus-sync comparison](handouts/demo02c_confluent_async_sync_compare.py)
- [Demo 02D serialization producer](handouts/demo02d_confluent_serialization_producer.py)
- [Shared Demo 02 producer module](handouts/demo02_producer_common.py)

The starter is based on the course Demo 02 sequence. Cite Demo 02 in your
README and clearly explain the implementation and benchmark extensions you
completed for the assignment.

## Required work: 20 base points

### 1. Confluent setup and credential safety

- Use a Confluent Cloud Kafka cluster and the topic created in Demo 01. The default course topic is `msds682.demo01.trip-events.v1`.
- Load `BOOTSTRAP_SERVERS`, `SASL_USERNAME`, and `SASL_PASSWORD` from `.env` or an equivalent ignored configuration file.
- Submit `.env.example` with blank values. **Never submit `.env`, API keys, secrets, or screenshots that expose credentials.**
- Confirm that all four producer parts write to the same topic.

### 2. Demo 02A: sync-style producer

Implement and run the sync-style teaching pattern:

```text
produce -> flush
produce -> flush
produce -> flush
```

Your code must use a delivery callback and report attempted, delivered, failed, and remaining-after-flush counts. Explain why calling `flush()` after every message is easy to understand but normally slow.

### 3. Demo 02B: asynchronous producer

Implement and run the normal asynchronous producer pattern:

```text
produce
produce
produce
poll callbacks while producing
one final flush
```

Use a delivery callback, call `poll(0)` or an equivalent callback-serving method while producing, and call `flush()` once at the end. Report the same delivery counts as Demo 02A.

### 4. Demo 02C: producer performance benchmark

Extend the Demo 02C comparison into a reproducible benchmark.

- Send at least **20,000 messages per strategy**: at least 20,000 async messages and at least 20,000 sync-style messages.
- Use the same event generator, payload shape, message count, and seed for both strategies. Use `682` as the default seed unless you document another fixed seed.
- Measure **500 messages per batch**. For each async batch, queue 500 messages, serve callbacks while producing, and flush once at the batch boundary. For each sync-style batch, flush after every message. Stop the batch timer only after that batch has completed delivery.
- Write one CSV row per strategy per 500-message batch. A 20,000-message comparison therefore produces at least 40 rows per strategy and at least 80 benchmark rows total.
- Include at least these columns:

```text
run_id
strategy
batch_index
batch_message_count
total_messages_so_far
elapsed_seconds
messages_per_second
batch_delivered
batch_failed
remaining_after_flush
```

- Save a secret-free configuration summary, such as security protocol, SASL mechanism, topic name, and whether required values were present. Do not write credentials to the CSV or logs.
- Every valid batch row must show `batch_delivered = 500`, `batch_failed = 0`, and `remaining_after_flush = 0`.

Because both strategies send the same deterministic logical events to one topic, duplicate logical events are expected in this benchmark.

### 5. Demo 02D: schema validation and serialization

Use an explicit event model such as the course `TripEvent` Pydantic model.

Demonstrate this path:

```text
validated Python event
-> JSON string
-> UTF-8 bytes
-> Kafka producer
```

Use a stable event key such as `trip_id`, include at least one sample serialized event in your report, and explain why Kafka ultimately stores keys and values as bytes.

### 6. Visualization and written analysis

Create a graph that compares async and sync-style elapsed time or throughput over each 500-message batch.

Write at least **150 words** addressing:

- which producer strategy was faster in your run,
- why the observed performance differs,
- advantages and disadvantages of each strategy,
- how callback handling, `poll()`, and `flush()` affect delivery and timing, and
- why one Confluent Cloud run should not be treated as a universal Kafka capacity claim.

Also answer these questions concisely, using one or two sentences each:

1. What configuration is required to create the producer, and why must it stay outside source code?
2. What does the delivery callback record for a success and for a failure?
3. What is the difference between `poll(0)` and `flush()` in these demos?
4. Why is one final `flush()` required before the asynchronous script exits?

### 7. Required Confluent evidence

Include secret-free evidence from all four producer parts:

- Demo 02A report,
- Demo 02B report,
- Demo 02C benchmark CSV and plot, and
- Demo 02D report showing serialization and successful delivery.

Reports must show the topic, attempted/delivered/failed counts, and completion status where applicable. A redacted Confluent UI screenshot is optional; secret-free code-generated reports are the preferred evidence.

### 8. AI assistance disclosure

In `report.md`, answer **Yes** or **No** to whether you used AI assistance for
code, debugging, analysis, writing, or visualization.

- If **No**, no separate AI log is required.
- If **Yes**, copy `AI_USAGE_TEMPLATE.md` to `AI_USAGE.md`, complete it, and
  include it in your ZIP. Document the tool, purpose, prompt or request, output
  summary, what you accepted or rejected, your own changes, and verification.

This disclosure is required whenever AI assistance was used. Completing the
disclosure does **not** automatically earn extra credit. The optional AI review
point below requires substantive engineering judgment and supporting evidence.
You remain responsible for understanding and verifying everything submitted.

Your disclosure must demonstrate two AI-use capabilities:

1. **Strategic use and accuracy judgment.** Explain why AI was appropriate at
   that point, what you already understood, how it improved efficiency, and how
   you independently evaluated whether its answer was accurate. AI should
   accelerate work you understand, not replace your understanding of Kafka,
   Python, the benchmark, or your own conclusions.
2. **Failure recovery and fallback.** Explain how you recognized and responded
   if AI gave an incorrect answer, became repetitive, or could not solve the
   problem. Possible responses include improving the prompt, adding relevant
   code/logs/documentation as context, running a focused test, narrowing the
   task, consulting primary documentation, debugging manually, or switching to
   another non-AI method. If no failure occurred, state the warning signs that
   would make you stop and the fallback you would use.

## Submission structure

Submit one ZIP file named `assignment1_<usf_username>.zip` with a structure equivalent to:

```text
assignment1_<usf_username>/
  README.md
  requirements.txt
  .env.example
  src/
    producer_common.py
    producer_sync.py
    producer_async.py
    producer_compare.py
    producer_serialization.py
  results/
    producer_benchmark.csv
    producer_benchmark.png
  evidence/
    demo02a_report.json
    demo02b_report.json
    demo02c_config.json
    demo02d_report.json
  report.md
  AI_USAGE.md              # required only if AI assistance was used
```

Equivalent clear filenames are acceptable. `README.md` must contain setup and run commands. Do not include `.env`, credentials, `.venv`, cached packages, or unrelated large files.

## Grading rubric: 20 base points

| Area | Points | Criteria |
|---|---:|---|
| Confluent setup and credential safety | 2 | Uses a real Confluent cluster and one shared topic; configuration is externalized; no secrets are submitted. |
| Demo 02A and Demo 02B | 4 | Implements sync-style and async behavior correctly, including callbacks, polling, flushing, and delivery counts. |
| Demo 02C benchmark | 5 | Sends at least 20,000 messages per strategy, records every 500-message batch, uses a fair fixed-seed comparison, and produces complete CSV evidence. |
| Demo 02D schema and serialization | 3 | Validates an explicit event model, uses a stable key, serializes to UTF-8 JSON bytes, and proves successful delivery. |
| Visualization and analysis | 3 | Provides a useful comparison graph and a data-supported analysis of performance, tradeoffs, noise, callbacks, polling, and flushing. |
| Producer-code understanding | 2 | Correctly answers the four configuration, callback, `poll()`, and `flush()` questions. |
| Submission quality | 1 | ZIP is organized, documented, runnable, readable, free of credentials/unnecessary files, and includes the required AI-use status and conditional disclosure. |
| **Base total** | **20** | |

## Extra credit: up to 3 additional points

Extra credit does not replace any required Confluent work. The maximum assignment score is 23 points.

### +1: deterministic local replay

Add a credential-free local replay or dry-run mode using the same event contract. Include a minimal replay test proving that the same seed produces the same logical event sequence. Clearly label local results as a harness check, not Kafka performance.

### +1: AI-assisted engineering review

Go beyond the required disclosure: use AI for a substantive engineering review,
identify concrete suggestions, accept and reject at least one suggestion with
reasons, and provide benchmark or test evidence supporting both decisions.
Also demonstrate strategic timing, an independent accuracy check, and a real
recovery action or a clear stop condition with a non-AI fallback. A usage log
without this judgment-and-evidence trail earns no extra-credit point.

### +1: advanced evaluation and observability

Run at least three additional independent comparisons per strategy with at least 2,000 messages per strategy per run. Report variability plus p50 and p95 batch latency, success/failure counts, and a secret-free producer configuration snapshot. Explain benchmark noise.

## Cost and cleanup

Confluent Cloud resources may consume credits while running. Monitor usage, stop or delete unused resources after collecting evidence, and never keep credentials in submitted files.

## Final checklist

- Real Confluent Cloud was used for all required producer runs.
- Demo 02A, 02B, 02C, and 02D are all included.
- Both benchmark strategies sent at least 20,000 messages.
- CSV contains one row per 500-message batch per strategy.
- Plot and 150-word analysis are included.
- Benchmark CSV has at least 80 rows: 40 async and 40 sync-style.
- Every benchmark batch reports 500 delivered, zero failed, and zero remaining after flush.
- `.env` and all credentials are excluded.
- Every `CODE START HERE` / `CODE ENDS HERE` block is implemented, and no
  `NotImplementedError` remains in required code.
- `report.md` says whether AI assistance was used; a completed `AI_USAGE.md` is
  included if the answer is Yes.
- ZIP filename and folder structure are clear.
