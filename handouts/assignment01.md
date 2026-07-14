# Assignment 1: Confluent Cloud Kafka Producer Performance Analysis

**Due:** Tuesday, July 21, 2026 at 11:59 PM PDT

> **Extended deadline:** The due date changed from July 18 to July 21. You have
> about one week to finish the assignment.

**Submission:** Upload one ZIP file to [Canvas](https://usfca.instructure.com/courses/1633704).

**Score:** 20 base points plus up to 3 extra-credit points; maximum 23 points. The assignment remains worth 20% of the course grade.

> The required assignment runs on **real Confluent Cloud Kafka**. A local-only benchmark does not satisfy the 20-point base assignment.

## Download the student starter

[Download `assignment01-starter.zip`](handouts/assignment01-starter.zip), unzip
it, and follow its `README.md`. The starter contains the required file layout,
report and AI-assistance templates, credential-free tests, docstrings, comments,
and clearly marked implementation blocks.

After downloading, rename the unzipped folder to
`assignment1_<usf_username>`. Work inside that folder and submit that completed
folder as a ZIP. Do not submit the unchanged course Demo 02 files in place of
the starter.

Complete every block between these exact markers:

```python
# ==================== CODE START HERE ====================
# Write your implementation here.
# ===================== CODE ENDS HERE =====================
```

Do not remove the markers, docstrings, or explanatory comments. You may add
small helper functions when they make your code clearer, but keep the provided
interfaces so the included tests remain useful.

## Student workflow

1. Download and unzip the student starter, then rename its folder with your USF
   username.
2. Complete Demo 01 and review the full Demo 02A–02D sequence. The demos teach
   the producer behaviors; the starter is the assignment scaffold you edit and
   submit.
3. Implement every marked code block in the starter while preserving its
   interfaces, docstrings, and comments.
4. Run `python -m pytest -q`. Contract-only tests may already pass before you
   finish; all tests must pass after implementation.
5. Configure `.env` locally and run the five commands in the starter README
   against your real Confluent Cloud cluster.
6. Inspect the generated evidence, CSV, and plot. Complete `report.md` and the
   conditional AI disclosure.
7. Use the submission tree and checkbox list below to audit your finished
   folder, create `assignment1_<usf_username>.zip`, and upload it to Canvas.

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

### How Demo 02 relates to the starter

Demo 02 is a worked course reference. It shows the core producer behavior with
small teaching runs. The student starter preserves those behaviors but adds the
assignment-scale benchmark, fixed output contracts, validation, tests, report
templates, and submission structure. You must implement and submit the starter;
do not simply rename or resubmit the demo scripts.

| Course reference | Starter file(s) to complete | Required output |
|---|---|---|
| Demo 02A: sync-style producer | `src/producer_sync.py` | `evidence/demo02a_report.json` |
| Demo 02B: asynchronous producer | `src/producer_async.py` | `evidence/demo02b_report.json` |
| Demo 02C: async versus sync-style | `src/producer_compare.py`, `src/analyze_results.py` | benchmark CSV, plot, and secret-free config JSON |
| Demo 02D: validation and serialization | `src/producer_serialization.py` | `evidence/demo02d_report.json` |
| Shared Demo 02 module | `src/producer_common.py` | one event/config/serialization contract used by all four parts |

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

Your code must use a delivery callback and report attempted, delivered, failed,
remaining-after-flush, and elapsed-time values. Explain why calling `flush()`
after every message is easy to understand but normally slow. A small run using
the starter default of **4 messages** is sufficient for Demo 02A.

### 3. Demo 02B: asynchronous producer

Implement and run the normal asynchronous producer pattern:

```text
produce
produce
produce
poll callbacks while producing
one final flush
```

Use a delivery callback, call `poll(0)` or an equivalent callback-serving method
while producing, and call `flush()` once at the end. Report the same delivery
counts and elapsed time as Demo 02A. A small run using the starter default of
**4 messages** is sufficient for Demo 02B.

### 4. Demo 02C: producer performance benchmark

Extend the Demo 02C comparison into a reproducible benchmark.

- Send at least **2,000 messages per strategy**: at least 2,000 async messages and at least 2,000 sync-style messages.
- Use the same event generator, payload shape, message count, and seed for both strategies. Use `682` as the default seed unless you document another fixed seed.
- Measure **500 messages per batch**. For each async batch, queue 500 messages, serve callbacks while producing, and flush once at the batch boundary. For each sync-style batch, flush after every message. Stop the batch timer only after that batch has completed delivery.
- Write one CSV row per strategy per 500-message batch. A 2,000-message comparison therefore produces at least 4 rows per strategy and at least 8 benchmark rows total.
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

The sync-style pass performs one cloud round trip per message because it flushes
after every send. The 2,000-message base benchmark is designed to complete in
about 20 minutes or less on a typical course setup, but actual network and cloud
latency vary. Start the run before the due-date evening.

### 5. Demo 02D: schema validation and serialization

Use an explicit event model such as the course `TripEvent` Pydantic model.

Demonstrate this path:

```text
validated Python event
-> JSON string
-> UTF-8 bytes
-> Kafka producer
```

Use a stable event key such as `trip_id`, enforce the nonnegative fare constraint
shown in Demo 02, include at least one sample serialized event in your report,
and explain why Kafka ultimately stores keys and values as bytes. A small run
using the starter default of **4 messages** is sufficient for Demo 02D.

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

Submit one ZIP file named `assignment1_<usf_username>.zip`. When the ZIP is
opened, it must contain one top-level folder with this structure:

```text
assignment1_<usf_username>/
|-- README.md
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- src/
|   |-- producer_common.py
|   |-- producer_sync.py
|   |-- producer_async.py
|   |-- producer_compare.py
|   `-- producer_serialization.py
|-- tests/
|   `-- test_producer_logic.py
|-- results/
|   |-- producer_benchmark.csv
|   `-- producer_benchmark.png
|-- evidence/
|   |-- demo02a_report.json
|   |-- demo02b_report.json
|   |-- demo02c_config.json
|   `-- demo02d_report.json
|-- report.md
`-- AI_USAGE.md                 # include only if AI assistance was used
```

The downloaded starter also contains `REPORT_TEMPLATE.md`,
`AI_USAGE_TEMPLATE.md`, and small README files inside output folders. You may
leave those template/helper files in the ZIP, but blank templates do not count
as completed deliverables. `README.md` must contain setup and run commands. Do
not include `.env`, credentials, `.venv`, cached packages, or unrelated large
files.

## Grading rubric: 20 base points

The rubric has 13 focused grading decisions. Essential P0 outcomes are worth 2
points each; supporting P1 outcomes are worth 1 point each.

| # | Priority | Grading criterion | Points | Pass condition |
|---:|---|---|---:|---|
| 1 | P0 | Confluent setup and credential safety | 2 | Uses real Confluent Cloud, one shared topic, externalized configuration, and no submitted secrets. |
| 2 | P0 | Demo 02A sync-style producer | 2 | Implements produce-then-flush-per-message correctly and submits complete delivery/timing evidence. |
| 3 | P0 | Demo 02B asynchronous producer | 2 | Implements produce plus callback polling and one final flush, with complete delivery/timing evidence. |
| 4 | P0 | Benchmark scale and fairness | 2 | Sends at least 2,000 messages per strategy using identical fixed-seed logical events. |
| 5 | P0 | Batch CSV and completed delivery | 2 | Records 500-message batches with required columns, sequential counts, 500 delivered, zero failed, and zero remaining. |
| 6 | P0 | Event validation and serialization | 2 | Enforces the event model/fare constraint and uses a stable UTF-8 key plus UTF-8 JSON value bytes. |
| 7 | P0 | Visualization and data-supported analysis | 2 | Submits a readable comparison plot and at least 150 words grounded in the submitted benchmark. |
| 8 | P1 | Producer behavior understanding | 1 | Correctly explains configuration, callbacks, `poll(0)`, `flush()`, and the final-flush requirement. |
| 9 | P1 | Benchmark limitations | 1 | Explains timing completion, network/cloud noise, and why one run is not a universal capacity claim. |
| 10 | P1 | Required evidence set | 1 | Includes every required 02A/02B/02C/02D report, CSV, plot, and secret-free config artifact. |
| 11 | P1 | Completed and tested starter | 1 | Included tests pass and no required marked block remains unimplemented. |
| 12 | P1 | AI-use disclosure | 1 | `report.md` declares Yes/No and includes a complete `AI_USAGE.md` when the answer is Yes. |
| 13 | P1 | Submission package quality | 1 | Required tree and README are complete; credentials, environments, caches, and unrelated files are excluded. |
| | | **Base total** | **20** | |

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

Run at least three additional independent comparisons, each with at least 2,000
messages per strategy. Use a distinct `--run-id` and `--output` CSV filename for
each run so results are not overwritten. Report variability plus p50 and p95
batch latency, success/failure counts, and a secret-free producer configuration
snapshot. Explain benchmark noise. These are additional runs; the base evidence
must still include its own complete 2,000-message-per-strategy comparison.

## Cost and cleanup

Confluent Cloud resources may consume credits while running. Monitor usage, stop or delete unused resources after collecting evidence, and never keep credentials in submitted files.

## Submission checklist

Before uploading to Canvas, check every box:

- [ ] I downloaded the official student starter and completed that scaffold rather than submitting unchanged Demo 02 files.
- [ ] My ZIP is named `assignment1_<usf_username>.zip` and opens to one top-level `assignment1_<usf_username>/` folder.
- [ ] `README.md` contains reproducible Python setup and run commands.
- [ ] `.env.example` contains blank credential values, and `.env`, API keys, passwords, and credential screenshots are excluded.
- [ ] Every `CODE START HERE` / `CODE ENDS HERE` block is implemented; no required `NotImplementedError` remains.
- [ ] `python -m pytest -q` passes in my completed submission folder.
- [ ] Demo 02A, 02B, 02C, and 02D all ran against the same real Confluent Cloud topic.
- [ ] `demo02a_report.json`, `demo02b_report.json`, `demo02c_config.json`, and `demo02d_report.json` are present and secret-free.
- [ ] Both benchmark strategies sent at least 2,000 messages using the same fixed-seed logical events.
- [ ] `producer_benchmark.csv` has at least 8 rows: at least 4 async and 4 sync-style rows, one per 500-message batch.
- [ ] Every benchmark row shows 500 delivered, zero failed, and zero remaining after flush.
- [ ] `producer_benchmark.png` clearly compares async and sync-style results.
- [ ] `report.md` includes the requested 150-word analysis, four producer-code answers, serialization sample, limitations, and cleanup confirmation.
- [ ] `report.md` answers Yes or No for AI assistance.
- [ ] If I used AI, I included a completed `AI_USAGE.md`; if I did not use AI, no separate AI log is required.
- [ ] `.venv`, `__pycache__`, `.pytest_cache`, compiled files, and unrelated large files are excluded.
