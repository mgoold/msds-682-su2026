# Assignment 2: Schema-Aware Kafka Consumer Application

**Released:** Thursday, July 23, 2026

**Due:** Friday, July 31, 2026 at 11:59 PM PDT

**Submission:** Upload one ZIP file to [Canvas](https://usfca.instructure.com/courses/1633704).

**Score:** 20 base points plus up to 3 extra-credit points; maximum 23 points. The assignment is worth 20% of the course grade.

> The required assignment runs on **real Confluent Cloud Kafka**. Credential-free
> tests help you develop safely, but they do not replace the required Cloud
> evidence.

## Download the student starter

[Download `assignment02-starter.zip`](handouts/assignment02-starter.zip), unzip
it, and follow its `README.md`. Rename the unzipped folder to
`assignment2_<usf_username>` before submitting it.

Complete every block between these exact markers:

```python
# ==================== CODE START HERE ====================
# Write your implementation here.
# ===================== CODE ENDS HERE =====================
```

Do not remove the markers, docstrings, or explanatory comments. Keep the
provided interfaces so the included tests remain useful.

> **Independent assignment:** You do not need Assignment 1 code, reports,
> benchmark output, topic data, or cluster history. Assignment 2 has its own
> deterministic input, topic, groups, schema, starter, tests, and evidence.
> Assignment 1 focused on producers; this assignment focuses on the consumer
> application path.

## Student workflow

1. Download and unzip the official starter, then rename the folder with your
   USF username.
2. Review Demo 03, Demo 04, and Demo 05 using the links below.
3. Implement every marked code block while preserving interfaces, docstrings,
   comments, and markers.
4. Run `python -m pytest -q`. All tests must pass after implementation.
5. Configure your own `.env`, then run the API seeder once to create exactly 12
   schema-aware input events in your independent topic.
6. Run the consumer in three phases: first run for 8 events, same-group resume
   for 4 events, and separate-group replay for all 12 events.
7. Inspect the generated evidence and JSONL results, complete `report.md`, and
   complete the conditional AI disclosure.
8. Audit the submission tree and checkbox list, create
   `assignment2_<usf_username>.zip`, and upload it to Canvas.

## Objective: one pipeline, three proofs

Build and explain this bounded end-to-end path:

```text
HTTP request
-> FastAPI and Pydantic request validation
-> TripEventV1
-> Avro serialization and Schema Registry
-> Kafka producer
-> assignment-specific topic
-> bounded Kafka consumer
-> Avro deserialization
-> strict Pydantic event validation
-> local processing artifact
-> synchronous consumer offset commit
```

Your submission uses that one pipeline to prove three observable behaviors:

| Proof | What you run | What it demonstrates |
|---|---|---|
| Input proof | FastAPI seeder sends 12 events | HTTP validation, event mapping, Avro serialization, Schema Registry, and broker acknowledgement work together. |
| Progress proof | First run consumes 8; the same group resumes for 4 | Processing happens before commit, and committed group progress controls where the next run starts. |
| Replay proof | A separate replay group consumes all 12 | Replay is intentional and isolated from the base group's committed history. |

Here, **commit means a Kafka consumer offset commit**. It does not mean a Git
commit, and it is not a producer delivery acknowledgement.

The current producer, topic-creation, and Schema Registry client plumbing is
provided. Your FastAPI work is the thin request-to-event boundary. Most of your
implementation and analysis concern consumer validation, processing, commits,
resume, and replay.

## Prerequisites and course resources

Complete or review:

- [Demo 03: Kafka consumers, offsets, groups, and replay](#/handouts/demo03)
- [Demo 04: Pydantic, Avro, and Schema Registry](#/handouts/demo04)
- [Demo 05: FastAPI and schema-aware Kafka integration](#/handouts/demo05)
- [FastAPI recap](#/handouts/fastapi-recap)

The demos are worked teaching references. The starter is the assignment
scaffold you edit and submit.

### How Demo 03/04/05 map to the starter

| Course reference | Starter responsibility | Required output |
|---|---|---|
| Demo 05 | Validate the HTTP request, map it to `TripEventV1`, and await broker acknowledgement | `evidence/api_seed_report.json` |
| Demo 04 | Deserialize Avro and validate the application model | validated records in all consumer reports |
| Demo 03A/03B | Use a bounded poll loop and commit only after processing succeeds | `evidence/consumer_first_run.json` |
| Demo 03B | Reuse the same group so the second run resumes | `evidence/consumer_resume_run.json` |
| Demo 03C | Use a separate replay group and explicitly start at the beginning | `evidence/consumer_replay_run.json` |

## Required work: 20 base points

### 1. Independent Confluent setup and credential safety

- Use a real Confluent Cloud Kafka cluster and Schema Registry.
- Use an assignment-specific topic. The starter default is
  `msds682.assignment02.trip-events-api-avro.v1`.
- Use an assignment-specific base group ID, such as
  `msds682.assignment02.<usf_username>.base.v1`.
- Load all Kafka and Schema Registry credentials from an ignored `.env`.
- Submit `.env.example` with blank secret values.
- Never submit `.env`, API keys, secrets, or screenshots that expose them.

The seeder can create the topic when you pass `--create-topic`. No topic or data
from Assignment 1 or Demo 01–05 is required.

### 2. FastAPI request-to-event boundary

Complete the provided request-to-event mapping and thin FastAPI route.

The route must:

1. accept a validated `CreateTripRequest`,
2. map it to the canonical `TripEventV1`,
3. await the lifespan-managed publisher,
4. return HTTP 202 only after broker acknowledgement, and
5. return HTTP 503 through the provided error boundary if publishing fails.

Do not create a new Kafka producer inside each request handler.

The seeder sends exactly **12 deterministic HTTP requests** by default. It
writes `api_seed_report.json` with HTTP status, broker acknowledgement, topic,
schema subject, run ID, and secret-free connection presence.

### 3. Schema-aware consumer validation

Use `AvroDeserializer` with Schema Registry to decode each Kafka value.
Validate the resulting object as the strict `TripEventV1` Pydantic model before
processing it.

The application model must:

- reject unexpected fields,
- require timezone-aware timestamps,
- enforce the published identifiers and service-zone values, and
- preserve the input `run_id` and `sequence_number`.

Use the UTF-8 `trip_id` as the Kafka key and verify that the decoded key matches
the event.

### 4. First run: bounded processing before commit

Run:

```bash
python src/run_consumer.py first --run-id assignment2 --max-messages 8
```

The consumer must:

- use `subscribe()` and a visible bounded poll loop,
- disable automatic commit and automatic offset storage,
- handle partition EOF separately from real errors,
- deserialize and validate before processing,
- write one secret-free JSONL result before committing that input message,
- commit synchronously with `consumer.commit(message=..., asynchronous=False)`,
- stop after 8 matching events or a bounded timeout, and
- call `close()` in `finally`.

The report must show exactly 8 processed records and 8 successful commit
requests.

### 5. Same-group resume

Run:

```bash
python src/run_consumer.py resume --run-id assignment2 --max-messages 4
```

Resume must use the same base consumer group as the first run. It must process
the remaining 4 events, not restart from the beginning. Append those events to
`results/processed_events.jsonl`.

Across the first and resume reports:

- exactly 12 records are processed,
- sequence numbers 0 through 11 are present once each,
- the record identities do not overlap, and
- every processed record is committed only after processing succeeds.

`auto.offset.reset=earliest` is only a fallback for partitions without a
committed position. It does not override an existing committed group position.

### 6. Explicit replay

Run:

```bash
python src/run_consumer.py replay --run-id assignment2 --max-messages 12
```

Replay must:

- use a separate replay group,
- override assigned partitions to `OFFSET_BEGINNING` in `on_assign`,
- process all 12 events again,
- write them to `results/replayed_events.jsonl`, and
- leave the base group's committed history unchanged.

The replay report must clearly identify the replay group, forced-beginning
mode, assignments, 12 processed records, and bounded stop reason.

### 7. Evidence and written analysis

Submit:

- `evidence/api_seed_report.json`,
- `evidence/consumer_first_run.json`,
- `evidence/consumer_resume_run.json`,
- `evidence/consumer_replay_run.json`,
- `results/processed_events.jsonl`, and
- `results/replayed_events.jsonl`.

Write at least **150 words** explaining:

- why the consumer processes and writes output before committing,
- what can happen if the application fails before or after the commit,
- why the same group resumes,
- why `auto.offset.reset` does not normally reset an existing group,
- why replay uses a separate group and explicit assignment override, and
- how FastAPI, Pydantic, Avro, Schema Registry, Kafka, the producer, and the
  consumer have different responsibilities.

Also answer in one or two sentences each:

1. What is a poll loop, and why must it have visible stop conditions here?
2. What exactly is stored by a Kafka consumer offset commit?
3. Why is producer acknowledgement different from consumer offset commit?
4. Why must deserialization and Pydantic validation happen before commit?

### 8. AI assistance disclosure

In `report.md`, answer **Yes** or **No** to whether you used AI assistance for
code, debugging, analysis, writing, or testing.

- If **No**, no separate AI log is required.
- If **Yes**, copy `AI_USAGE_TEMPLATE.md` to `AI_USAGE.md`, complete it, and
  include it in your ZIP. List every AI tool/model used and what part of the
  submitted work it assisted. For each substantial use, document the purpose,
  prompt or request, output summary, accepted and rejected suggestions, your
  own changes, and independent verification.

Your disclosure must demonstrate:

1. **Strategic use and accuracy judgment.** Explain why AI was appropriate,
   what you already understood, what you accepted or rejected, and how you
   independently verified accuracy.
2. **Failure recovery and fallback.** Explain how you responded or would
   respond if AI was incorrect, repetitive, or unable to solve the problem.
   Possible actions include improving the prompt, adding relevant context,
   narrowing the task, running a focused test, reading primary documentation,
   debugging manually, or switching to a non-AI method.

Disclosure is required when AI was used. Disclosure alone does not earn extra
credit. You remain responsible for understanding and verifying everything in
your submission.

## Submission structure

Submit one ZIP file named `assignment2_<usf_username>.zip`. When opened, it
must contain one top-level folder:

```text
assignment2_<usf_username>/
|-- README.md
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- schemas/
|   `-- trip_event_v1.avsc
|-- src/
|   |-- app.py
|   |-- cloud.py
|   |-- config.py
|   |-- consumer_runtime.py
|   |-- contracts.py
|   |-- run_consumer.py
|   `-- seed_input.py
|-- tests/
|   `-- test_consumer_application.py
|-- evidence/
|   |-- api_seed_report.json
|   |-- consumer_first_run.json
|   |-- consumer_resume_run.json
|   `-- consumer_replay_run.json
|-- results/
|   |-- processed_events.jsonl
|   `-- replayed_events.jsonl
|-- report.md
`-- AI_USAGE.md                 # include only if AI assistance was used
```

The starter also contains templates and small output-folder README files. They
may remain in the ZIP, but blank templates do not count as completed
deliverables.

## Grading rubric: 20 base points

The rubric has 12 focused decisions. Essential P0 outcomes are worth 2 points;
supporting P1 outcomes are worth 1 point.

| # | Priority | Grading criterion | Points | Pass condition |
|---:|---|---|---:|---|
| 1 | P0 | Real Confluent run and credential safety | 2 | Uses an independent real Confluent topic and Schema Registry with externalized configuration and no submitted secrets. |
| 2 | P0 | FastAPI request and event boundary | 2 | Validates the HTTP request, maps it to `TripEventV1`, uses one lifespan publisher, and returns 202 only after acknowledgement. |
| 3 | P0 | Avro and Pydantic consumer validation | 2 | Deserializes with Schema Registry, strictly validates `TripEventV1`, and verifies the stable UTF-8 key. |
| 4 | P0 | Bounded consumer lifecycle | 2 | Uses subscribe/poll/error handling/time limits/finally-close and never relies on an infinite loop. |
| 5 | P0 | Process-before-commit | 2 | Writes each accepted result before a successful synchronous message commit with auto commit/store disabled. |
| 6 | P0 | Same-group resume | 2 | First run processes 8; the same group resumes and processes the remaining nonoverlapping 4. |
| 7 | P0 | Explicit replay | 2 | A separate group forces assigned partitions to beginning and reprocesses all 12 without altering the base group. |
| 8 | P0 | Complete secret-free evidence | 2 | Four reports and two JSONL files agree on topic, run ID, counts, sequence coverage, groups, and completion. |
| 9 | P1 | Consumer reasoning | 1 | Correctly explains poll loop, commit, failure boundaries, offset fallback, resume, and replay. |
| 10 | P1 | Completed and tested starter | 1 | Credential-free tests pass and no required marked block remains unimplemented. |
| 11 | P1 | AI-use disclosure | 1 | `report.md` declares Yes/No and, when Yes, `AI_USAGE.md` identifies every tool/model, purpose, judgment, changes, and verification. |
| 12 | P1 | Submission package quality | 1 | Required tree and runnable README are complete; credentials, environments, caches, and unrelated files are excluded. |
| | | **Base total** | **20** | |

## Extra credit: up to 3 additional points

Extra credit does not replace base work. Maximum score: 23 points.

### +1: two-member consumer group

Run two bounded consumer members concurrently with the same new group. Submit
secret-free assignment evidence showing that no partition is assigned to both
members at the same time and that their combined accepted records cover the
intended run.

### +1: native asyncio consumer extension

Implement a bounded `AIOConsumer` version using the native
`confluent_kafka.aio` API. Show real assignment readiness, finite poll/time
limits, schema-aware validation, correct cleanup, and an equivalence test
against the synchronous consumer's accepted event identities.

### +1: AI-assisted engineering review

Go beyond disclosure. Use AI for a substantive review, accept and reject at
least one concrete suggestion with reasons, and provide focused test or Cloud
evidence supporting both decisions. Demonstrate an independent accuracy check
and a real recovery action or clear stop condition with a non-AI fallback.

## Cost and cleanup

Confluent Cloud resources may consume credits. After collecting evidence,
delete unused topics, API keys, Schema Registry resources, and clusters as
appropriate. You may retain a resource that is still needed for later course
work, but monitor it and document that decision in `report.md`. Never include
credentials in submitted files.

## Submission checklist

- [ ] I used the official Assignment 2 starter, not Assignment 1 code or unchanged demo files.
- [ ] My ZIP is named `assignment2_<usf_username>.zip` and opens to one top-level `assignment2_<usf_username>/` folder.
- [ ] `README.md` contains reproducible Python setup and run commands.
- [ ] `.env.example` contains blank secrets; `.env`, API keys, passwords, and credential screenshots are excluded.
- [ ] Every marked code block is implemented and no required `NotImplementedError` remains.
- [ ] `python -m pytest -q` passes.
- [ ] The seeder produced exactly 12 acknowledged Avro events in my independent real Confluent topic.
- [ ] The first consumer run processed and synchronously committed exactly 8 events.
- [ ] The same base group resumed and processed the remaining 4 nonoverlapping events.
- [ ] The separate replay group explicitly started at the beginning and processed all 12 events.
- [ ] All accepted values were deserialized through Schema Registry and validated as `TripEventV1` before processing and commit.
- [ ] All four evidence reports are present, mutually consistent, and secret-free.
- [ ] `processed_events.jsonl` contains the 12 base-run event identities exactly once.
- [ ] `replayed_events.jsonl` contains all 12 replayed event identities.
- [ ] `report.md` includes the 150-word analysis, four concise answers, and cleanup confirmation.
- [ ] `report.md` declares Yes or No for AI assistance.
- [ ] If I used AI, I included `AI_USAGE.md`; if I did not, no separate AI log is required.
- [ ] `.venv`, `__pycache__`, `.pytest_cache`, compiled files, and unrelated large files are excluded.
