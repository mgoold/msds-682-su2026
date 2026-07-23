# Assignment 2 Student Starter

This starter matches the official
[Assignment 2 specification](https://wgu9.github.io/msds-682-su2026/#/handouts/assignment02).
Complete every block between `CODE START HERE` and `CODE ENDS HERE`. Do not
remove the markers, docstrings, or explanatory comments.

This assignment is independent of Assignment 1. It creates its own input
records, topic history, consumer groups, reports, and results.

## 1. Set up Python 3.11.14

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 2. Run credential-free tests

```bash
python -m pytest -q
```

Before implementation, the published starter currently yields 9 expected
failures and 1 passing contract-only test. After implementation, all 10 tests
must pass. Tests do not replace the required real Confluent runs.

## 3. Configure your independent Confluent environment

Copy `.env.example` to `.env`. Fill in your own Kafka and Schema Registry
credentials and replace `<usf_username>` in the group ID.

```bash
cp .env.example .env
```

Never commit or submit `.env`.

The default topic is dedicated to Assignment 2:

```text
msds682.assignment02.trip-events-api-avro.v1
```

No Assignment 1 or prior demo data is required.

The first topic creation and schema registration may take tens of seconds.
Every provided command has a finite stop condition; if a command reaches its
timeout, inspect the error instead of leaving it running indefinitely.

## 4. Seed exactly 12 input events through FastAPI

Run from the starter's top-level directory:

```bash
python src/seed_input.py --run-id assignment2 --count 12 --create-topic
```

The seeder sends deterministic HTTP requests through the FastAPI application.
The app validates each request, creates `TripEventV1`, serializes it as Avro,
and waits for broker acknowledgement. It writes:

```text
evidence/api_seed_report.json
```

Run the seeder only once for the base evidence. If you intentionally rerun it,
use a new `--run-id` and repeat all three consumer phases with that run ID.

## 5. Run the three consumer phases

```bash
python src/run_consumer.py first --run-id assignment2 --max-messages 8
python src/run_consumer.py resume --run-id assignment2 --max-messages 4
python src/run_consumer.py replay --run-id assignment2 --max-messages 12
```

The phases write:

```text
evidence/consumer_first_run.json
evidence/consumer_resume_run.json
evidence/consumer_replay_run.json
results/processed_events.jsonl
results/replayed_events.jsonl
```

`first` and `resume` share one base consumer group. `replay` uses another group
and explicitly starts assigned partitions at the beginning.

If a phase fails, read its error before rerunning. Do not change group IDs or
delete evidence merely to hide an incomplete run.

## 6. Write the report and disclose AI assistance

Copy `REPORT_TEMPLATE.md` to `report.md` and complete every section.

- If you did not use AI assistance, select `No`.
- If you used AI assistance, select `Yes`, copy `AI_USAGE_TEMPLATE.md` to
  `AI_USAGE.md`, list every tool/model used and the submitted area it assisted,
  complete the detailed judgment/verification sections, and include it in your
  ZIP.

## 7. Package the submission

Rename the folder to `assignment2_<usf_username>`, remove `.env`, `.venv`, and
cache files, and submit `assignment2_<usf_username>.zip` to Canvas.

Use the official assignment's submission tree and checkbox list before
uploading.
