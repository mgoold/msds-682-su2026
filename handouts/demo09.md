# Demo 09: One Problem, Three Compute Owners

- **Lectures:** Lecture 9 and Lecture 10, Streaming SQL and Compute Ownership
- **Python:** 3.11.14
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **SQL owners:** ksqlDB and Flink SQL on Confluent Cloud
- **Cloud workflow:** Confluent Cloud UI for SQL; Python for produce, read, and verify
- **Baseline:** Demo 07 real-time pricing and delayed-outcome evaluation

> **Naming:** Demo 09A is the ksqlDB owner and Demo 09B is the Flink SQL
> owner. They do not replace or renumber Demo 07A-07F.

> ##### CURRENT STATUS
>
> **Live-certified on August 3, 2026** with run ID
> `demo09-live-20260803`. Python, ksqlDB, and Flink SQL each produced exactly
> eight evaluation rows, four per model. The independent verifier recalculated
> every field, matched all three owners within `0.0001 pp`, and all three
> recommended `ridge-v2` (`24.2049 pp` versus `3.1502 pp` mean absolute gap).
> The Flink statement was confirmed `Stopped`; the temporary 4-CSU ksqlDB
> application and its temporary resource key were deleted after verification.
> Product syntax and UI behavior can still change, so rerun the schema and plan
> gates before a future class rather than treating this dated proof as permanent.

> ##### KEY CONCEPT
>
> Demo 09 keeps one problem, one input run, one schema contract, and one metric.
> Only the owner of `join -> evaluate -> aggregate` changes. This makes the
> comparison about state, time, recovery, lifecycle, and ownership rather than
> about who can write the shortest SQL.

## 1. Objective and relationship to Demo 07

Demo 07 already answers this business question with a Python consumer:

> Which pricing version keeps realized markup closest to the 20% target?

Demo 09 does **not** create another pricing problem or another set of sample
events. It reuses the existing Demo 07 quotes and delayed outcomes, then asks
ksqlDB and Flink SQL to own the same stateful computation.

<div class="handout-flow" role="group" aria-label="Demo 07 and Demo 09 relationship">
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Produce once</span>
    <strong>Demo 07A-07D</strong>
    <p>Train, publish four requests, produce eight quotes, and publish four outcomes.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Change one owner</span>
    <strong>07E/07F · 09A · 09B</strong>
    <p>Python, ksqlDB, and Flink SQL each join, evaluate, and aggregate.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Judge once</span>
    <strong>Shared verifier</strong>
    <p>One Python verifier compares rows, metrics, recommendation, and cleanup evidence.</p>
  </section>
</div>

By the end of Demo 09, you should be able to:

1. explain why SQL changes the compute owner but does not remove state or time;
2. isolate one experiment by `run_id` before joining on value `trip_id`;
3. explain ksqlDB `WITHIN 1 HOUR` and a Flink `$rowtime` interval join;
4. distinguish persistent SQL from a bounded classroom proof;
5. verify two SQL outputs against the Demo 07 Python reference; and
6. treat stop and cloud-resource cleanup as part of correctness.

## 2. Frozen comparison contract

The comparison is valid only if all three owners see the same data and use the
same metric.

| Contract | Required value |
|---|---|
| Experiment identity | One explicit `run_id`, unchanged from produce through verify |
| Input | 8 fare quotes + 4 delayed outcomes from Demo 07 |
| Join order | Filter both inputs by `run_id`, then join on value `trip_id` |
| Per-owner output | Exactly 8 pricing-evaluation rows |
| Model counts | `rule-v1`: 4; `ridge-v2`: 4 |
| Business metric | Mean absolute realized-markup error from 20% |
| Precision | Per-row calculations round to 4 decimals before model average |
| Parity | SQL MAE differs from Python by at most `0.0001 pp` |
| Recommendation | `ridge-v2` for the deterministic teaching run |
| Lifecycle | `start -> produce once -> verify -> stop` |

### Canonical topics

The two input topic names and the business contract remain owned by
`demo07_common.py`. Demo 09 imports them instead of copying them into a second
registry.

| Role | Topic |
|---|---|
| Demo 07 fare quotes | `msds682.demo07.ml-fare-quotes-avro.v1` |
| Demo 07 delayed outcomes | `msds682.demo07.ml-trip-outcomes-avro.v1` |
| Python reference evaluations | `msds682.demo07.ml-pricing-evaluations-avro.v1` |
| Demo 09A ksqlDB evaluations | `msds682.demo09.ksqldb-pricing-evaluations-avro.v1` |
| Demo 09B Flink SQL evaluations | `msds682.demo09.flinksql-pricing-evaluations-avro.v1` |

The SQL outputs do not overwrite the Python evaluation topic. Both output value
subjects reuse `demo07_pricing_evaluation_v1.avsc`; there is no duplicate
ksqlDB or Flink business schema.

### Why the join uses the value field

Demo 07 publishes a raw UTF-8 Kafka key, but it does not register a key schema
in Schema Registry. The Avro **value** contains both `run_id` and `trip_id`.
Therefore both SQL paths use value `trip_id`; they do not assume that a raw
Kafka key becomes a SQL `STRING` automatically.

```text
quotes   WHERE run_id = this run --+
                                      +-- JOIN ON value.trip_id
outcomes WHERE run_id = this run --+
```

Filtering both inputs first matters because a `trip_id` can recur in a later
deterministic run. Joining the historical topics first and filtering afterward
can create plausible but incorrect cross-run pairs.

## 3. Tool and responsibility boundary

Demo 09 intentionally uses two interfaces, each for a different responsibility.

| Responsibility | Tool | Why |
|---|---|---|
| Produce Demo 07 inputs | Existing `confluent-kafka` Python programs | Keeps data generation and Avro contracts identical to Demo 07 |
| Prepare exact output topics and render SQL | `demo09_prepare.py` | Uses one registry and writes inspectable, secret-free artifacts |
| Create, inspect, and stop SQL | Confluent Cloud UI | Makes managed statement/application lifecycle and cost visible |
| Read outputs and compare owners | `demo09_verify.py` with `confluent-kafka` | Keeps the judge independent of both SQL engines |

<aside class="handout-callout handout-callout-important">
  <h5>NO CONFLUENT CLI</h5>
  <p>There is no Confluent CLI command in the baseline. Do not translate the UI steps into CLI commands. Local <code>python ... --flag</code> commands are normal Python program arguments, not Confluent CLI usage.</p>
</aside>

Creating a ksqlDB application, granting a principal, running a Flink statement,
and deleting cloud resources are instructor-managed operations. Do not create
billable resources in a personal course environment unless the instructor has
explicitly authorized the run.

Fully managed ksqlDB is not available on every Confluent Cloud Kafka cluster
type. If no active application exists, the Console may not show an application
creation path. The August 3 certification used the official Cloud management
REST API from a separately reviewed instructor-only Python process, because
the Console hid the menu until an application existed. This control-plane step
is not part of the student package; an instructor-provisioned compatible
application remains the classroom prerequisite.

The `confluent-kafka` Python client is the Kafka and Schema Registry data-plane
tool. It does not provision a ksqlDB application, a Flink compute pool, or a
Flink SQL statement. Those managed control-plane operations remain in the
Cloud UI or a separately approved Python REST workflow.

## 4. Download, setup, and local proof

- [Download `demo09-student.zip`](handouts/demo09-student.zip)

The package contains the Demo 09 files plus the Demo 07 files that remain the
data, topic, schema, and Python-reference owners. It does not contain `.env`,
credentials, a cloud account ID, or generated run evidence.

Extract it, enter its one top-level folder, and run:

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
pytest -q
```

These credential-free tests validate the shared registry, template variables,
rounding contract, expected counts, and secret-free artifacts. They do not
prove that today's managed SQL service accepts the templates.

For a live instructor-authorized run, copy `.env.example` to `.env` and enter
the Kafka cluster-scoped and Schema Registry resource-scoped credentials. Never
paste a Global Cloud API key into the Kafka client fields. Never show, submit,
or commit `.env`.

```bash
cp .env.example .env
```

Open `.env` in a local text editor and fill only the credential fields. Keep
the published Demo 07 and Demo 09 topic defaults unchanged for the shared
classroom run.

## 5. Read the package before running it

| File | One responsibility | What to inspect |
|---|---|---|
| `demo09_common.py` | Demo 09 topic registry and SQL template context | Imports Demo 07 topic/schema owners; validates engine and `run_id` |
| `demo09_prepare.py` | Read-only preflight, optional exact topic creation, SQL rendering | Mutating behavior requires `--create-output-topics` |
| `demo09a_ksqldb.sql.template` | 09A source, run filter, repartition, join, evaluation, aggregate, cleanup | `WITHIN 1 HOUR`, query IDs, no input-topic deletion |
| `demo09b_flinksql.sql.template` | 09B interval join, evaluation, aggregate, stop guidance | `$rowtime`, two-sided time bound, background statement lifecycle |
| `demo09_verify.py` | Independent three-owner evidence check | Recomputes every business field from source facts |
| `demo07_common.py` | Existing business/topic/schema SSOT | 20% target, formulas, topic names, Pydantic contracts |
| `demo07b`-`demo07f` | Existing data and Python reference path | Produce once; do not edit for Demo 09 |

The code comments are part of the lesson. In particular, find the comments that
answer these four questions before running the cloud paths:

1. Which file owns each topic name?
2. Why is output-topic creation behind an explicit flag?
3. Why does verification recompute fields instead of trusting SQL output?
4. Which exact cloud resources must be stopped or deleted?

## 6. Choose one run and render the SQL

Choose a new run ID. Use only lowercase letters, numbers, and hyphens so the
same identifier is safe in reports, SQL literals, and statement names.

```bash
RUN_ID=lec9-demo09-yourname
ROUTING_MODE=fixture
```

Start with the read-only preflight:

```bash
python demo09_prepare.py --run-id "$RUN_ID"
```

The command validates local contracts and renders the two templates under:

```text
outputs/runs/$RUN_ID/demo09_prepare/
├── demo09a_ksqldb.sql
├── demo09b_flinksql.sql
└── report.json
```

The output must contain no API key, secret, email address, or cloud account ID.
Read the report before asking the script to change Kafka resources.

Only after the instructor has approved the exact names, create the two output
topics and register their value contracts:

```bash
python demo09_prepare.py \
  --run-id "$RUN_ID" \
  --create-output-topics
```

This program may create only the two allowlisted Demo 09 output topics. It does
not create a ksqlDB application, create a Flink pool, run SQL, or delete any
Demo 07 resource.

## 7. Start both compute owners in the Cloud UI

The rendered SQL files are the executable teaching artifacts. Copy from those
files rather than replacing `${...}` placeholders by hand in the templates.

### 09A: ksqlDB owner

In the current Confluent Cloud UI:

1. Open the instructor-provided environment and Kafka cluster.
2. Open the dedicated Demo 09 ksqlDB application. Confirm its service account,
   region, capacity, and current cost before running a query.
3. Paste **Steps 1 through 3 only**, one statement at a time, from the rendered
   `demo09a_ksqldb.sql`. Do not paste the Step 4 observation query or cleanup
   statements yet.
4. Confirm each source and derived stream before starting the next persistent
   query. After Step 3, run
   `DESCRIBE D09A_PRICING_EVALUATIONS EXTENDED;` as the sink schema gate.
5. Record every persistent query ID shown by `SHOW QUERIES;`.
6. Do not produce Demo 07 input until every required query reports healthy.

The important shape is:

```sql
-- First isolate this experiment; historical records must not enter the join.
CREATE STREAM ... AS
SELECT * FROM ...
WHERE run_id = '<RUN_ID>'
PARTITION BY trip_id
EMIT CHANGES;

-- WITHIN is a state-retention contract, not optional decoration.
... FROM quotes_for_run AS q
INNER JOIN outcomes_for_run AS o
  WITHIN 1 HOUR
  ON q.trip_id = o.trip_id
EMIT CHANGES;
```

Why the intermediate streams exist:

- `WHERE run_id=...` isolates one fair experiment;
- `PARTITION BY trip_id` gives matching values the same state owner; and
- `WITHIN 1 HOUR` bounds how long the stream-stream join waits and retains
  unmatched state for this classroom session.

The source metadata ignores the unregistered raw key. Once `PARTITION BY`
creates a new `trip_id` key, the rendered derived-stream declarations set an
explicit Kafka key format and exact derived topic names. That transition is
necessary for serialization and makes cleanup auditable. The evaluation sink
likewise declares its raw string key and shared Avro value contract explicitly.
The sink aliases use backticks around lowercase field names. ksqlDB uppercases
unquoted aliases, but `VALUE_SCHEMA_ID` requires the generated value columns to
match the existing lowercase Avro fields exactly.

`CREATE ... AS SELECT` and `INSERT INTO ... SELECT` create persistent work.
Closing the editor does not stop it.

### 09B: Flink SQL owner

In the current Confluent Cloud UI:

1. Open the same environment, catalog/database, and instructor-provided compute
   pool.
2. Use `SHOW TABLES` and `DESCRIBE EXTENDED` to confirm both Demo 07 input
   topics expose raw `key VARBINARY`, the Avro value fields, and `$rowtime`.
3. Copy the rendered `INSERT INTO ... SELECT` into a scratch statement, prepend
   `EXPLAIN`, and run `EXPLAIN INSERT INTO ...` first.
4. Confirm the plan is an interval join rather than an unbounded regular join.
5. Remove only `EXPLAIN`, start the same `INSERT` as a background
   statement, and wait until it reports `RUNNING`.
6. Record the exact statement name before producing input.

The essential time predicate is:

```sql
-- Equality finds the entity. The two-sided interval bounds temporal state.
ON q.trip_id = o.trip_id
AND q.quote_rowtime
  BETWEEN o.outcome_rowtime - INTERVAL '1' HOUR
      AND o.outcome_rowtime + INTERVAL '1' HOUR
```

`$rowtime` is the Kafka record timestamp exposed by the managed table. Because
`SELECT *` excludes system columns, the rendered template explicitly projects
the two `$rowtime` values to named aliases before using them in the interval
predicate. The sink also supplies an explicit raw key derived from `quote_id`;
the 14 value columns continue to match the shared Avro contract. The one-hour
interval matches 09A's classroom time assumption. It is not a claim that
production outcomes always arrive within one hour.

## 8. Produce Demo 07 data once

Run Demo 07A-07D only after both SQL owners are healthy. Fixture routing keeps
the comparison deterministic.

```bash
python demo07a_train_cost_model.py --run-id "$RUN_ID"

python demo07b_produce_trip_requests.py \
  --run-id "$RUN_ID" --count 4 --create-topics

python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" --pricing-method rule-v1 \
  --max-messages 4 --routing-mode "$ROUTING_MODE"

python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" --pricing-method ridge-v2 \
  --model-artifact "outputs/runs/$RUN_ID/demo07a/ridge-cost-v2.json" \
  --max-messages 4 --routing-mode "$ROUTING_MODE"

python demo07d_produce_trip_outcomes.py \
  --run-id "$RUN_ID" --count 4 --routing-mode "$ROUTING_MODE"
```

Expected bounded input:

```text
4 trip requests
  -> 4 rule-v1 quotes + 4 ridge-v2 quotes
  -> 4 delayed outcomes
```

Do not rerun a producer merely because one SQL result is slow. First inspect
the statement, `run_id`, source offsets, partition key, timestamp interval, and
watermark. Reproducing input can create duplicates and hide the real failure.

## 9. Run the Python reference and shared verifier

Complete the unchanged Python owner first:

```bash
python demo07e_confluent_quote_outcome_evaluator.py \
  --run-id "$RUN_ID" --expected-trips 4

python demo07f_compare_models.py --run-id "$RUN_ID"
```

In each SQL UI, run the rendered foreground aggregation and wait until both
model counts are 4. A changing SQL result is evidence that the query is alive;
it is not the final judge.

Run the independent verifier:

```bash
python demo09_verify.py \
  --run-id "$RUN_ID" \
  --engines python ksqldb flinksql
```

During development, a subset is allowed:

```bash
python demo09_verify.py --run-id "$RUN_ID" --engines python flinksql
```

The verifier must:

1. read only the explicit `run_id` from each selected evaluation topic;
2. reject missing, duplicate, cross-run, or unexpected model rows;
3. recompute profit, markup, signed error, and absolute error per row;
4. compare each SQL row and summary with the Python reference; and
5. write a secret-free report without trusting an expected winner.

The shared report is written under the run's output directory. Its success
criteria are 8 rows per owner, 4 per model, MAE parity within `0.0001 pp`, and
the same recommendation.

## 10. Why rounding order is part of the contract

Each evaluation follows Demo 07 exactly:

```text
profit_cents = fare_cents - actual_cost_cents

raw_markup_pct =
    100 * profit_cents / actual_cost_cents

realized_markup_pct =
    round(raw_markup_pct, 4)

markup_error_pp =
    round(raw_markup_pct - target_markup_pct, 4)

absolute_markup_error_pp =
    round(abs(raw_markup_pct - target_markup_pct), 4)

within_target_tolerance =
    abs(raw_markup_pct - target_markup_pct) <= 2.0
```

Only then does each owner compute:

```text
mean_absolute_markup_error_pp =
    round(avg(absolute_markup_error_pp), 4)
```

The signed error, absolute error, and tolerance all use the original unrounded
markup expression, just as `demo07_common.evaluate_quote()` does; they do not
subtract the target from the already rounded display field. If an engine
changes that order or averages unrounded absolute errors, the answer may differ
by about `0.0001 pp`. That is not a meaningful model difference; it is a
contract difference. The templates deliberately make the boundary visible,
and the verifier recomputes it independently. ksqlDB stamps its output with
`FROM_UNIXTIME(UNIX_TIMESTAMP())`; Flink uses `CURRENT_ROW_TIMESTAMP()`.
Therefore `evaluated_at` can legitimately differ by engine, so the verifier
checks that it is valid and non-null rather than requiring timestamp equality.

For the course's deterministic `model-compare` fixture, the regression snapshot
is:

| Model | Records | Average realized markup | Mean absolute gap to 20% |
|---|---:|---:|---:|
| `rule-v1` | 4 | `-3.7613%` | `23.7613 pp` |
| `ridge-v2` | 4 | `20.5885%` | `3.7043 pp` |

This snapshot helps test the code. It does not replace verifying the fresh
classroom `run_id`, and four synthetic outcomes do not justify production
deployment.

## 11. Stop is part of done

Stop the owners immediately after the verifier has passed.

### Stop 09B Flink SQL

1. Stop the exact background statement recorded before the run.
2. Confirm the statement reports `STOPPED`.
3. Confirm no other Demo 09 statement remains running.
4. Confirm the compute pool has no running statements and can scale to zero.

Do not `DROP TABLE` for either Demo 07 source. In Confluent Cloud Flink, dropping
that table can delete the backing Kafka topic.

### Stop 09A ksqlDB

1. Use the recorded query IDs to terminate the join/output query first.
2. Terminate the two filter/repartition queries.
3. Drop only Demo 09 derived objects following the rendered cleanup section.
4. Drop source metadata without deleting the Demo 07 source topics.
5. Delete the temporary ksqlDB application when it is no longer needed so its
   application-level billing stops.

Never use a broad cleanup command. The physical-topic allowlist is the two Demo
09 evaluation sinks plus the two exact ksqlDB filter/repartition topics. The
SQL-object allowlist is limited to the objects named by the rendered files for
this run. Keep both evaluation sinks until evidence has been collected; never
delete a Demo 07 input or Python-reference topic.

## 12. Troubleshooting by contract

| Symptom | Check first | Do not do |
|---|---|---|
| Kafka authentication fails | Kafka cluster-scoped key and broker endpoint | Substitute a Global Cloud key |
| Schema read/write fails | Schema Registry URL and resource-scoped key | Paste credentials into SQL |
| SQL compiles but emits no rows | Both `run_id` filters, source offsets, value `trip_id` | Produce another data set immediately |
| ksqlDB join emits no rows | Derived-stream keys, co-partitioning, record timestamps, `WITHIN` | Remove the time boundary |
| ksqlDB reports changed or reordered value columns | Lowercase backtick-quoted sink aliases and Avro field order | Remove `VALUE_SCHEMA_ID` or publish a second schema |
| Flink join remains empty | Catalog/database, `$rowtime`, interval, watermark/idleness | Replace it with an unbounded regular join |
| More than 8 rows | Duplicate input, cross-run pair, prior output for same run | Accept the extra rows |
| MAE differs slightly | Per-row 4-decimal rounding before `AVG` | Increase tolerance until it passes |
| Verifier prefers a different model | Inspect all 8 source facts and formulas | Hardcode `ridge-v2` |
| Cost continues after class | Running statement/query/application | Assume closing the browser stopped compute |

## 13. What the comparison teaches

| Decision | Python · 07E/07F | ksqlDB · 09A | Flink SQL · 09B |
|---|---|---|---|
| Compute owner | Application process | Kafka-native persistent SQL application | Managed Flink SQL statement |
| State | Explicit Python dictionaries in bounded demo | Windowed stream state | Managed interval-join state |
| Time boundary | Expected finite set | `WITHIN 1 HOUR` | `$rowtime` plus a two-sided 1-hour interval |
| Recovery vocabulary | Offset, replay, duplicate policy | Query state, Kafka-backed recovery, persistent query | Checkpointed runtime state, statement restart, watermark |
| Output | Python evaluation topic | Independent ksqlDB evaluation topic | Independent Flink evaluation topic |
| Completion | Process reaches expected count | Verifier passes, queries terminate, app is deleted | Verifier passes, statement is stopped |

SQL did not remove keys, state, time, recovery, or operations. It moved those
responsibilities into a different owner and vocabulary.

## 14. Classroom boundary and production gaps

Demo 09 is a bounded comparison of compute owners, not a production A/B test.
Both quote versions are counterfactual evaluations on four synthetic outcomes;
no customer traffic is randomized, no model is deployed, and no causal lift is
estimated.

Not covered: production event-time policy, late-data correction, durable
deduplication, schema migration for a long-lived SQL sink, production service
accounts and RBAC review, capacity sizing, alerting, rollback, continuous model
monitoring, or automatic model promotion.

## 15. Completion checklist

- [ ] Credential-free tests pass.
- [ ] One unique `run_id` is recorded and reused everywhere.
- [ ] Read-only preflight renders both SQL files without secrets.
- [ ] The two exact Demo 09 output topics are approved before creation.
- [ ] 09A queries are healthy and their IDs are recorded before produce.
- [ ] 09B interval-join plan is inspected and its statement is `RUNNING` before produce.
- [ ] Demo 07 produces 8 quotes and 4 outcomes exactly once.
- [ ] Python, ksqlDB, and Flink SQL each produce exactly 8 evaluations.
- [ ] Each owner has 4 rows per model and the same per-row business facts.
- [ ] Each SQL MAE matches Python within `0.0001 pp`.
- [ ] The independent verifier recommends `ridge-v2` for the teaching run.
- [ ] The Flink statement is stopped and the ksqlDB queries/application are terminated/deleted.
- [ ] No Demo 07 topic, `.env`, API key, secret, email, or cloud account ID is published.

## 16. Official references

- [ksqlDB overview](https://docs.confluent.io/platform/current/ksqldb/overview.html)
- [ksqlDB stream and table joins](https://docs.confluent.io/platform/current/ksqldb/developer-guide/joins/join-streams-and-tables.html)
- [Confluent Cloud ksqlDB overview](https://docs.confluent.io/cloud/current/ksqldb/overview.html)
- [Confluent Cloud Flink SQL overview](https://docs.confluent.io/cloud/current/flink/overview.html)
- [Flink SQL joins](https://docs.confluent.io/cloud/current/flink/reference/queries/joins.html)
- [Flink SQL statements](https://docs.confluent.io/cloud/current/flink/concepts/statements.html)
- [Flink compute pools](https://docs.confluent.io/cloud/current/flink/concepts/compute-pools.html)
