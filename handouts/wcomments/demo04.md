# Demo 04: Data Contracts, Avro, and Schema Registry

- **Lecture:** Lecture 4 — Data Contracts and Serialization
- **Python:** 3.11.14
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **Application validation:** `pydantic==2.13.4`
- **Kafka platform for 04C/04D:** your Confluent Cloud Kafka cluster
- **Dedicated Avro topic:** `msds682.demo04.trip-events-avro.v1`

> **Core classroom point:** Kafka transports key/value bytes. Pydantic validates
> application meaning, Avro defines the binary wire contract, and Schema
> Registry stores schema versions and compatibility metadata.

> **Self-contained demo:** Demo 04 generates synthetic, deterministic trip
> events locally. No prior Kafka topic, retained data, or separately running
> producer is required.

## 1. Goals and demo map

By the end of Demo 04, you should be able to:

1. distinguish Pydantic application validation, an Avro wire contract, Schema
   Registry governance, and Kafka transport;
2. inspect the bytes that an Avro producer would use as a Kafka value;
3. explain schema ID framing and version-1 writer/version-2 reader resolution;
4. run one bounded real-Cloud producer/consumer round trip without exposing
   credentials; and
5. decide when standard clients are simpler than native asyncio clients.

| Step | Demo | Main question | Environment | Estimated classroom time |
|---:|---|---|---|---:|
| 1 | Demo 04A: application validation | Which records are valid before serialization? | Fully local | 4–5 minutes |
| 2 | Demo 04B: Avro round trip | What bytes would become the Kafka value, and how does reader/writer resolution work? | Local `mock://` Registry; no Kafka | 6–8 minutes |
| 3 | Demo 04C: real Cloud round trip | How do standard clients use direct Avro serdes with Registry? | Real Confluent Cloud | 8–12 minutes |
| 4 | Demo 04D: native asyncio extension | When should Kafka share an existing event loop? | Real Confluent Cloud; optional | 6–8 minutes |

Demo 04A and 04B are the required classroom baseline. Demo 04C proves the real
Schema Registry wire path. Demo 04D is an optional event-loop extension.

**Recommended 60-minute route:** environment/Cloud check → 04A → 04B → 04C →
discussion and cleanup. Treat 04D as the optional extension. If Cloud resource
provisioning is delayed, complete 04A/04B first; neither waits on the cluster.

The Ridesharing case study in the optional supplement is a conceptual
architecture exercise. It uses different `ridesharing.*` topics and is not
implemented by Demo 04A–04D.

## 2. Direct prerequisites

> **Start directly with Demo 04.** It does not read any prior topic, retained
> records, scripts, or consumer-group offsets. For 04C and 04D, each script
> starts its own bounded consumer and producer.

| Requirement | 04A | 04B | 04C | 04D |
|---|---:|---:|---:|---:|
| Python 3.11.14 and the course `requirements.txt` | Required | Required | Required | Required |
| Existing Kafka cluster | No | No | Required | Required |
| Kafka API key and secret for that cluster | No | No | Required | Required |
| Schema Registry endpoint plus its separate key and secret | No | No | Required | Required |
| Existing Demo 04 Avro topic | No | No | No; `--create-topic` creates it | Yes; run 04C once first |
| Existing records in the topic | No | No | No | No |
| A producer running in another terminal | No | No | No | No |

If you deleted your previous Confluent resources to control cost, use this
fresh-cluster path:

1. create a Kafka cluster and obtain a new bootstrap endpoint;
2. create a Kafka API key/secret for that cluster;
3. enable or locate Schema Registry and obtain its separate endpoint,
   API key, and API secret;
4. update your local `.env`; and
5. run 04C once with `--create-topic`.

`--create-topic` creates only `msds682.demo04.trip-events-avro.v1`. It does
**not** create a Confluent cluster or enable Schema Registry. No preexisting
topic data is required.

## 3. Student run, inspect, and explain checklist

These are activity TODOs. The published scripts are complete demonstrations;
there are no missing code blocks for students to fill in.

### Before running

- [ ] Download and extract `demo04-student.zip`, then enter its one top-level
      `demo04-student/` directory.
- [ ] Create/activate the Python 3.11.14 environment and run `pytest`.
- [ ] Decide whether you are completing only the local baseline (04A–04B) or
      also the real-Cloud extension (04C and optional 04D).

### Demo 04A TODOs — about 4–5 classroom minutes; execution under 5 seconds

- [ ] Run 04A and confirm all expected valid/invalid cases are classified.
- [ ] Inspect UTC normalization and the derived date, hour, and weekday.
- [ ] Explain why naive time, an extra field, a numeric string, negative fare,
      and invalid lifecycle combinations fail.

### Demo 04B TODOs — about 6–8 classroom minutes; execution under 5 seconds

- [ ] Run 04B with `--count 4`.
- [ ] Locate magic byte `0`, the schema ID, and the Avro body length.
- [ ] Confirm V1 round-trip equality and V2 `vehicle_type=null` resolution.
- [ ] Explain why the binary value is not UTF-8 JSON.
- [ ] Explain why Avro can encode a negative `double` while Pydantic rejects a
      negative fare as invalid business data.

### Demo 04C TODOs — about 8–12 classroom minutes; Cloud run typically 1–3 minutes

- [ ] Create the fresh Cloud resources and both credential sets if needed.
- [ ] Copy `.env.example` to ignored `.env` and fill the blank credentials.
- [ ] Run 04C first with `--create-topic` and a unique `--run-id`.
- [ ] Confirm `requested = delivered = consumed = 4`.
- [ ] Inspect the `<topic>-value` subject, schema ID/version, assignment,
      five-byte wire header, and post-validation commit rule.

### Demo 04D TODOs — about 6–8 classroom minutes; optional

- [ ] Confirm that 04C created the dedicated Avro topic; no retained data is required.
- [ ] Run the bounded AIO command with a unique `--run-id`.
- [ ] Confirm assignment readiness occurs before production.
- [ ] Confirm four deliveries, four consumed records, and expected-key filtering.
- [ ] Explain when an existing event loop justifies 04D instead of standard 04C.

### Cleanup TODOs — allow 3–5 minutes

- [ ] Confirm each command returned and its clients closed.
- [ ] Delete the Kafka cluster when you are finished; deleting only the topic
      does not stop cluster charges.
- [ ] Revoke obsolete Kafka and Schema Registry API keys.
- [ ] If reusing the Registry environment, optionally remove the Demo 04 subject
      to avoid clutter.
- [ ] Never submit or publish `.env`; only secret-free reports may be shared.

Planning estimate: local setup plus 04A–04B usually needs 10–15 minutes. Starting
from no Cloud resources, allow another 20–35 minutes for provisioning,
credentials, 04C, and cleanup. 04D adds about 6–8 optional classroom minutes.
Provisioning and network delays vary.

## 4. The four layers

Do not collapse these layers into one word such as “schema.”

| Layer | Owner | What it guarantees |
|---|---|---|
| Application model | Pydantic `TripEventV1` | Strict types, required fields, cross-field business rules, timezone-aware timestamps |
| Wire schema | `trip_event_v1.avsc` | Avro field names, Avro types, logical timestamp type, defaults used during reader/writer resolution |
| Schema service | Schema Registry | Subjects, schema IDs, versions, lookup, and compatibility checks |
| Transport | Kafka | Durable ordered key/value bytes plus metadata; Kafka does not interpret the business fields |

The course keeps both Pydantic and Avro. Avro does not express every business
rule—for example, a `double` field can structurally contain a negative value,
while the application may prohibit a negative fare.

## 5. How the synthetic data generator works

> ##### KEY CONCEPT — REPRODUCIBLE INPUT
>
> Demo 04 creates fake trip lifecycle events in memory and validates every one
> with `TripEventV1` before serialization. It does not read personal data or
> require records from Kafka.

The shared `deterministic_events(count, seed_offset=...)` function uses no
random-number generator:

1. start from `2026-07-16 17:00:00 UTC`, shifted by `seed_offset` minutes;
2. create one event every 17 seconds;
3. cycle lifecycle values through `trip_requested`, `driver_matched`,
   `trip_started`, and `trip_completed`;
4. cycle zones through `north`, `south`, and `west`;
5. derive stable `trip_####`, `rider_###`, and `driver_###` identifiers from
   the index; and
6. include a deterministic nonnegative fare only for completed trips.

Demo 04B uses a fixed `seed_offset`. Demo 04C and 04D derive it from `run_id`
with a stable CRC32 calculation. Therefore, the same `count` and `run_id`
produce the same application events. A different `run_id` gives a different,
still reproducible bounded run. The report records this generation metadata.

> ##### IMPORTANT NOTE — ONE VALUE FORMAT PER TOPIC
>
> Use the dedicated Demo 04 Avro topic. Do not mix values encoded with another
> serialization format into that topic.

## 6. Download the student files

Recommended: download the complete package, extract it, and run from the
top-level directory:

- [Download `demo04-student.zip`](handouts/demo04-student.zip)

The ZIP contains:

```text
demo04-student/
├── requirements.txt
├── .env.example
├── .gitignore
├── demo04_common.py
├── demo04a_schema_validation.py
├── demo04b_local_avro_roundtrip.py
├── demo04c_confluent_avro_roundtrip.py
├── demo04d_asyncio_avro_roundtrip.py
├── trip_event_v1.avsc
├── trip_event_v2_reader.avsc
└── tests/
    ├── conftest.py
    └── test_demo04_local.py
```

The package is the fastest classroom path. Individual files are also available
for inspection:

- [Course requirements](handouts/requirements.txt)
- [Blank environment template](handouts/.env.example)
- [Demo 04 shared module](handouts/demo04_common.py)
- [Demo 04A](handouts/demo04a_schema_validation.py)
- [Demo 04B](handouts/demo04b_local_avro_roundtrip.py)
- [Demo 04C](handouts/demo04c_confluent_avro_roundtrip.py)
- [Demo 04D](handouts/demo04d_asyncio_avro_roundtrip.py)
- [Avro writer schema](handouts/trip_event_v1.avsc)
- [Avro version-2 reader schema](handouts/trip_event_v2_reader.avsc)
- [Test configuration](handouts/demo04-tests/conftest.py)
- [Local test suite](handouts/demo04-tests/test_demo04_local.py)
- [Optional Ridesharing architecture supplement](#/handouts/lec4-ridesharing-architecture)

`demo04_common.py` owns the application models, schema paths, serialization
conversions, connection loaders, safe reports, deterministic events, and topic
constants. Keep it beside the four scripts so those contracts cannot drift.

The public course-level `handouts/requirements.txt` is the dependency SSOT. Do
not create a second public requirements list for Demo 04.

## 7. Install the exact environment

```bash
uv python install 3.11.14
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

Expected client version:

```text
2.15.0
```

Run the local tests:

```bash
pytest
```

All local tests must pass. The exact count may increase as regression coverage
is added; treat a zero exit status as the contract rather than a hardcoded test
count.

## 8. Contracts at a glance

`TripEventV1` accepts strict `trip_id`, `event_type`, `rider_id`, aware
`event_time`, `zone`, optional `driver_id`, and optional nonnegative `fare`.
Unknown fields and silent type coercion are rejected. Lifecycle rules require:

- requested trips: no driver or fare;
- matched/started trips: a driver and no fare;
- completed trips: a driver and nonnegative fare.

The Avro file defines the wire fields and types:

```text
Pydantic object -> magic byte + schema ID + Avro body -> Kafka
                                  |
                                  `-> full schema lives in Registry
```

## 9. Demo 04A — local application validation

> ##### KEY CONCEPT — APPLICATION RULES
>
> Pydantic validates domain meaning before any event reaches serialization.

```bash
python demo04a_schema_validation.py --run-id lec4-demo04a
```

Confirm all eight expectations pass. Inspect UTC normalization, strict-type and
unknown-field rejection, nonnegative fare, and lifecycle rules. Evidence:
`outputs/runs/lec4-demo04a/demo04a_schema_validation/report.json`.

> ##### STUDENT CHECKPOINT — 45-SECOND THINK
>
> Name one record that the Avro field types could encode but the application
> must still reject. Which layer owns that decision?
>
> **Common mistakes**
>
> - ❌ A Python dictionary is already a valid event.
>   ✓ It becomes trusted only after `TripEventV1` validation.
> - ❌ Avro replaces business validation.
>   ✓ Avro can encode a negative `double`; Pydantic rejects negative fare and
>   invalid lifecycle combinations.

## 10. Demo 04B — local Avro + mock Registry

> ##### IMPORTANT NOTE — LOCAL PROOF BOUNDARY
>
> `mock://` proves framing, registration, and reader/writer resolution locally.
> It does not prove Cloud authorization or compatibility endpoints.

```bash
python demo04b_local_avro_roundtrip.py --run-id lec4-demo04b --count 4
```

Confirm:

- V1 input equals the V1 round trip;
- the wire header has magic byte `0` plus a schema ID;
- the V2 reader supplies `vehicle_type=null`;
- Avro bytes are not UTF-8 JSON; and
- Avro structure does not replace Pydantic business validation.

Evidence: `outputs/runs/lec4-demo04b/demo04b_local_avro_roundtrip/report.json`.

> ##### STUDENT CHECKPOINT — 45-SECOND THINK
>
> Which information travels in each Kafka value, and which information remains
> in Schema Registry?
>
> **Common mistakes**
>
> - ❌ The Avro payload is JSON because the Avro schema is written as JSON.
>   ✓ The payload is Confluent framing plus binary Avro.
> - ❌ Schema Registry stores event records.
>   ✓ It stores schemas, IDs, versions, and compatibility configuration.

## 11. Configure Confluent Cloud for 04C/04D

```bash
cp .env.example .env
```

Fill the blank Kafka fields and the separate Schema Registry URL/key/secret.
Keep `DEMO04_TOPIC_NAME=msds682.demo04.trip-events-avro.v1`. Never submit
`.env`, credentials, or screenshots that expose them.

## 12. Demo 04C — standard clients on real Cloud

> ##### KEY CONCEPT — SAFE CONSUMER ORDER
>
> `deserialize → validate → process → commit`. A failed record must not be
> committed as completed work.

```bash
python demo04c_confluent_avro_roundtrip.py \
  --run-id lec4-demo04c --count 4 --create-topic
```

The script creates/checks the dedicated topic, waits for a real assignment,
produces its four deterministic events, deserializes and validates them, then
commits and writes secret-free evidence. Confirm
`requested = delivered = consumed = 4` in:
`outputs/runs/lec4-demo04c/demo04c_confluent_avro_roundtrip/report.json`.

> ##### STUDENT CHECKPOINT — 45-SECOND THINK
>
> If the consumer committed before validation and processing, what would a
> restart incorrectly assume?
>
> **Common mistakes**
>
> - ❌ Producer delivery proves that the consumer processed the record.
>   ✓ Delivery and consumption are separate results; verify both.
> - ❌ A consumer commit acknowledges producer delivery.
>   ✓ It records the consumer group's processing progress.

## 13. Demo 04D — optional native asyncio extension

> ##### KEY CONCEPT — REAL READINESS, NOT A SLEEP GUESS
>
> The producer waits for Kafka's actual partition assignment. The receive
> timeout starts after assignment, not during a cold group join.

Run 04C once first so the dedicated topic exists:

```bash
python demo04d_asyncio_avro_roundtrip.py \
  --run-id lec4-demo04d --count 4 --assignment-timeout 15
```

```text
one asyncio event loop
|-- AIOConsumer: assign -> poll -> deserialize -> validate -> commit
`-- AIOProducer: await assignment -> serialize -> produce -> flush
```

04D precomputes deterministic `trip_id` keys and accepts only this run's keys.
Use AIO when Kafka must share an existing event loop with other nonblocking I/O;
for a normal command-line program, 04C is simpler.

> ##### STUDENT CHECKPOINT — 45-SECOND THINK
>
> For a bounded command-line program with no other async I/O, would you choose
> 04C or 04D? Why?
>
> **Common mistakes**
>
> - ❌ Kafka needs `asyncio` to produce asynchronously.
>   ✓ The standard producer already manages asynchronous delivery internally.
> - ❌ A fixed sleep proves that a consumer is ready.
>   ✓ Demo 04D waits for Kafka's real assignment signal.

## 14. Registry and evolution quick reference

| Term | Meaning in this demo |
|---|---|
| Topic | Kafka log containing record bytes |
| Subject | `msds682.demo04.trip-events-avro.v1-value` |
| Version | Schema position within that subject |
| Schema ID | Registry-wide ID in the five-byte wire header |

The V2 reader adds nullable `vehicle_type` with default `null`, so it can read
V1 records under the demonstrated backward-reader rule. Adding a required field
without a default or changing to an incompatible type fails resolution. A type
compatible change can still alter business meaning, so Registry checks do not
replace application validation.

## 15. Troubleshooting

| Symptom | Likely cause | Direct action |
|---|---|---|
| `Missing required Kafka .env values` | Kafka connection values are absent | Fill the ignored `.env` |
| `Missing required Schema Registry .env values` | Registry URL/key/secret are absent | Create Schema Registry credentials and fill `.env` |
| Topic does not exist | 04C was run without creation, or 04D ran first | Run 04C once with `--create-topic` |
| Avro deserializer rejects a value | The topic contains a different value format | Use the dedicated Demo 04 Avro topic |
| `Expecting data framing...` | The value was not produced with a compatible Registry serializer | Check producer/consumer format and topic |
| Schema registration denied | Registry API key lacks permission | Create/assign the correct Registry credential |
| 04C/04D consume fewer than requested | Assignment, delivery, authorization, or timeout problem | Inspect the secret-free report and connection state |
| `No module named confluent_kafka.aio` | Wrong client/environment | Install the pinned requirements and verify 2.15.0 |
| Business-invalid data passes Avro | The Avro type is structurally valid | Keep application validation after deserialization |

## 16. Demo 04 summary

1. Kafka transports bytes and metadata; it does not understand `TripEvent`.
2. Pydantic validates application meaning before production and after consumption.
3. Avro is a binary wire format whose schema is written as JSON.
4. The Kafka value is Confluent framing plus Avro binary; Registry resolves the schema by ID.
5. Schema Registry stores schemas, subjects, IDs, versions, and compatibility configuration—not Kafka data.
6. Writer and reader schemas make compatible evolution possible.
7. Defaults are part of schema resolution; they do not replace application rules.
8. Use the explicit `AvroSerializer` and `AvroDeserializer` path shown here.
9. Use standard clients for ordinary scripts and native AIO clients for an existing event loop.
10. Keep JSON and Avro teaching values in separate topics and keep all credentials out of artifacts.

## Official references

- Confluent Python client overview: https://docs.confluent.io/kafka-clients/python/current/overview.html
- Confluent Python API reference: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- Confluent Python changelog: https://docs.confluent.io/kafka-clients/python/current/changelog.html
- Schema Registry fundamentals: https://docs.confluent.io/platform/current/schema-registry/fundamentals/index.html
- Avro serializer and wire format: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html
- Schema evolution and compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Apache Avro 1.12.0 specification: https://avro.apache.org/docs/1.12.0/specification/
- Python `dataclasses`: https://docs.python.org/3/library/dataclasses.html
- Python `datetime`: https://docs.python.org/3/library/datetime.html
