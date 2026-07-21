# Demo 05: FastAPI and schema-aware Kafka integration

- **Lecture:** Lecture 5 - Streaming APIs with FastAPI and Kafka
- **Python:** 3.11.14
- **FastAPI:** 0.139.0
- **Pydantic:** 2.13.4
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **Local server:** `uvicorn==0.50.0`
- **Test client:** `httpx2==2.7.0`
- **Cloud platform for 05C/05D:** your Confluent Cloud environment
- **Dedicated topic:** `msds682.demo05.trip-events-api-avro.v1`

> ##### KEY CONCEPT
>
> FastAPI owns the HTTP boundary. The bundled `TripEventV1` model owns
> application meaning, Avro owns the wire structure, Schema Registry owns
> schema identity and compatibility, and Kafka owns durable event transport.

## 1. Goals and demo map

By the end of Demo 05, you should be able to:

1. identify a FastAPI app, path operation, request model, response model,
   status code, lifespan, and OpenAPI document;
2. explain why the HTTP request and Kafka event are separate contracts;
3. keep one publisher for the application lifespan instead of creating one per
   request;
4. follow one request through Pydantic, Avro, Schema Registry, Kafka, a bounded
   consumer, application validation, and commit; and
5. distinguish HTTP acceptance, Kafka acknowledgement, and downstream
   processing completion.

| Step | Demo | Main question | Environment | Classroom time |
|---:|---|---|---|---:|
| 1 | 05A: local contract | What does FastAPI validate and document? | Fully local | 8-10 minutes |
| 2 | 05B: local live service | How do `/docs`, curl, routes, and lifespan fit together? | Fully local | 8-10 minutes |
| 3 | 05C: Cloud round trip | Does one HTTP request become an acknowledged and consumed Avro event? | Confluent Cloud | 12-18 minutes |
| 4 | 05D: live Cloud service | How does the same app behave as a running service? | Confluent Cloud; optional | 8-10 minutes |

**Recommended 60-minute route:** FastAPI recap -> 05A -> 05B -> 05C ->
discussion and cleanup. Treat 05D as the optional extension.

## 2. Relationship to Demo 04

Demo 05 does not re-teach Avro or Schema Registry. It keeps the same business
example and contract shape introduced in Demo 04, then changes the host
application. The Demo 05 ZIP is self-contained and never imports Demo 04.

| Demo 04 introduced | Self-contained Demo 05 adds |
|---|---|
| `TripEventV1` application contract | `CreateTripRequest` HTTP contract |
| `trip_event_v1.avsc` wire contract | `POST /trip-requests` |
| Avro conversion and serializer settings | request-to-event mapping |
| default five-byte Confluent framing | FastAPI response and status semantics |
| standard and native AIO client behavior | FastAPI lifespan owns one AIO producer |
| deserialize -> validate -> process -> commit | API-to-consumer end-to-end evidence |

The Demo 05 contract and infrastructure sources are:

- `trip_event_contract.py`: the one TripEvent and Avro authority inside Demo 05;
- `trip_event_v1.avsc`: the one version-1 writer schema;
- `confluent_demo_common.py`: Demo 05's credential/config/report boundary.

Every Demo 05 script imports these bundled sources. It must not redefine the
event model, schema, serializer configuration, or secret-reporting rules.

## 3. Direct prerequisites

> **Start directly with Demo 05.** No Demo 01-04 topic, records, offsets, or
> running producer are required.

> ##### IMPORTANT NOTE
>
> Demo 05 aligns with the frozen Lecture 4 contract but does not import Lecture
> 4 code. Keep the bundled Demo 05 files together; do not start a Demo 04
> process, copy its files, or read records from its topic.

| Requirement | 05A | 05B | 05C | 05D |
|---|---:|---:|---:|---:|
| Python 3.11.14 and `requirements.txt` | Required | Required | Required | Required |
| Existing Kafka cluster | No | No | Required | Required |
| Kafka API key and secret | No | No | Required | Required |
| Schema Registry endpoint/key/secret | No | No | Required | Required |
| Existing Demo 05 topic | No | No | No; use `--create-topic` | No; use `--create-topic` |
| Existing Kafka records | No | No | No | No |
| Another producer running | No | No | No | No |

> ##### IMPORTANT NOTE — TWO CREDENTIAL TYPES, NOT ONE SET PER DEMO
>
> Kafka credentials authorize access to a Kafka cluster. Schema Registry
> credentials authorize access to a Registry resource; a Kafka key cannot
> replace them. You may reuse working Demo 04 Kafka and Schema Registry
> credentials when they still have access to the same current resources. Demo
> 05 does not technically require newly created keys.

If you deleted earlier resources to control cost:

1. create a new Confluent Cloud Kafka cluster;
2. create a Kafka API key and secret;
3. enable or locate Schema Registry and obtain its separate credentials
   (reuse a valid Demo 04 Registry key or create one if needed);
4. copy `.env.example` to ignored `.env` and fill the blank secrets; and
5. run 05C with `--create-topic`.

The demo creates its own deterministic HTTP requests. It does not read an
external dataset or personal information. The dedicated Demo 05 topic uses the
same wire-contract shape as Lecture 4 but does not require Lecture 4 code,
topic data, offsets, or running processes.

## 4. FastAPI recap

Read the concise [FastAPI recap](fastapi-recap.md) before running 05A. Focus on
seven ideas: application, path operation, request model, response model, status
code, lifespan, and OpenAPI.

The application path is:

```text
HTTP JSON
  -> CreateTripRequest
  -> request_to_event(...)
  -> strict TripEventV1
  -> AsyncAvroSerializer
  -> AIOProducer
  -> Kafka
  -> bounded standard Consumer
  -> AvroDeserializer
  -> strict TripEventV1
  -> process
  -> commit
```

`202 Accepted` means the request was accepted. It does not mean that every
downstream business step has finished. The Cloud response reports
`broker_acknowledged` only after the Kafka delivery future completes.

## 5. Download and file structure

Recommended: download and extract `demo05-student.zip`, then run from its one
top-level folder.

- [Download `demo05-student.zip`](handouts/demo05-student.zip)

If you are reading `README.md` inside the extracted ZIP, the download step is
already complete; continue directly to Section 6.

```text
demo05-student/
├── README.md
├── fastapi-recap.md
├── requirements.txt
├── .env.example
├── .gitignore
├── confluent_demo_common.py
├── trip_event_contract.py
├── trip_event_v1.avsc
├── trip_event_v2_reader.avsc
├── demo05_common.py
├── demo05_app.py
├── demo05_kafka.py
├── demo05a_fastapi_contract.py
├── demo05b_fastapi_local_service.py
├── demo05c_confluent_fastapi_roundtrip.py
├── demo05d_live_confluent_service.py
├── assets/
│   └── demo05/
│       ├── demo05a-expected-result.jpg
│       ├── demo05b-swagger-overview.jpg
│       ├── demo05b-local-202-response.jpg
│       ├── demo05c-expected-result.jpg
│       ├── demo05c-confluent-topic.jpg
│       ├── demo05c-schema-registry.jpg
│       └── demo05d-cloud-202-response.jpg
└── tests/
    ├── conftest.py
    └── test_demo05_local.py
```

## 6. Setup

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
pytest -q
```

Do not install unpinned substitutes for FastAPI, Pydantic, the test client, or
the Kafka client during the exercise. The local test suite checks Python
3.11.14 and the exact Demo 05 framework/client pins before the Cloud run.

## 7. Demo 05A: local FastAPI contract

```bash
python demo05a_fastapi_contract.py \
  --run-id lec5-demo05a \
  --count 3
```

Expected evidence:

- health returns `200`;
- three valid requests return `202`;
- an invalid zone and extra field return `422`;
- OpenAPI includes `POST /trip-requests`;
- no Cloud credentials are read.

### What you should see

![Demo 05A secret-free local result summary](assets/demo05/demo05a-expected-result.jpg)

The exact generated IDs may differ, but the contract must remain the same:
health `200`, valid requests `202`, invalid input `422`, and no Cloud access.
This image summarizes an actual bounded 05A run from the published code.

> ##### STUDENT CHECKPOINT
>
> Why is `CreateTripRequest` allowed to parse an ISO timestamp string while
> `TripEventV1` remains the strict application model?

## 8. Demo 05B: local live service

Start the intentionally interactive service:

```bash
python demo05b_fastapi_local_service.py --port 8001
```

Open `http://127.0.0.1:8001/docs`, run the health endpoint, and submit one trip
request. The local publisher records validated events in memory and never opens
a Kafka connection. Stop it with `Ctrl+C` before continuing.

### What you should see

![Demo 05B Swagger UI with health and trip request operations](assets/demo05/demo05b-swagger-overview.jpg)

Swagger UI should expose `GET /health`, `POST /trip-requests`, and the Pydantic
request and response schemas.

![Demo 05B local request returning HTTP 202](assets/demo05/demo05b-local-202-response.jpg)

The response should be HTTP `202` with `"delivery": "local"`. This proves the
local FastAPI boundary, not a Kafka delivery.

## 9. Demo 05C: real Confluent round trip

```bash
python demo05c_confluent_fastapi_roundtrip.py \
  --run-id lec5-demo05c-yourname \
  --count 3 \
  --create-topic
```

05C starts the independent consumer first and waits for Kafka's real partition
assignment. It then executes the FastAPI application lifespan, posts three
deterministic HTTP requests, awaits three broker acknowledgements, consumes the
same three event keys, validates them, and commits them.

`--consumer-timeout` is the downstream completion budget. It starts after the
last HTTP request has finished publishing, so time spent obtaining broker
acknowledgements does not silently consume the consumer's remaining budget.

Expected report:

```text
requested = http_202 = broker_acknowledged = consumed = 3
```

The report may contain hosts, topic, subject, partition, offset, schema ID, and
credential-presence booleans. It must never contain credential values.

### What you should see

![Demo 05C secret-free Confluent Cloud round-trip summary](assets/demo05/demo05c-expected-result.jpg)

The run is complete only when all four counts agree. Partition numbers, offsets,
and schema IDs are assigned by your resources and will differ from this example.

![Demo 05 topic visible in Confluent Cloud](assets/demo05/demo05c-confluent-topic.jpg)

In Confluent Cloud, confirm that the dedicated topic
`msds682.demo05.trip-events-api-avro.v1` exists. The retained-byte count depends
on how many times you ran the demo.

![Demo 05 Avro subject visible in Schema Registry](assets/demo05/demo05c-schema-registry.jpg)

In Schema Registry, confirm that the Demo 05 value subject exists and its schema
type is Avro. Confluent Cloud navigation labels may change, so verify the subject
name and schema type rather than the exact screen layout.

## 10. Demo 05D: optional live Cloud service

```bash
python demo05d_live_confluent_service.py \
  --port 8001 \
  --create-topic
```

Open `/docs` and submit one request. The producer opens once in FastAPI lifespan
and closes when you stop Uvicorn with `Ctrl+C`. This service is intentionally
interactive; 05A and 05C remain the bounded automated proofs.

### What you should see

![Demo 05D live Cloud request returning HTTP 202 after broker acknowledgement](assets/demo05/demo05d-cloud-202-response.jpg)

The response should be HTTP `202` with `"delivery": "broker_acknowledged"`.
That confirms Kafka acknowledged the record; it does not claim that a downstream
consumer has completed its business work.

## 11. Key implementation decisions

### One producer per application lifespan

`demo05_app.py` opens the publisher before serving requests and closes it at
shutdown. A route does not create a producer, Registry client, or topic.

### Native AIO for the API host

FastAPI already owns an asyncio event loop. The Cloud publisher therefore uses
`AIOProducer`, `AsyncSchemaRegistryClient`, and `AsyncAvroSerializer`.

### Standard client for the command-line consumer

The verification consumer owns no application event loop. A standard bounded
`Consumer` is simpler and correct. It commits only after deserialization,
`TripEventV1` validation, and processing evidence.

### Request acceptance is not processing completion

- `422`: FastAPI rejected the HTTP contract.
- `503`: the publisher could not confirm acceptance.
- `202` with `local`: accepted by the local teaching boundary.
- `202` with `broker_acknowledged`: Kafka acknowledged the event.
- consumer report: downstream validation and commit completed.

## 12. Common mistakes

| Symptom | Likely cause | Fix |
|---|---|---|
| HTTP `422` | Invalid ID, timestamp, zone, or extra field | Read the response error locations |
| HTTP `503` | Kafka or Registry unavailable | Check credentials and cluster state; secrets stay in `.env` |
| Topic missing | Demo 05 topic was not created | Re-run 05C/05D with `--create-topic` |
| Consumer assignment timeout | Topic access or group join failed | Check Kafka credentials and network, then use a new run ID |
| `202` but no consumer record | API acceptance is not consumer completion | Inspect delivery evidence and consumer timeout separately |
| Event-loop stalls | Blocking Kafka operation was placed in `async def` | Use native AIO in the FastAPI path |

## 13. Cleanup checklist

- [ ] Stop the interactive 05B or 05D Uvicorn service.
- [ ] Confirm 05C returned and closed its producer, consumer, and Registry clients.
- [ ] Inspect only secret-free reports under `outputs/runs/<run-id>/`.
- [ ] Delete the Kafka cluster when the exercise is complete.
- [ ] Revoke obsolete Kafka and Schema Registry API keys.
- [ ] Never submit or publish `.env`.

## 14. Final check

- [ ] I can distinguish an HTTP request model from a Kafka event model.
- [ ] I can explain why the publisher belongs to FastAPI lifespan.
- [ ] I can explain `202`, `422`, and `503` in this application.
- [ ] I can distinguish API acceptance, Kafka acknowledgement, and consumer completion.
- [ ] I can trace deserialize -> validate -> process -> commit.
- [ ] I did not depend on Demo 01-04 code, processes, topic data, or credentials.
