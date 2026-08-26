# Demo 11: Observable API for an asynchronous Kafka workflow

[Download `demo11-student.zip`](handouts/demo11-student.zip)

> ##### KEY CONCEPT
>
> `202 Accepted` confirms that the API accepted the request. The worker may
> still be running. One stable `request_id` and `status_url` let the client
> observe completion and retry safely.

Demo 11A and the bounded Demo 11B Cloud path were verified on August 9, 2026.
Cloud state can change, so rerun Demo 11B before a future class.

## 1. What you will run

Demo 11 answers one question:

> After an API accepts work for Kafka, how does the client know what happened?

<div class="handout-flow" role="group" aria-label="Demo 11 teaching loop">
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Accept</span>
    <strong>POST returns 202</strong>
    <p>FastAPI validates the request and publishes one TripEventV1 event.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Process</span>
    <strong>Worker owns completion</strong>
    <p>One bounded worker step changes pending to completed.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Observe</span>
    <strong>GET returns status</strong>
    <p>The client reads current state by request_id.</p>
  </section>
</div>

| Step | Action | Time |
|---:|---|---:|
| 1 | Read the request, event, and status contracts | 3 minutes |
| 2 | POST, then GET `pending` | 3 minutes |
| 3 | Run one worker step, then GET `completed` | 5 minutes |
| 4 | Test an identical retry and a conflicting retry | 5 minutes |

## 2. How it continues Demo 05

Demo 05 ends after Kafka acknowledges the event. Demo 11 uses the same request,
event, schema, and topic, then adds processing status and result lookup.

```text
Demo 05                                      Demo 11
POST -> validate -> Kafka acknowledges  ->  worker -> status store -> GET
                      accepted                              completed or failed
```

Demo 11 runs a Python worker. Demo 09 showed that Python, ksqlDB, or Flink SQL
can own the downstream computation without changing the POST or GET contract.

## 3. Prerequisites

Start with the Demo 11 student ZIP. It includes the Demo 05 contract and
publisher modules. You do not need a separate Demo 05 download or process.

| Requirement | Demo 11A local | Demo 11B Cloud |
|---|---:|---:|
| Python 3.11 and packaged requirements | Required | Required |
| Kafka cluster and Schema Registry | No | Required |
| Kafka and Registry credentials in `.env` | No | Required |
| Existing Demo 05 topic | No | Optional; use `--create-topic` if missing |
| Demo 09, ksqlDB, Flink, or Confluent CLI | No | No |

## 4. Contracts, status, and retry

Keep the three contracts separate even when they share fields.

| Boundary | Contract | Responsibility |
|---|---|---|
| HTTP command | `CreateTripRequest` | Validate the client's request |
| Kafka event | `TripEventV1` | Retain the accepted business event |
| HTTP status | `TripStatusResponse` | Return current processing state |

| Response | Meaning | Client action |
|---|---|---|
| `202 Accepted` | First valid request was accepted; work remains | Follow `status_url` |
| `200 OK` | An identical retry found the existing request | Continue using the same ID |
| `409 Conflict` | The same ID has a different payload | Correct the request |
| `404 Not Found` | No status exists for that ID | Check the ID or POST response |
| `422 Unprocessable Entity` | Request validation failed | Fix the named fields |
| `503 Service Unavailable` | Kafka or Schema Registry did not acknowledge safely | Retry later with the same ID |

```text
same request_id + same payload       -> return the existing request
same request_id + different payload  -> reject with 409
```

Kafka retains the event history. The status store serves the GET request.

| Kafka event log | Status read model |
|---|---|
| Full ordered history | Current state by `request_id` |
| Supports replay | Supports fast point lookup |
| Source for rebuilding state | Replaceable projection |

## 5. Run Demo 11A locally

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
python demo11a_local_observable_roundtrip.py \
  --run-id lec11-demo11a
```

<ol class="handout-pipeline-list" aria-label="Expected Demo 11A trace">
  <li><span>Accept</span><strong>POST returns 202, pending, and one status URL.</strong></li>
  <li><span>Complete</span><strong>GET returns pending, the worker runs once, and GET returns completed.</strong></li>
  <li><span>Retry</span><strong>The same request returns 200 without another logical event.</strong></li>
  <li><span>Conflict</span><strong>A changed payload with the same ID returns 409.</strong></li>
</ol>

The command succeeds only when the report contains `contract_passed: true`.
The local publisher is a deterministic teaching substitute and provides no
Kafka delivery guarantee.

## 6. Run Demo 11B on Confluent Cloud

This path is optional. Copy `.env.example` to ignored `.env`, add the Kafka and
Schema Registry credentials, then run:

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b
```

If the Demo 05 topic is missing:

```bash
python demo11b_confluent_observable_roundtrip.py \
  --run-id lec11-demo11b \
  --create-topic
```

Demo 11B uses the Python Confluent client and runs one bounded check.

## 7. Common mistakes

| Symptom | Fix |
|---|---|
| The client treats `202` as completion | Follow `status_url` until the request reaches a terminal state |
| A retry creates another logical request | Reuse the original `request_id` and payload |
| A retry returns `409` | Use the original payload or create a new ID |
| GET returns `404` | Use the `status_url` returned by POST |
| Cloud POST returns `503` or the topic is missing | Check credentials and topic access; use `--create-topic` only when needed |

## 8. Finish safely

- [ ] Credential-free tests pass.
- [ ] Demo 11A reports `contract_passed: true`.
- [ ] You can explain `202 -> pending -> completed`.
- [ ] You can explain the `200` retry and `409` conflict.
- [ ] You can distinguish the Kafka event log from the status read model.
- [ ] Any Cloud run is bounded and uses the Python client.
- [ ] Stop local processes and keep `.env`, keys, secrets, and Cloud reports out of submissions.
