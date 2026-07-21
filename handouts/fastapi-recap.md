# FastAPI recap for Demo 05

This is the first FastAPI unit in the Summer 2026 course. The goal is to read
and run a small API, not to memorize the framework.

## Current course environment

| Component | Course version | Why it is here |
|---|---:|---|
| Python | 3.11.14 | Shared course runtime |
| FastAPI | 0.139.0 | API framework |
| Pydantic | 2.13.4 | Request, response, and event validation |
| Uvicorn | 0.50.0 | Local ASGI server |
| httpx2 | 2.7.0 | Current test client dependency used by FastAPI 0.139 |
| confluent-kafka | 2.15.0 | Kafka, Schema Registry, and native AIO clients |

Install the exact public baseline:

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Seven ideas to recognize

| Idea | Meaning in Demo 05 |
|---|---|
| FastAPI application | The Python object that owns routes and lifespan resources |
| Path operation | One HTTP method plus path, such as `POST /trip-requests` |
| Request model | Pydantic parses and validates incoming JSON |
| Response model | Pydantic documents and validates the returned shape |
| Status code | `202`, `422`, and `503` communicate different outcomes |
| Lifespan | Opens one publisher at startup and closes it at shutdown |
| OpenAPI | Machine-readable API contract used to generate `/docs` |

## Request flow

```text
HTTP JSON
  -> CreateTripRequest validation
  -> request_to_event(...)
  -> strict TripEventV1
  -> publisher
  -> TripAcceptedResponse
```

The HTTP request and Kafka event are different contracts. A client sends
`request_id`, while the application maps it to the event's stable `trip_id` and
adds `event_type="trip_requested"`.

## Run the local service

```bash
python demo05b_fastapi_local_service.py --port 8001
```

Open:

- Swagger UI: `http://127.0.0.1:8001/docs`
- OpenAPI JSON: `http://127.0.0.1:8001/openapi.json`
- Health endpoint: `http://127.0.0.1:8001/health`

Try one request:

```bash
curl -X POST http://127.0.0.1:8001/trip-requests \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "request_5900",
    "rider_id": "rider_590",
    "requested_at": "2026-07-20T17:00:00Z",
    "zone": "north"
  }'
```

The local response uses `"delivery": "local"`. The Cloud app uses
`"delivery": "broker_acknowledged"` only after Kafka acknowledges the record.

## Status codes used here

| Code | Meaning |
|---:|---|
| `200 OK` | Health request succeeded |
| `202 Accepted` | The API accepted the request; downstream trip processing is not complete |
| `422 Unprocessable Content` | Request JSON did not satisfy `CreateTripRequest` |
| `503 Service Unavailable` | The publisher could not confirm acceptance |

## `async def` does not fix blocking code

FastAPI already owns an asyncio event loop. Demo 05 therefore uses the native
`AIOProducer` and async Schema Registry serializer for the Cloud route. Merely
placing a blocking `Consumer.poll()` or `Producer.flush()` inside `async def`
would still block the event loop.

The bounded verification consumer is a separate command-line worker, so the
standard `Consumer` remains the simpler client there.

## Common mistakes

- Do not create a producer inside every request handler.
- Do not put credentials in source code, responses, logs, or reports.
- Do not return `202` before deciding what publisher acceptance means.
- Do not assume an HTTP response means a downstream consumer finished.
- Do not use the HTTP request model as a substitute for the Kafka event model.
- Do not run an unbounded consumer inside a FastAPI route.

## Student checkpoint

Before continuing, explain these two statements:

1. FastAPI owns the HTTP boundary; Kafka owns the durable event transport.
2. One application lifespan owns one reusable producer; a request handler only
   validates, maps, publishes, and returns.
