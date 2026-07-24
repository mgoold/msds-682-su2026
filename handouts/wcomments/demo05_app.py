"""
================================================================================
DEMO 05 APP - THE FASTAPI APPLICATION FACTORY  (annotated tutorial copy)
================================================================================

READ demo05_common.py FIRST. Everything imported from it below - the request
model, the response model, the error type, request_to_event() - is defined and
explained there.

WHAT THIS FILE TEACHES
    How to build ONE FastAPI application that can run in two completely
    different modes (credential-free "local" for 05A/05B, or real Confluent
    Cloud for 05C/05D) WITHOUT duplicating a single route. The trick is a
    small abstraction called a "publisher": something with a `publish()` and a
    `close()` method. The routes below only ever talk to that abstraction, so
    they neither know nor care whether a message ends up sitting in a Python
    list (local) or actually inside a Kafka topic (Cloud).

THE SEVEN FASTAPI IDEAS THIS FILE PUTS TOGETHER
    application    - the single FastAPI(...) object created by create_app()
    path operation - the two `@app.get`/`@app.post` decorated functions
    request model  - CreateTripRequest (parses and validates the POST body)
    response model - TripAcceptedResponse (shapes what create_trip() returns)
    status code    - 202 Accepted for a successful post, 503 for a publisher
                     failure, 422 (handled automatically by FastAPI) for bad
                     input
    lifespan       - the async context manager below that owns the publisher
                     for the whole life of the running application
    OpenAPI        - automatically generated from the type hints and models
                     above; visible at /docs (Demo 05B) or /openapi.json
                     (Demo 05A's automated check)

WHY THE PUBLISHER LIVES IN "LIFESPAN" AND NOT IN THE ROUTE
    A naive version of create_trip() might do `producer = Producer(config)`
    on every request. That would be wrong for the same reason you would not
    open a new database connection per request: constructing a Kafka producer
    is expensive (TLS handshake, SASL auth, a metadata fetch - see Demo 02's
    notes on Producer() startup cost) and it starts a background I/O thread.
    FastAPI's lifespan hook runs exactly once per running process: once before
    the server starts accepting requests, and once after it stops accepting
    them. That is precisely the lifetime a Kafka client should have.
================================================================================
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI, HTTPException, Request, status

from demo05_common import (
    CreateTripRequest,
    PublishError,
    PublishReceipt,
    TripAcceptedResponse,
    request_to_event,
    topic_name,
)
from trip_event_contract import TripEventV1


class AsyncTripPublisher(Protocol):
    """Small application boundary implemented by local and Kafka publishers.

    A Protocol is structural typing: ANY class with a matching `receipts`
    attribute and matching `publish`/`close` async methods satisfies this,
    with no inheritance required. LocalTripPublisher (below) and
    AsyncAvroTripPublisher (in demo05_kafka.py) are unrelated classes that both
    happen to satisfy it. create_app() is written entirely against this
    narrow interface, which is what lets the exact same routes work in both
    modes.
    """

    receipts: list[PublishReceipt]

    async def publish(self, event: TripEventV1) -> PublishReceipt: ...

    async def close(self) -> None: ...


# A PublisherFactory is an async, argument-free callable that BUILDS a
# publisher. It is a factory rather than a ready-made instance because
# building the real Kafka version involves awaiting network setup (opening a
# Schema Registry client, the AIO producer) - work that must happen inside the
# lifespan's own async context, not before create_app() is even called.
PublisherFactory = Callable[[], Awaitable[AsyncTripPublisher]]


class LocalTripPublisher:
    """Credential-free publisher used to learn FastAPI before Cloud setup.

    THIS CLASS TALKS TO NOTHING BUT MEMORY. No sockets, no credentials, no
    Kafka library calls. It exists purely so Demo 05A and 05B can teach every
    FastAPI concept above - request models, response models, status codes,
    lifespan, OpenAPI - before a student has created a single Confluent Cloud
    resource. Demo 05C/05D swap this for AsyncAvroTripPublisher and the routes
    do not change at all.
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.events: list[TripEventV1] = []
        self.receipts: list[PublishReceipt] = []
        self.closed = False

    async def publish(self, event: TripEventV1) -> PublishReceipt:
        """Record one validated event in memory."""

        if self.closed:
            raise PublishError("The local publisher is closed")
        self.events.append(event)
        receipt = PublishReceipt(
            # "local" is the delivery mode a real API client would see in the
            # response body - an honest signal that no broker was involved.
            delivery="local",
            topic=self.topic,
            key=event.trip_id,
        )
        self.receipts.append(receipt)
        return receipt

    async def close(self) -> None:
        """Mark the local lifecycle resource closed."""

        self.closed = True


def create_app(
    publisher_factory: PublisherFactory,
    *,
    mode: str,
    app_title: str = "MSDS 682 Demo 05 Streaming API",
) -> FastAPI:
    """Create one thin API around a lifespan-managed publisher.

    Both create_local_app() (below) and Demo 05C/05D's Cloud builders call
    this SAME function, passing only a different publisher_factory. Every
    route, status code, and response model is defined exactly once, here.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ====================================================================
        # KEY CONCEPT
        # One publisher belongs to the application lifecycle. Do not construct
        # a new Kafka producer inside every request handler.
        # ====================================================================
        # Everything before `yield` runs ONCE, at process startup, before
        # Uvicorn (or FastAPI's TestClient) begins serving requests.
        publisher = await publisher_factory()

        # app.state is FastAPI's sanctioned place for per-application shared
        # objects. Storing the publisher here - rather than a module-level
        # global - keeps it scoped to THIS app instance, which matters because
        # tests may construct multiple apps in the same process.
        app.state.publisher = publisher
        try:
            # Control returns to FastAPI here for the entire time the app is
            # serving requests. Nothing after this line runs until shutdown.
            yield
        finally:
            # Runs once, at shutdown (Ctrl+C for 05B/05D, or TestClient's
            # context-manager exit for 05A/05C) - guaranteed even if a request
            # handler raised, because it is in a `finally`.
            await publisher.close()

    app = FastAPI(
        title=app_title,
        version="2026.1",
        description=(
            "Validate one HTTP request, map it to TripEventV1, and publish the "
            "event through the configured local or Confluent boundary."
        ),
        # Registering the lifespan function here is what makes FastAPI call it
        # automatically around the app's running lifetime.
        lifespan=lifespan,
    )

    @app.get("/health", tags=["operations"])
    async def health() -> dict[str, str]:
        """Return process liveness without exposing credentials.

        Deliberately tiny: it proves the process is up and reports which mode
        it is running in ("local" or "confluent"), and nothing else. A health
        endpoint is not the place to reveal connection details.
        """

        return {"status": "ok", "mode": mode}

    @app.post(
        "/trip-requests",
        # response_model tells FastAPI (and OpenAPI) the exact shape of a
        # successful response, and FastAPI will validate the return value
        # against it - a bug that produced a malformed response would be
        # caught here rather than shipped to a client.
        response_model=TripAcceptedResponse,
        # 202 ACCEPTED, not 200 or 201: it communicates "your request was
        # accepted for processing", which is honest here because a completed
        # publish() call means the message was accepted by the configured
        # boundary (local memory or a broker), not that every downstream
        # consumer has finished handling it. See demo05.md section 11 for the
        # full acceptance-vs-completion distinction this status code encodes.
        status_code=status.HTTP_202_ACCEPTED,
        tags=["trip requests"],
    )
    async def create_trip(
        payload: CreateTripRequest,
        request: Request,
    ) -> TripAcceptedResponse:
        """Validate HTTP input, build the event, and await publisher acceptance.

        By the time this function body runs, FastAPI has ALREADY parsed and
        validated `payload` against CreateTripRequest - an invalid POST body
        never reaches this line; the caller gets an automatic 422 instead.
        """

        event = request_to_event(payload)

        # Retrieve the ONE publisher this whole application shares, created
        # once in lifespan() above. Every request reuses it; none constructs
        # its own.
        publisher: AsyncTripPublisher = request.app.state.publisher
        try:
            # ================================================================
            # IMPORTANT NOTE
            # Await publisher acceptance before returning 202. In Cloud mode,
            # acceptance means the broker delivery future has completed.
            # ================================================================
            receipt = await publisher.publish(event)
        except PublishError as exc:
            # Keep internal broker and credential details out of the HTTP body.
            # A caller sees only "temporarily unavailable"; the real cause
            # (timeout, connection refused, ...) stays server-side, attached
            # via `from exc` so it is still visible in server logs/tracebacks.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="The event publisher is temporarily unavailable.",
            ) from exc

        # Assemble the THIN response contract from demo05_common.py - not the
        # richer internal PublishReceipt, and never the raw TripEventV1.
        return TripAcceptedResponse(
            status="accepted",
            request_id=payload.request_id,
            trip_id=event.trip_id,
            topic=receipt.topic,
            delivery=receipt.delivery,
        )

    return app


def create_local_app(*, topic: str | None = None) -> FastAPI:
    """Build the credential-free app used by Demo 05A and 05B.

    Note this function never reads Kafka or Schema Registry configuration -
    it does not even import anything capable of doing so. That absence is
    itself the proof, referenced in demo05.md, that 05A/05B require no Cloud
    credentials at all.
    """

    selected_topic = topic or topic_name()

    async def factory() -> LocalTripPublisher:
        return LocalTripPublisher(selected_topic)

    return create_app(factory, mode="local")
