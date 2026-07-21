"""FastAPI application factory shared by Demo 05 local and Cloud paths."""

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
    """Small application boundary implemented by local and Kafka publishers."""

    receipts: list[PublishReceipt]

    async def publish(self, event: TripEventV1) -> PublishReceipt: ...

    async def close(self) -> None: ...


PublisherFactory = Callable[[], Awaitable[AsyncTripPublisher]]


class LocalTripPublisher:
    """Credential-free publisher used to learn FastAPI before Cloud setup."""

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
    """Create one thin API around a lifespan-managed publisher."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ====================================================================
        # KEY CONCEPT
        # One publisher belongs to the application lifecycle. Do not construct
        # a new Kafka producer inside every request handler.
        # ====================================================================
        publisher = await publisher_factory()
        app.state.publisher = publisher
        try:
            yield
        finally:
            await publisher.close()

    app = FastAPI(
        title=app_title,
        version="2026.1",
        description=(
            "Validate one HTTP request, map it to TripEventV1, and publish the "
            "event through the configured local or Confluent boundary."
        ),
        lifespan=lifespan,
    )

    @app.get("/health", tags=["operations"])
    async def health() -> dict[str, str]:
        """Return process liveness without exposing credentials."""

        return {"status": "ok", "mode": mode}

    @app.post(
        "/trip-requests",
        response_model=TripAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["trip requests"],
    )
    async def create_trip(
        payload: CreateTripRequest,
        request: Request,
    ) -> TripAcceptedResponse:
        """Validate HTTP input, build the event, and await publisher acceptance."""

        event = request_to_event(payload)
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
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="The event publisher is temporarily unavailable.",
            ) from exc
        return TripAcceptedResponse(
            status="accepted",
            request_id=payload.request_id,
            trip_id=event.trip_id,
            topic=receipt.topic,
            delivery=receipt.delivery,
        )

    return app


def create_local_app(*, topic: str | None = None) -> FastAPI:
    """Build the credential-free app used by Demo 05A and 05B."""

    selected_topic = topic or topic_name()

    async def factory() -> LocalTripPublisher:
        return LocalTripPublisher(selected_topic)

    return create_app(factory, mode="local")
