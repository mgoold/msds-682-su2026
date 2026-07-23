"""Thin FastAPI boundary around one lifespan-managed event publisher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI, HTTPException, Request, status

from contracts import (
    CreateTripRequest,
    PublishError,
    PublishReceipt,
    TripAcceptedResponse,
    TripEventV1,
    request_to_event,
)


class AsyncTripPublisher(Protocol):
    """Small boundary implemented by the Cloud publisher and test double."""

    async def publish(self, event: TripEventV1) -> PublishReceipt: ...

    async def close(self) -> None: ...


PublisherFactory = Callable[[], Awaitable[AsyncTripPublisher]]


def create_app(publisher_factory: PublisherFactory) -> FastAPI:
    """Create the Assignment 2 API without constructing global clients."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # One publisher belongs to the application lifecycle. Do not create a
        # producer inside each HTTP request.
        publisher = await publisher_factory()
        app.state.publisher = publisher
        try:
            yield
        finally:
            await publisher.close()

    app = FastAPI(
        title="MSDS 682 Assignment 2 Input API",
        version="2026.1",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Return process liveness without exposing connection details."""

        return {"status": "ok"}

    @app.post(
        "/trip-requests",
        response_model=TripAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_trip(
        payload: CreateTripRequest,
        request: Request,
    ) -> TripAcceptedResponse:
        """Validate, map, publish, and return only after acknowledgement."""

        # ==================== CODE START HERE ====================
        # TODO:
        # 1. map payload with request_to_event();
        # 2. await the lifespan publisher;
        # 3. convert PublishError to the provided HTTP 503 boundary; and
        # 4. return TripAcceptedResponse from the receipt.
        raise NotImplementedError("Implement the FastAPI publish boundary")
        # ===================== CODE ENDS HERE =====================

    return app
