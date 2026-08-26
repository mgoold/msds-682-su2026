"""FastAPI application that adds observable completion to Demo 05."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, status

from demo05_app import AsyncTripPublisher, LocalTripPublisher, PublisherFactory
from demo05_common import CreateTripRequest, PublishError, request_to_event, topic_name
from demo11_common import (
    Clock,
    SQLiteTripStatusRepository,
    TripStatusResponse,
    TripSubmissionResponse,
    request_fingerprint,
    utc_now,
)


def create_observable_app(
    publisher_factory: PublisherFactory,
    *,
    repository: SQLiteTripStatusRepository,
    mode: str,
    clock: Clock = utc_now,
) -> FastAPI:
    """Create one API whose contract survives a compute-owner change."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        publisher = await publisher_factory()
        app.state.publisher = publisher
        app.state.accept_lock = asyncio.Lock()
        try:
            yield
        finally:
            await publisher.close()

    app = FastAPI(
        title="MSDS 682 Demo 11 Observable Streaming API",
        version="2026.1",
        description=(
            "Continue Demo 05 from broker acknowledgement to an observable "
            "pending or completed workflow state."
        ),
        lifespan=lifespan,
    )
    app.state.status_repository = repository

    @app.get("/health", tags=["operations"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "mode": mode}

    @app.post(
        "/trip-requests",
        response_model=TripSubmissionResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["trip requests"],
    )
    async def create_trip(
        payload: CreateTripRequest,
        request: Request,
        response: Response,
    ) -> TripSubmissionResponse:
        """Accept one command once, then return its stable status URL."""

        event = request_to_event(payload)
        repository_owner: SQLiteTripStatusRepository = (
            request.app.state.status_repository
        )
        publisher: AsyncTripPublisher = request.app.state.publisher

        # The lock keeps the local teaching example honest for concurrent
        # retries. A production service would use an atomic durable reservation.
        async with request.app.state.accept_lock:
            existing = repository_owner.get(payload.request_id)
            if existing is not None:
                if existing.request_fingerprint != request_fingerprint(payload):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            "request_id already belongs to a different payload."
                        ),
                    )
                response.status_code = status.HTTP_200_OK
                return existing.submission_response(created=False)

            try:
                receipt = await publisher.publish(event)
            except PublishError as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="The event publisher is temporarily unavailable.",
                ) from exc

            record = repository_owner.add_pending(
                request=payload,
                event=event,
                receipt=receipt,
                updated_at=clock(),
            )
            return record.submission_response(created=True)

    @app.get(
        "/trip-requests/{request_id}",
        response_model=TripStatusResponse,
        tags=["trip requests"],
    )
    async def trip_status(request_id: str, request: Request) -> TripStatusResponse:
        """Read current workflow state without scanning the Kafka log."""

        repository_owner: SQLiteTripStatusRepository = (
            request.app.state.status_repository
        )
        record = repository_owner.get(request_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Unknown request_id.",
            )
        return record.status_response()

    return app


def create_local_observable_app(
    *,
    repository: SQLiteTripStatusRepository,
    topic: str | None = None,
    clock: Clock = utc_now,
) -> FastAPI:
    """Build the credential-free application used by Demo 11A."""

    selected_topic = topic or topic_name()

    async def factory() -> LocalTripPublisher:
        return LocalTripPublisher(selected_topic)

    return create_observable_app(
        factory,
        repository=repository,
        mode="local-teaching-double",
        clock=clock,
    )

