"""Status contract and repository for Demo 11's observable async API."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from demo05_common import CreateTripRequest, PublishReceipt
from trip_event_contract import TripEventV1


WorkflowStatus = Literal["pending", "completed", "failed"]
Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return one timezone-aware UTC timestamp."""

    return datetime.now(UTC)


class TripSubmissionResponse(BaseModel):
    """HTTP representation returned when a command is accepted or reused."""

    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: str
    trip_id: str
    status: WorkflowStatus
    status_url: str
    delivery: Literal["local", "broker_acknowledged"]
    created: bool


class TripStatusResponse(BaseModel):
    """Current read-model representation for one asynchronous request."""

    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: str
    trip_id: str
    status: WorkflowStatus
    updated_at: AwareDatetime
    result: dict[str, str] | None = None


class StoredTripRequest(BaseModel):
    """Internal row owned by the status repository."""

    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: str
    request_fingerprint: str
    trip_id: str
    status: WorkflowStatus
    delivery: Literal["local", "broker_acknowledged"]
    updated_at: AwareDatetime
    result: dict[str, str] | None = None

    def submission_response(self, *, created: bool) -> TripSubmissionResponse:
        return TripSubmissionResponse(
            request_id=self.request_id,
            trip_id=self.trip_id,
            status=self.status,
            status_url=f"/trip-requests/{self.request_id}",
            delivery=self.delivery,
            created=created,
        )

    def status_response(self) -> TripStatusResponse:
        return TripStatusResponse(
            request_id=self.request_id,
            trip_id=self.trip_id,
            status=self.status,
            updated_at=self.updated_at,
            result=self.result,
        )


def request_fingerprint(request: CreateTripRequest) -> str:
    """Hash canonical request JSON for an idempotency comparison."""

    canonical = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class SQLiteTripStatusRepository:
    """One small read-model owner backed by SQLite.

    The repository is the only module that writes workflow status. The API and
    worker express intent through its methods instead of issuing SQL directly.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trip_request_status (
                request_id TEXT PRIMARY KEY,
                request_fingerprint TEXT NOT NULL,
                trip_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending','completed','failed')),
                delivery TEXT NOT NULL CHECK (delivery IN ('local','broker_acknowledged')),
                updated_at TEXT NOT NULL,
                result_json TEXT
            )
            """
        )
        self._connection.commit()

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> StoredTripRequest:
        result = json.loads(row["result_json"]) if row["result_json"] else None
        return StoredTripRequest(
            request_id=row["request_id"],
            request_fingerprint=row["request_fingerprint"],
            trip_id=row["trip_id"],
            status=row["status"],
            delivery=row["delivery"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
            result=result,
        )

    def get(self, request_id: str) -> StoredTripRequest | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM trip_request_status WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return self._row_to_record(row) if row else None

    def add_pending(
        self,
        *,
        request: CreateTripRequest,
        event: TripEventV1,
        receipt: PublishReceipt,
        updated_at: datetime,
    ) -> StoredTripRequest:
        """Materialize one accepted request after publisher acknowledgement."""

        fingerprint = request_fingerprint(request)
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO trip_request_status (
                    request_id, request_fingerprint, trip_id, status,
                    delivery, updated_at, result_json
                ) VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                """,
                (
                    request.request_id,
                    fingerprint,
                    event.trip_id,
                    receipt.delivery,
                    updated_at.astimezone(UTC).isoformat(),
                ),
            )
            self._connection.commit()
        record = self.get(request.request_id)
        if record is None:  # pragma: no cover - defensive database boundary
            raise RuntimeError("Accepted request was not materialized")
        return record

    def mark_completed(
        self,
        *,
        request_id: str,
        result: dict[str, str],
        updated_at: datetime,
    ) -> StoredTripRequest:
        """Move one existing request to completed with a small result."""

        result_json = json.dumps(result, sort_keys=True, separators=(",", ":"))
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE trip_request_status
                SET status = 'completed', updated_at = ?, result_json = ?
                WHERE request_id = ?
                """,
                (updated_at.astimezone(UTC).isoformat(), result_json, request_id),
            )
            self._connection.commit()
        if cursor.rowcount != 1:
            raise KeyError(f"Unknown request_id: {request_id}")
        record = self.get(request_id)
        if record is None:  # pragma: no cover - defensive database boundary
            raise RuntimeError("Completed request disappeared")
        return record

    def mark_failed(
        self,
        *,
        request_id: str,
        error_code: str,
        updated_at: datetime,
    ) -> StoredTripRequest:
        """Move one accepted request to a secret-free terminal failure."""

        if not error_code or any(character.isspace() for character in error_code):
            raise ValueError("error_code must be one nonempty token")
        result_json = json.dumps(
            {"error_code": error_code},
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE trip_request_status
                SET status = 'failed', updated_at = ?, result_json = ?
                WHERE request_id = ?
                """,
                (updated_at.astimezone(UTC).isoformat(), result_json, request_id),
            )
            self._connection.commit()
        if cursor.rowcount != 1:
            raise KeyError(f"Unknown request_id: {request_id}")
        record = self.get(request_id)
        if record is None:  # pragma: no cover - defensive database boundary
            raise RuntimeError("Failed request disappeared")
        return record

    def close(self) -> None:
        with self._lock:
            self._connection.close()


def request_id_from_event(event: TripEventV1) -> str:
    """Reverse Demo 05's documented request-to-trip identity mapping."""

    if not event.trip_id.startswith("trip_"):
        raise ValueError("Demo 11 requires the Demo 05 trip_id contract")
    return event.trip_id.replace("trip_", "request_", 1)


def complete_trip_event(
    repository: SQLiteTripStatusRepository,
    event: TripEventV1,
    *,
    compute_owner: str,
    clock: Clock = utc_now,
) -> TripStatusResponse:
    """Bounded worker effect: mark one validated event as completed."""

    record = repository.mark_completed(
        request_id=request_id_from_event(event),
        result={
            "compute_owner": compute_owner,
            "event_type": event.event_type,
        },
        updated_at=clock(),
    )
    return record.status_response()
