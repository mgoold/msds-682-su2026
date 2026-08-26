from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from demo05_app import LocalTripPublisher
from demo05_common import PublishError, deterministic_requests
from demo11_app import create_local_observable_app, create_observable_app
from demo11_common import SQLiteTripStatusRepository, complete_trip_event


def fixed_clock(*rows: datetime):
    values = iter(rows)
    return lambda: next(values)


def test_observable_lifecycle_retry_and_conflict() -> None:
    base = datetime(2026, 8, 10, 17, 30, tzinfo=UTC)
    repository = SQLiteTripStatusRepository()
    app = create_local_observable_app(
        repository=repository,
        clock=fixed_clock(base),
    )
    payload = deterministic_requests(1, seed_offset=11)[0]

    with TestClient(app) as client:
        submitted = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        assert submitted.status_code == 202
        assert submitted.json() == {
            "request_id": payload.request_id,
            "trip_id": payload.request_id.replace("request_", "trip_"),
            "status": "pending",
            "status_url": f"/trip-requests/{payload.request_id}",
            "delivery": "local",
            "created": True,
        }
        pending = client.get(submitted.json()["status_url"])
        assert pending.status_code == 200
        assert pending.json()["status"] == "pending"
        assert pending.json()["result"] is None

        publisher = app.state.publisher
        assert isinstance(publisher, LocalTripPublisher)
        assert len(publisher.events) == 1
        complete_trip_event(
            repository,
            publisher.events[0],
            compute_owner="python",
            clock=fixed_clock(base + timedelta(seconds=3)),
        )
        completed = client.get(submitted.json()["status_url"])
        assert completed.json()["status"] == "completed"
        assert completed.json()["result"] == {
            "compute_owner": "python",
            "event_type": "trip_requested",
        }

        duplicate = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        assert duplicate.status_code == 200
        assert duplicate.json()["created"] is False
        assert duplicate.json()["status"] == "completed"
        assert len(publisher.events) == 1

        changed = payload.model_dump(mode="json")
        changed["zone"] = "south" if changed["zone"] != "south" else "north"
        conflict = client.post("/trip-requests", json=changed)
        assert conflict.status_code == 409
        assert len(publisher.events) == 1

    repository.close()


def test_validation_unknown_and_openapi_contracts() -> None:
    repository = SQLiteTripStatusRepository()
    app = create_local_observable_app(repository=repository)
    payload = deterministic_requests(1)[0].model_dump(mode="json")
    with TestClient(app) as client:
        invalid = dict(payload)
        invalid["extra"] = "forbidden"
        assert client.post("/trip-requests", json=invalid).status_code == 422
        assert client.get("/trip-requests/request_9999").status_code == 404
        paths = client.get("/openapi.json").json()["paths"]
        assert "/trip-requests" in paths
        assert "/trip-requests/{request_id}" in paths
        publisher = app.state.publisher
        assert isinstance(publisher, LocalTripPublisher)
        assert publisher.events == []
    repository.close()


def test_publish_failure_returns_secret_free_503_without_status_row() -> None:
    class FailingPublisher:
        receipts = []

        async def publish(self, _event):
            raise PublishError("secret broker detail")

        async def close(self) -> None:
            return None

    async def factory() -> FailingPublisher:
        return FailingPublisher()

    repository = SQLiteTripStatusRepository()
    app = create_observable_app(
        factory,
        repository=repository,
        mode="failure-test",
    )
    payload = deterministic_requests(1)[0]
    with TestClient(app) as client:
        response = client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        )
        assert response.status_code == 503
        assert response.json() == {
            "detail": "The event publisher is temporarily unavailable."
        }
        assert "secret" not in response.text
        assert repository.get(payload.request_id) is None
    repository.close()


def test_downstream_failure_is_terminal_and_secret_free() -> None:
    base = datetime(2026, 8, 10, 17, 30, tzinfo=UTC)
    repository = SQLiteTripStatusRepository()
    app = create_local_observable_app(
        repository=repository,
        clock=fixed_clock(base),
    )
    payload = deterministic_requests(1, seed_offset=31)[0]
    with TestClient(app) as client:
        assert client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        ).status_code == 202
        repository.mark_failed(
            request_id=payload.request_id,
            error_code="processing_failed",
            updated_at=base + timedelta(seconds=4),
        )
        failed = client.get(f"/trip-requests/{payload.request_id}")
        assert failed.status_code == 200
        assert failed.json()["status"] == "failed"
        assert failed.json()["result"] == {"error_code": "processing_failed"}
        assert "secret" not in failed.text
    repository.close()


def test_sqlite_status_survives_repository_reopen(tmp_path: Path) -> None:
    database = tmp_path / "demo11-status.sqlite3"
    base = datetime(2026, 8, 10, 17, 30, tzinfo=UTC)
    payload = deterministic_requests(1, seed_offset=21)[0]
    first = SQLiteTripStatusRepository(database)
    app = create_local_observable_app(
        repository=first,
        clock=fixed_clock(base),
    )
    with TestClient(app) as client:
        assert client.post(
            "/trip-requests",
            json=payload.model_dump(mode="json"),
        ).status_code == 202
    first.close()

    reopened = SQLiteTripStatusRepository(database)
    stored = reopened.get(payload.request_id)
    assert stored is not None
    assert stored.status == "pending"
    assert stored.trip_id == payload.request_id.replace("request_", "trip_")
    reopened.close()


def test_demo11_reuses_demo05_contract_and_topic_owners() -> None:
    handouts = Path(__file__).resolve().parents[1]
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in handouts.glob("demo11*.py")
    )
    assert "class CreateTripRequest" not in sources
    assert "class TripEventV1" not in sources
    assert "DEMO11_TOPIC" not in sources
    assert "demo05_common" in sources
