"""Provider-neutral routing boundary for Demo 07."""

from __future__ import annotations

from typing import Literal, Protocol

import httpx

from demo07_common import DEMO_ROUTE_FIXTURES, GeoPointV1, RouteMeasurement

DEFAULT_OSRM_BASE_URL = "https://router.project-osrm.org"


class RoutingError(RuntimeError):
    """Raised when a route cannot be calculated or validated."""


class RoutingClient(Protocol):
    """Minimal boundary implemented by live and deterministic providers."""

    def estimate(
        self,
        pickup: GeoPointV1,
        dropoff: GeoPointV1,
    ) -> RouteMeasurement: ...


# ============================================================================
# KEY CONCEPT
# The application contract names latitude and longitude. OSRM's URL uses
# longitude,latitude. Only this routing adapter owns that conversion.
# ============================================================================
def osrm_route_url(
    pickup: GeoPointV1,
    dropoff: GeoPointV1,
    *,
    base_url: str = DEFAULT_OSRM_BASE_URL,
) -> str:
    """Build an OSRM driving URL using its longitude,latitude order."""

    coordinates = (
        f"{pickup.longitude:.6f},{pickup.latitude:.6f};"
        f"{dropoff.longitude:.6f},{dropoff.latitude:.6f}"
    )
    return f"{base_url.rstrip('/')}/route/v1/driving/{coordinates}"


class OSRMRoutingClient:
    """Client for public OSRM driving-route distance and duration estimates.

    OSRM uses its configured road-network profile and weights. This adapter
    does not claim live-traffic ETA guarantees.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_OSRM_BASE_URL,
        timeout_seconds: float = 10.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds

    def estimate(
        self,
        pickup: GeoPointV1,
        dropoff: GeoPointV1,
    ) -> RouteMeasurement:
        url = osrm_route_url(pickup, dropoff, base_url=self.base_url)
        try:
            response = httpx.get(
                url,
                params={
                    "overview": "false",
                    "steps": "false",
                    "alternatives": "false",
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RoutingError(f"OSRM request failed: {exc}") from exc

        routes = payload.get("routes") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or payload.get("code") != "Ok"
            or not isinstance(routes, list)
            or not routes
        ):
            code = payload.get("code", "unknown") if isinstance(payload, dict) else "unknown"
            raise RoutingError(f"OSRM returned no usable route: {code}")
        first = routes[0]
        try:
            return RouteMeasurement(
                distance_meters=float(first["distance"]),
                duration_seconds=float(first["duration"]),
                provider="osrm",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RoutingError("OSRM route omitted valid distance or duration") from exc


class FixtureRoutingClient:
    """Offline lookup for predefined, reproducible teaching measurements.

    A fixture is a known input and expected output stored in the course code.
    It is not a third-party call and it never silently substitutes for OSRM.
    """

    def estimate(
        self,
        pickup: GeoPointV1,
        dropoff: GeoPointV1,
    ) -> RouteMeasurement:
        for known_pickup, known_dropoff, meters, seconds in DEMO_ROUTE_FIXTURES:
            if pickup == known_pickup and dropoff == known_dropoff:
                return RouteMeasurement(
                    distance_meters=meters,
                    duration_seconds=seconds,
                    provider="fixture",
                )
        raise RoutingError(
            "Fixture mode only supports the deterministic Demo 07 coordinates"
        )


def routing_client(
    mode: Literal["osrm", "fixture"],
    *,
    timeout_seconds: float = 10.0,
) -> RoutingClient:
    """Create an explicitly selected provider; never silently fall back."""

    if mode == "osrm":
        return OSRMRoutingClient(timeout_seconds=timeout_seconds)
    if mode == "fixture":
        return FixtureRoutingClient()
    raise ValueError(f"Unsupported routing mode: {mode}")
