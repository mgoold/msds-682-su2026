"""Shared contracts, business rules, model artifact, and topic registry for Demo 07."""

from __future__ import annotations

import hashlib
import json
import os
import random
import zlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Literal

from confluent_kafka.schema_registry import topic_subject_name_strategy
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from confluent_demo_common import load_dotenv_for_demo, validate_run_id

BUNDLE_DIR = Path(__file__).resolve().parent
TRIP_REQUEST_SCHEMA_PATH = BUNDLE_DIR / "demo07_trip_request_v1.avsc"
FARE_QUOTE_SCHEMA_PATH = BUNDLE_DIR / "demo07_fare_quote_v1.avsc"
TRIP_OUTCOME_SCHEMA_PATH = BUNDLE_DIR / "demo07_trip_outcome_v1.avsc"
PRICING_EVALUATION_SCHEMA_PATH = (
    BUNDLE_DIR / "demo07_pricing_evaluation_v1.avsc"
)

# ============================================================================
# KEY CONCEPT
# One registry owns every Demo 07 Kafka topic. The online pricing path uses
# requests and quotes. Outcomes and evaluations close the delayed-label loop.
# ============================================================================
DEFAULT_TRIP_REQUESTS_TOPIC = "msds682.demo07.ml-trip-requests-avro.v1"
DEFAULT_FARE_QUOTES_TOPIC = "msds682.demo07.ml-fare-quotes-avro.v1"
DEFAULT_TRIP_OUTCOMES_TOPIC = "msds682.demo07.ml-trip-outcomes-avro.v1"
DEFAULT_PRICING_EVALUATIONS_TOPIC = (
    "msds682.demo07.ml-pricing-evaluations-avro.v1"
)

CURRENCY = "USD"
TARGET_MARKUP_PCT = Decimal("20")
TARGET_MARKUP_MULTIPLIER = Decimal("1.20")
TARGET_MARKUP_TOLERANCE_PP = Decimal("2")

# Rule v1 is the original transparent fare heuristic. Money is stored in
# integer cents; distance is miles and duration is minutes.
RULE_V1_PER_MILE_CENTS = Decimal("350")
RULE_V1_PER_MINUTE_CENTS = Decimal("20")
RULE_V1_MODEL_VERSION = "rule-v1"
RIDGE_V2_MODEL_VERSION = "ridge-v2"

# Synthetic delayed outcomes use this hidden cost-generating process. Money is
# stored in cents, distance is miles, and time cost is expressed per hour. The
# model is trained from examples; it does not read these constants.
ACTUAL_FIXED_COST_CENTS = Decimal("500")
ACTUAL_PER_MILE_COST_CENTS = Decimal("75")
ACTUAL_PER_HOUR_COST_CENTS = Decimal("2000")

METERS_PER_MILE = Decimal("1609.344")
SECONDS_PER_MINUTE = Decimal("60")
REQUEST_BASE_TIME = datetime(2026, 7, 27, 17, 30, tzinfo=UTC)
TRAINING_SEED = 682
TRAINING_RECORDS = 160
TRAINING_COUNT = 120
VALIDATION_COUNT = 40
MODEL_FEATURE_NAMES = ["estimated_miles", "estimated_minutes"]

PricingMethod = Literal["rule-v1", "ridge-v2"]


class GeoPointV1(BaseModel):
    """Validated latitude/longitude used by Demo 07 request and quote contracts."""

    model_config = ConfigDict(extra="forbid", strict=True)

    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)


class TripContextV1(BaseModel):
    """Fields that preserve one trip identity across request and quote events."""

    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: str = Field(min_length=1, max_length=80)
    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")
    requested_at: AwareDatetime
    pickup: GeoPointV1
    dropoff: GeoPointV1

    @field_validator("run_id")
    @classmethod
    def valid_run_id(cls, value: str) -> str:
        return validate_run_id(value)

    @field_validator("requested_at")
    @classmethod
    def normalize_requested_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class TripRequestV1(TripContextV1):
    """One immutable trip request published to the online input topic."""


class FareQuoteV1(TripContextV1):
    """One versioned fare quote produced from routing features."""

    source_record_id: str = Field(min_length=1)
    quote_id: str = Field(min_length=1)
    estimated_miles: float = Field(gt=0.0)
    estimated_minutes: float = Field(gt=0.0)
    predicted_cost_cents: int | None = Field(default=None, ge=0)
    fare_cents: int = Field(ge=0)
    currency: Literal["USD"]
    pricing_method: PricingMethod
    model_version: PricingMethod
    model_feature_names: list[str]
    target_markup_pct: float = Field(gt=0.0)
    routing_provider: str = Field(min_length=1)
    quoted_at: AwareDatetime

    @field_validator("quoted_at")
    @classmethod
    def normalize_quoted_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class TripOutcomeV1(BaseModel):
    """Delayed realized trip cost used as a synthetic supervised label."""

    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: str = Field(min_length=1, max_length=80)
    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")
    actual_miles: float = Field(gt=0.0)
    actual_minutes: float = Field(gt=0.0)
    actual_cost_cents: int = Field(gt=0)
    currency: Literal["USD"]
    completed_at: AwareDatetime
    outcome_source: Literal["synthetic-delayed-v1"]

    @field_validator("run_id")
    @classmethod
    def valid_run_id(cls, value: str) -> str:
        return validate_run_id(value)

    @field_validator("completed_at")
    @classmethod
    def normalize_completed_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class PricingEvaluationV1(BaseModel):
    """Per-quote business evaluation after the matching outcome arrives."""

    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: str = Field(min_length=1, max_length=80)
    trip_id: str = Field(pattern=r"^trip_[0-9]{4}$")
    quote_id: str = Field(min_length=1)
    model_version: PricingMethod
    fare_cents: int = Field(ge=0)
    actual_cost_cents: int = Field(gt=0)
    profit_cents: int
    realized_markup_pct: float
    target_markup_pct: float = Field(gt=0.0)
    markup_error_pp: float
    absolute_markup_error_pp: float = Field(ge=0.0)
    cost_prediction_error_cents: int | None = None
    within_target_tolerance: bool
    evaluated_at: AwareDatetime

    @field_validator("run_id")
    @classmethod
    def valid_run_id(cls, value: str) -> str:
        return validate_run_id(value)

    @field_validator("evaluated_at")
    @classmethod
    def normalize_evaluated_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class RouteMeasurement(BaseModel):
    """Provider-neutral route result returned by a routing boundary."""

    model_config = ConfigDict(extra="forbid", strict=True)

    distance_meters: float = Field(gt=0.0)
    duration_seconds: float = Field(gt=0.0)
    provider: str = Field(min_length=1)


class SyntheticCostRecord(BaseModel):
    """One deterministic training or validation example."""

    model_config = ConfigDict(extra="forbid", strict=True)

    estimated_miles: float = Field(gt=0.0)
    estimated_minutes: float = Field(gt=0.0)
    actual_miles: float = Field(gt=0.0)
    actual_minutes: float = Field(gt=0.0)
    actual_cost_cents: int = Field(gt=0)


class CostModelArtifactV1(BaseModel):
    """Transparent, safe JSON representation of the trained linear cost model."""

    model_config = ConfigDict(extra="forbid", strict=True)

    artifact_id: str = Field(min_length=1)
    artifact_format_version: Literal[1]
    model_version: Literal["ridge-v2"]
    estimator: Literal["sklearn.linear_model.Ridge"]
    sklearn_version: str = Field(min_length=1)
    feature_names: list[str]
    target_name: Literal["actual_cost_cents"]
    intercept_cents: float
    coefficients_cents: list[float]
    alpha: float = Field(gt=0.0)
    training_seed: int
    training_records: int = Field(gt=0)
    validation_records: int = Field(gt=0)
    validation_mae_cents: float = Field(ge=0.0)


def _topic(env_name: str, default: str) -> str:
    load_dotenv_for_demo()
    return os.getenv(env_name, default)


def topic_names() -> dict[str, str]:
    """Return every Demo 07 topic from one registry."""

    return {
        "requests": _topic(
            "DEMO07_TRIP_REQUESTS_TOPIC", DEFAULT_TRIP_REQUESTS_TOPIC
        ),
        "quotes": _topic("DEMO07_FARE_QUOTES_TOPIC", DEFAULT_FARE_QUOTES_TOPIC),
        "outcomes": _topic(
            "DEMO07_TRIP_OUTCOMES_TOPIC", DEFAULT_TRIP_OUTCOMES_TOPIC
        ),
        "evaluations": _topic(
            "DEMO07_PRICING_EVALUATIONS_TOPIC",
            DEFAULT_PRICING_EVALUATIONS_TOPIC,
        ),
    }


def _schema_str(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    json.loads(raw)
    return raw


def trip_request_schema_str() -> str:
    return _schema_str(TRIP_REQUEST_SCHEMA_PATH)


def fare_quote_schema_str() -> str:
    return _schema_str(FARE_QUOTE_SCHEMA_PATH)


def trip_outcome_schema_str() -> str:
    return _schema_str(TRIP_OUTCOME_SCHEMA_PATH)


def pricing_evaluation_schema_str() -> str:
    return _schema_str(PRICING_EVALUATION_SCHEMA_PATH)


def serializer_conf() -> dict[str, Any]:
    """Use TopicNameStrategy consistently for every Demo 07 value."""

    return {
        "auto.register.schemas": True,
        "subject.name.strategy": topic_subject_name_strategy,
        "validate.strict": True,
        "validate.strict.allow.default": False,
    }


def model_to_avro_dict(model: BaseModel, _ctx: Any = None) -> dict[str, Any]:
    """Convert a validated Pydantic model to an Avro-ready mapping."""

    return model.model_dump(mode="python")


def avro_dict_to_request(data: dict[str, Any], _ctx: Any = None) -> TripRequestV1:
    return TripRequestV1.model_validate(data)


def avro_dict_to_quote(data: dict[str, Any], _ctx: Any = None) -> FareQuoteV1:
    return FareQuoteV1.model_validate(data)


def avro_dict_to_outcome(data: dict[str, Any], _ctx: Any = None) -> TripOutcomeV1:
    return TripOutcomeV1.model_validate(data)


def avro_dict_to_evaluation(
    data: dict[str, Any], _ctx: Any = None
) -> PricingEvaluationV1:
    return PricingEvaluationV1.model_validate(data)


def stable_seed_offset(run_id: str) -> int:
    """Create reproducible IDs and timestamps without randomized Python hashes."""

    return zlib.crc32(validate_run_id(run_id).encode("utf-8")) % 90


# Public San Francisco coordinate pairs plus predefined offline route
# measurements. These teaching fixtures are synthetic requests, not actual
# passenger records or private location histories. The measurements let every
# student reproduce the same comparison without calling a third-party service.
DEMO_ROUTE_FIXTURES: tuple[
    tuple[GeoPointV1, GeoPointV1, float, float], ...
] = (
    (
        GeoPointV1(latitude=37.7749, longitude=-122.4194),
        GeoPointV1(latitude=37.7955, longitude=-122.3937),
        3218.688,
        900.0,
    ),
    (
        GeoPointV1(latitude=37.7840, longitude=-122.4075),
        GeoPointV1(latitude=37.7680, longitude=-122.4290),
        4184.3,
        1020.0,
    ),
    (
        GeoPointV1(latitude=37.8078, longitude=-122.4177),
        GeoPointV1(latitude=37.7857, longitude=-122.4011),
        3701.5,
        840.0,
    ),
    (
        GeoPointV1(latitude=37.7609, longitude=-122.4350),
        GeoPointV1(latitude=37.7898, longitude=-122.3942),
        5632.7,
        1260.0,
    ),
)


def deterministic_trip_requests(run_id: str, count: int = 4) -> list[TripRequestV1]:
    """Create reproducible requests from predefined public coordinate pairs.

    A request contains pickup and dropoff coordinates, not route estimates.
    The selected routing provider produces estimated miles and minutes later.
    """

    validate_run_id(run_id)
    if not 1 <= count <= len(DEMO_ROUTE_FIXTURES):
        raise ValueError(
            f"count must be between 1 and {len(DEMO_ROUTE_FIXTURES)}"
        )
    seed_offset = stable_seed_offset(run_id)
    return [
        TripRequestV1(
            run_id=run_id,
            trip_id=f"trip_{7000 + seed_offset * 10 + index:04d}",
            requested_at=REQUEST_BASE_TIME
            + timedelta(minutes=seed_offset, seconds=index * 30),
            pickup=pickup,
            dropoff=dropoff,
        )
        for index, (pickup, dropoff, _meters, _seconds) in enumerate(
            DEMO_ROUTE_FIXTURES[:count]
        )
    ]


def source_record_id(topic: str, partition: int, offset: int) -> str:
    return f"{topic}:{partition}:{offset}"


def route_values(route: RouteMeasurement) -> tuple[float, float]:
    """Convert provider meters/seconds to quote miles/minutes.

    Published route features are rounded to 0.01 mile and 0.1 minute.
    """

    miles = (Decimal(str(route.distance_meters)) / METERS_PER_MILE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    minutes = (Decimal(str(route.duration_seconds)) / SECONDS_PER_MINUTE).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )
    return float(miles), float(minutes)


def calculate_rule_v1_fare_cents(
    estimated_miles: float,
    estimated_minutes: float,
) -> int:
    """Return fare cents from estimated miles and estimated minutes."""

    if estimated_miles <= 0 or estimated_minutes <= 0:
        raise ValueError("estimated mileage and minutes must be positive")
    cents = (
        Decimal(str(estimated_miles)) * RULE_V1_PER_MILE_CENTS
        + Decimal(str(estimated_minutes)) * RULE_V1_PER_MINUTE_CENTS
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(cents)


def fare_for_target_markup(predicted_cost_cents: int) -> int:
    """Apply 20% markup on predicted cost, not 20% profit margin."""

    if predicted_cost_cents <= 0:
        raise ValueError("predicted_cost_cents must be positive")
    return int(
        (Decimal(predicted_cost_cents) * TARGET_MARKUP_MULTIPLIER).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )


def calculate_actual_cost_cents(
    actual_miles: float,
    actual_minutes: float,
    *,
    noise_cents: int = 0,
) -> int:
    """Return synthetic cost cents from actual miles/minutes and cent noise."""

    if actual_miles <= 0 or actual_minutes <= 0:
        raise ValueError("actual mileage and minutes must be positive")
    cents = (
        ACTUAL_FIXED_COST_CENTS
        + Decimal(str(actual_miles)) * ACTUAL_PER_MILE_COST_CENTS
        + (
            Decimal(str(actual_minutes))
            / Decimal("60")
            * ACTUAL_PER_HOUR_COST_CENTS
        )
        + Decimal(noise_cents)
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return max(1, int(cents))


def deterministic_training_dataset(
    *,
    count: int = TRAINING_RECORDS,
    seed: int = TRAINING_SEED,
) -> list[SyntheticCostRecord]:
    """Generate reproducible training labels without personal or external data."""

    if count < TRAINING_COUNT + VALIDATION_COUNT:
        raise ValueError(
            f"count must be at least {TRAINING_COUNT + VALIDATION_COUNT}"
        )
    rng = random.Random(seed)
    records: list[SyntheticCostRecord] = []
    for _ in range(count):
        estimated_miles = round(rng.uniform(0.8, 14.0), 2)
        estimated_minutes = round(
            4.0 + estimated_miles * rng.uniform(1.6, 3.2) + rng.uniform(0, 10),
            1,
        )
        actual_miles = round(estimated_miles * rng.uniform(0.97, 1.08), 2)
        actual_minutes = round(estimated_minutes * rng.uniform(0.94, 1.16), 1)
        noise_cents = rng.randint(-60, 60)
        records.append(
            SyntheticCostRecord(
                estimated_miles=estimated_miles,
                estimated_minutes=estimated_minutes,
                actual_miles=actual_miles,
                actual_minutes=actual_minutes,
                actual_cost_cents=calculate_actual_cost_cents(
                    actual_miles,
                    actual_minutes,
                    noise_cents=noise_cents,
                ),
            )
        )
    return records


def train_ridge_cost_model(
    *,
    seed: int = TRAINING_SEED,
    alpha: float = 1.0,
) -> CostModelArtifactV1:
    """Fit Ridge on deterministic data and export safe JSON coefficients."""

    if alpha <= 0:
        raise ValueError("alpha must be positive")
    from importlib.metadata import version

    from sklearn.linear_model import Ridge
    from sklearn.metrics import mean_absolute_error

    records = deterministic_training_dataset(seed=seed)
    train = records[:TRAINING_COUNT]
    validation = records[TRAINING_COUNT : TRAINING_COUNT + VALIDATION_COUNT]
    train_x = [
        [record.estimated_miles, record.estimated_minutes] for record in train
    ]
    train_y = [record.actual_cost_cents for record in train]
    validation_x = [
        [record.estimated_miles, record.estimated_minutes]
        for record in validation
    ]
    validation_y = [record.actual_cost_cents for record in validation]
    estimator = Ridge(alpha=alpha)
    estimator.fit(train_x, train_y)
    validation_predictions = estimator.predict(validation_x)
    payload: dict[str, Any] = {
        "artifact_format_version": 1,
        "model_version": RIDGE_V2_MODEL_VERSION,
        "estimator": "sklearn.linear_model.Ridge",
        "sklearn_version": version("scikit-learn"),
        "feature_names": MODEL_FEATURE_NAMES,
        "target_name": "actual_cost_cents",
        "intercept_cents": round(float(estimator.intercept_), 8),
        "coefficients_cents": [
            round(float(value), 8) for value in estimator.coef_
        ],
        "alpha": float(alpha),
        "training_seed": seed,
        "training_records": len(train),
        "validation_records": len(validation),
        "validation_mae_cents": round(
            float(mean_absolute_error(validation_y, validation_predictions)),
            4,
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload["artifact_id"] = (
        f"{RIDGE_V2_MODEL_VERSION}-"
        f"{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:12]}"
    )
    return CostModelArtifactV1.model_validate(payload)


def save_model_artifact(
    artifact: CostModelArtifactV1,
    path: Path,
) -> Path:
    """Persist transparent coefficients, not an executable pickle."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        artifact.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def load_model_artifact(path: Path) -> CostModelArtifactV1:
    """Load and validate one JSON model artifact before online inference."""

    return CostModelArtifactV1.model_validate_json(path.read_text(encoding="utf-8"))


def predict_cost_cents(
    artifact: CostModelArtifactV1,
    *,
    estimated_miles: float,
    estimated_minutes: float,
) -> int:
    """Predict cost cents from estimated miles/minutes using the JSON artifact."""

    if artifact.feature_names != MODEL_FEATURE_NAMES:
        raise ValueError(
            f"Expected features {MODEL_FEATURE_NAMES}, got {artifact.feature_names}"
        )
    if len(artifact.coefficients_cents) != len(MODEL_FEATURE_NAMES):
        raise ValueError("Model coefficient count does not match feature count")
    prediction = (
        Decimal(str(artifact.intercept_cents))
        + Decimal(str(artifact.coefficients_cents[0]))
        * Decimal(str(estimated_miles))
        + Decimal(str(artifact.coefficients_cents[1]))
        * Decimal(str(estimated_minutes))
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return max(1, int(prediction))


def quote_from_route(
    request: TripRequestV1,
    route: RouteMeasurement,
    *,
    source_id: str,
    pricing_method: PricingMethod,
    quoted_at: datetime,
    artifact: CostModelArtifactV1 | None = None,
) -> FareQuoteV1:
    """Create either the rule baseline or trained-model fare quote."""

    estimated_miles, estimated_minutes = route_values(route)
    if pricing_method == RULE_V1_MODEL_VERSION:
        predicted_cost_cents = None
        fare_cents = calculate_rule_v1_fare_cents(
            estimated_miles, estimated_minutes
        )
        feature_names: list[str] = []
    elif pricing_method == RIDGE_V2_MODEL_VERSION:
        if artifact is None:
            raise ValueError("ridge-v2 requires a validated model artifact")
        predicted_cost_cents = predict_cost_cents(
            artifact,
            estimated_miles=estimated_miles,
            estimated_minutes=estimated_minutes,
        )
        fare_cents = fare_for_target_markup(predicted_cost_cents)
        feature_names = list(artifact.feature_names)
    else:
        raise ValueError(f"Unsupported pricing method: {pricing_method}")

    quote_id = f"{request.trip_id}:{pricing_method}"
    return FareQuoteV1(
        **request.model_dump(),
        source_record_id=source_id,
        quote_id=quote_id,
        estimated_miles=estimated_miles,
        estimated_minutes=estimated_minutes,
        predicted_cost_cents=predicted_cost_cents,
        fare_cents=fare_cents,
        currency=CURRENCY,
        pricing_method=pricing_method,
        model_version=pricing_method,
        model_feature_names=feature_names,
        target_markup_pct=float(TARGET_MARKUP_PCT),
        routing_provider=route.provider,
        quoted_at=quoted_at.astimezone(UTC),
    )


def _outcome_adjustments(trip_id: str) -> tuple[Decimal, Decimal, int]:
    value = zlib.crc32(trip_id.encode("utf-8"))
    miles_factor = Decimal("0.98") + Decimal(value % 9) / Decimal("100")
    minutes_factor = Decimal("0.96") + Decimal((value // 11) % 19) / Decimal("100")
    noise_cents = int((value // 17) % 101) - 50
    return miles_factor, minutes_factor, noise_cents


def outcome_from_route(
    request: TripRequestV1,
    route: RouteMeasurement,
) -> TripOutcomeV1:
    """Simulate one post-trip outcome from a route estimate.

    Demo 07 has no vehicle telemetry and does not wait in real time for a trip.
    It applies stable trip_id-based adjustments to the selected route estimate,
    then calculates a synthetic realized fulfillment-cost label.
    """

    estimated_miles, estimated_minutes = route_values(route)
    miles_factor, minutes_factor, noise_cents = _outcome_adjustments(
        request.trip_id
    )
    actual_miles = float(
        (Decimal(str(estimated_miles)) * miles_factor).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    )
    actual_minutes = float(
        (Decimal(str(estimated_minutes)) * minutes_factor).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )
    )
    actual_cost_cents = calculate_actual_cost_cents(
        actual_miles,
        actual_minutes,
        noise_cents=noise_cents,
    )
    return TripOutcomeV1(
        run_id=request.run_id,
        trip_id=request.trip_id,
        actual_miles=actual_miles,
        actual_minutes=actual_minutes,
        actual_cost_cents=actual_cost_cents,
        currency=CURRENCY,
        completed_at=request.requested_at + timedelta(minutes=actual_minutes),
        outcome_source="synthetic-delayed-v1",
    )


def evaluate_quote(
    quote: FareQuoteV1,
    outcome: TripOutcomeV1,
    *,
    evaluated_at: datetime,
) -> PricingEvaluationV1:
    """Join one quote with its delayed outcome and score the business target."""

    if quote.run_id != outcome.run_id or quote.trip_id != outcome.trip_id:
        raise ValueError("Quote and outcome must share run_id and trip_id")
    profit_cents = quote.fare_cents - outcome.actual_cost_cents
    realized_markup = (
        Decimal(profit_cents)
        / Decimal(outcome.actual_cost_cents)
        * Decimal("100")
    )
    markup_error = realized_markup - TARGET_MARKUP_PCT
    prediction_error = (
        None
        if quote.predicted_cost_cents is None
        else quote.predicted_cost_cents - outcome.actual_cost_cents
    )
    return PricingEvaluationV1(
        run_id=quote.run_id,
        trip_id=quote.trip_id,
        quote_id=quote.quote_id,
        model_version=quote.model_version,
        fare_cents=quote.fare_cents,
        actual_cost_cents=outcome.actual_cost_cents,
        profit_cents=profit_cents,
        realized_markup_pct=round(float(realized_markup), 4),
        target_markup_pct=float(TARGET_MARKUP_PCT),
        markup_error_pp=round(float(markup_error), 4),
        absolute_markup_error_pp=round(float(abs(markup_error)), 4),
        cost_prediction_error_cents=prediction_error,
        within_target_tolerance=(
            abs(markup_error) <= TARGET_MARKUP_TOLERANCE_PP
        ),
        evaluated_at=evaluated_at.astimezone(UTC),
    )


def summarize_evaluations(
    evaluations: list[PricingEvaluationV1],
) -> dict[str, dict[str, float | int]]:
    """Aggregate model quality and business-target metrics by version."""

    grouped: dict[str, list[PricingEvaluationV1]] = {}
    for evaluation in evaluations:
        grouped.setdefault(evaluation.model_version, []).append(evaluation)
    summary: dict[str, dict[str, float | int]] = {}
    for model_version, records in sorted(grouped.items()):
        count = len(records)
        prediction_errors = [
            abs(record.cost_prediction_error_cents)
            for record in records
            if record.cost_prediction_error_cents is not None
        ]
        summary[model_version] = {
            "records": count,
            "average_realized_markup_pct": round(
                sum(record.realized_markup_pct for record in records) / count,
                4,
            ),
            "mean_absolute_markup_error_pp": round(
                sum(record.absolute_markup_error_pp for record in records) / count,
                4,
            ),
            "within_target_tolerance": sum(
                record.within_target_tolerance for record in records
            ),
            "average_profit_cents": round(
                sum(record.profit_cents for record in records) / count,
                2,
            ),
            "cost_prediction_mae_cents": (
                round(sum(prediction_errors) / len(prediction_errors), 2)
                if prediction_errors
                else -1
            ),
        }
    return summary


def architecture_comparison() -> list[dict[str, Any]]:
    """Contrast the implemented service boundary with the rejected field split."""

    return [
        {
            "architecture": "A_direct_quote",
            "implemented": True,
            "online_topics": 2,
            "processors": 1,
            "routing_calls_per_trip": 1,
            "join_state_for_online_quote": False,
            "teaching_conclusion": (
                "Recommended because one routing response owns distance and duration."
            ),
        },
        {
            "architecture": "B_split_mileage_duration",
            "implemented": False,
            "online_topics": 4,
            "processors": 3,
            "routing_calls_per_trip": 2,
            "join_state_for_online_quote": True,
            "teaching_conclusion": (
                "Rejected because it splits fields that arrive from one response."
            ),
        },
    ]
