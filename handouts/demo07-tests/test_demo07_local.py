from __future__ import annotations

import importlib.metadata
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

import demo07_routing
from demo07_common import (
    MODEL_FEATURE_NAMES,
    RULE_V1_MODEL_VERSION,
    RIDGE_V2_MODEL_VERSION,
    TARGET_MARKUP_PCT,
    GeoPointV1,
    PricingEvaluationV1,
    TripRequestV1,
    architecture_comparison,
    avro_dict_to_evaluation,
    avro_dict_to_outcome,
    avro_dict_to_quote,
    avro_dict_to_request,
    calculate_actual_cost_cents,
    calculate_rule_v1_fare_cents,
    deterministic_training_dataset,
    deterministic_trip_requests,
    evaluate_quote,
    fare_for_target_markup,
    fare_quote_schema_str,
    load_model_artifact,
    model_to_avro_dict,
    outcome_from_route,
    predict_cost_cents,
    pricing_evaluation_schema_str,
    quote_from_route,
    route_values,
    save_model_artifact,
    serializer_conf,
    source_record_id,
    summarize_evaluations,
    topic_names,
    train_ridge_cost_model,
    trip_outcome_schema_str,
    trip_request_schema_str,
)
from demo07_kafka import (
    acknowledged_produce,
    commit_message,
    commit_message_batch,
)
from demo07_routing import (
    FixtureRoutingClient,
    OSRMRoutingClient,
    RoutingError,
    osrm_route_url,
)
from demo07f_compare_models import compare_model_summaries, run_comparison

COURSE_VERSIONS = {
    "confluent-kafka": "2.15.0",
    "httpx2": "2.7.0",
    "pydantic": "2.13.4",
    "pytest": "9.1.1",
    "scikit-learn": "1.9.0",
}


def test_course_runtime_uses_exact_pins() -> None:
    assert sys.version_info[:3] == (3, 11, 14)
    assert {
        package: importlib.metadata.version(package)
        for package in COURSE_VERSIONS
    } == COURSE_VERSIONS


def test_topic_registry_has_four_distinct_natural_event_owners() -> None:
    topics = topic_names()
    assert set(topics) == {"requests", "quotes", "outcomes", "evaluations"}
    assert len(set(topics.values())) == 4
    assert all("demo07.ml-" in topic for topic in topics.values())


def test_architecture_b_is_explicitly_not_implemented() -> None:
    comparison = architecture_comparison()
    assert comparison[0]["architecture"] == "A_direct_quote"
    assert comparison[0]["implemented"] is True
    assert comparison[0]["routing_calls_per_trip"] == 1
    assert comparison[1]["architecture"] == "B_split_mileage_duration"
    assert comparison[1]["implemented"] is False
    assert comparison[1]["join_state_for_online_quote"] is True


def test_all_four_avro_schemas_are_valid_and_owned() -> None:
    schemas = [
        json.loads(trip_request_schema_str()),
        json.loads(fare_quote_schema_str()),
        json.loads(trip_outcome_schema_str()),
        json.loads(pricing_evaluation_schema_str()),
    ]
    assert [schema["name"] for schema in schemas] == [
        "TripRequestV1",
        "FareQuoteV1",
        "TripOutcomeV1",
        "PricingEvaluationV1",
    ]
    assert all(
        schema["namespace"] == "edu.usfca.msds682.demo07"
        for schema in schemas
    )


def test_synthetic_requests_are_finite_reproducible_and_independent() -> None:
    first = deterministic_trip_requests("local-demo", 4)
    second = deterministic_trip_requests("local-demo", 4)
    different = deterministic_trip_requests("different-run", 4)
    assert first == second
    assert first != different
    assert len(first) == 4
    assert all(request.run_id == "local-demo" for request in first)


def test_request_contract_rejects_invalid_location_and_extra_field() -> None:
    request = deterministic_trip_requests("validation", 1)[0]
    with pytest.raises(ValidationError):
        TripRequestV1.model_validate(
            {
                **request.model_dump(),
                "pickup": {"latitude": 91.0, "longitude": -122.0},
            }
        )
    with pytest.raises(ValidationError):
        TripRequestV1.model_validate(
            {**request.model_dump(), "unexpected": True}
        )


def test_fixture_first_trip_is_two_miles_and_fifteen_minutes() -> None:
    request = deterministic_trip_requests("fixture-example", 1)[0]
    route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
    assert route_values(route) == (2.0, 15.0)
    assert calculate_rule_v1_fare_cents(2.0, 15.0) == 1000


def test_markup_and_profit_margin_are_not_confused() -> None:
    assert fare_for_target_markup(1000) == 1200
    true_twenty_percent_margin_fare = 1000 / 0.8
    assert true_twenty_percent_margin_fare == 1250
    assert float(TARGET_MARKUP_PCT) == 20.0


def test_synthetic_cost_uses_fixed_distance_and_hourly_components() -> None:
    assert calculate_actual_cost_cents(2.0, 15.0) == 1150
    assert calculate_actual_cost_cents(2.0, 15.0, noise_cents=25) == 1175


def test_training_dataset_is_deterministic_and_has_160_records() -> None:
    first = deterministic_training_dataset()
    second = deterministic_training_dataset()
    assert first == second
    assert len(first) == 160
    assert len({record.actual_cost_cents for record in first}) > 100


def test_ridge_artifact_is_reproducible_and_explicit() -> None:
    first = train_ridge_cost_model()
    second = train_ridge_cost_model()
    assert first == second
    assert first.model_version == RIDGE_V2_MODEL_VERSION
    assert first.feature_names == MODEL_FEATURE_NAMES
    assert len(first.coefficients_cents) == 2
    assert first.training_records == 120
    assert first.validation_records == 40
    assert first.validation_mae_cents < 200


def test_model_artifact_roundtrip_uses_validated_json(tmp_path: Path) -> None:
    artifact = train_ridge_cost_model()
    path = save_model_artifact(artifact, tmp_path / "model.json")
    assert load_model_artifact(path) == artifact
    raw = path.read_text(encoding="utf-8")
    assert "pickle" not in raw.lower()


def test_cost_prediction_requires_feature_contract() -> None:
    artifact = train_ridge_cost_model()
    predicted = predict_cost_cents(
        artifact,
        estimated_miles=2.0,
        estimated_minutes=15.0,
    )
    assert 900 <= predicted <= 1400
    invalid = artifact.model_copy(update={"feature_names": ["minutes", "miles"]})
    with pytest.raises(ValueError, match="Expected features"):
        predict_cost_cents(
            invalid,
            estimated_miles=2.0,
            estimated_minutes=15.0,
        )


def test_rule_and_ridge_quotes_are_versioned_differently() -> None:
    request = deterministic_trip_requests("quotes", 1)[0]
    route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
    artifact = train_ridge_cost_model()
    rule = quote_from_route(
        request,
        route,
        source_id="requests:0:1",
        pricing_method=RULE_V1_MODEL_VERSION,
        quoted_at=datetime(2026, 7, 27, 18, 0, tzinfo=UTC),
    )
    ridge = quote_from_route(
        request,
        route,
        source_id="requests:0:1",
        pricing_method=RIDGE_V2_MODEL_VERSION,
        artifact=artifact,
        quoted_at=datetime(2026, 7, 27, 18, 0, tzinfo=UTC),
    )
    assert rule.quote_id == f"{request.trip_id}:rule-v1"
    assert rule.predicted_cost_cents is None
    assert rule.fare_cents == 1000
    assert ridge.quote_id == f"{request.trip_id}:ridge-v2"
    assert ridge.predicted_cost_cents is not None
    assert ridge.model_feature_names == MODEL_FEATURE_NAMES


def test_delayed_outcome_is_deterministic_and_is_not_a_quote_field() -> None:
    request = deterministic_trip_requests("outcome", 1)[0]
    route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
    first = outcome_from_route(request, route)
    second = outcome_from_route(request, route)
    assert first == second
    assert first.actual_cost_cents > 0
    assert first.completed_at > request.requested_at


def test_evaluation_uses_realized_cost_and_marks_tolerance() -> None:
    request = deterministic_trip_requests("evaluation", 1)[0]
    route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
    artifact = train_ridge_cost_model()
    quote = quote_from_route(
        request,
        route,
        source_id="requests:0:1",
        pricing_method=RIDGE_V2_MODEL_VERSION,
        artifact=artifact,
        quoted_at=datetime(2026, 7, 27, 18, 0, tzinfo=UTC),
    )
    outcome = outcome_from_route(request, route)
    evaluation = evaluate_quote(
        quote,
        outcome,
        evaluated_at=datetime(2026, 7, 27, 19, 0, tzinfo=UTC),
    )
    assert evaluation.profit_cents == quote.fare_cents - outcome.actual_cost_cents
    assert evaluation.cost_prediction_error_cents == (
        quote.predicted_cost_cents - outcome.actual_cost_cents
    )
    assert evaluation.absolute_markup_error_pp == pytest.approx(
        abs(evaluation.realized_markup_pct - 20.0),
        abs=0.001,
    )


def _four_trip_evaluations() -> list[PricingEvaluationV1]:
    artifact = train_ridge_cost_model()
    evaluations: list[PricingEvaluationV1] = []
    for request in deterministic_trip_requests("model-compare", 4):
        route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
        outcome = outcome_from_route(request, route)
        for method in (RULE_V1_MODEL_VERSION, RIDGE_V2_MODEL_VERSION):
            quote = quote_from_route(
                request,
                route,
                source_id=f"requests:0:{request.trip_id}",
                pricing_method=method,
                artifact=artifact if method == RIDGE_V2_MODEL_VERSION else None,
                quoted_at=datetime(2026, 7, 27, 18, 0, tzinfo=UTC),
            )
            evaluations.append(
                evaluate_quote(
                    quote,
                    outcome,
                    evaluated_at=datetime(2026, 7, 27, 19, 0, tzinfo=UTC),
                )
            )
    return evaluations


def test_ridge_v2_is_closer_to_target_on_same_four_outcomes() -> None:
    summary = summarize_evaluations(_four_trip_evaluations())
    decision = compare_model_summaries(summary)
    assert decision["recommended_version"] == RIDGE_V2_MODEL_VERSION
    assert decision["candidate_error_pp"] < decision["baseline_error_pp"]
    assert "winner" not in decision
    assert "promotion_decision" not in decision
    assert summary["rule-v1"]["cost_prediction_mae_cents"] == -1
    assert summary["ridge-v2"]["cost_prediction_mae_cents"] >= 0


def test_all_contracts_roundtrip_through_mock_schema_registry() -> None:
    request = deterministic_trip_requests("avro-roundtrip", 1)[0]
    route = FixtureRoutingClient().estimate(request.pickup, request.dropoff)
    artifact = train_ridge_cost_model()
    quote = quote_from_route(
        request,
        route,
        source_id=source_record_id("test.requests", 0, 1),
        pricing_method=RIDGE_V2_MODEL_VERSION,
        artifact=artifact,
        quoted_at=datetime(2026, 7, 27, 18, 0, tzinfo=UTC),
    )
    outcome = outcome_from_route(request, route)
    evaluation = evaluate_quote(
        quote,
        outcome,
        evaluated_at=datetime(2026, 7, 27, 19, 0, tzinfo=UTC),
    )
    cases = [
        ("request", request, trip_request_schema_str(), avro_dict_to_request),
        ("quote", quote, fare_quote_schema_str(), avro_dict_to_quote),
        ("outcome", outcome, trip_outcome_schema_str(), avro_dict_to_outcome),
        (
            "evaluation",
            evaluation,
            pricing_evaluation_schema_str(),
            avro_dict_to_evaluation,
        ),
    ]
    with SchemaRegistryClient.new_client(
        {"url": "mock://demo07-local"}
    ) as registry:
        for name, model, schema, from_dict in cases:
            context = SerializationContext(f"test.{name}", MessageField.VALUE)
            serializer = AvroSerializer(
                registry,
                schema,
                to_dict=model_to_avro_dict,
                conf=serializer_conf(),
            )
            deserializer = AvroDeserializer(
                registry,
                schema,
                from_dict=from_dict,
            )
            encoded = serializer(model, context)
            assert encoded is not None
            assert deserializer(encoded, context) == model


def test_osrm_url_uses_longitude_latitude_order() -> None:
    pickup = GeoPointV1(latitude=37.7749, longitude=-122.4194)
    dropoff = GeoPointV1(latitude=37.7955, longitude=-122.3937)
    assert osrm_route_url(pickup, dropoff) == (
        "https://router.project-osrm.org/route/v1/driving/"
        "-122.419400,37.774900;-122.393700,37.795500"
    )


def test_osrm_client_parses_distance_and_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "code": "Ok",
                "routes": [{"distance": 3218.688, "duration": 900.0}],
            }

    def fake_get(url: str, **kwargs: object) -> Response:
        calls.append({"url": url, **kwargs})
        return Response()

    monkeypatch.setattr(demo07_routing.httpx, "get", fake_get)
    request = deterministic_trip_requests("osrm-parse", 1)[0]
    route = OSRMRoutingClient(timeout_seconds=3.0).estimate(
        request.pickup,
        request.dropoff,
    )
    assert route.distance_meters == 3218.688
    assert route.duration_seconds == 900.0
    assert route.provider == "osrm"
    assert calls[0]["timeout"] == 3.0


def test_osrm_client_never_silently_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> object:
        raise demo07_routing.httpx.ConnectError("offline")

    monkeypatch.setattr(demo07_routing.httpx, "get", fail)
    request = deterministic_trip_requests("osrm-failure", 1)[0]
    with pytest.raises(RoutingError, match="OSRM request failed"):
        OSRMRoutingClient().estimate(request.pickup, request.dropoff)


class FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    def produce(self, topic: str, **kwargs: object) -> None:
        callback = kwargs["on_delivery"]
        assert callable(callback)
        callback(
            RuntimeError("failed") if self.fail else None,
            FakeMessage(topic, 0, 2),
        )

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return 0


class FakeConsumer:
    def commit(
        self,
        *,
        message: FakeMessage | None = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool,
    ) -> list[TopicPartition]:
        assert asynchronous is False
        if message is not None:
            return [
                TopicPartition(
                    message.topic(),
                    message.partition(),
                    message.offset() + 1,
                )
            ]
        assert offsets is not None
        return offsets


def test_output_acknowledgement_and_commit_helpers_are_explicit() -> None:
    delivery = acknowledged_produce(
        FakeProducer(),
        topic="quotes",
        key=b"q1",
        value=b"value",
        delivery_timeout=1.0,
    )
    assert delivery["offset"] == 2
    with pytest.raises(RuntimeError, match="not acknowledged"):
        acknowledged_produce(
            FakeProducer(fail=True),
            topic="quotes",
            key=b"q1",
            value=b"value",
            delivery_timeout=1.0,
        )
    messages = [
        FakeMessage("quotes", 0, 4),
        FakeMessage("quotes", 0, 7),
        FakeMessage("outcomes", 0, 2),
    ]
    assert commit_message(FakeConsumer(), messages[0])[0]["offset"] == 5
    committed = commit_message_batch(FakeConsumer(), messages)
    assert {(item["topic"], item["offset"]) for item in committed} == {
        ("quotes", 8),
        ("outcomes", 3),
    }


def test_comparison_report_is_run_scoped_and_reproducible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    run_id = "comparison-report"
    summary = summarize_evaluations(_four_trip_evaluations())
    source = tmp_path / "evaluation.json"
    source.write_text(
        json.dumps({"run_id": run_id, "model_summary": summary}),
        encoding="utf-8",
    )
    report = run_comparison(
        run_id=run_id,
        evaluation_report_path=source,
    )
    assert report["decision"]["recommended_version"] == RIDGE_V2_MODEL_VERSION
    assert report["online_architecture_comparison"][1]["implemented"] is False
    assert Path(report["report_path"]).is_file()
