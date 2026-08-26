from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from demo07_common import (
    RULE_V1_MODEL_VERSION,
    RIDGE_V2_MODEL_VERSION,
    FareQuoteV1,
    PricingEvaluationV1,
    TripOutcomeV1,
    deterministic_trip_requests,
    evaluate_quote,
    outcome_from_route,
    quote_from_route,
    topic_names,
    train_ridge_cost_model,
)
from demo07_routing import FixtureRoutingClient
from demo09_common import (
    FLINKSQL_TEMPLATE_PATH,
    KSQLDB_TEMPLATE_PATH,
    ComputeEngine,
    engine_evaluation_topics,
    output_topic_names,
    render_sql_template,
    sql_template_context,
)
from demo09_verify import verify_contract


def _fixture(
    run_id: str = "demo09-local-contract",
) -> tuple[
    list[FareQuoteV1],
    list[TripOutcomeV1],
    list[PricingEvaluationV1],
]:
    router = FixtureRoutingClient()
    artifact = train_ridge_cost_model()
    quotes: list[FareQuoteV1] = []
    outcomes: list[TripOutcomeV1] = []
    evaluations: list[PricingEvaluationV1] = []
    evaluated_at = datetime(2026, 8, 3, 20, 0, tzinfo=UTC)
    for request in deterministic_trip_requests(run_id, 4):
        route = router.estimate(request.pickup, request.dropoff)
        outcome = outcome_from_route(request, route)
        outcomes.append(outcome)
        for model in (RULE_V1_MODEL_VERSION, RIDGE_V2_MODEL_VERSION):
            quote = quote_from_route(
                request,
                route,
                source_id=f"fixture:{request.trip_id}",
                pricing_method=model,
                artifact=artifact if model == RIDGE_V2_MODEL_VERSION else None,
                quoted_at=evaluated_at,
            )
            quotes.append(quote)
            evaluations.append(
                evaluate_quote(quote, outcome, evaluated_at=evaluated_at)
            )
    return quotes, outcomes, evaluations


def test_demo09_registry_reuses_demo07_inputs_and_isolates_outputs() -> None:
    demo07_topics = topic_names()
    outputs = output_topic_names()
    engines = engine_evaluation_topics()
    assert engines[ComputeEngine.PYTHON.value] == demo07_topics["evaluations"]
    assert set(outputs) == {"ksqldb", "flinksql"}
    assert len(set(outputs.values())) == 2
    assert demo07_topics["evaluations"] not in outputs.values()


def test_context_imports_demo07_topics_instead_of_copying_them() -> None:
    context = sql_template_context(
        "lecture9-demo",
        ksqldb_value_schema_id=123,
    )
    assert context["DEMO07_FARE_QUOTES_TOPIC"] == topic_names()["quotes"]
    assert context["DEMO07_TRIP_OUTCOMES_TOPIC"] == topic_names()["outcomes"]
    source = Path(__file__).resolve().parents[1] / "demo09_common.py"
    text = source.read_text(encoding="utf-8")
    assert topic_names()["quotes"] not in text
    assert topic_names()["outcomes"] not in text


@pytest.mark.parametrize(
    "template_path",
    [KSQLDB_TEMPLATE_PATH, FLINKSQL_TEMPLATE_PATH],
)
def test_sql_templates_render_one_run_without_unresolved_values(
    template_path: Path,
) -> None:
    rendered = render_sql_template(
        template_path,
        run_id="lecture9-demo",
        ksqldb_value_schema_id=123,
    )
    assert "${" not in rendered
    assert "lecture9-demo" in rendered
    assert topic_names()["quotes"] in rendered
    assert topic_names()["outcomes"] in rendered
    assert "Confluent CLI" in rendered


def test_both_sql_templates_filter_run_before_join_and_bound_state() -> None:
    ksql = render_sql_template(
        KSQLDB_TEMPLATE_PATH,
        run_id="bounded-run",
        ksqldb_value_schema_id=123,
    )
    flink = render_sql_template(
        FLINKSQL_TEMPLATE_PATH,
        run_id="bounded-run",
        ksqldb_value_schema_id=123,
    )
    assert ksql.count("WHERE RUN_ID='bounded-run'") >= 2
    assert "WITHIN 1 HOUR" in ksql
    assert flink.count("WHERE run_id='bounded-run'") >= 2
    assert "INTERVAL '1' HOUR" in flink
    assert "q.trip_id = o.trip_id" in flink


def test_ksqldb_preserves_the_sink_schema_field_case() -> None:
    rendered = render_sql_template(
        KSQLDB_TEMPLATE_PATH,
        run_id="case-contract",
        ksqldb_value_schema_id=123,
    )
    sink_fields = (
        "run_id",
        "trip_id",
        "quote_id",
        "model_version",
        "fare_cents",
        "actual_cost_cents",
        "profit_cents",
        "realized_markup_pct",
        "target_markup_pct",
        "markup_error_pp",
        "absolute_markup_error_pp",
        "cost_prediction_error_cents",
        "within_target_tolerance",
        "evaluated_at",
    )
    # ksqlDB uppercases unquoted aliases.  The backticks are required because
    # VALUE_SCHEMA_ID binds this CSAS to Demo 07's lowercase Avro field names.
    for field in sink_fields:
        assert f"AS `{field}`" in rendered
    assert "WHERE `run_id`='case-contract'" in rendered
    assert "GROUP BY `model_version`" in rendered
    assert "DESCRIBE D09A_PRICING_EVALUATIONS EXTENDED;" in rendered
    assert "DESCRIBE EXTENDED D09A_PRICING_EVALUATIONS" not in rendered


def test_three_compute_owners_pass_the_same_local_contract() -> None:
    quotes, outcomes, evaluations = _fixture()
    result = verify_contract(
        run_id="demo09-local-contract",
        quotes=quotes,
        outcomes=outcomes,
        evaluations_by_engine={
            "python": evaluations,
            "ksqldb": [record.model_copy() for record in evaluations],
            "flinksql": [record.model_copy() for record in evaluations],
        },
    )
    assert result["contract_passed"] is True
    assert result["input_counts"] == {"quotes": 8, "outcomes": 4}
    assert set(result["engines"]) == {"python", "ksqldb", "flinksql"}
    assert all(
        value["decision"]["recommended_version"] == "ridge-v2"
        for value in result["engines"].values()
    )


def test_verifier_rejects_missing_or_duplicate_rows() -> None:
    quotes, outcomes, evaluations = _fixture()
    with pytest.raises(ValueError, match="produced 7 rows"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=quotes,
            outcomes=outcomes,
            evaluations_by_engine={"flinksql": evaluations[:-1]},
        )
    duplicate = [*evaluations[:-1], evaluations[0].model_copy()]
    with pytest.raises(ValueError, match="missing or duplicate quote IDs"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=quotes,
            outcomes=outcomes,
            evaluations_by_engine={"ksqldb": duplicate},
        )


def test_verifier_recalculates_fields_instead_of_trusting_sql() -> None:
    quotes, outcomes, evaluations = _fixture()
    altered = list(evaluations)
    altered[0] = altered[0].model_copy(
        update={"absolute_markup_error_pp": 0.0}
    )
    with pytest.raises(ValueError, match="disagrees with Demo 07"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=quotes,
            outcomes=outcomes,
            evaluations_by_engine={"ksqldb": altered},
        )


def test_verifier_rejects_non_finite_sql_metrics() -> None:
    quotes, outcomes, evaluations = _fixture()
    altered = list(evaluations)
    # model_copy deliberately bypasses Pydantic validation to reproduce a bad
    # value that an external SQL runtime or deserializer could hand us.
    altered[0] = altered[0].model_copy(
        update={"absolute_markup_error_pp": float("nan")}
    )
    with pytest.raises(ValueError, match="disagrees with Demo 07"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=quotes,
            outcomes=outcomes,
            evaluations_by_engine={"flinksql": altered},
        )


def test_verifier_rejects_mislabeled_source_run() -> None:
    quotes, outcomes, evaluations = _fixture()
    wrong_quotes = list(quotes)
    wrong_quotes[0] = wrong_quotes[0].model_copy(
        update={"run_id": "another-run"}
    )
    with pytest.raises(ValueError, match="requested run_id"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=wrong_quotes,
            outcomes=outcomes,
            evaluations_by_engine={"python": evaluations},
        )


def test_verifier_rejects_inconsistent_quote_identity() -> None:
    quotes, outcomes, evaluations = _fixture()
    altered = list(quotes)
    # Keep the set of quote_id values unchanged while breaking its relationship
    # with trip_id.  Counting eight IDs alone must not certify this input.
    altered[0] = altered[0].model_copy(update={"trip_id": quotes[2].trip_id})
    with pytest.raises(ValueError, match="Quote identity"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=altered,
            outcomes=outcomes,
            evaluations_by_engine={"ksqldb": evaluations},
        )


def test_verifier_rejects_a_different_run() -> None:
    quotes, outcomes, evaluations = _fixture()
    wrong = list(evaluations)
    wrong[0] = wrong[0].model_copy(update={"run_id": "another-run"})
    with pytest.raises(ValueError, match="disagrees with Demo 07"):
        verify_contract(
            run_id="demo09-local-contract",
            quotes=quotes,
            outcomes=outcomes,
            evaluations_by_engine={"flinksql": wrong},
        )
