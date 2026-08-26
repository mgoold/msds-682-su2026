"""Shared registry and SQL rendering contract for Demo 09.

Demo 09 deliberately does not copy Demo 07's business rules, input topic names,
or Avro contract.  It changes only the owner of the join/evaluate/aggregate
computation: Python, ksqlDB, or Flink SQL.
"""

from __future__ import annotations

import os
import re
from enum import Enum
from pathlib import Path

from confluent_demo_common import load_dotenv_for_demo, validate_run_id
from demo07_common import topic_names

BUNDLE_DIR = Path(__file__).resolve().parent
KSQLDB_TEMPLATE_PATH = BUNDLE_DIR / "demo09a_ksqldb.sql.template"
FLINKSQL_TEMPLATE_PATH = BUNDLE_DIR / "demo09b_flinksql.sql.template"

# ============================================================================
# KEY CONCEPT: output isolation
# Demo 07 remains the Python baseline.  Each SQL engine gets a new output topic,
# so running the comparison cannot overwrite or contaminate Demo 07 evidence.
# ============================================================================
DEFAULT_KSQLDB_EVALUATIONS_TOPIC = (
    "msds682.demo09.ksqldb-pricing-evaluations-avro.v1"
)
DEFAULT_FLINKSQL_EVALUATIONS_TOPIC = (
    "msds682.demo09.flinksql-pricing-evaluations-avro.v1"
)
DEFAULT_KSQLDB_QUOTES_RUN_TOPIC = "msds682.demo09.ksqldb-quotes-run-avro.v1"
DEFAULT_KSQLDB_OUTCOMES_RUN_TOPIC = (
    "msds682.demo09.ksqldb-outcomes-run-avro.v1"
)

_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


class ComputeEngine(str, Enum):
    """The three owners compared by one verification contract."""

    PYTHON = "python"
    KSQLDB = "ksqldb"
    FLINKSQL = "flinksql"


def _topic(env_name: str, default: str) -> str:
    """Read one optional override without exposing credentials."""

    load_dotenv_for_demo()
    value = os.getenv(env_name, default).strip()
    if not value:
        raise ValueError(f"{env_name} must not be empty")
    return value


def output_topic_names() -> dict[str, str]:
    """Return only the two new Demo 09 sink topics."""

    topics = {
        ComputeEngine.KSQLDB.value: _topic(
            "DEMO09_KSQLDB_EVALUATIONS_TOPIC",
            DEFAULT_KSQLDB_EVALUATIONS_TOPIC,
        ),
        ComputeEngine.FLINKSQL.value: _topic(
            "DEMO09_FLINKSQL_EVALUATIONS_TOPIC",
            DEFAULT_FLINKSQL_EVALUATIONS_TOPIC,
        ),
    }
    if len(set(topics.values())) != len(topics):
        raise ValueError("Demo 09 output topics must be distinct")
    if topic_names()["evaluations"] in topics.values():
        raise ValueError("A Demo 09 output topic must not overwrite Demo 07")
    return topics


def engine_evaluation_topics() -> dict[str, str]:
    """Map every compute owner to the topic checked by the shared verifier."""

    return {
        ComputeEngine.PYTHON.value: topic_names()["evaluations"],
        **output_topic_names(),
    }


def ksqldb_derived_topic_names() -> dict[str, str]:
    """Return ksqlDB's two explicit filter/repartition topic names.

    Naming these topics makes state ownership and cleanup reviewable.  ksqlDB
    creates them when the CSAS statements start; ``demo09_prepare.py`` does not.
    """

    return {
        "quotes_run": _topic(
            "DEMO09_KSQLDB_QUOTES_RUN_TOPIC",
            DEFAULT_KSQLDB_QUOTES_RUN_TOPIC,
        ),
        "outcomes_run": _topic(
            "DEMO09_KSQLDB_OUTCOMES_RUN_TOPIC",
            DEFAULT_KSQLDB_OUTCOMES_RUN_TOPIC,
        ),
    }


def sql_template_context(
    run_id: str,
    *,
    ksqldb_value_schema_id: int,
) -> dict[str, str]:
    """Build one context from the Demo 07 and Demo 09 registries.

    KEY CONCEPT: ``run_id`` is filtered on both sides before the join.  A
    ``trip_id`` is unique within one classroom run, not across all past runs.
    """

    safe_run_id = validate_run_id(run_id)
    inputs = topic_names()
    outputs = output_topic_names()
    derived = ksqldb_derived_topic_names()
    if ksqldb_value_schema_id <= 0:
        raise ValueError("ksqldb_value_schema_id must be positive")
    return {
        "RUN_ID": safe_run_id,
        "DEMO07_FARE_QUOTES_TOPIC": inputs["quotes"],
        "DEMO07_TRIP_OUTCOMES_TOPIC": inputs["outcomes"],
        "DEMO09_KSQLDB_EVALUATIONS_TOPIC": outputs[
            ComputeEngine.KSQLDB.value
        ],
        "DEMO09_KSQLDB_QUOTES_RUN_TOPIC": derived["quotes_run"],
        "DEMO09_KSQLDB_OUTCOMES_RUN_TOPIC": derived["outcomes_run"],
        "DEMO09_KSQLDB_VALUE_SCHEMA_ID": str(ksqldb_value_schema_id),
        "DEMO09_FLINKSQL_EVALUATIONS_TABLE": outputs[
            ComputeEngine.FLINKSQL.value
        ],
    }


def render_sql_template(
    template_path: Path,
    *,
    run_id: str,
    ksqldb_value_schema_id: int,
) -> str:
    """Render an auditable SQL file for manual use in Confluent Cloud UI.

    This is deliberately a small renderer, not a SQL executor.  It rejects
    unknown and unresolved placeholders so a student never runs a half-rendered
    statement against the cloud account.
    """

    template = template_path.read_text(encoding="utf-8")
    context = sql_template_context(
        run_id,
        ksqldb_value_schema_id=ksqldb_value_schema_id,
    )
    requested = set(_PLACEHOLDER_PATTERN.findall(template))
    unknown = requested - set(context)
    if unknown:
        raise ValueError(
            f"Unknown SQL template placeholders: {sorted(unknown)}"
        )
    rendered = template
    for key in requested:
        rendered = rendered.replace(f"${{{key}}}", context[key])
    unresolved = sorted(set(_PLACEHOLDER_PATTERN.findall(rendered)))
    if unresolved:
        raise ValueError(f"Unresolved SQL placeholders: {unresolved}")
    return rendered


def rendered_sql(
    run_id: str,
    *,
    ksqldb_value_schema_id: int,
) -> dict[str, str]:
    """Render both engine templates from the same experiment context."""

    return {
        ComputeEngine.KSQLDB.value: render_sql_template(
            KSQLDB_TEMPLATE_PATH,
            run_id=run_id,
            ksqldb_value_schema_id=ksqldb_value_schema_id,
        ),
        ComputeEngine.FLINKSQL.value: render_sql_template(
            FLINKSQL_TEMPLATE_PATH,
            run_id=run_id,
            ksqldb_value_schema_id=ksqldb_value_schema_id,
        ),
    }
