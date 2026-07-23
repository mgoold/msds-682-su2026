from __future__ import annotations

import importlib.metadata
import json
import sys
from pathlib import Path

import pytest
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import ValidationError

from demo06_common import (
    AssignmentTracker,
    DatagenOrderV1,
    OrderMetricV1,
    connector_console_plan,
    datagen_order_schema_str,
    derive_order_metric,
    deterministic_orders,
    order_metric_schema_str,
    wait_for_assignment,
)
from demo06c_confluent_stream_processor import process_one_message
from demo06d_confluent_resume_replay import validate_resume_replay

COURSE_VERSIONS = {
    "confluent-kafka": "2.15.0",
    "pydantic": "2.13.4",
    "pytest": "9.1.1",
}


def test_course_runtime_uses_exact_pins() -> None:
    assert sys.version_info[:3] == (3, 11, 14)
    assert {
        package: importlib.metadata.version(package)
        for package in COURSE_VERSIONS
    } == COURSE_VERSIONS


def test_both_avro_schemas_are_valid_and_have_distinct_owners() -> None:
    input_schema = json.loads(datagen_order_schema_str())
    output_schema = json.loads(order_metric_schema_str())
    assert input_schema["name"] == "orders"
    assert input_schema["namespace"] == "ksql"
    assert input_schema["connect.name"] == "ksql.orders"
    assert output_schema["name"] == "OrderMetricV1"
    assert {field["name"] for field in output_schema["fields"]} == {
        "source_topic",
        "source_partition",
        "source_offset",
        "source_record_id",
        "orderid",
        "itemid",
        "orderunits",
        "size_band",
    }


def test_deterministic_fallback_matches_strict_input_contract() -> None:
    first = deterministic_orders(4, seed_offset=9)
    second = deterministic_orders(4, seed_offset=9)
    different = deterministic_orders(4, seed_offset=10)
    assert first == second
    assert first != different
    assert all(isinstance(row, DatagenOrderV1) for row in first)


def test_input_contract_forbids_extra_fields_and_invalid_item() -> None:
    payload = deterministic_orders(1, seed_offset=2)[0].model_dump()
    with pytest.raises(ValidationError):
        DatagenOrderV1.model_validate({**payload, "unexpected": True})
    with pytest.raises(ValidationError):
        DatagenOrderV1.model_validate({**payload, "itemid": "bad"})


def test_derived_metric_is_deterministic_and_provenance_keyed() -> None:
    order = deterministic_orders(1, seed_offset=3)[0]
    metric = derive_order_metric(
        order,
        source_topic="test.input",
        source_partition=1,
        source_offset=27,
    )
    assert metric == OrderMetricV1(
        source_topic="test.input",
        source_partition=1,
        source_offset=27,
        source_record_id="test.input:1:27",
        orderid=order.orderid,
        itemid=order.itemid,
        orderunits=order.orderunits,
        size_band="large" if order.orderunits >= 0.5 else "standard",
    )


def test_connector_plan_is_secret_free_and_bounded_for_class() -> None:
    plan = connector_console_plan(topic="test.orders", max_interval_ms=2_000)
    serialized = json.dumps(plan).lower()
    assert plan["quickstart"] == "ORDERS"
    assert plan["output_data_format"] == "AVRO"
    assert plan["schema_keyfield"] == "orderid"
    assert plan["tasks_max"] == 1
    assert "api.secret" not in serialized
    assert "password" not in serialized
    assert "pause or delete" in plan["stop_condition"].lower()


def test_assignment_poll_preserves_first_data_message() -> None:
    class Partition:
        topic = "test.input"
        partition = 0
        offset = -1001

    class Message:
        def error(self):
            return None

    first_message = Message()

    class Consumer:
        on_assign = None

        def subscribe(self, _topics, *, on_assign, on_revoke=None) -> None:
            self.on_assign = on_assign

        def assign(self, _partitions) -> None:
            return None

        def poll(self, _timeout):
            assert self.on_assign is not None
            self.on_assign(self, [Partition()])
            return first_message

    consumer = Consumer()
    tracker = AssignmentTracker()
    consumer.subscribe(["test.input"], on_assign=tracker.on_assign)
    _wait, pending = wait_for_assignment(consumer, tracker, timeout=0.1)
    assert pending == [first_message]


class FakeMessage:
    def __init__(self, *, offset: int = 4) -> None:
        self._offset = offset

    def value(self) -> bytes:
        return b"input"

    def topic(self) -> str:
        return "test.input"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeOutputMessage:
    def topic(self) -> str:
        return "test.output"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 8


def test_process_one_message_acknowledges_output_before_input_commit() -> None:
    events: list[str] = []
    order = deterministic_orders(1, seed_offset=4)[0]

    class Producer:
        def produce(self, _topic: str, **kwargs) -> None:
            events.append("produce")
            kwargs["on_delivery"](None, FakeOutputMessage())
            events.append("ack")

        def poll(self, _timeout: float) -> None:
            return None

        def flush(self, _timeout: float) -> int:
            events.append("flush")
            return 0

    class Consumer:
        def commit(self, *, message: object, asynchronous: bool) -> None:
            assert isinstance(message, FakeMessage)
            assert asynchronous is False
            events.append("commit")

    result = process_one_message(
        message=FakeMessage(),
        consumer=Consumer(),
        producer=Producer(),
        input_deserializer=lambda _value, _ctx: order.model_dump(),
        output_serializer=lambda _metric, _ctx: b"output",
        input_context=SerializationContext("test.input", MessageField.VALUE),
        output_context=SerializationContext("test.output", MessageField.VALUE),
        output_topic="test.output",
        delivery_timeout=1.0,
    )
    assert events == ["produce", "ack", "flush", "commit"]
    assert result["source_record_id"] == "test.input:0:4"
    assert result["input_commit"] == "sync_after_output_ack"


def test_output_failure_prevents_input_commit() -> None:
    order = deterministic_orders(1, seed_offset=5)[0]
    commits: list[object] = []

    class Producer:
        def produce(self, _topic: str, **kwargs) -> None:
            kwargs["on_delivery"](RuntimeError("broker failure"), None)

        def poll(self, _timeout: float) -> None:
            return None

        def flush(self, _timeout: float) -> int:
            return 0

    class Consumer:
        def commit(self, **kwargs) -> None:
            commits.append(kwargs)

    with pytest.raises(RuntimeError, match="input offset was not committed"):
        process_one_message(
            message=FakeMessage(),
            consumer=Consumer(),
            producer=Producer(),
            input_deserializer=lambda _value, _ctx: order.model_dump(),
            output_serializer=lambda _metric, _ctx: b"output",
            input_context=SerializationContext("test.input", MessageField.VALUE),
            output_context=SerializationContext("test.output", MessageField.VALUE),
            output_topic="test.output",
            delivery_timeout=1.0,
        )
    assert commits == []


def test_resume_replay_validation_distinguishes_group_behavior() -> None:
    def report(group: str, coordinates: list[str]) -> dict:
        return {
            "group_id": group,
            "records": [{"source_record_id": value} for value in coordinates],
        }

    result = validate_resume_replay(
        first=report("base", ["input:0:0", "input:0:1"]),
        resume=report("base", ["input:0:2", "input:0:3"]),
        replay=report("replay", ["input:0:0", "input:0:1"]),
    )
    assert all(result["checks"].values())


def test_handout_has_current_api_and_no_legacy_migration_notes() -> None:
    package_root = Path(__file__).resolve().parents[1]
    handout = package_root / "demo06.md"
    if not handout.is_file():
        handout = package_root / "README.md"
    assert handout.is_file()
    text = handout.read_text(encoding="utf-8")
    assert "confluent-kafka[avro,schemaregistry]==2.15.0" in text
    assert "2023" not in text
    assert "Faust" not in text
    assert "deprecated" not in text.lower()


def test_handout_screenshots_exist_and_are_referenced() -> None:
    package_root = Path(__file__).resolve().parents[1]
    handout = package_root / "demo06.md"
    if not handout.is_file():
        handout = package_root / "README.md"
    text = handout.read_text(encoding="utf-8")
    expected = {
        "assets/demo06/demo06a-connector-configuration.jpg",
        "assets/demo06/demo06a-connector-running.jpg",
        "assets/demo06/demo06b-topic-messages.jpg",
        "assets/demo06/demo06b-topic-schema.jpg",
        "assets/demo06/demo06c-actual-result.jpg",
        "assets/demo06/demo06d-resume-replay.jpg",
    }
    sentinel = "assets/demo06/demo06a-connector-configuration.jpg"
    asset_root = (
        package_root
        if (package_root / sentinel).is_file()
        else package_root.parent
    )
    for relative_path in expected:
        assert relative_path in text
        assert (asset_root / relative_path).is_file()
