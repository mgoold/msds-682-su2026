"""Current Confluent producer and bounded consumer used by Demo 05C/05D."""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import AsyncExitStack
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.aio import AIOProducer
from confluent_kafka.schema_registry import (
    AsyncSchemaRegistryClient,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import (
    AsyncAvroSerializer,
    AvroDeserializer,
)
from confluent_kafka.serialization import MessageField, SerializationContext

from confluent_demo_common import kafka_config
from demo05_common import PublishError, PublishReceipt
from trip_event_contract import (
    TripEventV1,
    avro_dict_to_event,
    deserializer_conf,
    event_key,
    event_to_avro_dict,
    parse_confluent_wire_header,
    schema_v1_str,
    serializer_conf,
)


async def publish_one_event(
    producer: Any,
    serializer: Any,
    context: SerializationContext,
    *,
    topic: str,
    event: TripEventV1,
    delivery_timeout: float,
) -> PublishReceipt:
    """Serialize and await the broker delivery future for one API event."""

    try:
        # ====================================================================
        # KEY CONCEPT
        # Native AIO keeps the FastAPI event loop nonblocking. Serialization,
        # enqueue, and broker acknowledgement are three distinct stages.
        # ====================================================================
        value_bytes = await serializer(event, context)
        if value_bytes is None:
            raise RuntimeError("AsyncAvroSerializer unexpectedly returned None")
        delivery_future = await producer.produce(
            topic,
            key=event_key(event),
            value=value_bytes,
        )
        message = await asyncio.wait_for(delivery_future, timeout=delivery_timeout)
    except Exception as exc:
        raise PublishError("Kafka delivery was not acknowledged") from exc

    delivered_value = message.value() or value_bytes
    return PublishReceipt(
        delivery="broker_acknowledged",
        topic=message.topic(),
        key=event.trip_id,
        partition=message.partition(),
        offset=message.offset(),
        wire=parse_confluent_wire_header(delivered_value),
    )


class AsyncAvroTripPublisher:
    """One lifespan-managed AIO producer with async Schema Registry serdes."""

    def __init__(
        self,
        *,
        stack: AsyncExitStack,
        producer: AIOProducer,
        serializer: AsyncAvroSerializer,
        topic: str,
        delivery_timeout: float,
    ) -> None:
        self._stack = stack
        self._producer = producer
        self._serializer = serializer
        self._context = SerializationContext(topic, MessageField.VALUE)
        self.topic = topic
        self.delivery_timeout = delivery_timeout
        self.receipts: list[PublishReceipt] = []
        self.closed = False

    @classmethod
    async def create(
        cls,
        *,
        topic: str,
        producer_config: dict[str, Any],
        registry_config: dict[str, Any],
        delivery_timeout: float,
    ) -> "AsyncAvroTripPublisher":
        """Open the producer and Registry clients for one app lifespan."""

        stack = AsyncExitStack()
        try:
            registry = await stack.enter_async_context(
                AsyncSchemaRegistryClient(registry_config)
            )
            serializer = await AsyncAvroSerializer(
                registry,
                schema_v1_str(),
                to_dict=event_to_avro_dict,
                conf=serializer_conf(),
            )
            config = dict(producer_config)
            config["delivery.timeout.ms"] = int(delivery_timeout * 1000)
            producer = await stack.enter_async_context(AIOProducer(config))
            return cls(
                stack=stack,
                producer=producer,
                serializer=serializer,
                topic=topic,
                delivery_timeout=delivery_timeout,
            )
        except BaseException:
            await stack.aclose()
            raise

    async def publish(self, event: TripEventV1) -> PublishReceipt:
        """Publish one event and return only after broker acknowledgement."""

        if self.closed:
            raise PublishError("The Kafka publisher is closed")
        receipt = await publish_one_event(
            self._producer,
            self._serializer,
            self._context,
            topic=self.topic,
            event=event,
            delivery_timeout=self.delivery_timeout,
        )
        self.receipts.append(receipt)
        return receipt

    async def close(self) -> None:
        """Flush queued records and close both clients with bounded waits."""

        if self.closed:
            return
        self.closed = True
        primary_error: Exception | None = None
        try:
            remaining = await asyncio.wait_for(
                self._producer.flush(self.delivery_timeout),
                timeout=self.delivery_timeout + 1.0,
            )
            if remaining:
                raise RuntimeError(
                    f"AIOProducer still had {remaining} queued messages at shutdown"
                )
        except asyncio.CancelledError:
            # Cancellation remains cancellation, but owned clients still close.
            try:
                await self._stack.aclose()
            except Exception:
                pass
            raise
        except Exception as exc:
            primary_error = exc
        try:
            await asyncio.wait_for(
                self._stack.aclose(),
                timeout=self.delivery_timeout + 1.0,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if primary_error is None:
                primary_error = exc
        if primary_error is not None:
            raise PublishError("Kafka publisher cleanup did not complete") from primary_error


class BoundedTripConsumer:
    """Independent standard-client worker for one finite expected-key set."""

    def __init__(
        self,
        *,
        topic: str,
        group_id: str,
        expected_keys: frozenset[bytes],
        registry_config: dict[str, Any],
        assignment_timeout: float,
        consumer_timeout: float,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.expected_keys = expected_keys
        self.registry_config = registry_config
        self.assignment_timeout = assignment_timeout
        self.consumer_timeout = consumer_timeout
        self.ready = threading.Event()
        self.publishing_complete = threading.Event()
        self.stop_requested = threading.Event()
        self.records: list[dict[str, Any]] = []
        self.assignments: list[list[dict[str, int | str]]] = []
        self.skipped = 0
        self.error: BaseException | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the independent consumer thread."""

        if self._thread is not None:
            raise RuntimeError("Consumer worker was already started")
        self._thread = threading.Thread(
            target=self._run,
            name="demo05-bounded-consumer",
            daemon=True,
        )
        self._thread.start()

    def wait_until_ready(self) -> None:
        """Wait for Kafka's real partition assignment callback."""

        if not self.ready.wait(self.assignment_timeout + 1.0):
            raise RuntimeError("Consumer thread did not report assignment readiness")
        if self.error is not None:
            raise RuntimeError("Consumer failed before assignment") from self.error
        if not self.assignments:
            raise RuntimeError("Consumer did not receive a partition assignment")

    def stop(self) -> None:
        """Request bounded early shutdown after an API-side failure."""

        self.stop_requested.set()

    def mark_publishing_complete(self) -> None:
        """Start the downstream completion budget after the final HTTP post."""

        self.publishing_complete.set()

    def join(self) -> list[dict[str, Any]]:
        """Wait for the bounded consumer and surface its original error."""

        if self._thread is None:
            raise RuntimeError("Consumer worker was not started")
        self._thread.join(self.assignment_timeout + self.consumer_timeout + 3.0)
        if self._thread.is_alive():
            self.stop()
            self._thread.join(1.0)
            raise RuntimeError("Consumer worker exceeded its bounded timeout")
        if self.error is not None:
            raise RuntimeError("Consumer worker failed") from self.error
        return self.records

    @staticmethod
    def _partition_rows(partitions: Any) -> list[dict[str, int | str]]:
        return [
            {
                "topic": partition.topic,
                "partition": partition.partition,
                "offset": partition.offset,
            }
            for partition in partitions
        ]

    def _process_message(
        self,
        *,
        consumer: Consumer,
        deserializer: Any,
        context: SerializationContext,
        message: Any,
        consumed_keys: set[bytes],
    ) -> None:
        """Validate, record, and commit one polled Kafka message if expected."""

        if message.error():
            error = message.error()
            if error.code() == KafkaError._PARTITION_EOF:
                return
            raise RuntimeError(f"Consumer error: {error}")
        key = message.key()
        if key not in self.expected_keys or key in consumed_keys:
            self.skipped += 1
            return
        event = deserializer(message.value(), context)
        if not isinstance(event, TripEventV1):
            raise TypeError("Expected AvroDeserializer to return TripEventV1")
        if event_key(event) != key:
            raise ValueError("Kafka key does not match the deserialized trip_id")
        consumed_keys.add(key)
        self.records.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": key.decode("utf-8") if key else None,
                "wire": parse_confluent_wire_header(message.value()),
                "event": event.report_dict(),
            }
        )
        # ====================================================================
        # KEY CONCEPT
        # Deserialize, validate, and process before committing.
        # ====================================================================
        consumer.commit(message=message, asynchronous=False)

    def _run(self) -> None:
        consumer: Consumer | None = None
        try:
            context = SerializationContext(self.topic, MessageField.VALUE)
            with SchemaRegistryClient(self.registry_config) as registry:
                deserializer = AvroDeserializer(
                    registry,
                    schema_v1_str(),
                    from_dict=avro_dict_to_event,
                    conf=deserializer_conf(),
                )
                # ============================================================
                # IMPORTANT NOTE
                # This bounded worker runs outside FastAPI's event loop, so the
                # standard Consumer is simpler than an AIO consumer here.
                # ============================================================
                consumer_config: dict[str, Any] = {
                    **kafka_config(client_id="msds682-demo05-consumer"),
                    "group.id": self.group_id,
                    "group.protocol": "classic",
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                }
                consumer = Consumer(consumer_config)
                consumed_keys: set[bytes] = set()

                def on_assign(active_consumer: Consumer, partitions: Any) -> None:
                    active_consumer.assign(partitions)
                    self.assignments.append(self._partition_rows(partitions))
                    self.ready.set()

                consumer.subscribe([self.topic], on_assign=on_assign)
                assignment_deadline = time.monotonic() + self.assignment_timeout
                while not self.ready.is_set() and time.monotonic() < assignment_deadline:
                    message = consumer.poll(0.25)
                    if message is not None:
                        # A poll that triggers assignment may also return data.
                        # Route it through the same validation and commit path.
                        self._process_message(
                            consumer=consumer,
                            deserializer=deserializer,
                            context=context,
                            message=message,
                            consumed_keys=consumed_keys,
                        )
                if not self.ready.is_set():
                    raise RuntimeError("Consumer assignment timed out")

                completion_deadline: float | None = None
                while (
                    len(consumed_keys) < len(self.expected_keys)
                    and not self.stop_requested.is_set()
                ):
                    if completion_deadline is None and self.publishing_complete.is_set():
                        completion_deadline = time.monotonic() + self.consumer_timeout
                    if (
                        completion_deadline is not None
                        and time.monotonic() >= completion_deadline
                    ):
                        break
                    message = consumer.poll(0.5)
                    if message is None:
                        continue
                    self._process_message(
                        consumer=consumer,
                        deserializer=deserializer,
                        context=context,
                        message=message,
                        consumed_keys=consumed_keys,
                    )
        except BaseException as exc:
            self.error = exc
            self.ready.set()
        finally:
            if consumer is not None:
                consumer.close()
