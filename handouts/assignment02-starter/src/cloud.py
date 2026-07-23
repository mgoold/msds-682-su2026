"""Current Confluent topic, producer, and Schema Registry helpers."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any

from confluent_kafka.admin import AdminClient, NewTopic
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

from contracts import (
    PublishError,
    PublishReceipt,
    TripEventV1,
    avro_dict_to_event,
    event_key,
    event_to_avro_dict,
    schema_str,
    serializer_conf,
)


def ensure_topic(
    admin: AdminClient,
    *,
    topic: str,
    create: bool,
    partitions: int,
    replication_factor: int,
    timeout: float = 15.0,
) -> str:
    """Require or create one dedicated assignment topic."""

    metadata = admin.list_topics(timeout=timeout)
    if topic in metadata.topics:
        return "existing"
    if not create:
        raise RuntimeError(
            f"Topic {topic!r} does not exist; rerun with --create-topic"
        )
    future = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=replication_factor,
            )
        ]
    )[topic]
    future.result(timeout=timeout)
    return "created"


class AsyncAvroPublisher:
    """One AIO producer and Registry client owned by a FastAPI lifespan."""

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
        registry_config: dict[str, str],
        delivery_timeout: float,
    ) -> "AsyncAvroPublisher":
        """Open all owned clients or close partial state on failure."""

        stack = AsyncExitStack()
        try:
            registry = await stack.enter_async_context(
                AsyncSchemaRegistryClient(registry_config)
            )
            serializer = await AsyncAvroSerializer(
                registry,
                schema_str(),
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
        """Serialize and wait for one broker delivery acknowledgement."""

        if self.closed:
            raise PublishError("The publisher is already closed")
        try:
            value = await self._serializer(event, self._context)
            if value is None:
                raise RuntimeError("Avro serialization returned no value")
            delivery_future = await self._producer.produce(
                self.topic,
                key=event_key(event),
                value=value,
            )
            message = await asyncio.wait_for(
                delivery_future,
                timeout=self.delivery_timeout,
            )
        except Exception as exc:
            raise PublishError("Kafka delivery was not acknowledged") from exc

        receipt = PublishReceipt(
            topic=message.topic(),
            key=event.trip_id,
            partition=message.partition(),
            offset=message.offset(),
            delivery="broker_acknowledged",
        )
        self.receipts.append(receipt)
        return receipt

    async def close(self) -> None:
        """Flush and close all lifespan-owned clients with finite waits."""

        if self.closed:
            return
        self.closed = True
        try:
            remaining = await asyncio.wait_for(
                self._producer.flush(self.delivery_timeout),
                timeout=self.delivery_timeout + 1.0,
            )
            if remaining:
                raise RuntimeError(f"{remaining} producer messages remain queued")
        except Exception as exc:
            try:
                await self._stack.aclose()
            finally:
                raise PublishError("Publisher cleanup did not complete") from exc
        await asyncio.wait_for(
            self._stack.aclose(),
            timeout=self.delivery_timeout + 1.0,
        )


def make_avro_deserializer(
    registry_configuration: dict[str, str],
) -> AvroDeserializer:
    """Build the synchronous consumer's schema-aware value deserializer."""

    registry = SchemaRegistryClient(registry_configuration)
    return AvroDeserializer(
        registry,
        schema_str(),
        from_dict=avro_dict_to_event,
    )
