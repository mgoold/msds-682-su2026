"""
================================================================================
DEMO 05 KAFKA - THE REAL CONFLUENT PUBLISHER AND BOUNDED CONSUMER
(annotated tutorial copy)
================================================================================

READ demo05_common.py AND demo05_app.py FIRST. This file supplies the Cloud
half of the "publisher" abstraction those files defined: AsyncAvroTripPublisher
satisfies the same AsyncTripPublisher Protocol as LocalTripPublisher, but it
really talks to Kafka and Schema Registry.

WHAT THIS FILE TEACHES
    Two things that Demo 02/03/04 did not need together, because those demos
    always ran producer and consumer as separate, independently-invoked
    scripts:

    1. AN ASYNC ("AIO") PRODUCER, because FastAPI already owns an asyncio
       event loop. Using the STANDARD blocking confluent_kafka.Producer inside
       an `async def` route would block that shared event loop and stall every
       other request the server is handling - a much bigger problem in a
       server than in a one-shot script. confluent_kafka.aio.AIOProducer and
       AsyncAvroSerializer/AsyncSchemaRegistryClient exist specifically so a
       FastAPI route can `await` Kafka work instead of blocking on it.

    2. A CONSUMER RUNNING ON A BACKGROUND THREAD, independent of the API
       process, used only to VERIFY (in Demo 05C) that what the API published
       really arrived and can be read back. It uses the plain, synchronous
       confluent_kafka.Consumer from Demo 03/04 - no event loop conflict here,
       because it is a threading.Thread, not a coroutine competing with
       FastAPI's loop.

WHY TWO DIFFERENT CLIENT STYLES IN ONE FILE
    "Use async where you already have an event loop to protect (the API); use
    the plain blocking client where you do not (an independent worker
    thread)." Demo 05.md section 11 states this rule directly - this file is
    where it is implemented.
================================================================================
"""

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
    """Serialize and await the broker delivery future for one API event.

    THE THREE STAGES OF PUBLISHING AN EVENT, made explicit by three separate
    awaited calls below - a level of granularity Demo 02's blocking produce()
    hid inside one function call:

        1. serialize   (validated object -> Avro bytes)
        2. enqueue     (hand the bytes to the client's internal queue)
        3. acknowledge (wait for the broker's delivery confirmation)
    """

    try:
        # ====================================================================
        # KEY CONCEPT
        # Native AIO keeps the FastAPI event loop nonblocking. Serialization,
        # enqueue, and broker acknowledgement are three distinct stages.
        # ====================================================================

        # STAGE 1: OBJECT -> AVRO BYTES. AsyncAvroSerializer may need to fetch
        # or register the schema against Schema Registry over the network -
        # exactly like Demo 04's AvroSerializer, but here it is `await`-ed so
        # that network round trip never blocks FastAPI's event loop.
        value_bytes = await serializer(event, context)
        if value_bytes is None:
            raise RuntimeError("AsyncAvroSerializer unexpectedly returned None")

        # STAGE 2: ENQUEUE. AIOProducer.produce() returns an awaitable that
        # resolves to ANOTHER awaitable - a delivery future - once the record
        # has been accepted into the client's internal queue. This is the AIO
        # equivalent of Demo 02's synchronous producer.produce() call, which
        # returned instantly because it only enqueues.
        delivery_future = await producer.produce(
            topic,
            key=event_key(event),
            value=value_bytes,
        )

        # STAGE 3: ACKNOWLEDGE. Awaiting the delivery future is what makes this
        # equivalent to Demo 02A's "sync-style" flush()-per-message pattern -
        # except here it does not block a thread; it suspends only this one
        # coroutine, and FastAPI's event loop remains free to serve other
        # requests concurrently. asyncio.wait_for bounds how long we will wait
        # for that acknowledgement before giving up.
        message = await asyncio.wait_for(delivery_future, timeout=delivery_timeout)
    except Exception as exc:
        # ANY failure across all three stages - serialization, enqueue
        # rejection, or a delivery timeout/error - becomes the SAME PublishError
        # type, which demo05_app.py catches and turns into an honest HTTP 503.
        # The caller never needs to distinguish which stage failed.
        raise PublishError("Kafka delivery was not acknowledged") from exc

    # message.value() returns the bytes the broker actually stored; falling
    # back to value_bytes covers the rare case where the client does not echo
    # the payload back on the delivered message.
    delivered_value = message.value() or value_bytes
    return PublishReceipt(
        # "broker_acknowledged" - the honest counterpart to LocalTripPublisher's
        # "local". By the time this line runs, Kafka has genuinely confirmed
        # receipt of the record.
        delivery="broker_acknowledged",
        topic=message.topic(),
        key=event.trip_id,
        partition=message.partition(),
        offset=message.offset(),
        wire=parse_confluent_wire_header(delivered_value),
    )


class AsyncAvroTripPublisher:
    """One lifespan-managed AIO producer with async Schema Registry serdes.

    This is the Cloud-mode implementation of the AsyncTripPublisher Protocol
    from demo05_app.py. FastAPI's lifespan hook constructs exactly one of
    these (see `create()` below) and every request reuses it.
    """

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
        # Built once and reused for every publish() call, rather than
        # reconstructed per-request - it never changes for a given topic.
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
        """Open the producer and Registry clients for one app lifespan.

        A classmethod factory rather than a plain __init__, because opening
        these clients is genuinely asynchronous (they perform real network
        setup) - and __init__ itself can never be `async def` in Python.
        """

        # AsyncExitStack collects several "must be closed together" async
        # resources (the Registry client, the producer) so that close() later
        # can release all of them in one call, in reverse order, even if only
        # some of them finished opening successfully.
        stack = AsyncExitStack()
        try:
            # enter_async_context both opens the resource (calling its
            # __aenter__) AND registers it to be closed automatically by
            # stack.aclose() later - one line does both jobs.
            registry = await stack.enter_async_context(
                AsyncSchemaRegistryClient(registry_config)
            )

            # Same Avro machinery as Demo 04C's AvroSerializer - the SAME
            # schema string, the SAME to_dict converter, the SAME
            # serializer_conf() settings from trip_event_contract.py - just
            # the "Async" variant, because it must be awaited rather than
            # called directly from inside an `async def` FastAPI route.
            serializer = await AsyncAvroSerializer(
                registry,
                schema_v1_str(),
                to_dict=event_to_avro_dict,
                conf=serializer_conf(),
            )

            # delivery.timeout.ms bounds how long librdkafka's background
            # thread will keep retrying a message internally before giving up
            # on it - a client-level ceiling that should be at least as large
            # as the per-call timeout this publisher itself enforces above.
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
            # If ANYTHING above failed partway through (say, the Registry
            # client opened but the producer construction then raised), close
            # whatever DID open before re-raising - otherwise a failed startup
            # would leak an open network client.
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
        """Flush queued records and close both clients with bounded waits.

        Called once, from demo05_app.py's lifespan `finally` block, at
        shutdown. Two things must happen, and the ordering/error-handling
        below exists to make sure BOTH happen even if one of them fails.
        """

        if self.closed:
            return
        self.closed = True

        # Remembers whichever error happened first, so a SECOND failure
        # during cleanup (below) never masks the original one - the same
        # "primary_error" pattern Demo 04C used in its own finally block.
        primary_error: Exception | None = None
        try:
            # Wait for any in-flight/queued records to be delivered before
            # tearing down the client out from under them. The OUTER
            # asyncio.wait_for is a hard backstop in case flush() itself never
            # returns within its own timeout for some reason.
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
            # Uvicorn cancels the lifespan's shutdown task on a forced
            # shutdown; even then, the Registry/producer connections should
            # not be abandoned open.
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
    """Independent standard-client worker for one finite expected-key set.

    THIS CLASS IS PURELY FOR VERIFICATION (used only by Demo 05C). It runs on
    its own background thread using the plain synchronous Consumer from Demo
    03/04 - not the AIO client - because it has no FastAPI event loop of its
    own to protect. Its job: prove that every event the API published can
    also be read back and validated by an ordinary consumer, independent of
    the API process.
    """

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
        # The FINITE, KNOWN-IN-ADVANCE set of keys this run expects to see -
        # computed by Demo 05C from the same deterministic requests it is
        # about to post. This is what lets the consumer know when it is DONE,
        # rather than polling forever.
        self.expected_keys = expected_keys
        self.registry_config = registry_config
        self.assignment_timeout = assignment_timeout
        self.consumer_timeout = consumer_timeout

        # Three threading.Event flags coordinate the background thread with
        # the main thread WITHOUT sharing mutable state unsafely:
        self.ready = threading.Event()                # partition assignment confirmed
        self.publishing_complete = threading.Event()   # API finished posting
        self.stop_requested = threading.Event()        # main thread wants early exit

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
        """Wait for Kafka's real partition assignment callback.

        Demo 05C calls this BEFORE posting a single HTTP request - the exact
        same "wait for real assignment before producing" discipline Demo 04C
        used with its own on_assign callback, just running on a background
        thread instead of the main one.
        """

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
        """Start the downstream completion budget after the final HTTP post.

        WHY THIS EXISTS: consumer_timeout should measure how long it takes for
        already-published messages to arrive and be processed, NOT how long
        the HTTP posting phase itself took. Calling this only after the last
        POST has returned means the completion clock (see _run below) starts
        at the right moment rather than the instant the worker thread began.
        """

        self.publishing_complete.set()

    def join(self) -> list[dict[str, Any]]:
        """Wait for the bounded consumer and surface its original error."""

        if self._thread is None:
            raise RuntimeError("Consumer worker was not started")
        self._thread.join(self.assignment_timeout + self.consumer_timeout + 3.0)
        if self._thread.is_alive():
            # The thread exceeded every internal timeout it was given AND our
            # generous outer join budget - something is genuinely stuck.
            # Request it to stop and give it one more brief chance to exit
            # cleanly before reporting failure.
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

        # THE SHARED-TOPIC FILTER, same idea as Demo 04C's run-id header
        # check, but implemented on the KEY instead of a header: this
        # consumer is looking ONLY for the specific keys its own run
        # generated. Anything else - another student's traffic, or keys
        # already consumed - is counted and skipped rather than raising.
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
        # Same at-least-once ordering rule as Demo 03B/04C: the commit is the
        # LAST thing that happens for this message, after it has already been
        # decoded, validated, and appended to the evidence list.
        consumer.commit(message=message, asynchronous=False)

    def _run(self) -> None:
        """The background thread's entire body - runs until done or timed out.

        Everything below runs OFF the main thread, which is why results are
        communicated back through the instance attributes set up in __init__
        (self.records, self.error, the threading.Events) rather than a return
        value - a thread's return value is otherwise inaccessible.
        """

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
                    # Classic protocol: this code supplies its own on_assign
                    # callback and calls consumer.assign() itself, exactly the
                    # pattern Demo 03/04 used.
                    "group.protocol": "classic",
                    # "latest" - deliberately not "earliest". This run should
                    # see ONLY messages the upcoming HTTP posts produce, not
                    # this shared topic's entire history. That is also exactly
                    # why assignment must be confirmed before any POST happens.
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                }
                consumer = Consumer(consumer_config)
                consumed_keys: set[bytes] = set()

                def on_assign(active_consumer: Consumer, partitions: Any) -> None:
                    active_consumer.assign(partitions)
                    self.assignments.append(self._partition_rows(partitions))
                    # Signals wait_until_ready() on the MAIN thread that this
                    # worker genuinely owns partitions now.
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

                # THE COMPLETION-BUDGET LOOP. Rather than a single fixed
                # timeout for the whole run, the countdown to consumer_timeout
                # only STARTS once mark_publishing_complete() has been called
                # by the main thread - so time spent waiting for HTTP
                # responses never eats into this budget.
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
            # Store the exception rather than letting it terminate the thread
            # silently - Python threads do not propagate exceptions to
            # join(); this attribute is how the main thread finds out.
            self.error = exc
            # Also set `ready` even on failure, so a caller blocked in
            # wait_until_ready() does not simply hang until its own timeout -
            # it wakes up immediately and finds self.error populated.
            self.ready.set()
        finally:
            if consumer is not None:
                consumer.close()
