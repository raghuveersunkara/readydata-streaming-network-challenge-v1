from __future__ import annotations

import logging
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import Any

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaError, KafkaException, Message, Producer

from .config import BridgeSettings
from .routing import RoutingError, route_message


LOGGER = logging.getLogger("readydata.mqtt_kafka_bridge")


@dataclass(frozen=True)
class PendingDelivery:
    connection_generation: int
    mqtt_mid: int
    mqtt_qos: int
    kafka_topic: str
    kafka_key: bytes
    kafka_value: bytes


@dataclass
class BridgeCounters:
    mqtt_received: int = 0
    kafka_delivered: int = 0
    kafka_delivery_failures: int = 0
    mqtt_acknowledged: int = 0
    mqtt_ack_failures: int = 0
    mqtt_redeliveries: int = 0
    routing_errors: int = 0
    backpressure_occurrences: int = 0
    stale_delivery_callbacks: int = 0
    delivered_by_topic: dict[str, int] = field(default_factory=dict)


@dataclass
class BridgeState:
    mqtt_connected: bool = False
    mqtt_subscribed: bool = False
    kafka_available: bool = False
    ready: bool = False
    accepting: bool = True
    connection_generation: int = 0
    pending: dict[tuple[int, int], PendingDelivery] = field(default_factory=dict)


class MqttKafkaBridge:
    def __init__(
        self,
        settings: BridgeSettings,
        readiness_path: Path,
        *,
        producer: Producer | None = None,
        mqtt_client: mqtt.Client | None = None,
    ) -> None:
        self.settings = settings
        self.readiness_path = readiness_path
        self.stop_requested = threading.Event()
        self._lock = threading.RLock()
        self.state = BridgeState()
        self.counters = BridgeCounters()
        self._retry_ids: deque[tuple[int, int]] = deque()
        self._retry_id_set: set[tuple[int, int]] = set()
        self._next_healthcheck = 0.0
        self._next_summary = 0.0
        self._producer = producer if producer is not None else self._create_producer()
        self._mqtt_client = (
            mqtt_client if mqtt_client is not None else self._create_mqtt_client()
        )

    def _create_producer(self) -> Producer:
        kafka_config = self.settings.config["kafka"]
        return Producer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "client.id": kafka_config["client_id"],
                "acks": kafka_config["acks"],
                "enable.idempotence": kafka_config["enable_idempotence"],
                "linger.ms": kafka_config["linger_ms"],
                "delivery.timeout.ms": kafka_config["delivery_timeout_ms"],
                "queue.buffering.max.messages": kafka_config[
                    "queue_buffering_max_messages"
                ],
                "error_cb": self._on_kafka_error,
            }
        )

    def _create_mqtt_client(self) -> mqtt.Client:
        mqtt_config = self.settings.config["mqtt"]
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=mqtt_config["client_id"],
            clean_session=mqtt_config["clean_session"],
            protocol=mqtt.MQTTv311,
            manual_ack=True,
        )
        client.on_connect = self._on_mqtt_connect
        client.on_connect_fail = self._on_mqtt_connect_fail
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_subscribe = self._on_mqtt_subscribe
        client.on_message = self._on_mqtt_message
        client.reconnect_delay_set(
            min_delay=mqtt_config["reconnect_min_seconds"],
            max_delay=mqtt_config["reconnect_max_seconds"],
        )
        return client

    def _refresh_readiness_locked(self) -> None:
        should_be_ready = (
            self.state.accepting
            and self.state.mqtt_connected
            and self.state.mqtt_subscribed
            and self.state.kafka_available
        )
        if should_be_ready == self.state.ready:
            return
        self.state.ready = should_be_ready
        if should_be_ready:
            self.readiness_path.parent.mkdir(parents=True, exist_ok=True)
            self.readiness_path.touch()
            LOGGER.info("bridge is ready: MQTT subscribed and Kafka available")
        else:
            self.readiness_path.unlink(missing_ok=True)
            LOGGER.warning("bridge is unready")

    def _set_kafka_available(self, available: bool) -> None:
        with self._lock:
            changed = self.state.kafka_available != available
            self.state.kafka_available = available
            self._refresh_readiness_locked()
        if changed:
            if available:
                LOGGER.info("Kafka target metadata is available")
            else:
                LOGGER.warning("Kafka target metadata is unavailable")

    def _check_kafka(self) -> bool:
        timeout = self.settings.config["runtime"]["kafka_healthcheck_timeout_seconds"]
        try:
            metadata = self._producer.list_topics(timeout=timeout)
            available = all(
                topic in metadata.topics
                and metadata.topics[topic].error is None
                and bool(metadata.topics[topic].partitions)
                for topic in self.settings.target_topics
            )
        except (KafkaException, RuntimeError) as error:
            LOGGER.warning("Kafka metadata check failed: %s", error)
            available = False
        self._set_kafka_available(available)
        return available

    def _on_kafka_error(self, error: KafkaError) -> None:
        LOGGER.warning("Kafka client error: %s", error)
        self._set_kafka_available(False)

    def _on_mqtt_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        if reason_code != 0:
            LOGGER.warning("MQTT connection rejected: %s", reason_code)
            with self._lock:
                self.state.mqtt_connected = False
                self.state.mqtt_subscribed = False
                self._refresh_readiness_locked()
            return

        with self._lock:
            self.state.connection_generation += 1
            generation = self.state.connection_generation
            self.state.mqtt_connected = True
            self.state.mqtt_subscribed = False
            self._refresh_readiness_locked()
        LOGGER.info(
            "connected to MQTT broker: generation=%d session_present=%s",
            generation,
            flags.session_present,
        )
        subscriptions = [
            (route.mqtt_subscription, route.mqtt_qos)
            for route in self.settings.routes
        ]
        result, _mid = client.subscribe(subscriptions)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("MQTT subscription request failed: rc=%s", result)
            with self._lock:
                self.state.mqtt_subscribed = False
                self._refresh_readiness_locked()

    def _on_mqtt_connect_fail(self, _client: mqtt.Client, _userdata: Any) -> None:
        LOGGER.warning("MQTT connection attempt failed")
        with self._lock:
            self.state.mqtt_connected = False
            self.state.mqtt_subscribed = False
            self._refresh_readiness_locked()

    def _on_mqtt_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        with self._lock:
            self.state.mqtt_connected = False
            self.state.mqtt_subscribed = False
            self._refresh_readiness_locked()
        if not self.stop_requested.is_set():
            LOGGER.warning("MQTT disconnected (%s); forwarding paused", reason_code)

    def _on_mqtt_subscribe(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _mid: int,
        reason_codes: list[mqtt.ReasonCode],
        _properties: mqtt.Properties | None,
    ) -> None:
        accepted = (
            len(reason_codes) == len(self.settings.routes)
            and all(not code.is_failure for code in reason_codes)
        )
        with self._lock:
            self.state.mqtt_subscribed = accepted
            self._refresh_readiness_locked()
        if accepted:
            LOGGER.info(
                "all %d MQTT event-family subscriptions acknowledged at QoS 1",
                len(self.settings.routes),
            )
        else:
            LOGGER.error("MQTT subscription rejected: %s", reason_codes)

    def _ack_mqtt(self, pending: PendingDelivery) -> bool:
        result = self._mqtt_client.ack(pending.mqtt_mid, pending.mqtt_qos)
        if result == mqtt.MQTT_ERR_SUCCESS:
            self.counters.mqtt_acknowledged += 1
            return True
        self.counters.mqtt_ack_failures += 1
        LOGGER.warning(
            "MQTT acknowledgement failed: generation=%d mid=%d rc=%s",
            pending.connection_generation,
            pending.mqtt_mid,
            result,
        )
        return False

    def _on_delivery(
        self,
        pending_id: tuple[int, int],
        error: KafkaError | None,
        message: Message,
    ) -> None:
        with self._lock:
            pending = self.state.pending.get(pending_id)
            if pending is None:
                return
            if error is not None:
                self.counters.kafka_delivery_failures += 1
                self.state.kafka_available = False
                self._queue_retry_locked(pending_id)
                self._refresh_readiness_locked()
                LOGGER.warning(
                    "Kafka delivery failed: topic=%s generation=%d mid=%d error=%s",
                    message.topic(),
                    pending.connection_generation,
                    pending.mqtt_mid,
                    error,
                )
                return

            self.state.pending.pop(pending_id, None)
            self._retry_id_set.discard(pending_id)
            self.counters.kafka_delivered += 1
            self.counters.delivered_by_topic[pending.kafka_topic] = (
                self.counters.delivered_by_topic.get(pending.kafka_topic, 0) + 1
            )
            if (
                self.state.mqtt_connected
                and pending.connection_generation == self.state.connection_generation
            ):
                self._ack_mqtt(pending)
            else:
                self.counters.stale_delivery_callbacks += 1
                LOGGER.warning(
                    "Kafka delivered after MQTT generation changed; awaiting redelivery: "
                    "generation=%d mid=%d",
                    pending.connection_generation,
                    pending.mqtt_mid,
                )

    def _queue_retry_locked(self, pending_id: tuple[int, int]) -> None:
        if pending_id in self._retry_id_set:
            return
        self._retry_id_set.add(pending_id)
        self._retry_ids.append(pending_id)

    def _produce_pending_locked(
        self,
        pending_id: tuple[int, int],
        pending: PendingDelivery,
    ) -> bool:
        try:
            self._producer.produce(
                pending.kafka_topic,
                key=pending.kafka_key,
                value=pending.kafka_value,
                on_delivery=lambda error, kafka_message, delivery_id=pending_id: self._on_delivery(
                    delivery_id, error, kafka_message
                ),
            )
            return True
        except BufferError:
            self.counters.backpressure_occurrences += 1
            self._queue_retry_locked(pending_id)
            return False
        except KafkaException as error:
            self.counters.kafka_delivery_failures += 1
            self.state.kafka_available = False
            self._queue_retry_locked(pending_id)
            self._refresh_readiness_locked()
            LOGGER.warning(
                "Kafka produce rejected: topic=%s generation=%d mid=%d error=%s",
                pending.kafka_topic,
                pending.connection_generation,
                pending.mqtt_mid,
                error,
            )
            return False

    def _retry_failed_deliveries(self) -> None:
        with self._lock:
            if not self.state.kafka_available or not self._retry_ids:
                return
            attempts = len(self._retry_ids)
            for _ in range(attempts):
                pending_id = self._retry_ids.popleft()
                self._retry_id_set.discard(pending_id)
                pending = self.state.pending.get(pending_id)
                if pending is None:
                    continue
                if not self._produce_pending_locked(pending_id, pending):
                    break

    def _ack_unroutable(self, client: mqtt.Client, message: mqtt.MQTTMessage) -> None:
        result = client.ack(message.mid, message.qos)
        with self._lock:
            if result == mqtt.MQTT_ERR_SUCCESS:
                self.counters.mqtt_acknowledged += 1
            else:
                self.counters.mqtt_ack_failures += 1

    def _on_mqtt_message(
        self,
        client: mqtt.Client,
        _userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        with self._lock:
            self.counters.mqtt_received += 1
            if message.dup:
                self.counters.mqtt_redeliveries += 1
            if not self.state.accepting:
                return
            generation = self.state.connection_generation

        try:
            routed = route_message(message.topic, bytes(message.payload), self.settings.routes)
        except RoutingError as error:
            with self._lock:
                self.counters.routing_errors += 1
            LOGGER.error("discarding unroutable MQTT message: mid=%d error=%s", message.mid, error)
            self._ack_unroutable(client, message)
            return

        pending_id = (generation, message.mid)
        with self._lock:
            if pending_id in self.state.pending:
                return
            maximum = self.settings.config["runtime"]["max_outstanding_messages"]
            if len(self.state.pending) >= maximum:
                self.counters.backpressure_occurrences += 1
                return
            pending = PendingDelivery(
                generation,
                message.mid,
                message.qos,
                routed.kafka_topic,
                routed.key,
                routed.value,
            )
            self.state.pending[pending_id] = pending
            self._produce_pending_locked(pending_id, pending)

    def _log_summary(self, now: float) -> None:
        if now < self._next_summary:
            return
        with self._lock:
            LOGGER.info(
                "forwarding summary: mqtt_received=%d kafka_delivered=%d "
                "delivery_failures=%d mqtt_acked=%d redeliveries=%d "
                "routing_errors=%d outstanding=%d backpressure=%d by_topic=%s",
                self.counters.mqtt_received,
                self.counters.kafka_delivered,
                self.counters.kafka_delivery_failures,
                self.counters.mqtt_acknowledged,
                self.counters.mqtt_redeliveries,
                self.counters.routing_errors,
                len(self.state.pending),
                self.counters.backpressure_occurrences,
                dict(sorted(self.counters.delivered_by_topic.items())),
            )
        self._next_summary = now + self.settings.config["logging"][
            "summary_interval_seconds"
        ]

    def _request_stop(self, signum: int, _frame: FrameType | None) -> None:
        LOGGER.info("received signal %s; beginning graceful shutdown", signum)
        with self._lock:
            self.state.accepting = False
            self._refresh_readiness_locked()
        self.stop_requested.set()

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)

    def _shutdown(self) -> None:
        with self._lock:
            self.state.accepting = False
            self._refresh_readiness_locked()
        remaining = self._producer.flush(
            self.settings.config["runtime"]["shutdown_flush_seconds"]
        )
        try:
            self._mqtt_client.disconnect()
        finally:
            self._mqtt_client.loop_stop()
        with self._lock:
            pending = len(self.state.pending)
        if remaining or pending:
            LOGGER.warning(
                "bridge shutdown left records eligible for MQTT redelivery: "
                "producer_remaining=%d outstanding=%d",
                remaining,
                pending,
            )
        LOGGER.info(
            "bridge stopped: mqtt_received=%d kafka_delivered=%d mqtt_acked=%d",
            self.counters.mqtt_received,
            self.counters.kafka_delivered,
            self.counters.mqtt_acknowledged,
        )

    def run(self) -> int:
        self._install_signal_handlers()
        self.readiness_path.unlink(missing_ok=True)
        LOGGER.info(
            "starting bridge: mqtt=%s:%d kafka=%s routes=%s",
            self.settings.mqtt_host,
            self.settings.mqtt_port,
            self.settings.kafka_bootstrap_servers,
            ",".join(
                f"{route.mqtt_subscription}->{route.kafka_topic}"
                for route in self.settings.routes
            ),
        )
        self._check_kafka()
        mqtt_config = self.settings.config["mqtt"]
        self._mqtt_client.connect_async(
            self.settings.mqtt_host,
            self.settings.mqtt_port,
            keepalive=mqtt_config["keepalive_seconds"],
        )
        self._mqtt_client.loop_start()
        now = time.monotonic()
        self._next_healthcheck = now + self.settings.config["runtime"][
            "kafka_healthcheck_interval_seconds"
        ]
        self._next_summary = now + self.settings.config["logging"][
            "summary_interval_seconds"
        ]

        try:
            while not self.stop_requested.is_set():
                poll_interval = self.settings.config["runtime"]["poll_interval_seconds"]
                self._producer.poll(poll_interval)
                now = time.monotonic()
                if now >= self._next_healthcheck:
                    self._check_kafka()
                    self._next_healthcheck = now + self.settings.config["runtime"][
                        "kafka_healthcheck_interval_seconds"
                    ]
                self._retry_failed_deliveries()
                self._log_summary(now)
        finally:
            self._shutdown()
        return 0
