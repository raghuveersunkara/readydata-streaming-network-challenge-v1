from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class ConfigurationError(ValueError):
    """Raised when bridge configuration or runtime settings are invalid."""


@dataclass(frozen=True)
class Route:
    mqtt_subscription: str
    mqtt_qos: int
    kafka_topic: str
    key_topic_segment_index: int


@dataclass(frozen=True)
class BridgeSettings:
    config: dict[str, Any]
    routes: tuple[Route, ...]
    mqtt_host: str
    mqtt_port: int
    kafka_bootstrap_servers: str

    @property
    def target_topics(self) -> frozenset[str]:
        return frozenset(route.kafka_topic for route in self.routes)


EXPECTED_TOP_LEVEL_KEYS = {
    "config_version",
    "routes_version",
    "routes",
    "mqtt",
    "kafka",
    "runtime",
    "logging",
}
EXPECTED_ROUTE_KEYS = {
    "mqtt_subscription",
    "mqtt_qos",
    "kafka_topic",
    "key_topic_segment_index",
}
EXPECTED_MQTT_KEYS = {
    "client_id",
    "clean_session",
    "keepalive_seconds",
    "reconnect_min_seconds",
    "reconnect_max_seconds",
}
EXPECTED_KAFKA_KEYS = {
    "client_id",
    "acks",
    "enable_idempotence",
    "linger_ms",
    "delivery_timeout_ms",
    "queue_buffering_max_messages",
}
EXPECTED_RUNTIME_KEYS = {
    "max_outstanding_messages",
    "poll_interval_seconds",
    "kafka_healthcheck_interval_seconds",
    "kafka_healthcheck_timeout_seconds",
    "shutdown_flush_seconds",
}


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ConfigurationError(
            f"{name} keys differ: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


def _require_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{name} must be a nonempty string")
    return value


def _require_integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer")
    if value < minimum:
        raise ConfigurationError(f"{name} must be at least {minimum}")
    return value


def _require_number(value: Any, name: str, *, minimum: float = 0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ConfigurationError(f"{name} must be finite and at least {minimum}")
    return result


def _validate_subscription(subscription: str, name: str) -> list[str]:
    segments = subscription.split("/")
    if len(segments) != 4 or any(not segment for segment in segments):
        raise ConfigurationError(f"{name} must contain exactly four nonempty levels")
    if "#" in segments or segments.count("+") != 2:
        raise ConfigurationError(f"{name} must contain exactly two single-level wildcards")
    return segments


def validate_config(config: dict[str, Any]) -> tuple[Route, ...]:
    _require_exact_keys(config, EXPECTED_TOP_LEVEL_KEYS, "config")
    if config["config_version"] != 1 or config["routes_version"] != 1:
        raise ConfigurationError("only bridge config and routes version 1 are supported")

    raw_routes = config["routes"]
    if not isinstance(raw_routes, list) or len(raw_routes) != 5:
        raise ConfigurationError("bridge V1 requires exactly five active routes")
    routes_list: list[Route] = []
    for index, raw_route in enumerate(raw_routes):
        name = f"routes[{index}]"
        if not isinstance(raw_route, dict):
            raise ConfigurationError(f"{name} must be an object")
        _require_exact_keys(raw_route, EXPECTED_ROUTE_KEYS, name)
        subscription = _require_string(
            raw_route["mqtt_subscription"], f"{name}.mqtt_subscription"
        )
        segments = _validate_subscription(subscription, f"{name}.mqtt_subscription")
        mqtt_qos = _require_integer(raw_route["mqtt_qos"], f"{name}.mqtt_qos")
        kafka_topic = _require_string(raw_route["kafka_topic"], f"{name}.kafka_topic")
        key_index = _require_integer(
            raw_route["key_topic_segment_index"],
            f"{name}.key_topic_segment_index",
        )
        if key_index >= len(segments) or segments[key_index] != "+":
            raise ConfigurationError(f"{name} key index does not select a wildcard")
        routes_list.append(Route(subscription, mqtt_qos, kafka_topic, key_index))

    expected_routes = {
        ("network/+/+/connectivity", 1, "network.connectivity", 2),
        ("network/+/+/throughput", 1, "network.throughput", 2),
        ("network/+/+/health", 1, "network.device_health", 2),
        ("network/+/+/state", 1, "network.state", 2),
        ("network/+/+/infrastructure", 1, "network.infrastructure", 2),
    }
    actual_routes = {
        (
            route.mqtt_subscription,
            route.mqtt_qos,
            route.kafka_topic,
            route.key_topic_segment_index,
        )
        for route in routes_list
    }
    if actual_routes != expected_routes or len(actual_routes) != len(routes_list):
        raise ConfigurationError("bridge V1 routes differ from the active event contracts")
    routes = tuple(routes_list)

    mqtt_config = config["mqtt"]
    if not isinstance(mqtt_config, dict):
        raise ConfigurationError("mqtt must be an object")
    _require_exact_keys(mqtt_config, EXPECTED_MQTT_KEYS, "mqtt")
    _require_string(mqtt_config["client_id"], "mqtt.client_id")
    if mqtt_config["clean_session"] is not False:
        raise ConfigurationError("bridge V1 requires mqtt.clean_session=false")
    for field in ["keepalive_seconds", "reconnect_min_seconds", "reconnect_max_seconds"]:
        _require_integer(mqtt_config[field], f"mqtt.{field}", minimum=1)
    if mqtt_config["reconnect_min_seconds"] > mqtt_config["reconnect_max_seconds"]:
        raise ConfigurationError("MQTT reconnect minimum exceeds maximum")

    kafka_config = config["kafka"]
    if not isinstance(kafka_config, dict):
        raise ConfigurationError("kafka must be an object")
    _require_exact_keys(kafka_config, EXPECTED_KAFKA_KEYS, "kafka")
    _require_string(kafka_config["client_id"], "kafka.client_id")
    if kafka_config["acks"] != "all" or kafka_config["enable_idempotence"] is not True:
        raise ConfigurationError("bridge V1 requires Kafka acks=all and idempotence enabled")
    _require_integer(kafka_config["linger_ms"], "kafka.linger_ms")
    _require_integer(kafka_config["delivery_timeout_ms"], "kafka.delivery_timeout_ms", minimum=1000)
    _require_integer(
        kafka_config["queue_buffering_max_messages"],
        "kafka.queue_buffering_max_messages",
        minimum=1,
    )

    runtime = config["runtime"]
    if not isinstance(runtime, dict):
        raise ConfigurationError("runtime must be an object")
    _require_exact_keys(runtime, EXPECTED_RUNTIME_KEYS, "runtime")
    _require_integer(runtime["max_outstanding_messages"], "runtime.max_outstanding_messages", minimum=1)
    for field in [
        "poll_interval_seconds",
        "kafka_healthcheck_interval_seconds",
        "kafka_healthcheck_timeout_seconds",
        "shutdown_flush_seconds",
    ]:
        _require_number(runtime[field], f"runtime.{field}", minimum=0.001)
    if runtime["max_outstanding_messages"] > kafka_config["queue_buffering_max_messages"]:
        raise ConfigurationError("max outstanding messages exceeds the Kafka producer queue")

    logging_config = config["logging"]
    if not isinstance(logging_config, dict):
        raise ConfigurationError("logging must be an object")
    _require_exact_keys(logging_config, {"summary_interval_seconds"}, "logging")
    _require_number(logging_config["summary_interval_seconds"], "logging.summary_interval_seconds", minimum=1)
    return routes


def _environment_port(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if not 1 <= value <= 65535:
        raise ConfigurationError(f"{name} must be between 1 and 65535")
    return value


def load_settings(
    config_path: Path,
    environ: Mapping[str, str] | None = None,
) -> BridgeSettings:
    environment = os.environ if environ is None else environ
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"cannot read bridge config {config_path}: {error}") from error
    if not isinstance(config, dict):
        raise ConfigurationError("bridge config must be a JSON object")
    routes = validate_config(config)

    mqtt_host = environment.get("MQTT_HOST", "mosquitto").strip()
    kafka_bootstrap = environment.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").strip()
    if not mqtt_host:
        raise ConfigurationError("MQTT_HOST cannot be empty")
    if not kafka_bootstrap:
        raise ConfigurationError("KAFKA_BOOTSTRAP_SERVERS cannot be empty")

    return BridgeSettings(
        config=config,
        routes=routes,
        mqtt_host=mqtt_host,
        mqtt_port=_environment_port(environment, "MQTT_PORT", 1883),
        kafka_bootstrap_servers=kafka_bootstrap,
    )
