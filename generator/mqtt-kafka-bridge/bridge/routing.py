from __future__ import annotations

import re
from dataclasses import dataclass

from .config import Route


IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


class RoutingError(ValueError):
    """Raised when an MQTT topic cannot be routed safely."""


@dataclass(frozen=True)
class RoutedRecord:
    kafka_topic: str
    key: bytes
    value: bytes


def _matches(subscription: str, topic: str) -> bool:
    subscription_segments = subscription.split("/")
    topic_segments = topic.split("/")
    if len(subscription_segments) != len(topic_segments):
        return False
    return all(
        expected == "+" or expected == actual
        for expected, actual in zip(subscription_segments, topic_segments, strict=True)
    )


def route_message(topic: str, payload: bytes, routes: tuple[Route, ...]) -> RoutedRecord:
    if not isinstance(topic, str):
        raise RoutingError("MQTT topic must be text")
    if not isinstance(payload, bytes):
        raise RoutingError("MQTT payload must be bytes")
    topic_segments = topic.split("/")
    if any(not segment for segment in topic_segments):
        raise RoutingError(f"MQTT topic contains an empty level: {topic!r}")

    matches = [route for route in routes if _matches(route.mqtt_subscription, topic)]
    if len(matches) != 1:
        raise RoutingError(f"MQTT topic matched {len(matches)} configured routes: {topic!r}")
    route = matches[0]
    try:
        key_text = topic_segments[route.key_topic_segment_index]
    except IndexError as error:
        raise RoutingError(f"MQTT topic lacks its configured key segment: {topic!r}") from error
    if not IDENTIFIER.fullmatch(key_text):
        raise RoutingError(f"MQTT key segment is invalid: {key_text!r}")
    return RoutedRecord(
        kafka_topic=route.kafka_topic,
        key=key_text.encode("utf-8"),
        value=payload,
    )
