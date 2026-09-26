from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from .metrics import (
    ConnectivityMetrics,
    DeviceHealthMetrics,
    DeviceProfile,
    ThroughputMetrics,
)


def device_session_id(run_id: uuid.UUID, device_id: str) -> uuid.UUID:
    return uuid.uuid5(run_id, device_id)


def event_id(
    namespace: uuid.UUID,
    session_id: uuid.UUID,
    device_id: str,
    event_type: str,
    sequence_number: int,
) -> uuid.UUID:
    return uuid.uuid5(
        namespace,
        f"{session_id}|{device_id}|{event_type}|{sequence_number}",
    )


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("event timestamp must be timezone-aware")
    utc = value.astimezone(timezone.utc)
    return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def metrics_payload(
    metrics: ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics,
) -> dict[str, float]:
    if isinstance(metrics, ConnectivityMetrics):
        return {
            "latency_ms": metrics.latency_ms,
            "jitter_ms": metrics.jitter_ms,
            "packet_loss_pct": metrics.packet_loss_pct,
        }
    if isinstance(metrics, ThroughputMetrics):
        return {
            "download_mbps": metrics.download_mbps,
            "upload_mbps": metrics.upload_mbps,
        }
    payload = {
        "cpu_pct": metrics.cpu_pct,
        "memory_pct": metrics.memory_pct,
        "temperature_c": metrics.temperature_c,
    }
    if metrics.technology_signal_field is not None:
        if metrics.technology_signal_value is None:
            raise ValueError("technology health signal field lacks a value")
        payload[metrics.technology_signal_field] = metrics.technology_signal_value
    elif metrics.technology_signal_value is not None:
        raise ValueError("technology health signal value lacks a field")
    return payload


def build_event(
    profile: DeviceProfile,
    metrics: ConnectivityMetrics | ThroughputMetrics | DeviceHealthMetrics,
    event_type: str,
    session_id: uuid.UUID,
    sequence_number: int,
    event_timestamp: datetime,
    config: dict[str, Any],
) -> dict[str, Any]:
    if event_type not in config["event_families"]:
        raise ValueError(f"unsupported event type: {event_type}")
    return _build_envelope(
        profile,
        metrics_payload(metrics),
        event_type,
        session_id,
        sequence_number,
        event_timestamp,
        config,
    )


def build_control_event(
    profile: DeviceProfile,
    payload: dict[str, str],
    event_type: str,
    session_id: uuid.UUID,
    sequence_number: int,
    event_timestamp: datetime,
    config: dict[str, Any],
) -> dict[str, Any]:
    if event_type not in config["control_event_families"]:
        raise ValueError(f"unsupported control event type: {event_type}")
    return _build_envelope(
        profile,
        payload,
        event_type,
        session_id,
        sequence_number,
        event_timestamp,
        config,
    )


def _build_envelope(
    profile: DeviceProfile,
    payload: dict[str, Any],
    event_type: str,
    session_id: uuid.UUID,
    sequence_number: int,
    event_timestamp: datetime,
    config: dict[str, Any],
) -> dict[str, Any]:
    namespace = uuid.UUID(config["event_uuid_namespace"])
    return {
        "schema_version": config["schema_version"],
        "event_id": str(
            event_id(
                namespace,
                session_id,
                profile.device_id,
                event_type,
                sequence_number,
            )
        ),
        "event_type": event_type,
        "event_timestamp": format_timestamp(event_timestamp),
        "device_session_id": str(session_id),
        "sequence_number": sequence_number,
        "site_id": profile.site_id,
        "device_id": profile.device_id,
        "payload": payload,
    }


def serialize_event(event: dict[str, Any]) -> bytes:
    return json.dumps(
        event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def event_topic(
    profile: DeviceProfile,
    event_type: str,
    config: dict[str, Any],
) -> str:
    family = config["event_families"].get(event_type)
    if family is None:
        family = config["control_event_families"].get(event_type)
    if family is None:
        raise ValueError(f"unsupported event type: {event_type}")
    return family["topic_template"].format(
        site_id=profile.site_id,
        device_id=profile.device_id,
    )
