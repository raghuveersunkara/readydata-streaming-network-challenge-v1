from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def parse_event_timestamp(value: str) -> datetime:
    # Contract format is ISO-8601 UTC, e.g. '2026-08-19T14:32:10.125Z'.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def ms_to_datetime(value: Optional[int]) -> Optional[datetime]:
    # Kafka and time.time() give epoch milliseconds; convert explicitly so they
    # are never misread as microseconds by the "us" Arrow columns.
    return None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)


@dataclass
class Envelope:
    schema_version: int
    event_id: str
    event_type: str
    event_timestamp: str
    device_session_id: str
    sequence_number: int
    site_id: str
    device_id: str
    _kafka_topic: str
    _kafka_partition: int
    _kafka_offset: int
    _kafka_timestamp: int
    _kafka_key: Optional[str] = None
    _ingested_at: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        row = asdict(self)
        row["event_timestamp"] = parse_event_timestamp(self.event_timestamp)
        row["_kafka_timestamp"] = ms_to_datetime(self._kafka_timestamp)
        row["_ingested_at"] = ms_to_datetime(self._ingested_at)
        return row

@dataclass
class ConnectivityEvent(Envelope):
    latency_ms: float = 0.0
    jitter_ms: float = 0.0
    packet_loss_pct: float = 0.0

@dataclass
class ThroughputEvent(Envelope):
    download_mbps: float = 0.0
    upload_mbps: float = 0.0

@dataclass
class DeviceHealthEvent(Envelope):
    cpu_pct: float = 0.0
    memory_pct: float = 0.0
    temperature_c: float = 0.0
    optical_rx_power_dbm: Optional[float] = None
    snr_db: Optional[float] = None
    signal_strength_dbm: Optional[float] = None
    signal_quality_pct: Optional[float] = None

@dataclass
class ConnectivityStateEvent(Envelope):
    previous_state: str = ""
    new_state: str = ""
    reason: str = ""

@dataclass
class InfrastructureEvent(Envelope):
    incident_id: str = ""
    event_kind: str = ""
    severity: str = ""

@dataclass
class RejectedEvent:
    raw_payload: str
    _kafka_topic: str
    _kafka_partition: int
    _kafka_offset: int
    _kafka_timestamp: int
    rejection_reason: str
    rejected_at: int
    raw_value: Optional[bytes] = None
    _kafka_key: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        row = asdict(self)
        row["_kafka_timestamp"] = ms_to_datetime(self._kafka_timestamp)
        row["rejected_at"] = ms_to_datetime(self.rejected_at)
        return row
