import json
from unittest.mock import MagicMock
import pytest
from src.ingest import IcebergIngest
from src.validator import ContractValidator
from src.models import ConnectivityEvent, RejectedEvent


class DummyKafkaMessage:
    def __init__(
        self,
        value: str,
        topic: str = "network.connectivity",
        partition: int = 0,
        offset: int = 42,
        timestamp: int = 1700000000000,
        key: bytes = b"dev-99",
    ):
        self._value = value.encode("utf-8") if isinstance(value, str) else value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._key = key

    def key(self):
        return self._key

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def timestamp(self):
        return (1, self._timestamp)


def test_process_record_malformed_json():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_catalog = MagicMock()
    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    msg = DummyKafkaMessage("{ invalid json payload ")
    ingestor.process_record(msg)

    assert ingestor.total_buffered() == 1
    assert len(ingestor.buffers["rejected_events"]) == 1

    rejected_obj = ingestor.buffers["rejected_events"][0]
    assert isinstance(rejected_obj, RejectedEvent)
    assert "Invalid JSON" in rejected_obj.rejection_reason


def test_process_record_failed_validation():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_validator.validate.return_value = (False, "Field 'latency_ms' missing")
    mock_catalog = MagicMock()

    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    payload = {"event_type": "connectivity_metric"}
    msg = DummyKafkaMessage(json.dumps(payload))
    ingestor.process_record(msg)

    assert len(ingestor.buffers["rejected_events"]) == 1
    rejected_obj = ingestor.buffers["rejected_events"][0]
    assert rejected_obj.rejection_reason == "Field 'latency_ms' missing"


def test_process_record_unmapped_topic():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_validator.validate.return_value = (True, "")
    mock_catalog = MagicMock()

    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    payload = {"event_type": "unknown-event"}
    msg = DummyKafkaMessage(json.dumps(payload), topic="network.unknown_topic")
    ingestor.process_record(msg)

    assert len(ingestor.buffers["rejected_events"]) == 1
    rejected_obj = ingestor.buffers["rejected_events"][0]
    assert "Unmapped topic" in rejected_obj.rejection_reason


def test_process_record_valid_event_routing():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_validator.validate.return_value = (True, "")
    mock_catalog = MagicMock()

    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    valid_payload = {
        "schema_version": 1,
        "event_id": "evt-99",
        "event_type": "connectivity_metric",
        "event_timestamp": "2026-09-27T18:00:00Z",
        "device_session_id": "sess-99",
        "sequence_number": 10,
        "site_id": "site-99",
        "device_id": "dev-99",
        "payload": {
            "latency_ms": 22.5,
            "jitter_ms": 2.1,
            "packet_loss_pct": 0.01,
        },
    }

    msg = DummyKafkaMessage(json.dumps(valid_payload), topic="network.connectivity")
    ingestor.process_record(msg)

    assert len(ingestor.buffers["connectivity_events"]) == 1
    event_obj = ingestor.buffers["connectivity_events"][0]
    assert isinstance(event_obj, ConnectivityEvent)
    assert event_obj.latency_ms == 22.5
    assert event_obj._kafka_topic == "network.connectivity"


def test_flush_appends_to_catalog_and_clears_buffer():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    # Manually populate a buffer
    reject = RejectedEvent(
        raw_payload="{}",
        _kafka_topic="topic",
        _kafka_partition=0,
        _kafka_offset=1,
        _kafka_timestamp=1000,
        rejection_reason="Test reason",
        rejected_at=1000,
    )
    ingestor.buffers["rejected_events"].append(reject)

    assert ingestor.total_buffered() == 1
    success = ingestor.flush()

    assert success
    assert ingestor.total_buffered() == 0
    mock_catalog.load_table.assert_called_once_with("solution.rejected_events")
    mock_table.append.assert_called_once()


def test_ensure_tables_creates_namespace_and_all_tables():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_catalog = MagicMock()
    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)

    ingestor.ensure_tables()

    mock_catalog.create_namespace_if_not_exists.assert_called_once_with("solution")
    created = {c.args[0] for c in mock_catalog.create_table_if_not_exists.call_args_list}
    assert created == {
        "solution.connectivity_events",
        "solution.throughput_events",
        "solution.device_health_events",
        "solution.connectivity_state_events",
        "solution.infrastructure_events",
        "solution.rejected_events",
    }


def test_flush_partial_failure_keeps_only_uncommitted_buffers():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_validator.validate.return_value = (True, "")
    mock_catalog = MagicMock()
    ok_table, failing_table = MagicMock(), MagicMock()
    failing_table.append.side_effect = RuntimeError("S3 unavailable")
    mock_catalog.load_table.side_effect = (
        lambda name: failing_table if name == "solution.rejected_events" else ok_table
    )

    ingestor = IcebergIngest(validator=mock_validator, catalog=mock_catalog)
    valid = {
        "schema_version": 1, "event_id": "e1", "event_type": "connectivity_metric",
        "event_timestamp": "2026-09-27T18:00:00Z", "device_session_id": "s1",
        "sequence_number": 1, "site_id": "site-1", "device_id": "dev-1",
        "payload": {"latency_ms": 1.0, "jitter_ms": 1.0, "packet_loss_pct": 0.0},
    }
    ingestor.process_record(DummyKafkaMessage(json.dumps(valid)))
    ingestor.process_record(DummyKafkaMessage("{ bad json", offset=43))

    assert not ingestor.flush()
    # Committed table is cleared so a retry cannot duplicate it; the failed one is kept.
    assert ingestor.buffers["connectivity_events"] == []
    assert len(ingestor.buffers["rejected_events"]) == 1

    failing_table.append.side_effect = None
    assert ingestor.flush()
    assert ok_table.append.call_count == 1
    assert ingestor.total_buffered() == 0


def _ingestor_accepting_all():
    mock_validator = MagicMock(spec=ContractValidator)
    mock_validator.validate.return_value = (True, "")
    return IcebergIngest(validator=mock_validator, catalog=MagicMock())


def test_non_utf8_value_is_rejected_losslessly():
    ingestor = _ingestor_accepting_all()
    raw = b'{"a": "' + bytes([0xFF, 0xFE]) + b'"}'
    ingestor.process_record(DummyKafkaMessage(raw))

    reject = ingestor.buffers["rejected_events"][0]
    assert reject.raw_value == raw  # exact bytes preserved
    assert reject._kafka_key == "dev-99"
    assert reject._kafka_offset == 42
    assert "Invalid JSON" in reject.rejection_reason


@pytest.mark.parametrize("raw", ["5", "[1, 2]", "null"])
def test_json_that_is_not_an_object_is_rejected(raw):
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(raw))
    assert "expected an object" in ingestor.buffers["rejected_events"][0].rejection_reason


def test_empty_value_is_rejected():
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(b""))
    assert ingestor.buffers["rejected_events"][0].rejection_reason == "Empty Kafka value"


def _valid_connectivity(**overrides):
    event = {
        "schema_version": 1, "event_id": "e1", "event_type": "connectivity_metric",
        "event_timestamp": "2026-09-27T18:00:00.000Z", "device_session_id": "s1",
        "sequence_number": 1, "site_id": "site-1", "device_id": "dev-1",
        "payload": {"latency_ms": 1.0, "jitter_ms": 1.0, "packet_loss_pct": 0.0},
    }
    event.update(overrides)
    return json.dumps(event)


def test_event_type_on_wrong_topic_is_rejected():
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(_valid_connectivity(), topic="network.throughput"))
    assert ingestor.buffers["throughput_events"] == []
    assert "not allowed on topic network.throughput" in ingestor.buffers["rejected_events"][0].rejection_reason


def test_impossible_timestamp_is_rejected_not_crashing_flush():
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(_valid_connectivity(event_timestamp="2026-13-45T18:00:00.000Z")))
    assert ingestor.buffers["connectivity_events"] == []
    assert "Cannot map to connectivity_events" in ingestor.buffers["rejected_events"][0].rejection_reason


def test_accepted_event_carries_kafka_key_and_ingest_time():
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(_valid_connectivity(), key=b"dev-1"))
    row = ingestor.buffers["connectivity_events"][0].to_dict()
    assert row["_kafka_key"] == "dev-1"
    assert row["_ingested_at"] is not None


def test_millisecond_kafka_times_are_not_misread_as_microseconds():
    ingestor = _ingestor_accepting_all()
    ingestor.process_record(DummyKafkaMessage(_valid_connectivity(), timestamp=1790000000123))
    ingestor.process_record(DummyKafkaMessage("not json", timestamp=1790000000123))
    event_row = ingestor.buffers["connectivity_events"][0].to_dict()
    reject_row = ingestor.buffers["rejected_events"][0].to_dict()
    for ts in (event_row["_kafka_timestamp"], reject_row["_kafka_timestamp"]):
        assert ts.year == 2026 and ts.microsecond == 123000
    # Round-trips through the Arrow schema used for the Iceberg write.
    from src.schemas import EVENT_SCHEMAS
    import pyarrow as pa
    table = pa.Table.from_pylist([event_row], schema=EVENT_SCHEMAS["connectivity_events"])
    assert table.column("_kafka_timestamp")[0].as_py().year == 2026
