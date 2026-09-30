from datetime import datetime, timezone

import pyarrow as pa
from src.schemas import EVENT_SCHEMAS, REJECTED_SCHEMA


def test_rejected_schema_structure():
    assert isinstance(REJECTED_SCHEMA, pa.Schema)
    expected_fields = [
        "raw_payload",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "rejection_reason",
        "rejected_at",
        "raw_value",
        "_kafka_key",
    ]
    actual_fields = [field.name for field in REJECTED_SCHEMA]
    assert actual_fields == expected_fields


def test_event_schemas_coverage():
    expected_tables = {
        "connectivity_events",
        "throughput_events",
        "device_health_events",
        "connectivity_state_events",
        "infrastructure_events",
    }
    assert set(EVENT_SCHEMAS.keys()) == expected_tables


def test_common_fields_in_event_schemas():
    common_envelope_fields = {
        "schema_version",
        "event_id",
        "event_type",
        "event_timestamp",
        "device_session_id",
        "sequence_number",
        "site_id",
        "device_id",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    }

    for table_name, schema in EVENT_SCHEMAS.items():
        assert isinstance(schema, pa.Schema)
        schema_field_names = set(schema.names)
        assert common_envelope_fields.issubset(
            schema_field_names
        ), f"Missing common envelope fields in {table_name}"


def test_pyarrow_table_construction_with_schema():
    sample_data = [
        {
            "schema_version": 1,
            "event_id": "evt-01",
            "event_type": "connectivity_metric",
            "event_timestamp": datetime(2026, 9, 27, 18, 0, tzinfo=timezone.utc),
            "device_session_id": "sess-01",
            "sequence_number": 1,
            "site_id": "site-1",
            "device_id": "dev-1",
            "_kafka_topic": "network.connectivity",
            "_kafka_partition": 0,
            "_kafka_offset": 100,
            "_kafka_timestamp": 1700000000000,
            "latency_ms": 15.5,
            "jitter_ms": 0.5,
            "packet_loss_pct": 0.0,
        }
    ]

    schema = EVENT_SCHEMAS["connectivity_events"]
    table = pa.Table.from_pylist(sample_data, schema=schema)
    assert table.num_rows == 1
    assert table.schema == schema
