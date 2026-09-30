import pyarrow as pa

# Iceberg may store timestamps in micro seconds; writing "us" avoids a lossy cast and
# is required by pyiceberg-core's partition transforms.
TS_UTC = pa.timestamp("us", tz="UTC")

COMMON_FIELDS = [
    ("schema_version", pa.int32()),
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("event_timestamp", TS_UTC),
    ("device_session_id", pa.string()),
    ("sequence_number", pa.int64()),
    ("site_id", pa.string()),
    ("device_id", pa.string()),
    ("_kafka_topic", pa.string()),
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),
    ("_kafka_timestamp", TS_UTC),
]

# Lineage added after the first release. Kept at the end of every schema so a
# freshly created table and an evolved one have the same column order.
LINEAGE_FIELDS = [
    ("_kafka_key", pa.string()),
    ("_ingested_at", TS_UTC),
]

REJECTED_SCHEMA = pa.schema([
    ("raw_payload", pa.string()),  # UTF-8 text for humans; lossy only for non-UTF-8 input
    ("_kafka_topic", pa.string()),
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),
    ("_kafka_timestamp", TS_UTC),
    ("rejection_reason", pa.string()),
    ("rejected_at", TS_UTC),
    ("raw_value", pa.binary()),  # exact Kafka value bytes (lossless)
    ("_kafka_key", pa.string()),
])

# Partition column per table: events by measurement day, rejects by arrival day
# (a rejected record may have no usable event_timestamp).
PARTITION_COLUMNS = {
    "connectivity_events": "event_timestamp",
    "throughput_events": "event_timestamp",
    "device_health_events": "event_timestamp",
    "connectivity_state_events": "event_timestamp",
    "infrastructure_events": "event_timestamp",
    "rejected_events": "_kafka_timestamp",
}

EVENT_SCHEMAS = {
    "connectivity_events": pa.schema(COMMON_FIELDS + [
        ("latency_ms", pa.float64()),
        ("jitter_ms", pa.float64()),
        ("packet_loss_pct", pa.float64()),
    ] + LINEAGE_FIELDS),
    "throughput_events": pa.schema(COMMON_FIELDS + [
        ("download_mbps", pa.float64()),
        ("upload_mbps", pa.float64()),
    ] + LINEAGE_FIELDS),
    "device_health_events": pa.schema(COMMON_FIELDS + [
        ("cpu_pct", pa.float64()),
        ("memory_pct", pa.float64()),
        ("temperature_c", pa.float64()),
        ("optical_rx_power_dbm", pa.float64()),
        ("snr_db", pa.float64()),
        ("signal_strength_dbm", pa.float64()),
        ("signal_quality_pct", pa.float64()),
    ] + LINEAGE_FIELDS),
    "connectivity_state_events": pa.schema(COMMON_FIELDS + [
        ("previous_state", pa.string()),
        ("new_state", pa.string()),
        ("reason", pa.string()),
    ] + LINEAGE_FIELDS),
    "infrastructure_events": pa.schema(COMMON_FIELDS + [
        ("incident_id", pa.string()),
        ("event_kind", pa.string()),
        ("severity", pa.string()),
    ] + LINEAGE_FIELDS),
}
