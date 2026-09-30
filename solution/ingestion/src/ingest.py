import json
import time
import logging
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from .config import TOPIC_TO_TABLE, TOPIC_EVENT_TYPE
from .schemas import EVENT_SCHEMAS, REJECTED_SCHEMA, PARTITION_COLUMNS
from .tables import NAMESPACE, ensure_table, load_solution_catalog
from .validator import ContractValidator
from .models import (
    ConnectivityEvent, ThroughputEvent, DeviceHealthEvent,
    ConnectivityStateEvent, InfrastructureEvent, RejectedEvent, parse_event_timestamp
)

logger = logging.getLogger(__name__)

@dataclass
class RejectContext:
    """Kafka coordinates of the record being processed, kept for reject rows."""
    raw_bytes: Optional[bytes]
    kafka_key: Optional[str]
    topic: str
    partition: int
    offset: int
    kafka_ts: int


MODEL_MAPPING = {
    "connectivity_events": ConnectivityEvent,
    "throughput_events": ThroughputEvent,
    "device_health_events": DeviceHealthEvent,
    "connectivity_state_events": ConnectivityStateEvent,
    "infrastructure_events": InfrastructureEvent,
}

class IcebergIngest:
    def __init__(self, validator: ContractValidator, catalog=None):
        self.validator = validator
        self.catalog = catalog or load_solution_catalog()
        self.buffers = {table: [] for table in EVENT_SCHEMAS.keys()}
        self.buffers["rejected_events"] = []

    def ensure_tables(self) -> None:
        """Create or converge the namespace and all ingestion tables (idempotent)."""
        self.catalog.create_namespace_if_not_exists(NAMESPACE)
        all_schemas = {**EVENT_SCHEMAS, "rejected_events": REJECTED_SCHEMA}
        for table_name, schema in all_schemas.items():
            ensure_table(self.catalog, table_name, schema, PARTITION_COLUMNS[table_name])
        logger.info(f"Ensured {len(all_schemas)} tables exist in namespace '{NAMESPACE}'.")

    def process_record(self, msg) -> None:
        # Every failure path below becomes a reject row; a single bad message
        # must never raise out of here and stop the consumer.
        raw_bytes = msg.value()
        key_bytes = msg.key()
        reject = RejectContext(
            raw_bytes=raw_bytes,
            kafka_key=key_bytes.decode("utf-8", errors="replace") if key_bytes else None,
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            kafka_ts=msg.timestamp()[1],
        )
        topic = reject.topic

        if not raw_bytes:
            self._add_reject(reject, "Empty Kafka value")
            return

        try:
            data = json.loads(raw_bytes)
        except Exception as e:
            self._add_reject(reject, f"Invalid JSON: {str(e)}")
            return

        if not isinstance(data, dict):
            self._add_reject(reject, f"Invalid JSON: expected an object, got {type(data).__name__}")
            return

        is_valid, reason = self.validator.validate(data)
        if not is_valid:
            self._add_reject(reject, reason)
            return

        target_table = TOPIC_TO_TABLE.get(topic)
        if not target_table:
            self._add_reject(reject, f"Unmapped topic: {topic}")
            return

        expected_type = TOPIC_EVENT_TYPE[topic]
        if data.get("event_type") != expected_type:
            self._add_reject(
                reject, f"event_type '{data.get('event_type')}' not allowed on topic {topic} (expected '{expected_type}')"
            )
            return

        payload = data.get("payload", {})
        common_kwargs = {
            "schema_version": data.get("schema_version"),
            "event_id": data.get("event_id"),
            "event_type": data.get("event_type"),
            "event_timestamp": data.get("event_timestamp"),
            "device_session_id": data.get("device_session_id"),
            "sequence_number": data.get("sequence_number"),
            "site_id": data.get("site_id"),
            "device_id": data.get("device_id"),
            "_kafka_topic": topic,
            "_kafka_partition": reject.partition,
            "_kafka_offset": reject.offset,
            "_kafka_timestamp": reject.kafka_ts,
            "_kafka_key": reject.kafka_key,
            "_ingested_at": int(time.time() * 1000),
            **payload
        }

        try:
            # The contract regex admits impossible dates (e.g. month 13); catch
            # them here rather than failing the whole batch at flush time.
            parse_event_timestamp(common_kwargs["event_timestamp"])
            event_obj = MODEL_MAPPING[target_table](**common_kwargs)
        except Exception as e:
            self._add_reject(reject, f"Cannot map to {target_table}: {e}")
            return
        self.buffers[target_table].append(event_obj)

    def _add_reject(self, ctx: "RejectContext", reason: str):
        reject_obj = RejectedEvent(
            raw_payload=ctx.raw_bytes.decode("utf-8", errors="replace") if ctx.raw_bytes else "",
            _kafka_topic=ctx.topic,
            _kafka_partition=ctx.partition,
            _kafka_offset=ctx.offset,
            _kafka_timestamp=ctx.kafka_ts,
            rejection_reason=reason,
            rejected_at=int(time.time() * 1000),
            raw_value=ctx.raw_bytes,
            _kafka_key=ctx.kafka_key,
        )
        self.buffers["rejected_events"].append(reject_obj)

    def total_buffered(self) -> int:
        return sum(len(b) for b in self.buffers.values())

    def discard_buffers(self) -> None:
        """Drop buffered rows without writing them. Only safe when their offsets are
        uncommitted, so Kafka delivers them again (used when partitions are revoked or lost)."""
        for rows in self.buffers.values():
            rows.clear()

    def flush(self) -> bool:
        if self.total_buffered() == 0:
            return True

        logger.info(f"Flushing {self.total_buffered()} records to Iceberg...")
        for table_name, objects in self.buffers.items():
            if not objects:
                continue

            dict_rows = [obj.to_dict() for obj in objects]
            schema = REJECTED_SCHEMA if table_name == "rejected_events" else EVENT_SCHEMAS[table_name]
            arrow_table = pa.Table.from_pylist(dict_rows, schema=schema)

            try:
                table = self.catalog.load_table(f"{NAMESPACE}.{table_name}")
                table.append(arrow_table)
                logger.info(f"Appended {len(objects)} records to {NAMESPACE}.{table_name}")
            except NoSuchTableError:
                logger.error(f"Target table {NAMESPACE}.{table_name} missing from Iceberg catalog!")
                return False
            except Exception as e:
                logger.error(f"Failed to append to {NAMESPACE}.{table_name}: {e}")
                return False

            # Clear only what was committed so a later table's failure does not
            # re-append this table's rows on retry.
            objects.clear()

        return True
