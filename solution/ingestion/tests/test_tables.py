"""ensure_table (src/tables.py) runs on every start against tables that may already hold
data, so it must only ever add what is missing and do nothing when a table is current."""
from types import SimpleNamespace

import pyarrow as pa

from src.tables import TABLE_PROPERTIES, ensure_table

SCHEMA = pa.schema([("event_id", pa.string()), ("event_timestamp", pa.timestamp("us", tz="UTC")),
                    ("_kafka_key", pa.string())])


class _Recorder:
    """Context manager standing in for update_schema()/update_spec()/transaction()."""

    def __init__(self, table, kind):
        self.table, self.kind = table, kind

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def union_by_name(self, schema):
        self.table.changes.append(("add_columns", tuple(schema.names)))

    def add_field(self, source, transform, name):
        self.table.changes.append(("add_partition", name))

    def set_properties(self, **props):
        self.table.changes.append(("set_properties", tuple(sorted(props))))


class FakeTable:
    def __init__(self, columns, partition_fields=(), properties=None):
        self._columns = list(columns)
        self._partitions = list(partition_fields)
        self.properties = dict(properties or {})
        self.changes = []

    def schema(self):
        return SimpleNamespace(column_names=self._columns)

    def spec(self):
        return SimpleNamespace(fields=[SimpleNamespace(name=n) for n in self._partitions])

    def update_schema(self):
        return _Recorder(self, "schema")

    def update_spec(self):
        return _Recorder(self, "spec")

    def transaction(self):
        return _Recorder(self, "tx")


class FakeCatalog:
    def __init__(self, table):
        self.table = table

    def create_table_if_not_exists(self, identifier, schema, properties):
        return self.table

    def load_table(self, identifier):
        return self.table


def test_up_to_date_table_is_left_alone():
    table = FakeTable(SCHEMA.names, ["event_timestamp_day"], TABLE_PROPERTIES)
    ensure_table(FakeCatalog(table), "connectivity_events", SCHEMA, "event_timestamp")
    assert table.changes == []


def test_existing_table_gets_only_what_is_missing():
    # An older table: without _kafka_key, unpartitioned, and one property out of date.
    stale = {**TABLE_PROPERTIES, "write.parquet.compression-codec": "snappy"}
    table = FakeTable(["event_id", "event_timestamp"], [], stale)
    ensure_table(FakeCatalog(table), "connectivity_events", SCHEMA, "event_timestamp")
    assert table.changes == [
        ("add_columns", tuple(SCHEMA.names)),
        ("add_partition", "event_timestamp_day"),
        ("set_properties", ("write.parquet.compression-codec",)),
    ]


def test_running_twice_changes_nothing_the_second_time():
    table = FakeTable(SCHEMA.names, [], TABLE_PROPERTIES)
    ensure_table(FakeCatalog(table), "connectivity_events", SCHEMA, "event_timestamp")
    table._partitions.append("event_timestamp_day")  # what the first run's evolution produced
    table.changes.clear()
    ensure_table(FakeCatalog(table), "connectivity_events", SCHEMA, "event_timestamp")
    assert table.changes == []


def test_reference_tables_are_not_partitioned():
    table = FakeTable(SCHEMA.names, [], TABLE_PROPERTIES)
    ensure_table(FakeCatalog(table), "sites", SCHEMA)
    assert table.changes == []
