"""Catalog access and idempotent table creation/evolution shared by all writers."""
import logging
from typing import Optional

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from .config import NESSIE_URI, S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY

logger = logging.getLogger(__name__)

NAMESPACE = "solution"

# Applied to every table on create and re-asserted on startup. format-version is
# fixed at create time (pyiceberg default is v2) and cannot be set as a property.
TABLE_PROPERTIES = {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    "write.metadata.previous-versions-max": "100",
    "write.metadata.delete-after-commit.enabled": "true",
}


def load_solution_catalog() -> Catalog:
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.region": S3_REGION,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


def ensure_table(
    catalog: Catalog,
    table_name: str,
    schema: pa.Schema,
    day_partition_column: Optional[str] = None,
) -> Table:
    """Create the table if absent, then converge it to the desired definition.

    Every step is additive and skipped when already applied, so this is safe to
    run on each start against tables that already hold data:
    - new columns are added (union by name); existing columns are never dropped;
    - day partitioning is added via partition evolution (old files keep their
      original spec, new writes are partitioned);
    - table properties are set when they differ.
    """
    identifier = f"{NAMESPACE}.{table_name}"
    table = catalog.create_table_if_not_exists(identifier, schema=schema, properties=TABLE_PROPERTIES)

    existing_columns = set(table.schema().column_names)
    missing_columns = [name for name in schema.names if name not in existing_columns]
    if missing_columns:
        with table.update_schema() as update:
            update.union_by_name(schema)
        logger.info(f"{identifier}: added columns {missing_columns}")

    if day_partition_column:
        partition_name = f"{day_partition_column}_day"
        if partition_name not in {field.name for field in table.spec().fields}:
            with table.update_spec() as update:
                update.add_field(day_partition_column, DayTransform(), partition_name)
            logger.info(f"{identifier}: partitioned by day({day_partition_column})")

    stale_properties = {k: v for k, v in TABLE_PROPERTIES.items() if table.properties.get(k) != v}
    if stale_properties:
        with table.transaction() as tx:
            tx.set_properties(**stale_properties)

    return catalog.load_table(identifier)
