"""Planning and verification logic for the iceberg_maintenance DAG.

Kept free of Airflow and Trino imports so it can be unit-tested directly.
"""
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

# Allowlist: DAG params can only name these tables, so they are safe to
# interpolate into SQL identifiers.
SOLUTION_TABLES = [
    "connectivity_events",
    "throughput_events",
    "device_health_events",
    "connectivity_state_events",
    "infrastructure_events",
    "rejected_events",
    "sites",
    "devices",
    "device_dependencies",
    "census_block_groups",
]

MB = 1024 * 1024


@dataclass
class TableFileStats:
    table: str
    data_files: int
    small_files: int
    small_bytes: int
    total_bytes: int
    records: int
    spec_ids: List[int]


@dataclass
class TablePlan:
    table: str
    action: str  # "compact" or "skip"
    reason: str
    stats: Dict


def validate_tables(tables: List[str]) -> List[str]:
    unknown = sorted(set(tables) - set(SOLUTION_TABLES))
    if unknown:
        raise ValueError(f"Unknown tables {unknown}; allowed: {SOLUTION_TABLES}")
    if not tables:
        raise ValueError("No tables selected")
    return [t for t in SOLUTION_TABLES if t in tables]  # stable order, no duplicates


def file_stats_sql(catalog: str, schema: str, table: str, small_file_threshold_bytes: int) -> str:
    t = f'{catalog}.{schema}."{table}$files"'
    thr = int(small_file_threshold_bytes)
    # content = 0 selects data files (1/2 are position/equality delete files).
    return f"""
        SELECT
          count(*) AS data_files,
          count_if(file_size_in_bytes < {thr}) AS small_files,
          coalesce(sum(if(file_size_in_bytes < {thr}, file_size_in_bytes)), 0) AS small_bytes,
          coalesce(sum(file_size_in_bytes), 0) AS total_bytes,
          coalesce(sum(record_count), 0) AS records,
          coalesce(array_sort(array_agg(DISTINCT spec_id)), ARRAY[]) AS spec_ids
        FROM {t}
        WHERE content = 0
    """


def stats_from_row(table: str, row) -> TableFileStats:
    data_files, small_files, small_bytes, total_bytes, records, spec_ids = row
    return TableFileStats(table, int(data_files), int(small_files), int(small_bytes),
                          int(total_bytes), int(records), [int(s) for s in spec_ids or []])


def decide(stats: TableFileStats, min_small_files: int, max_rewrite_mb: int) -> TablePlan:
    """Compact only when it is justified (enough small files) and bounded
    (the small-file bytes a run would rewrite are under the cap)."""
    small_mb = stats.small_bytes / MB
    if stats.small_files < min_small_files:
        return TablePlan(stats.table, "skip",
                         f"{stats.small_files} small file(s) < min_small_files={min_small_files}; not worth a rewrite",
                         asdict(stats))
    if small_mb > max_rewrite_mb:
        return TablePlan(stats.table, "skip",
                         f"{small_mb:.1f} MB of small files exceeds max_rewrite_mb={max_rewrite_mb}; "
                         "raise the limit deliberately or schedule a larger window",
                         asdict(stats))
    avg_kb = stats.small_bytes / stats.small_files / 1024
    legacy = " Includes files from an older partition spec, which will be rewritten under the current spec." \
        if len(stats.spec_ids) > 1 else ""
    return TablePlan(stats.table, "compact",
                     f"{stats.small_files} small files (avg {avg_kb:.1f} KB) totalling {small_mb:.1f} MB.{legacy}",
                     asdict(stats))


def optimize_sql(catalog: str, schema: str, table: str, small_file_threshold_mb: int) -> str:
    # Only files below the threshold are rewritten; larger files are left alone.
    # Whole-table form: a WHERE on day(event_timestamp) is rejected by Trino
    # while pre-partitioning files still exist.
    return (f"ALTER TABLE {catalog}.{schema}.{table} "
            f"EXECUTE optimize(file_size_threshold => '{int(small_file_threshold_mb)}MB')")


def record_count_sql(catalog: str, schema: str, table: str) -> str:
    # Metadata-only: sums record_count over the current snapshot's data files.
    return f'SELECT coalesce(sum(record_count), 0) FROM {catalog}.{schema}."{table}$files" WHERE content = 0'


def current_snapshot_sql(catalog: str, schema: str, table: str) -> str:
    # Nessie exposes only the current snapshot in Iceberg metadata (history
    # lives in Nessie's commit log), so this returns at most one row.
    return f"""
        SELECT snapshot_id, operation, summary['added-records'], summary['deleted-records'],
               summary['added-data-files'], summary['deleted-data-files']
        FROM {catalog}.{schema}."{table}$snapshots"
        ORDER BY committed_at DESC
        LIMIT 1
    """


def verify_compaction(snapshot_row: Optional[tuple], records_before: int, records_after: int) -> Dict:
    """Check that compaction did not lose or invent rows.

    Strong check: if the current snapshot is still our `replace`, it must add
    exactly as many records as it removed.
    Fallback: if ingestion committed an append after the rewrite, the replace
    snapshot is no longer visible (see current_snapshot_sql). Compaction never
    adds rows and appends only add them, so records_after < records_before
    proves rows were lost.
    """
    if snapshot_row and snapshot_row[1] == "replace":
        snapshot_id, _, added_records, deleted_records, added_files, deleted_files = snapshot_row
        ok = added_records is not None and added_records == deleted_records
        return {"verified": ok, "check": "replace-snapshot", "snapshot_id": snapshot_id,
                "records_added": added_records, "records_removed": deleted_records,
                "files_added": added_files, "files_removed": deleted_files,
                "detail": "replace snapshot preserved row count" if ok else "RECORD COUNT MISMATCH in replace snapshot"}
    ok = records_after >= records_before
    return {"verified": ok, "check": "no-row-loss", "records_before": records_before, "records_after": records_after,
            "detail": ("no rows lost (weaker check: an append was committed after the rewrite)" if ok
                       else f"ROWS LOST: {records_before} before, {records_after} after")}
