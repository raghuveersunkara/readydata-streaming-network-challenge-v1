"""manual_sql_to_csv: run one checked-in, read-only SQL report through Trino and
publish its full result as a CSV under solution/exports/<report>/.

- Streams rows from Trino in batches of READYDATA_EXPORT_FETCH_SIZE, so result
  size is bounded by disk, not task memory; only a small summary goes to XCom.
- Writes to a hidden .part file and publishes it with an atomic link that never
  replaces an existing file. A failed attempt leaves no CSV behind.
- Each run and attempt gets its own file name (<report>__<run_id>__try<n>.csv), so
  reruns and retries never overwrite earlier exports.
- A JSON sidecar next to the CSV records the report, SQL checksum, row count,
  byte size and CSV SHA-256; `verify` re-reads the published file to confirm them.
See lakehouse_ops/export.py for the CSV format (quoting, NULL handling, encodings).
"""
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task

from lakehouse_ops.export import (
    export_file_name,
    export_rows,
    list_reports,
    load_report,
    verify_csv,
    write_sidecar,
)
from lakehouse_ops.trino_client import connect

SQL_ROOT = os.getenv("READYDATA_SQL_ROOT", "/opt/airflow/sql")
EXPORT_ROOT = os.getenv("READYDATA_EXPORT_ROOT", "/opt/airflow/exports")
FETCH_SIZE = int(os.getenv("READYDATA_EXPORT_FETCH_SIZE", "1000"))
REPORTS = list_reports(SQL_ROOT)


@dag(
    dag_id="manual_sql_to_csv",
    description="Export one checked-in SQL report to CSV under solution/exports",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["readydata", "export"],
    params={
        "report": Param(REPORTS[0] if REPORTS else "", enum=REPORTS or [""],
                        description="Checked-in report under solution/sql/ to export"),
    },
    doc_md=__doc__,
)
def manual_sql_to_csv():

    @task(retries=1, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=30))
    def export() -> dict:
        ctx = get_current_context()
        report = ctx["params"]["report"]
        sql = load_report(SQL_ROOT, report)

        out_dir = os.path.join(EXPORT_ROOT, report[:-4])
        name = export_file_name(report, ctx["run_id"], ctx["ti"].try_number)
        started = datetime.now(timezone.utc)

        conn = connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [d[0] for d in cur.description]

            def batches():
                while True:
                    batch = cur.fetchmany(FETCH_SIZE)
                    if not batch:
                        return
                    yield batch

            stats = export_rows(columns, batches(), out_dir, name)
        finally:
            conn.close()

        metadata = {
            "report": report,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "run_id": ctx["run_id"],
            "try_number": ctx["ti"].try_number,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "host_path": os.path.join("solution/exports", report[:-4], name),
            **stats,
        }
        write_sidecar(stats["csv_path"], metadata)
        print(json.dumps(metadata, indent=2))
        return metadata

    @task
    def verify(metadata: dict) -> dict:
        result = verify_csv(metadata["csv_path"], metadata["rows"], metadata["sha256"])
        print(result)
        if not (result["sha256_matches"] and result["row_count_matches"]):
            raise ValueError(f"Published CSV does not match its metadata: {result}")
        return result

    verify(export())


manual_sql_to_csv()
