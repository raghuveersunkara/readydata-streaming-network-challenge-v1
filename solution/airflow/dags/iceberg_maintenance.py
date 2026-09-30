"""iceberg_maintenance: inspect small-file behaviour and, only when asked,
compact the tables where it is justified and bounded.

Defaults to mode=plan, which only reads Iceberg metadata tables and changes
nothing. mode=apply runs Trino `optimize` one table at a time and verifies
each result. The rewrite is non-destructive: replaced files are not deleted
and stay reachable from earlier Nessie commits. Snapshot expiry and orphan-file removal delete data and
are deliberately out of scope.
"""
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from lakehouse_ops.maintenance import (
    MB,
    SOLUTION_TABLES,
    current_snapshot_sql,
    decide,
    file_stats_sql,
    optimize_sql,
    record_count_sql,
    stats_from_row,
    validate_tables,
    verify_compaction,
)
from lakehouse_ops.trino_client import CATALOG, SCHEMA, fetch_all


def _inspect(tables, threshold_mb, min_small_files, max_rewrite_mb):
    plans = []
    for table in tables:
        row = fetch_all(file_stats_sql(CATALOG, SCHEMA, table, threshold_mb * MB))[0]
        plans.append(decide(stats_from_row(table, row), min_small_files, max_rewrite_mb).__dict__)
    return plans


def _log_plan(title, plans):
    print(f"\n{title}")
    print(f"{'table':<28}{'action':<9}{'files':>7}{'small':>7}{'small MB':>10}{'specs':>8}  reason")
    for p in plans:
        s = p["stats"]
        print(f"{p['table']:<28}{p['action']:<9}{s['data_files']:>7}{s['small_files']:>7}"
              f"{s['small_bytes'] / MB:>10.1f}{str(s['spec_ids']):>8}  {p['reason']}")


@dag(
    dag_id="iceberg_maintenance",
    description="Plan (default) or apply bounded small-file compaction on iceberg.solution tables",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never two rewrites of the same table at once
    tags=["readydata", "maintenance"],
    params={
        "mode": Param("plan", enum=["plan", "apply"],
                      description="plan = report only (no changes); apply = compact the tables marked 'compact'"),
        "tables": Param(SOLUTION_TABLES, type="array", description="Subset of iceberg.solution tables to consider"),
        "small_file_threshold_mb": Param(32, type="integer", minimum=1, maximum=512,
                                         description="Files smaller than this count as small and are rewritten"),
        "min_small_files": Param(20, type="integer", minimum=2,
                                 description="Compact a table only if it has at least this many small files"),
        "max_rewrite_mb": Param(2048, type="integer", minimum=1,
                                description="Skip a table if its small files exceed this many MB (bounds one run)"),
    },
    doc_md=__doc__,
)
def iceberg_maintenance():

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def plan() -> list:
        """Read-only: file statistics per table from the $files metadata table."""
        p = get_current_context()["params"]
        plans = _inspect(validate_tables(p["tables"]), p["small_file_threshold_mb"],
                         p["min_small_files"], p["max_rewrite_mb"])
        _log_plan(f"Maintenance plan (mode={p['mode']})", plans)
        return plans

    @task(retries=0, execution_timeout=timedelta(hours=1))
    def compact(plans: list) -> list:
        """Apply mode only. Tables are compacted sequentially; each optimize is
        a single atomic commit, so a failure leaves that table unchanged. All
        selected tables are attempted before the task fails."""
        p = get_current_context()["params"]
        if p["mode"] != "apply":
            print("mode=plan: nothing changed. Re-trigger with mode=apply to compact the tables marked 'compact'.")
            return []

        results, failures = [], []
        for item in (x for x in plans if x["action"] == "compact"):
            table = item["table"]
            try:
                records_before = fetch_all(record_count_sql(CATALOG, SCHEMA, table))[0][0]
                metrics = dict(fetch_all(optimize_sql(CATALOG, SCHEMA, table, p["small_file_threshold_mb"])))
                snapshot = fetch_all(current_snapshot_sql(CATALOG, SCHEMA, table))
                records_after = fetch_all(record_count_sql(CATALOG, SCHEMA, table))[0][0]
                check = verify_compaction(snapshot[0] if snapshot else None, records_before, records_after)
                result = {"table": table, "status": "ok" if check["verified"] else "unverified",
                          "metrics": metrics, **check}
                if not check["verified"]:
                    failures.append(f"{table}: {check['detail']}")
            except Exception as e:  # one table's failure must not stop the others
                result = {"table": table, "status": "failed", "detail": str(e)[:500]}
                failures.append(f"{table}: {e}")
            print(result)
            results.append(result)

        if failures:
            raise AirflowFailException("Compaction problems (other tables were still processed):\n" + "\n".join(failures))
        return results

    @task(trigger_rule="all_done")
    def report(plans: list) -> list:
        """Always runs, even after a failed compact: shows the current state."""
        p = get_current_context()["params"]
        after = _inspect(validate_tables(p["tables"]), p["small_file_threshold_mb"],
                         p["min_small_files"], p["max_rewrite_mb"])
        before = {x["table"]: x["stats"] for x in plans}
        print(f"\n{'table':<28}{'files before':>13}{'files after':>12}{'small before':>13}{'small after':>12}")
        for a in after:
            b = before.get(a["table"], {})
            print(f"{a['table']:<28}{b.get('data_files', '-'):>13}{a['stats']['data_files']:>12}"
                  f"{b.get('small_files', '-'):>13}{a['stats']['small_files']:>12}")
        return after

    plans = plan()
    compact(plans) >> report(plans)


iceberg_maintenance()
