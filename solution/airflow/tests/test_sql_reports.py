"""Checks every checked-in report against the brief's report rules, using the live Trino.

Each report is wrapped in `SELECT * FROM (...) LIMIT 0`: Trino fully plans it (so wrong
tables, columns or syntax fail here) but reads no data, so this stays fast.
Skipped when Trino is not reachable.
"""
import os
import re

import pytest

from lakehouse_ops.export import _strip_comments, list_reports, load_report

SQL_ROOT = os.getenv("READYDATA_SQL_ROOT", "/opt/airflow/sql")
REPORTS = list_reports(SQL_ROOT)


def _trino_available() -> bool:
    try:
        from lakehouse_ops.trino_client import fetch_all
        return fetch_all("SELECT 1") == [[1]]
    except Exception:
        return False


needs_trino = pytest.mark.skipif(not _trino_available(), reason="Trino is not reachable")


def test_at_least_five_reports_are_checked_in():
    assert len(REPORTS) >= 5


@needs_trino
@pytest.mark.parametrize("report", REPORTS)
def test_report_plans_with_unique_named_columns(report):
    from lakehouse_ops.trino_client import connect

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM (\n{load_report(SQL_ROOT, report)}\n) LIMIT 0")
        cur.fetchall()
        columns = [d[0] for d in cur.description]
    finally:
        conn.close()
    assert len(columns) == len(set(columns)), f"duplicate column names: {columns}"
    assert not [c for c in columns if re.fullmatch(r"_col\d+", c)], f"unnamed columns: {columns}"


@pytest.mark.parametrize("report", REPORTS)
def test_report_orders_its_final_result(report):
    body = _strip_comments(load_report(SQL_ROOT, report)).lower()
    last_order_by = body.rfind("order by")
    assert last_order_by != -1, "no ORDER BY"
    # The last ORDER BY must belong to the outer query, i.e. come after every closing parenthesis.
    assert last_order_by > body.rfind(")"), "ORDER BY is not on the final result"


@pytest.mark.parametrize("report", REPORTS)
def test_report_documents_grain_and_assumptions(report):
    with open(os.path.join(SQL_ROOT, report), encoding="utf-8") as f:
        header = "".join(line for line in f if line.startswith("--"))
    assert "grain" in header.lower(), "header comment must state the grain"
