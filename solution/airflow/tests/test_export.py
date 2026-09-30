import csv
import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from lakehouse_ops.export import (
    export_file_name, export_rows, format_value, list_reports, load_report, publish, verify_csv,
    write_csv, write_sidecar,
)

SQL_ROOT = os.getenv("READYDATA_SQL_ROOT", "/opt/airflow/sql")


def _sql_dir(tmp_path, files):
    for name, body in files.items():
        (tmp_path / name).write_text(body)
    return str(tmp_path)


def test_only_numbered_top_level_reports_are_listed(tmp_path):
    (tmp_path / "checks").mkdir()
    (tmp_path / "checks" / "x.sql").write_text("SELECT 1")
    root = _sql_dir(tmp_path, {"01_a.sql": "SELECT 1", "README.md": "", "notes.sql": "SELECT 1"})
    assert list_reports(root) == ["01_a.sql"]


@pytest.mark.parametrize("body, message", [
    ("DROP TABLE x", "SELECT or WITH"),
    ("SELECT 1; SELECT 2", "one statement"),
    ("WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", "non-read-only"),
])
def test_rejects_non_read_only_or_multi_statement(tmp_path, body, message):
    root = _sql_dir(tmp_path, {"01_bad.sql": body})
    with pytest.raises(ValueError, match=message):
        load_report(root, "01_bad.sql")


def test_rejects_unknown_or_path_traversal_names(tmp_path):
    root = _sql_dir(tmp_path, {"01_a.sql": "SELECT 1"})
    for name in ["../etc/passwd", "02_missing.sql", "checks/duplicate_origin.sql"]:
        with pytest.raises(ValueError, match="Unknown report"):
            load_report(root, name)


def test_comments_and_trailing_semicolon_are_fine(tmp_path):
    root = _sql_dir(tmp_path, {"01_a.sql": "-- drop; delete\nSELECT 1 AS x;\n"})
    assert load_report(root, "01_a.sql").endswith("SELECT 1 AS x")


@pytest.mark.skipif(not os.path.isdir(SQL_ROOT), reason="solution/sql not mounted")
def test_every_checked_in_report_passes_the_read_only_check():
    reports = list_reports(SQL_ROOT)
    assert len(reports) >= 5
    for name in reports:
        load_report(SQL_ROOT, name)


def test_value_formatting():
    assert format_value(None) is None
    assert format_value(True) == "true"
    assert format_value(Decimal("0E-16")) == "0.0000000000000000"
    assert format_value(datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)) == "2026-09-29T04:00:00+00:00"


def test_file_name_is_unique_per_run_and_attempt():
    a = export_file_name("03_x.sql", "manual__2026-09-29T04:00:00+00:00", 1)
    assert a == "03_x__manual__2026-09-29T04_00_00_00_00__try1.csv"
    assert a != export_file_name("03_x.sql", "manual__2026-09-29T04:00:00+00:00", 2)


def test_streams_batches_with_header_quoting_and_nulls(tmp_path):
    path = str(tmp_path / "out.csv")
    batches = iter([[("a", None, 1)], [('say "hi", ok', "", 2.5)]])
    stats = write_csv(["name", "note", "n"], batches, path)
    with open(path, encoding="utf-8", newline="") as f:
        raw = f.read()
    assert raw == 'name,note,n\r\na,,1\r\n"say ""hi"", ok",,2.5\r\n'
    assert stats["rows"] == 2
    assert verify_csv(path, 2, stats["sha256"]) == {"sha256_matches": True, "row_count_matches": True, "rows_read": 2}


def test_duplicate_column_names_are_refused(tmp_path):
    with pytest.raises(ValueError, match="Duplicate column"):
        write_csv(["a", "a"], iter([]), str(tmp_path / "x.csv"))


def test_never_overwrites_an_existing_export(tmp_path):
    final = tmp_path / "r.csv"
    final.write_text("earlier export")
    tmp = tmp_path / ".r.csv.part"
    tmp.write_text("new")
    with pytest.raises(FileExistsError):
        publish(str(tmp), str(final))
    assert final.read_text() == "earlier export"
    with pytest.raises(FileExistsError):
        write_csv(["a"], iter([]), str(final))  # O_EXCL: the part file must be new too


def test_failure_mid_stream_leaves_no_csv_and_no_part_file(tmp_path):
    def batches():
        yield [("a", 1)]
        raise RuntimeError("Trino connection lost")  # e.g. query failed after the first batch

    with pytest.raises(RuntimeError):
        export_rows(["name", "n"], batches(), str(tmp_path), "r.csv")
    assert os.listdir(tmp_path) == []


def test_successful_export_publishes_csv_and_sidecar(tmp_path):
    stats = export_rows(["n"], iter([[(1,), (2,)]]), str(tmp_path / "03_x"), "r.csv")
    sidecar = write_sidecar(stats["csv_path"], {"rows": stats["rows"]})
    assert sorted(os.listdir(tmp_path / "03_x")) == ["r.csv", "r.json"]
    assert stats["rows"] == 2 and sidecar.endswith("r.json")


def test_verify_detects_a_modified_or_truncated_csv(tmp_path):
    stats = export_rows(["n"], iter([[(1,), (2,)]]), str(tmp_path), "r.csv")
    with open(stats["csv_path"], "a", encoding="utf-8") as f:
        f.write("3\r\n")  # appended after export
    result = verify_csv(stats["csv_path"], stats["rows"], stats["sha256"])
    assert result == {"sha256_matches": False, "row_count_matches": False, "rows_read": 3}
