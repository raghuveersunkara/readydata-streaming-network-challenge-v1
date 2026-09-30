"""Helpers for the manual_sql_to_csv DAG (no Airflow or Trino imports, so they are unit-testable).

CSV contract:
- UTF-8 without BOM, RFC 4180 style: comma delimiter, CRLF line endings, fields
  quoted only when needed (csv.QUOTE_MINIMAL), embedded quotes doubled.
- First row is the header, taken from the query's column names (must be unique).
- NULL is written as an empty field. The checked-in reports never return empty
  strings, so an empty field always means NULL.
- Timestamps are ISO 8601; decimals are plain notation (no exponent); booleans
  are true/false.
"""
import csv
import hashlib
import io
import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, List, Optional

REPORT_NAME_RE = re.compile(r"^\d{2}_[a-z0-9_]+\.sql$")
FORBIDDEN_RE = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|call|execute|set|reset)\b",
    re.IGNORECASE,
)


def list_reports(sql_root: str) -> List[str]:
    """Checked-in reports: top-level NN_name.sql files (checks/ is excluded)."""
    try:
        names = os.listdir(sql_root)
    except FileNotFoundError:
        return []
    return sorted(n for n in names if REPORT_NAME_RE.match(n) and os.path.isfile(os.path.join(sql_root, n)))


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def load_report(sql_root: str, name: str) -> str:
    """Return the report's SQL after checking it is a known, single, read-only query."""
    if name not in list_reports(sql_root):
        raise ValueError(f"Unknown report {name!r}; choose one of {list_reports(sql_root)}")
    with open(os.path.join(sql_root, name), encoding="utf-8") as f:
        sql = f.read()
    body = _strip_comments(sql).strip().rstrip(";").strip()
    if ";" in body:
        raise ValueError(f"{name}: must contain exactly one statement")
    first_word = body.split(None, 1)[0].lower() if body else ""
    if first_word not in ("select", "with"):
        raise ValueError(f"{name}: must start with SELECT or WITH (read-only), found {first_word!r}")
    forbidden = FORBIDDEN_RE.search(body)
    if forbidden:
        raise ValueError(f"{name}: contains non-read-only keyword {forbidden.group(0)!r}")
    return sql.strip().rstrip(";").strip()


def format_value(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    return str(value)


def export_file_name(report: str, run_id: str, try_number: int) -> str:
    """Unique per run and attempt, so a rerun or retry never overwrites an earlier export."""
    safe_run = re.sub(r"[^A-Za-z0-9._-]", "_", run_id)
    return f"{report[:-4]}__{safe_run}__try{try_number}.csv"


def write_csv(columns: List[str], batches: Iterable[list], path: str) -> dict:
    """Stream batches of rows into a new file at `path` (must not exist) and return stats."""
    if len(set(columns)) != len(columns):
        raise ValueError(f"Duplicate column names in result: {columns}")
    digest = hashlib.sha256()
    rows = 0
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
        writer = csv.writer(text, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(columns)
        for batch in batches:
            writer.writerows([format_value(v) for v in row] for row in batch)
            rows += len(batch)
        text.flush()
        raw.flush()
        os.fsync(raw.fileno())
        text.detach()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return {"rows": rows, "bytes": os.path.getsize(path), "sha256": digest.hexdigest(), "columns": columns}


def publish(tmp_path: str, final_path: str) -> None:
    """Atomically expose a finished file without ever replacing an existing one."""
    os.link(tmp_path, final_path)  # fails if final_path exists
    os.unlink(tmp_path)


def export_rows(columns: List[str], batches: Iterable[list], out_dir: str, name: str) -> dict:
    """Write rows to a hidden .part file, then publish it as `name`. If anything fails
    (query error mid-stream, disk full, name already taken) no CSV and no .part file is
    left behind, so a reader never sees a partial export."""
    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, name)
    tmp_path = os.path.join(out_dir, f".{name}.part")
    try:
        stats = write_csv(columns, batches, tmp_path)
        publish(tmp_path, final_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return {**stats, "csv_path": final_path}


def write_sidecar(csv_path: str, metadata: dict) -> str:
    """Publish the metadata JSON next to the CSV, with the same never-overwrite rule."""
    final_path = csv_path[:-4] + ".json"
    tmp_path = os.path.join(os.path.dirname(csv_path), f".{os.path.basename(final_path)}.part")
    try:
        with open(tmp_path, "x", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        publish(tmp_path, final_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return final_path


def verify_csv(path: str, expected_rows: int, expected_sha256: str) -> dict:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    with open(path, encoding="utf-8", newline="") as f:
        data_rows = sum(1 for _ in csv.reader(f)) - 1  # minus header; csv handles quoted newlines
    return {
        "sha256_matches": digest.hexdigest() == expected_sha256,
        "row_count_matches": data_rows == expected_rows,
        "rows_read": data_rows,
    }
