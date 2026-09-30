# Spec: `query` developer tool

| | |
| --- | --- |
| **Code** | [`query.py`](query.py), [`Dockerfile`](Dockerfile), [`requirements.txt`](requirements.txt) (`trino==0.340.0`) |
| **Runs as** | On-demand Compose service `query` in the `tools` profile (`up` doesn't start it); `solution/sql` is mounted at `/sql` |
| **Status** | Developer convenience, not part of the pipeline or the brief's deliverables |

## Purpose

Run SQL against Trino from the terminal with the Python `trino` package: a
single statement, a `.sql` file, or an interactive prompt.

## Requirements

| ID | Requirement | Verified by |
| --- | --- | --- |
| QRY-01 | `query "<sql>"` runs one or more statements; `query -f <file>` runs a file's statements; with no arguments it opens a prompt where statements end with `;` and `quit` exits. | Manual runs, 2026-09-28 |
| QRY-02 | Statements are split on `;` outside quotes and `--` comments, and the trailing `;` is dropped before each is sent to Trino. | Manual: `SELECT 'a;b' …; SELECT … -- comment; …` ran as two statements |
| QRY-03 | Table output prints NULL as `NULL` and at most `--max-rows` rows (default 100; `0` prints all). `--csv` writes the whole result as CSV to stdout. | Manual runs |
| QRY-04 | A query error prints `Query failed: <message>` and exits with code 1. | Manual run against a table that doesn't exist |
| QRY-05 | Connection settings come from `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG` and `TRINO_SCHEMA`. | Review |

## Known limitations

- It has no automated tests, because it's a developer tool. The same Trino
  access is tested through the Airflow DAGs.
- Result sets are fully fetched for CSV output. For large or reviewed exports
  use `manual_sql_to_csv`, which streams to disk.
- `scratch.py` in this folder is a personal scratchpad. It isn't committed and
  isn't covered by this spec.

## Sign-off

| Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- |
| | | | |
