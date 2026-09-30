"""Run SQL against Trino (iceberg.solution by default) using the `trino` package.

Usage:
  python query.py "SELECT count(*) FROM throughput_events"   # one statement
  python query.py -f /sql/checks/duplicate_origin.sql         # statements from a file
  python query.py                                            # interactive prompt
  python query.py --csv "SELECT ..." > out.csv                # CSV instead of a table

Connection settings come from TRINO_HOST, TRINO_PORT, TRINO_USER,
TRINO_CATALOG and TRINO_SCHEMA (defaults suit the Compose network).
"""
import argparse
import csv
import os
import sys
import time

import trino


def connect() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "query-cli"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "solution"),
    )


def split_statements(sql: str) -> list:
    """Split on semicolons that are outside quotes and comments.
    Trino's API takes one statement at a time, without the trailing ';'."""
    statements, current, i, n = [], [], 0, len(sql)
    quote = None
    while i < n:
        ch = sql[i]
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            current.append(ch)
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end == -1 else end
            continue
        elif ch == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def format_table(columns: list, rows: list) -> str:
    cells = [[("NULL" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    line = " | ".join(c.ljust(w) for c, w in zip(columns, widths))
    sep = "-+-".join("-" * w for w in widths)
    body = [" | ".join(v.ljust(w) for v, w in zip(r, widths)) for r in cells]
    return "\n".join([line, sep, *body])


def run(conn, sql: str, as_csv: bool, max_rows: int) -> None:
    cur = conn.cursor()
    started = time.monotonic()
    cur.execute(sql)
    rows = cur.fetchmany(max_rows) if max_rows else cur.fetchall()
    truncated = bool(max_rows) and cur.fetchone() is not None
    columns = [d[0] for d in cur.description] if cur.description else []
    if as_csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        writer.writerows(rows)
        return
    if columns:
        print(format_table(columns, rows))
    more = f", showing first {max_rows}" if truncated else ""
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''}{more}; {time.monotonic() - started:.1f}s)\n")


def repl(conn, max_rows: int) -> None:
    print("Connected to Trino. End statements with ';'. Ctrl-D or 'quit' to exit.")
    buffer = []
    while True:
        try:
            line = input("trino> " if not buffer else "    -> ")
        except EOFError:
            print()
            return
        if not buffer and line.strip().lower() in ("quit", "exit"):
            return
        buffer.append(line)
        if line.rstrip().endswith(";"):
            for statement in split_statements("\n".join(buffer)):
                try:
                    run(conn, statement, as_csv=False, max_rows=max_rows)
                except trino.exceptions.TrinoQueryError as e:
                    print(f"Query failed: {e.message}", file=sys.stderr)
            buffer = []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql", nargs="?", help="SQL to run (omit for an interactive prompt)")
    parser.add_argument("-f", "--file", help="Run the statements in this .sql file")
    parser.add_argument("--csv", action="store_true", help="Write results as CSV to stdout")
    parser.add_argument("--max-rows", type=int, default=100,
                        help="Rows to print per statement (0 = all; default 100)")
    args = parser.parse_args()

    conn = connect()
    try:
        if args.file:
            with open(args.file) as f:
                sql = f.read()
        elif args.sql:
            sql = args.sql
        else:
            repl(conn, args.max_rows)
            return 0
        for statement in split_statements(sql):
            run(conn, statement, args.csv, 0 if args.csv else args.max_rows)
        return 0
    except trino.exceptions.TrinoQueryError as e:
        print(f"Query failed: {e.message}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
