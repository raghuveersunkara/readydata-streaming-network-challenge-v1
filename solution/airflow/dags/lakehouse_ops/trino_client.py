"""Trino connection shared by the solution DAGs, configured from the supplied
READYDATA_TRINO_* environment variables."""
import os

import trino

CATALOG = os.getenv("READYDATA_TRINO_CATALOG", "iceberg")
SCHEMA = os.getenv("READYDATA_TRINO_SCHEMA", "solution")


def connect() -> "trino.dbapi.Connection":
    return trino.dbapi.connect(
        host=os.getenv("READYDATA_TRINO_HOST", "trino"),
        port=int(os.getenv("READYDATA_TRINO_PORT", "8080")),
        user=os.getenv("READYDATA_TRINO_USER", "readydata"),
        catalog=CATALOG,
        schema=SCHEMA,
    )


def fetch_all(sql: str) -> list:
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()
