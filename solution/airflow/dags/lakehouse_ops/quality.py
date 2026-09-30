"""Checks for the lakehouse_quality DAG (no Airflow or Trino imports, so they are unit-testable).

Each check is one SQL query returning a single number, compared against a threshold:
the check fires when value > threshold.
- severity "fail": the data cannot be trusted (broken references, unknown devices,
  counts that disagree with the source manifests). The DAG run fails.
- severity "warn": worth investigating but expected to happen sometimes (source
  duplicates, rejects, clock skew, stale ingestion). Reported, the run still succeeds.
"""
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

EVENT_TABLES = [
    "connectivity_events",
    "throughput_events",
    "device_health_events",
    "connectivity_state_events",
    "infrastructure_events",
]


@dataclass(frozen=True)
class Check:
    name: str
    category: str       # "reference" or "event"
    severity: str       # "fail" or "warn"
    threshold: float    # fires when value > threshold
    description: str
    sql: str


def evaluate(check: Check, value: Optional[float], error: Optional[str] = None) -> Dict:
    """Turn a check's value into a finding. A query error counts as a failure."""
    if error is not None:
        status = "fail"
    elif value is not None and value > check.threshold:
        status = check.severity
    else:
        status = "ok"
    finding = asdict(check)
    finding.pop("sql")
    finding.update(value=value, status=status, error=error)
    return finding


def manifest_counts(inventory_counts: Dict[str, int], census_counts: Dict[str, int]) -> Dict[str, int]:
    """Map the supplied manifest.json count keys to the reference table names."""
    return {
        "sites": inventory_counts["sites"],
        "devices": inventory_counts["devices"],
        "device_dependencies": inventory_counts["dependencies"],
        "census_block_groups": census_counts["block_groups"],
    }


STATUS_ORDER = {"fail": 0, "warn": 1, "ok": 2}


def summarize(reference_findings: Optional[List[Dict]], event_findings: Optional[List[Dict]]) -> Dict:
    """Combine both check groups. The run must fail if any check failed, or if a whole
    group produced no findings (its task crashed), since missing checks are not a pass.
    Warnings never fail the run."""
    missing_groups = [name for name, found in (("reference", reference_findings), ("event", event_findings))
                      if found is None]
    findings = sorted((reference_findings or []) + (event_findings or []),
                      key=lambda f: (STATUS_ORDER[f["status"]], f["name"]))
    counts = {status: sum(1 for f in findings if f["status"] == status) for status in STATUS_ORDER}
    return {
        "summary": counts,
        "missing_groups": missing_groups,
        "failed": bool(counts["fail"] or missing_groups),
        "findings": findings,
    }


def reference_checks(schema: str, manifest_counts: Dict[str, int]) -> List[Check]:
    """Completeness and relationships of the reference tables.
    manifest_counts: expected rows per table, from the supplied manifest.json files."""
    s = schema
    checks = [
        Check(f"{table}_row_count_vs_manifest", "reference", "fail", 0,
              f"|rows in {table} - manifest count {expected}|",
              f"SELECT abs(count(*) - {int(expected)}) FROM {s}.{table}")
        for table, expected in sorted(manifest_counts.items())
    ]
    for table, key in [("sites", "site_id"), ("devices", "device_id"),
                       ("device_dependencies", "downstream_device_id"),
                       ("census_block_groups", "block_group_geoid")]:
        checks.append(Check(f"{table}_duplicate_keys", "reference", "fail", 0,
                            f"rows sharing a {key}",
                            f"SELECT count(*) - count(DISTINCT {key}) FROM {s}.{table}"))
    for name, sql in [
        ("sites_orphan_block_group",
         f"SELECT count(*) FROM {s}.sites x LEFT JOIN {s}.census_block_groups c "
         f"ON c.block_group_geoid = x.block_group_geoid WHERE c.block_group_geoid IS NULL"),
        ("devices_orphan_site",
         f"SELECT count(*) FROM {s}.devices x LEFT JOIN {s}.sites y ON y.site_id = x.site_id WHERE y.site_id IS NULL"),
        ("dependencies_orphan_downstream",
         f"SELECT count(*) FROM {s}.device_dependencies x LEFT JOIN {s}.devices d "
         f"ON d.device_id = x.downstream_device_id WHERE d.device_id IS NULL"),
        ("dependencies_orphan_upstream",
         f"SELECT count(*) FROM {s}.device_dependencies x LEFT JOIN {s}.devices d "
         f"ON d.device_id = x.upstream_device_id WHERE d.device_id IS NULL"),
        ("non_ixp_devices_without_upstream",
         f"SELECT count(*) FROM {s}.devices d LEFT JOIN {s}.device_dependencies x "
         f"ON x.downstream_device_id = d.device_id "
         f"WHERE d.infrastructure_role <> 'ixp' AND x.downstream_device_id IS NULL"),
    ]:
        checks.append(Check(name, "reference", "fail", 0, "rows breaking the relationship", sql))
    return checks


def _events(schema: str, columns: str, window_hours: int) -> str:
    """The five event tables stacked, limited to records that arrived in the window."""
    return "\n    UNION ALL\n    ".join(
        f"SELECT {columns} FROM {schema}.{t} "
        f"WHERE _kafka_timestamp >= current_timestamp - INTERVAL '{int(window_hours)}' HOUR"
        for t in EVENT_TABLES
    )


def event_checks(schema: str, window_hours: int) -> List[Check]:
    """Event quality over records that arrived in the last `window_hours` (Kafka arrival time)."""
    s, w = schema, int(window_hours)
    window = f"_kafka_timestamp >= current_timestamp - INTERVAL '{w}' HOUR"
    return [
        Check("events_from_unknown_devices", "event", "fail", 0,
              "event rows whose device_id is not in devices",
              f"SELECT count(*) FROM ({_events(s, 'device_id', w)}) e "
              f"LEFT JOIN {s}.devices d ON d.device_id = e.device_id WHERE d.device_id IS NULL"),
        Check("events_with_wrong_site", "event", "fail", 0,
              "event rows whose site_id differs from the inventory site of their device",
              f"SELECT count(*) FROM ({_events(s, 'device_id, site_id', w)}) e "
              f"JOIN {s}.devices d ON d.device_id = e.device_id WHERE e.site_id <> d.site_id"),
        Check("replay_duplicate_rows", "event", "warn", 0,
              "same Kafka record stored twice: ingestion replayed after a crash (TRADEOFFS.md, 'Delivery is at-least-once')",
              f"SELECT count(*) - count(DISTINCT ROW(_kafka_topic, _kafka_partition, _kafka_offset)) "
              f"FROM ({_events(s, '_kafka_topic, _kafka_partition, _kafka_offset', w)})"),
        Check("upstream_duplicate_pct", "event", "warn", 1.0,
              "% of rows that repeat an event_id (source resends; ~0.25% is normal)",
              f"SELECT 100.0 * (count(*) - count(DISTINCT event_id)) / nullif(count(*), 0) "
              f"FROM ({_events(s, 'event_id', w)})"),
        Check("reject_pct", "event", "warn", 1.0,
              "% of delivered records rejected (~0.02% is normal)",
              f"SELECT 100.0 * r.n / nullif(r.n + a.n, 0) FROM "
              f"(SELECT count(*) AS n FROM {s}.rejected_events WHERE {window}) r, "
              f"(SELECT count(*) AS n FROM ({_events(s, 'event_id', w)})) a"),
        Check("minutes_since_last_arrival", "event", "warn", 15,
              "minutes since the newest stored record arrived in Kafka (ingestion stalled?)",
              f"SELECT to_milliseconds(current_timestamp - max(_kafka_timestamp)) / 60000.0 "
              f"FROM {s}.connectivity_events WHERE {window}"),
        Check("stalled_beyond_kafka_retention", "event", "fail", 120,
              "minutes since the newest arrival; over 120 means Kafka's 2 h retention may already "
              "have deleted unread records (TRADEOFFS.md, 'Recovery gaps')",
              f"SELECT coalesce(to_milliseconds(current_timestamp - max(_kafka_timestamp)) / 60000.0, 1e9) "
              f"FROM {s}.connectivity_events WHERE {window}"),
        Check("source_clock_skew_minutes", "event", "warn", 5,
              "median minutes between measurement time and Kafka arrival (simulator clock lag; "
              "docs/exploring.md step 1)",
              f"SELECT approx_percentile(to_milliseconds(_kafka_timestamp - event_timestamp), 0.5) / 60000.0 "
              f"FROM {s}.connectivity_events WHERE {window}"),
        Check("silent_access_devices", "event", "warn", 0,
              "inventory access devices with no connectivity events in the window",
              f"SELECT count(*) FROM {s}.devices d WHERE d.infrastructure_role = 'subscriber_edge' "
              f"AND d.device_id NOT IN (SELECT device_id FROM {s}.connectivity_events WHERE {window})"),
    ]
