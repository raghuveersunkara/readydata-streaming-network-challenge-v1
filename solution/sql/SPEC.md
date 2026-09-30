# Spec: analytical SQL reports

| | |
| --- | --- |
| **Code** | `01_…` to `10_…` `.sql` in this folder (one query per file), [`checks/duplicate_origin.sql`](checks/duplicate_origin.sql) |
| **Inventory and definitions** | [`README.md`](README.md): perspective mapping, grain, thresholds, peer groups, windows |
| **Tests** | [`../airflow/tests/test_sql_reports.py`](../airflow/tests/test_sql_reports.py), [`../airflow/tests/test_export.py`](../airflow/tests/test_export.py) |
| **Brief** | [README §3 Analytical SQL](../../README.md#3-analytical-sql) |

## Purpose

Read-only reports over `iceberg.solution`, one per analytical perspective in
the brief (ten reports cover all ten perspectives). They can be exported
through `manual_sql_to_csv`.

## Rules every report must follow

| ID | Requirement | Verified by |
| --- | --- | --- |
| SQL-01 | Each file MUST contain exactly one read-only statement (`SELECT` or `WITH`). | `test_every_checked_in_report_passes_the_read_only_check` |
| SQL-02 | Each report MUST plan successfully in Trino, and its columns MUST be uniquely named (no unnamed `_colN`). | `test_report_plans_with_unique_named_columns` (runs `LIMIT 0` against the live Trino, so no data is read) |
| SQL-03 | The final result MUST have an `ORDER BY`, so the output order is stable. | `test_report_orders_its_final_result` |
| SQL-04 | The header comment MUST state the grain and the assumptions. | `test_report_documents_grain_and_assumptions`; the assumptions are reviewed |
| SQL-05 | At least five reports, each for a different perspective, mapped in `README.md`. | `test_at_least_five_reports_are_checked_in`; review of the README table |
| SQL-06 | No hard-coded identifiers or expected results. Windows MUST be relative (to `current_timestamp`, or to the newest measurement in the data). | Review; a search of the reports finds no `device-…`, `site-…` or date literals |
| SQL-07 | Duplicate handling MUST be deliberate and documented. Counts deduplicate on `event_id` (reports 1 and 2). Statistics (3–7) and report 10 don't deduplicate: a duplicate is an identical copy of about 0.25% of rows, and deduplicating would exceed Trino's memory. | Review of each header and README → Definitions |
| SQL-08 | Every report MUST run within the stack's Trino memory limit (2 GB heap) once the tables are compacted. | Live: all ten ran in 1–8 s on 2026-09-29 without Trino restarting |

## Per-report acceptance

The definitions are in [README.md](README.md#definitions-thresholds-and-peer-groups).
The **acceptance** column gives what a reviewer can check against real
results; the evidence comes from runs on 2026-09-28 and 2026-09-29.

| Report | Perspective | Acceptance criterion | Evidence |
| --- | --- | --- | --- |
| `01_accepted_event_totals` | 1 | Duplicate-safe event counts per event type × provider × technology × region × role. Joins don't multiply rows, because `device_id` and `site_id` are unique. | 381 rows in 3 s; key uniqueness confirmed (654/654 devices, 623/623 sites) |
| `02_reconciliation` | 2 | `delivered = accepted + rejected` and `stored = unique + duplicate` add up per topic. Rates are close to the simulator's configuration (duplicates 0.25%, gaps 0.1% plus poison records 0.02%). | 6-hour window: 2.1M rows, 0.3% duplicates, 401 rejects |
| `03_device_statistics` | 3 | One row per access device, with min, median, p95 and max for latency, jitter and loss, and min, p05, median and max for throughput. | 600 rows (one per access device) |
| `04_technology_statistics` | 4 | The same statistics per technology, ranked as expected: fiber < coax < fixed wireless ≪ satellite latency. | Median latency: fiber ~12 ms, satellite ~658 ms |
| `05_underperforming_devices` | 5 | Peers are the same technology and plan tier. A device is flagged below 70% of peer download or above 1.5× peer latency, with peer groups of at least 5 devices. | 41 devices, e.g. one at 48% of peer download and 3× peer latency |
| `06_provider_technology_cohorts` | 6 | Delivered speed as % of the advertised plan and latency tails, per provider × technology. | 20 cohorts; coax/fiber ~89–91% of plan, fixed wireless ~70%, satellite ~59% |
| `07_geographic_performance` | 7 | Region × urban/rural, with median block-group population density showing that the "rural" rows really are sparse. | Median density: urban ~2,491 vs rural ~106 per sq mi |
| `08_connectivity_state_availability` | 8 | Time per state from event time, with availability = share of time not offline. Must be consistent with report 9. | 22 devices at ~90% availability, exactly the devices behind the router that report 9 shows failing |
| `09_incident_impact` | 9 | Impacted devices found by following the dependency chain (≤ 3 hops), including critical sites, and confirmed by state changes during the incident. | Each incident on `agg-northwest-moon_link` impacted 22 devices, including 1 critical site, and all 22 changed state |
| `10_late_records_by_measurement_time` | 10 | Lateness attributed to the measurement hour, using the source clock only. The maximum lateness must equal the simulator's configured late delay. | Maximum 90 s (connectivity, health) and 120 s (throughput) = max(90 s, 2 × cadence); late rate ≈ 0.05% = configured 0.0005 |

## Known limitations

- Reports 1, 2 and 10 cover 6 hours, because exact distinct counts or window
  functions over all history exceed Trino's per-query memory
  ([README → Time windows](README.md#time-windows)).
- Reports 3–9 use "last day relative to now". While the simulator clock lags,
  that window covers less recent source time.

## Sign-off

| Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- |
| | | | |
