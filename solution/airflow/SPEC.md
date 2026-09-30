# Spec: Airflow workflows

| | |
| --- | --- |
| **Code** | DAG files in [`dags/`](dags/); logic in [`dags/lakehouse_ops/`](dags/lakehouse_ops/) (`export.py`, `quality.py`, `maintenance.py`, `trino_client.py`); image in [`Dockerfile`](Dockerfile) |
| **Tests** | [`tests/`](tests/): `test_dag_integrity.py`, `test_export.py`, `test_quality.py`, `test_maintenance.py`, `test_sql_reports.py` |
| **Brief** | [README §4 Airflow workflows](../../README.md#4-airflow-workflows) |

Each DAG has its own section and sign-off, so they can be approved separately.

## Shared requirements (all DAGs)

| ID | Requirement | Verified by |
| --- | --- | --- |
| DAG-01 | Every DAG file MUST load without import errors, and `manual_sql_to_csv`, `lakehouse_quality` and `iceberg_maintenance` MUST exist. | `test_every_dag_file_imports`, `test_the_three_required_dags_exist`; live `airflow dags list-import-errors` → "No data found" |
| DAG-02 | DAGs MUST be manual-only (`schedule=None`) with `catchup=False`. | `test_dags_are_manual_only_and_do_not_backfill` |
| DAG-03 | DAGs MUST be independent: no DAG triggers, waits for or imports another. Each reads Iceberg only through Trino, configured by `READYDATA_TRINO_*`. | Review: no `ExternalTaskSensor` or `TriggerDagRunOperator`, and DAG files import only from `lakehouse_ops/` |
| DAG-04 | No DAG may destructively clean up the catalog or object storage (no snapshot expiry, orphan-file removal, `DROP` or `DELETE`). | Review: `maintenance.optimize_sql` is the only statement that writes; export rejects write keywords (`test_rejects_non_read_only_or_multi_statement`) |
| DAG-05 | Logic MUST live in `lakehouse_ops/` with no Airflow imports, so it can be unit-tested. DAG files hold the orchestration. | Review |

---

## `manual_sql_to_csv`

**Purpose:** run one checked-in, read-only SQL report through Trino and publish
its complete result as a CSV under `solution/exports/`.

**Param:** `report`, a dropdown of `solution/sql/NN_name.sql` files, built when
the DAG is parsed.

**Tasks:** `export` (1 retry, 30-minute timeout) → `verify`.

**Output:** `solution/exports/<report>/<report>__<run_id>__try<n>.csv`, plus a
`.json` file with the same name. Exports are git-ignored.

| ID | Requirement | Verified by |
| --- | --- | --- |
| EXP-01 | Only numbered top-level reports in `solution/sql/` can be selected; `checks/` and other files are excluded. The dropdown MUST equal that list. | `test_only_numbered_top_level_reports_are_listed`, `test_export_dag_offers_exactly_the_checked_in_reports` |
| EXP-02 | A report MUST be a single statement starting with `SELECT` or `WITH`, with no write or DDL keywords (comments ignored). Unknown names and path tricks MUST be rejected before Trino is queried. Every checked-in report MUST pass. | `test_rejects_non_read_only_or_multi_statement`, `test_rejects_unknown_or_path_traversal_names`, `test_comments_and_trailing_semicolon_are_fine`, `test_every_checked_in_report_passes_the_read_only_check` |
| EXP-03 | Rows MUST be streamed from Trino in batches of `READYDATA_EXPORT_FETCH_SIZE` (1,000) straight to disk. Only a small metadata dict goes to XCom. | `test_streams_batches_with_header_quoting_and_nulls`; review of the `export` task |
| EXP-04 | CSV format: UTF-8 without BOM; a header from the result's column names, which must be unique; comma-separated; CRLF line endings; quoting only where needed, with embedded quotes doubled; **NULL as an empty field**; ISO 8601 timestamps; plain-notation decimals; `true`/`false`. | `test_streams_batches_with_header_quoting_and_nulls`, `test_value_formatting`, `test_duplicate_column_names_are_refused` |
| EXP-05 | Every run and every attempt MUST get its own file name, and an existing file MUST never be overwritten. | `test_file_name_is_unique_per_run_and_attempt`, `test_never_overwrites_an_existing_export` |
| EXP-06 | The CSV MUST appear only when complete. It's written as a hidden `.part` file and linked into place atomically. A failure at any point MUST leave neither a CSV nor a `.part` file. | `test_failure_mid_stream_leaves_no_csv_and_no_part_file`, `test_successful_export_publishes_csv_and_sidecar` |
| EXP-07 | A `.json` file next to the CSV MUST record: report, SQL SHA-256, run ID, attempt number, start and finish times, container and host paths, rows, bytes, CSV SHA-256 and columns. | `test_successful_export_publishes_csv_and_sidecar`; live export of report 9 (81 rows; your Mac's `shasum` matched the recorded SHA-256) |
| EXP-08 | `verify` MUST re-read the published CSV and fail if its SHA-256 or row count differs from the metadata. | `test_verify_detects_a_modified_or_truncated_csv` |

---

## `lakehouse_quality`

**Purpose:** check that the reference data is complete and consistent, and
reconcile event quality. Report findings, and separate failures from things to
investigate.

**Param:** `window_hours` (1–48, default 6). Event checks cover records that
arrived in Kafka in that window.

**Tasks:** `reference` and `events` (1 retry each) → `summarize`
(`trigger_rule="all_done"`, so it always runs).

| ID | Requirement | Verified by |
| --- | --- | --- |
| QUA-01 | Each check MUST be one SQL query returning a single number, compared with a threshold: over the threshold → the check's severity (`fail` or `warn`), otherwise `ok`. | `test_fail_and_warn_are_distinguished` |
| QUA-02 | Reference checks, all **fail**: row count against `manifest.json` for all four tables; duplicate keys; orphaned site→block group, device→site and dependency→device links; non-IXP devices with no upstream. Manifest key names MUST map to table names. | `test_reference_counts_come_from_the_manifest`, `test_manifest_keys_map_to_table_names`, `test_supplied_manifests_have_the_expected_count_keys` |
| QUA-03 | Event checks cover all five event tables within the window. **fail:** events from unknown devices; events whose site differs from their device's site; no arrivals for more than 120 minutes. **warn:** replay duplicates > 0; source duplicates > 1%; rejects > 1%; no arrivals for more than 15 minutes; median source clock skew > 5 minutes; inventory access devices with no events. | `test_event_checks_use_the_requested_window_and_all_five_tables`; thresholds reviewed in `quality.event_checks` |
| QUA-04 | A check whose query errors MUST count as `fail`, and the other checks MUST still run. | `test_query_errors_count_as_failures`; review of the per-check `try/except` in `_run` |
| QUA-05 | The run MUST fail if any check fails, or if a whole group is missing because its task crashed. Warnings alone MUST NOT fail the run. | `test_warnings_alone_do_not_fail_the_run`, `test_any_failed_check_fails_the_run_and_is_listed_first`, `test_a_check_group_that_did_not_run_fails_the_run` |
| QUA-06 | Findings MUST be printed as a table sorted fail → warn → ok, with value, threshold and description, and returned in XCom. | `test_any_failed_check_fails_the_run_and_is_listed_first`; live runs on 2026-09-29 |
| QUA-07 | Every check MUST be read-only (`SELECT`), and check names MUST be unique. | `test_check_names_are_unique`; review of `quality.py` |

**Live evidence (2026-09-29):**

- **First run:** 19 of 22 checks passed, including every reference and
  consistency check. It raised three warnings, each a real problem: ingestion
  looked stalled (the bridge outage), 1,000 replay duplicates (Kafka stalled),
  and clock skew.
- **After the fixes:** 0 fails, 1 warning (clock skew), 21 passes.

---

## `iceberg_maintenance`

**Purpose:** inspect small-file behaviour, and only on request, compact tables
where it's justified and the work is bounded. It's not destructive.

**Params:** `mode` (`plan` by default, or `apply`); `tables` (defaults to all
10); `small_file_threshold_mb` (32); `min_small_files` (20); `max_rewrite_mb`
(2048).

**Tasks:** `plan` (1 retry) → `compact` (no retries, 1-hour timeout) →
`report` (`trigger_rule="all_done"`). `max_active_runs=1`.

| ID | Requirement | Verified by |
| --- | --- | --- |
| MNT-01 | The default mode MUST be `plan`, which only reads metadata tables (`$files`) and changes nothing. | `test_maintenance_defaults_to_plan_only`; review of `plan` and `compact` (returns early unless `mode == "apply"`) |
| MNT-02 | `tables` MUST be limited to the 10 `iceberg.solution` tables before any name reaches SQL. | `test_table_allowlist_blocks_unknown_and_injection` |
| MNT-03 | **Justified:** a table is compacted only if it has at least `min_small_files` files smaller than the threshold. | `test_skips_when_too_few_small_files`, `test_compacts_when_justified_and_bounded` |
| MNT-04 | **Bounded:** a table is skipped, with the reason given, if its small files total more than `max_rewrite_mb`. | `test_skips_when_rewrite_exceeds_bound` |
| MNT-05 | Only files below the threshold are rewritten (`optimize(file_size_threshold => …)`). The plan MUST say when older unpartitioned files will be moved under the current partitioning. | `test_optimize_only_rewrites_files_below_threshold`, `test_mentions_legacy_partition_spec` |
| MNT-06 | Tables MUST be compacted one at a time, each in a single atomic commit. Every selected table MUST be attempted before the task fails, and the error MUST name each failing table. | Review of the `compact` loop; live apply runs |
| MNT-07 | Each compaction MUST keep every row. If the `replace` snapshot is still current, its added and deleted record counts must be equal. Otherwise (an append came in meanwhile), rows after ≥ rows before. | `test_verify_uses_replace_snapshot_when_still_current`, `test_verify_falls_back_to_no_row_loss_after_concurrent_append` |
| MNT-08 | `report` MUST always run and show file counts before and after. Only one run at a time is allowed (`max_active_runs=1`). | Review; live runs |

**Live evidence:**

- **2026-09-28:** the small tables went from about 1,730 files to 6.
- **2026-09-29:** `connectivity_events`, `throughput_events` and
  `device_health_events` went from 64,808 files to 8. Every table passed the
  `replace`-snapshot check (for example, 9,084,933 rows added and removed),
  and Trino didn't restart.

**Known limitations:**

- Compaction covers whole tables: Trino rejects `optimize … WHERE` on the
  partition column while older unpartitioned files exist.
- Nessie exposes only the current snapshot, so when an append arrives during
  compaction, the row check falls back to the weaker comparison.

## Sign-off

| DAG / section | Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- | --- |
| Shared (DAG-*) | | | | |
| `manual_sql_to_csv` (EXP-*) | | | | |
| `lakehouse_quality` (QUA-*) | | | | |
| `iceberg_maintenance` (MNT-*) | | | | |
