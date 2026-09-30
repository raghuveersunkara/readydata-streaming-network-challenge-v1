# Spec: reference-data loader (`reference-loader`)

| | |
| --- | --- |
| **Code** | [`load_reference.py`](load_reference.py), [`src/reference.py`](src/reference.py), [`src/tables.py`](src/tables.py) |
| **Tests** | [`tests/test_reference.py`](tests/test_reference.py), [`tests/test_load_reference.py`](tests/test_load_reference.py) |
| **Runs as** | One-shot Compose service `reference-loader`, `restart: "no"`, started by `up`. Re-run with `docker compose ... run --rm reference-loader`. |

## Purpose

Validate the four supplied reference files as a whole, and publish them as the
tables `sites`, `devices`, `device_dependencies` and `census_block_groups`.
Loading MUST be all-or-nothing on validation, and re-running MUST be harmless.

## Non-goals

- Keeping a history of inventory changes (slowly changing dimensions); each
  load fully replaces the tables.
- Loading the optional GeoJSON files and Census Places.
- Checking that events refer to known devices; that's `lakehouse_quality`'s job.

## Interfaces

**Inputs:** `reference-data/` mounted read-only at `REFERENCE_DATA_DIR`
(default `/app/reference-data`):

- `census/v1/census_block_groups.csv`
- `inventory/v1/sites.csv`, `inventory/v1/devices.csv`,
  `inventory/v1/device_dependencies.csv`
- both `manifest.json` files (checksums and counts)

**Outputs:** the four tables. Each also gets `_source_file`, `_source_sha256`
and `_loaded_at`, and each snapshot records `reference.source-sha256` and
`reference.loader-version`.

**Exit codes:**

| Code | Meaning |
| --- | --- |
| 0 | Every table was loaded or was already current |
| 1 | Validation failed, and nothing was written |
| 2 | The catalog or storage was still unavailable after `MAX_ATTEMPTS` (6) tries |

## Requirements

| ID | Requirement | Verified by |
| --- | --- | --- |
| REF-01 | Files MUST be read with explicit column types. `block_group_geoid` MUST stay a string, so leading zeros are kept. Booleans, dates and doubles MUST be parsed to their types. Empty provider and speed fields MUST become NULL. | `test_types_are_parsed_and_geoid_keeps_leading_zero` |
| REF-02 | All validation MUST happen before anything is written. Any error MUST end the run with exit 1 without opening the catalog, and a valid set of files MUST produce no errors. | `test_validation_failure_exits_1_and_never_touches_the_catalog`, `test_valid_bundle_has_no_errors` (the baseline each REF-03…08 test breaks one rule of) |
| REF-03 | Each file's header MUST match its documented columns, and its SHA-256 MUST match its `manifest.json`. | `test_header_drift`, `test_checksum_mismatch` |
| REF-04 | Row counts MUST match the manifest: totals per file, the access/infrastructure device split, and the service/network site split. | `test_count_mismatch_against_manifest` |
| REF-05 | Primary keys MUST be unique and required fields non-empty. | `test_duplicate_primary_key`, `test_inventory_rule_violations_are_reported[empty required value]` |
| REF-06 | Relationships MUST hold: sites → block groups, devices → sites, and both ends of every dependency → devices. | `test_orphaned_foreign_keys` |
| REF-07 | Value rules MUST hold: coordinates inside Arkansas; 12-digit GEOIDs starting with `05`; no negative measures; access devices have a provider and advertised speed; infrastructure devices have no advertised speed; each service site has exactly one device. | `test_inventory_rule_violations_are_reported` (9 cases) |
| REF-08 | Topology MUST hold: IXP devices have no upstream device; every other device has exactly one; every chain reaches an IXP without a cycle. | `test_dependency_cycle_is_detected`, `test_inventory_rule_violations_are_reported[IXP with an upstream]`, `[device without an upstream]` |
| REF-09 | Each table MUST be replaced with one atomic Iceberg overwrite. A table whose current snapshot already records the same source checksum and loader version MUST be skipped, so re-runs create no new snapshots. | `test_write_skips_tables_already_holding_same_source`; live second run reported every table "unchanged" (2026-09-28) |
| REF-10 | If the catalog or storage is unavailable, the loader MUST retry with backoff, exit 2 after `MAX_ATTEMPTS`, and exit 0 if a later attempt succeeds. | `test_storage_failure_exits_2_after_retries`, `test_success_after_a_transient_storage_error_exits_0` |
| REF-11 | The supplied reference data MUST pass validation, with 2,294 block groups, 623 sites, 654 devices and 650 dependencies. | `test_supplied_reference_data_is_valid` |
| REF-12 | Reference tables MUST NOT be partitioned. | `test_reference_tables_are_not_partitioned` |

## Known limitations

- The four tables are committed one at a time. If the loader is interrupted
  partway, a re-run completes the rest, because tables already current are
  skipped (REF-09).
- Each load fully replaces the tables, with no history. The production plan
  is in [TRADEOFFS.md → Scaling the system](../TRADEOFFS.md#scaling-the-system).
- Changing the table schemas or transformations requires bumping
  `LOADER_VERSION` in `src/reference.py`. Otherwise unchanged files would be
  skipped.

## Sign-off

| Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- |
| | | | |
