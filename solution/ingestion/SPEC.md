# Spec: streaming ingestion (`solution-ingestion`)

| | |
| --- | --- |
| **Code** | [`main.py`](main.py), [`src/pipeline.py`](src/pipeline.py), [`src/ingest.py`](src/ingest.py), [`src/validator.py`](src/validator.py), [`src/models.py`](src/models.py), [`src/schemas.py`](src/schemas.py), [`src/tables.py`](src/tables.py), [`src/config.py`](src/config.py) |
| **Tests** | [`tests/`](tests/): `test_pipeline.py`, `test_ingest.py`, `test_validator.py`, `test_schemas.py`, `test_tables.py` |
| **Runs as** | Long-running Compose service `ingestion`, `restart: unless-stopped` |

## Purpose

Continuously consume the five supplied kafka topics, validate records
against the events contract. Ingest accepted records in iceberg event tables
and invalid records in `rejected_events`. Events are never lost and the service never throws error for an invalid payload.

## Non-goals

- Removing duplicates. Delivery is at-least-once, and reports handle
  duplicates ([TRADEOFFS.md → Delivery is at-least-once](../TRADEOFFS.md#delivery-is-at-least-once-so-tables-can-contain-duplicates)).
- Exactly-once delivery.
- Loading reference data (see [REFERENCE_LOADER_SPEC.md](REFERENCE_LOADER_SPEC.md)).
- Running under Airflow ([TRADEOFFS.md → Ingestion does not run in Airflow](../TRADEOFFS.md#ingestion-does-not-run-in-airflow)).

## Interfaces

**Inputs:** Kafka topics, with the contracts in `contracts/events/` mounted
read-only.

| Topic | Only accepted `event_type` | Target table |
| --- | --- | --- |
| `network.connectivity` | `connectivity_metric` | `connectivity_events` |
| `network.throughput` | `throughput_test` | `throughput_events` |
| `network.device_health` | `device_health` | `device_health_events` |
| `network.state` | `connectivity_state` | `connectivity_state_events` |
| `network.infrastructure` | `infrastructure_event` | `infrastructure_events` |
| any, when a record is invalid | — | `rejected_events` |

**Outputs:** the six tables in `iceberg.solution`, with column definitions in
[`src/schemas.py`](src/schemas.py) and described in the
[README](../README.md#iceberg-tables).

**Configuration** (environment variables; defaults suit the Compose network):

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka brokers |
| `KAFKA_GROUP_ID` | `iceberg-ingestion-group` | Consumer group; committed offsets are stored under it |
| `NESSIE_URI` | `http://nessie:19120/iceberg` | Iceberg REST catalog |
| `S3_ENDPOINT`, `S3_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Garage defaults | Object storage |
| `CONTRACTS_DIR` | `/app/contracts/events` | JSON Schema contracts |
| `BATCH_SIZE` | `500` | Write when this many records are buffered… |
| `FLUSH_INTERVAL_SEC` | `5.0` | …or when this much time has passed with any records buffered |

## Requirements

### Handling each record

| ID | Requirement | Verified by |
| --- | --- | --- |
| ING-01 | Each topic MUST accept only its own `event_type`. A record of another type, or from an unmapped topic, MUST be rejected. | `test_process_record_valid_event_routing`, `test_event_type_on_wrong_topic_is_rejected`, `test_process_record_unmapped_topic` |
| ING-02 | Every record MUST be validated against its contract using the draft the contracts declare (2020-12), with `format` enforced (e.g. `uuid`). An invalid record MUST be rejected with a reason starting `Schema validation error`. | `test_supplied_examples_pass_real_contracts`, `test_real_contract_rejects_broken_events` (12 cases), `test_validator_success`, `test_validator_missing_required_fields`, `test_validator_unknown_event_type`, `test_validator_initialization_non_existent_path` (without contracts, every event is rejected rather than accepted unchecked), `test_process_record_failed_validation` |
| ING-03 | No single message may raise out of record processing. These MUST each become a reject row: an empty value, bytes that aren't valid UTF-8, invalid JSON, JSON that isn't an object, and a timestamp that matches the pattern but isn't a real date. | `test_empty_value_is_rejected`, `test_non_utf8_value_is_rejected_losslessly`, `test_process_record_malformed_json`, `test_json_that_is_not_an_object_is_rejected`, `test_impossible_timestamp_is_rejected_not_crashing_flush` |
| ING-04 | A reject row MUST keep the exact original bytes (`raw_value`), a readable UTF-8 copy (`raw_payload`), the Kafka topic, partition, offset, timestamp and key, a short `rejection_reason`, and `rejected_at`. | `test_non_utf8_value_is_rejected_losslessly`, `test_rejected_schema_structure` |
| ING-05 | An accepted row MUST keep every contract envelope field, the typed payload fields, and lineage: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_kafka_key`, `_ingested_at`. | `test_process_record_valid_event_routing`, `test_accepted_event_carries_kafka_key_and_ingest_time`, `test_common_fields_in_event_schemas`, `test_event_schemas_coverage`, `test_pyarrow_table_construction_with_schema` |
| ING-06 | Timestamps MUST be stored as UTC `timestamptz` in microseconds. Epoch-millisecond values (Kafka timestamps, ingest time) MUST be converted explicitly, never read as microseconds. | `test_millisecond_kafka_times_are_not_misread_as_microseconds` |

### Delivery and failures

| ID | Requirement | Verified by |
| --- | --- | --- |
| ING-10 | Kafka offsets MUST be committed only after every buffered row has been written to Iceberg. | `test_offsets_are_committed_only_after_a_successful_write` |
| ING-11 | A write MUST start when `BATCH_SIZE` records are buffered, or when `FLUSH_INTERVAL_SEC` has passed with at least one record buffered. | `test_batch_size_triggers_a_write_before_the_interval`, `test_offsets_are_committed_only_after_a_successful_write` |
| ING-12 | If a write fails, consumption MUST pause (once, not on every retry) and no offsets may be committed. Retries MUST wait 5 → 10 → 20 → 40 → 60 s, capped at 60 s. The first successful write MUST resume consumption. | `test_failed_write_pauses_consumption_and_never_commits`, `test_retry_delay_doubles_and_is_capped`, `test_retries_back_off_and_resume_after_recovery` |
| ING-13 | If one table's write fails after others succeeded, the succeeded tables' buffers MUST already be cleared, so the retry writes only what's left and doesn't duplicate rows. | `test_flush_partial_failure_keeps_only_uncommitted_buffers`, `test_flush_appends_to_catalog_and_clears_buffer` |
| ING-14 | A failed offset commit MUST be logged and MUST NOT stop the service. The rows are already written and may be written again after a restart. "Nothing to commit" is not an error. | `test_failed_commit_is_survivable_and_logged`, `test_commit_with_nothing_to_commit_is_not_an_error` |
| ING-15 | On SIGTERM or SIGINT the service MUST write what's buffered, commit, and close. If that final write fails, it MUST NOT commit, so those records are read again after the restart. | `test_shutdown_flushes_commits_and_closes`, `test_shutdown_with_failed_final_write_does_not_commit` |
| ING-16 | Kafka error messages (for example end-of-partition or transport errors) MUST NOT be processed as records. | `test_kafka_errors_are_not_processed_as_records` |
| ING-17 | Delivery is **at-least-once**. Rows may be duplicated, both by the source and by our replays, and the Kafka lineage columns MUST make the two distinguishable. | Live: [`sql/checks/duplicate_origin.sql`](../sql/checks/duplicate_origin.sql) and `lakehouse_quality` split duplicates into replays and source resends (1,000 replays observed and explained on 2026-09-29) |
| ING-18 | Rebalances MUST NOT cause avoidable duplicates. When partitions are revoked, buffered rows MUST be written and committed while still owned; if that write fails, the buffer MUST be discarded, not written later. When partitions are lost, the buffer MUST be discarded. Partitions assigned while writes are failing MUST be paused too. | `test_revoke_writes_and_commits_while_we_still_own_the_partitions`, `test_revoke_with_failed_write_discards_buffer_and_never_commits`, `test_revoke_with_nothing_buffered_does_nothing`, `test_lost_partitions_discard_the_buffer_without_writing`, `test_partitions_assigned_while_paused_are_paused_too`, `test_partitions_assigned_normally_are_not_paused`, `test_loop_and_callbacks_share_the_paused_state` |
| ING-19 | Skipping data MUST NOT be silent. On assignment, if a partition's committed offset is older than the oldest offset Kafka still retains, the service MUST log an ERROR starting `DATA LOSS` with the partition and the number of deleted records, and keep a running total. A first start with no committed offset is not data loss. | `test_offsets_deleted_by_retention_are_reported_as_data_loss`, `test_first_start_without_committed_offsets_is_not_data_loss`; live: restarts on 2026-09-30 raised no false alarm |

### Tables

| ID | Requirement | Verified by |
| --- | --- | --- |
| ING-20 | On startup the service MUST create the `solution` namespace and all six tables if they're missing, retrying with backoff until the catalog is reachable, before it starts consuming. | `test_ensure_tables_creates_namespace_and_all_tables`; the retry is covered by review of `ensure_tables_with_retry` in `main.py` |
| ING-21 | Updating a table's definition MUST only add things: add missing columns (never drop or retype), add day partitioning if it's missing, and set table properties that differ. A table that's already up to date MUST be left unchanged. | `test_up_to_date_table_is_left_alone`, `test_existing_table_gets_only_what_is_missing`, `test_running_twice_changes_nothing_the_second_time` |
| ING-22 | Layout: event tables partitioned by `day(event_timestamp)`, `rejected_events` by `day(_kafka_timestamp)`, reference tables not partitioned. Parquet, zstd, 128 MiB target file size, old metadata files limited to 100. | `test_reference_tables_are_not_partitioned`, `test_existing_table_gets_only_what_is_missing`; review `TABLE_PROPERTIES` and `PARTITION_COLUMNS` |

## Live verification

- **2026-09-29:** redeployed. About 3,500 events were written in the first
  20 s, with no errors and no new rejects, so the uuid format check isn't
  rejecting valid events.
- **2026-09-30:** stopped with `./solution/ingestion/run.sh stop` (SIGTERM).
  It wrote the 393 buffered rows, committed, logged "Pipeline stopped
  cleanly" and exited with code 0 in about 1 s, well inside the 30 s grace
  period (ING-15). On restart it resumed without a `DATA LOSS` alarm (ING-19).
- **Continuous:** measured duplicate, gap and late rates match the simulator's
  configured anomaly rates ([sql/README.md](../sql/README.md)).

## Known limitations

- Duplicates are expected (ING-17). Reports deduplicate on `event_id`.
- Downtime longer than Kafka's 2-hour retention loses data. It's now
  detected, not prevented: a `DATA LOSS` error on restart (ING-19), and
  `lakehouse_quality`'s `stalled_beyond_kafka_retention` failure. Preventing
  it needs longer retention and lag alerting
  ([TRADEOFFS.md → Recovery gaps](../TRADEOFFS.md#where-ingestion-recovery-falls-short-and-what-to-add)).
- A hung process looks healthy to Docker; there's no health check yet.
- The contract README says an access-device health payload has exactly one
  technology field, but the schema doesn't enforce it, so events with two pass
  validation (`test_contract_does_not_enforce_one_technology_health_field`
  records this). The rule depends on the inventory, so it belongs in a
  data-quality check.
- There's one consumer process, and a write every 5 s creates many small files
  until `iceberg_maintenance` compacts them.
- Recovery gaps are covered in [TRADEOFFS.md → Recovery gaps](../TRADEOFFS.md#where-ingestion-recovery-falls-short-and-what-to-add).

## Sign-off

| Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- |
| | | | |
