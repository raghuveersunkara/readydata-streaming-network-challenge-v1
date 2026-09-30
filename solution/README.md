# Solution

> **Reviewing?** Start with [SPECS.md](SPECS.md). Each module has a spec of
> numbered requirements, each traced to the test or live check that proves
> it, so it can be approved without reading the code. Trade-offs and incidents
> are in [TRADEOFFS.md](TRADEOFFS.md); a hands-on tour of the data is in
> [docs/exploring.md](docs/exploring.md).

**Contents:** [Architecture](#architecture) ·
[Operating the solution](#operating-the-solution) ·
[Delivery, rejection, replay and recovery](#delivery-rejection-replay-and-recovery) ·
[Iceberg tables](#iceberg-tables) · [Reference data](#reference-data) ·
[SQL reports](#sql-reports) · Airflow:
[`iceberg_maintenance`](#airflow-iceberg_maintenance),
[`manual_sql_to_csv`](#airflow-manual_sql_to_csv),
[`lakehouse_quality`](#airflow-lakehouse_quality) ·
[Trade-offs and production evolution](#trade-offs-and-production-evolution) ·
[Testing](#testing)

## Architecture

```text
 simulator ──MQTT──▶ bridge ──▶ Kafka (5 topics, keyed by device_id, ~2 h retention)
  (supplied)          (supplied)        │
                                        ▼
                         ┌─ solution-ingestion (always on) ──────────────────────┐
                         │ validate against contracts → buffer → write → commit   │
                         │ bad messages → rejected_events; lineage on every row   │
                         └───────────────────────┬───────────────────────────────┘
 reference-data/ ─▶ reference-loader (one-shot) ─┤ writes Iceberg tables
                                                 ▼
          Nessie (catalog: table → current metadata)   Garage (S3: Parquet files)
                                                 ▲
                                   Trino (SQL; reads both)
                                     ▲                ▲
                     solution/sql reports      Airflow DAGs (manual): export CSV,
                     (query tool, run.sh)      quality checks, maintenance
```

### Components and responsibilities

| Component | Runs as | Responsibility | Spec |
| --- | --- | --- | --- |
| `solution-ingestion` | Always-on Compose service (`restart: unless-stopped`, 30 s stop grace) | Consume the 5 topics; validate each record against its contract; write accepted rows to 5 event tables and invalid ones to `rejected_events`; commit Kafka offsets only after a successful write | [ingestion/SPEC.md](ingestion/SPEC.md) |
| `reference-loader` | One-shot Compose service (`restart: "no"`) | Validate the 4 reference files as a whole, then load `sites`, `devices`, `device_dependencies`, `census_block_groups` | [ingestion/REFERENCE_LOADER_SPEC.md](ingestion/REFERENCE_LOADER_SPEC.md) |
| Iceberg tables | Parquet in Garage, catalog in Nessie | 10 tables in `iceberg.solution`; the layout is described [below](#physical-layout-and-table-properties) | [ingestion/SPEC.md](ingestion/SPEC.md) |
| SQL reports | `solution/sql/*.sql`, run through Trino | Ten read-only reports, one per analytical perspective | [sql/SPEC.md](sql/SPEC.md) |
| Airflow DAGs | Supplied Airflow, DAGs in `solution/airflow/dags` | `manual_sql_to_csv`, `lakehouse_quality`, `iceberg_maintenance` | [airflow/SPEC.md](airflow/SPEC.md) |
| Runtime overlay | `solution/docker-compose.yml` | Adds the services above, caps Trino's heap at 2 GB, adds the on-demand `query` tool | [SPECS.md → runtime overlay](SPECS.md#spec-runtime-overlay-solutiondocker-composeyml) |

### Tracing one event

1. A device's connectivity measurement leaves the simulator on MQTT topic
   `network/<site>/<device>/connectivity`. The bridge forwards the exact
   bytes to Kafka topic `network.connectivity`, keyed by `device_id`, so all of
   one device's records land in the same partition, in order.
2. `solution-ingestion` polls it and validates it against
   `contracts/events/connectivity-metric.v1.schema.json`. It checks that
   `event_type` is `connectivity_metric`, the only type allowed on that topic,
   and maps it to a row with typed payload fields plus lineage:
   `_kafka_topic/partition/offset/timestamp/key` and `_ingested_at`. An
   invalid record becomes a `rejected_events` row with its exact bytes, Kafka
   coordinates and reason.
3. Rows are buffered and written when 500 accumulate or 5 s pass. The write is
   an Iceberg append: Parquet files in Garage, and a new snapshot registered in
   Nessie. **Only after that write succeeds** is the Kafka offset committed.
4. Trino reads the row immediately through the Iceberg catalog. Reports join
   it to `devices` → `sites` → `census_block_groups`, and the DAGs query,
   export, check and compact the tables through Trino.

### Failure boundaries

| If this fails… | What happens | Data lost? |
| --- | --- | --- |
| One bad message | It becomes a `rejected_events` row, and processing continues | No; the exact bytes are kept |
| Iceberg, Nessie or Garage | Ingestion pauses consumption, retries with delays growing from 5 s to 60 s, commits nothing, and resumes on success | No |
| The ingestion process (crash, kill, restart) | Docker restarts it; it resumes from the last committed offset | No, but rows written and not yet committed are written again (replay duplicates) |
| A Kafka stall or rebalance | Buffered rows are written and committed before the partitions are revoked, or discarded (to be re-read) if that isn't possible | No |
| Ingestion down for longer than Kafka's 2 h retention | Records deleted by Kafka are never read. Ingestion logs `DATA LOSS … N records` on restart; `lakehouse_quality` fails. | **Yes**, the only loss path; see [trade-offs → Recovery gaps](TRADEOFFS.md#where-ingestion-recovery-falls-short-and-what-to-add) |
| The reference files fail validation | The loader exits with code 1 and writes nothing | No; the previous tables stay |
| A DAG task | That DAG run fails. Ingestion and the other DAGs are unaffected. | No |
| A heavy query | Fails with a Trino memory error, instead of the VM killing Trino | No |

### Airflow's role

Airflow runs the **finite, manually triggered** work: CSV exports, quality
checks and table maintenance. **Ingestion does not run in Airflow**, because
the Kafka topics never end, and a scheduler of batch runs would add latency,
rebalances and a dependency on Airflow's uptime
([trade-offs → Ingestion does not run in Airflow](TRADEOFFS.md#ingestion-does-not-run-in-airflow)).
Airflow never starts, stops or feeds ingestion. It only reads and maintains
the tables ingestion writes, so either side keeps working while the other is
down.

## Operating the solution

All commands run from the repository root. The shortcut scripts
([below](#shortcut-scripts)) wrap the long `docker compose` commands.

### Build and start

```bash
./solution/run.sh up
```

This is the same as
`docker compose -f docker-compose.yml -f solution/docker-compose.yml up -d --build`.
It starts the supplied platform, builds and starts `solution-ingestion`, and
runs `reference-loader` once. Both wait for Kafka, Nessie and Garage to be
healthy. No other initialisation is needed: tables are created on first start.
Let the simulator and ingestion run for **at least 10 minutes** before judging
the analytical results.

### Observe

| What | Command |
| --- | --- |
| Container health | `./solution/run.sh status` |
| Ingestion progress (latest writes, warnings) | `./solution/ingestion/run.sh status` or `./solution/ingestion/run.sh logs` |
| Data quality (fail vs warn findings) | `./solution/airflow/run.sh quality` |
| Small-file state (read-only plan) | `./solution/airflow/run.sh maintenance plan` |
| Airflow UI | <http://localhost:8081> (`airflow` / `airflow`) |
| Trino UI (running queries) | <http://localhost:8080> (any user name) |

### Query

```bash
./solution/query/run.sh "SHOW TABLES"                    # any SQL
./solution/sql/run.sh list                               # the reports
./solution/sql/run.sh 04_technology_statistics            # run one report
./solution/query/run.sh                                  # interactive prompt
```

### Demonstration checklist

What the brief asks to see, and how to show each:

| Brief asks for | Command | What to look for |
| --- | --- | --- |
| All reference and event tables queryable | `./solution/query/run.sh "SHOW TABLES"` | 10 tables in `iceberg.solution` |
| Imperfect source data handled, with evidence | `./solution/sql/run.sh 02_reconciliation` and `./solution/sql/run.sh check duplicate_origin` | Rejects, duplicates (source vs replay) and sequence gaps per topic, while valid data keeps flowing |
| Useful analytics | `./solution/sql/run.sh all`, then any single report | Every report returns rows; see [sql/SPEC.md](sql/SPEC.md) for what each should show |
| The quality DAG on a healthy system | `./solution/airflow/run.sh quality` | `Summary: {'fail': 0, …}` |
| A safe maintenance plan | `./solution/airflow/run.sh maintenance plan` | A compact or skip decision per table, with reasons; nothing is changed |
| A host-visible CSV from a report | `./solution/airflow/run.sh export 01_accepted_event_totals.sql` | The printed `host_path` under `solution/exports/`, plus its `.json` with row count and SHA-256 |

**Windows and late data.** The simulator delivers late records up to 120 s
after measurement. Reports 3–9 cover the last day, reports 1 and 10 the 6
hours before the newest measurement, and report 2 the 6 hours before the newest
Kafka arrival ([sql/README.md → Time windows](sql/README.md#time-windows)). No
extra drain time is needed beyond the 10-minute warm-up. Late records land in
the partition of their own measurement day, and report 10 attributes them to
their measurement hour.

### Test

```bash
./solution/run.sh test        # both suites (91 ingestion + 71 Airflow tests)
```

See [Testing](#testing) for what each suite covers.

### Stop and reset

| Goal | Command |
| --- | --- |
| Stop ingestion only (writes and commits its buffer first) | `./solution/ingestion/run.sh stop`; start again with `start` |
| Stop everything, **keeping** all data | `./solution/run.sh down`; start again with `up` |
| **Full reset: deletes Kafka, Iceberg, Nessie and Airflow data** | `docker compose -f docker-compose.yml -f solution/docker-compose.yml down -v`, then `./solution/run.sh up` recreates the tables and reloads the reference data |
| Fix a lagging simulator clock (see [docs/exploring.md](docs/exploring.md#1-check-the-simulators-clock)) | `docker compose restart simulator` |

### Shortcut scripts

Each module has a `run.sh` that wraps the long compose commands. They work from
any directory; run one with no arguments to see its commands.

| Script | Commands |
| --- | --- |
| `./solution/run.sh` | `up` (same as the command above), `down` (keeps data), `status`, `logs [service]`, `test` (both suites) |
| `./solution/ingestion/run.sh` | `start`, `stop`, `restart` (after a code change), `status` (latest writes), `logs`, `test`, `load-reference` |
| `./solution/airflow/run.sh` | `list`, `export <report.sql>`, `quality [hours]`, `maintenance [plan\|apply] [table …]`, `trigger <dag> [json]`, `test` |
| `./solution/sql/run.sh` | `list`, `<report> [--csv]`, `check <name>`, `all` |
| `./solution/query/run.sh` | any SQL, `-f <file>`, `--csv`, no arguments for the prompt, `scratch` |

`airflow/run.sh export|quality|maintenance` runs the DAG immediately and prints
only its result, such as the CSV path and checksum, or the findings table. Use
`trigger` for a normal scheduled run that you can watch in the Airflow UI.
The shared compose command lives in
[`scripts/lib.sh`](scripts/lib.sh). For a full reset that **deletes all data**,
there's deliberately no shortcut:
`docker compose -f docker-compose.yml -f solution/docker-compose.yml down -v`.

### The equivalent compose commands

To re-run the reference load (it's idempotent) or run the tests without the
scripts:

```bash
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm reference-loader
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps ingestion python -m pytest -q tests
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps \
  -e PYTHONPATH=/opt/airflow/dags -v "$PWD/solution/airflow/tests:/opt/solution-airflow-tests:ro" \
  --entrypoint python airflow-scheduler -m pytest -q -p no:cacheprovider /opt/solution-airflow-tests
```

## Delivery, rejection, replay and recovery

- **Delivery is at-least-once.** Kafka offsets are committed only after the
  rows are written to Iceberg, so a crash between the two re-reads and
  rewrites those rows. Nothing is committed without being written.
- **Duplicates have two sources, told apart by lineage.** The source resends
  about 0.25% of events as identical copies (same `event_id`, different Kafka
  offset). Our replays store the same Kafka offset twice. Reports count on
  `event_id`, and `sql/checks/duplicate_origin.sql` and `lakehouse_quality`
  split the two. Exactly-once was considered and deferred, and the reasons are
  in [trade-offs → Delivery is at-least-once](TRADEOFFS.md#delivery-is-at-least-once-so-tables-can-contain-duplicates).
- **Rejection.** A record that is empty, isn't valid UTF-8 or JSON, fails its
  contract, arrives on the wrong topic, or carries an impossible timestamp is
  stored in `rejected_events` with its exact bytes, its Kafka coordinates and
  a reason. It never stops processing.
- **Interruption.** On `stop` or `restart` (SIGTERM), ingestion writes its
  buffer, commits, and exits, well inside its 30 s grace period. A crash
  resumes from the last committed offset.
- **Storage outage.** Consumption pauses (polling continues, so the consumer
  stays in its group). Writes are retried after 5 → 10 → 20 → 40 → 60 s, and
  consumption resumes after the first success.
- **Rebalances.** Before partitions are taken away, the buffer is written and
  committed. If that's impossible, it's discarded and re-read by the next
  owner, never written late.
- **The one loss path.** An outage longer than Kafka's ~2 h retention loses the
  records Kafka deletes meanwhile. It's detected (a `DATA LOSS` log on
  restart, and a failing `lakehouse_quality`), not prevented; see
  [trade-offs → Recovery gaps](TRADEOFFS.md#where-ingestion-recovery-falls-short-and-what-to-add).
- **Replay on purpose.** Resetting the consumer group's offsets re-reads
  retained data, for example after fixing a contract. The result is
  duplicates, which the reports tolerate, not a corrupted table.

## Iceberg tables

All tables are in the `iceberg.solution` namespace (Nessie REST catalog, data
in Garage).

| Kind | Table | Written by | Partitioning |
| --- | --- | --- | --- |
| Reference | `sites` | `reference-loader` (overwrite) | none |
| Reference | `devices` | `reference-loader` (overwrite) | none |
| Reference | `device_dependencies` | `reference-loader` (overwrite) | none |
| Reference | `census_block_groups` | `reference-loader` (overwrite) | none |
| Event | `connectivity_events` | `ingestion` (append) | `day(event_timestamp)` |
| Event | `throughput_events` | `ingestion` (append) | `day(event_timestamp)` |
| Event | `device_health_events` | `ingestion` (append) | `day(event_timestamp)` |
| Event | `connectivity_state_events` | `ingestion` (append) | `day(event_timestamp)` |
| Event | `infrastructure_events` | `ingestion` (append) | `day(event_timestamp)` |
| Invalid input | `rejected_events` | `ingestion` (append) | `day(_kafka_timestamp)` |

### Event table columns

Every event table has the same core columns, then its typed payload fields,
then lineage columns. Arrow schemas are in
[`ingestion/src/schemas.py`](ingestion/src/schemas.py).

| Group | Columns |
| --- | --- |
| Contract envelope | `schema_version`, `event_id`, `event_type`, `event_timestamp` (timestamptz, measurement time), `device_session_id`, `sequence_number`, `site_id`, `device_id` |
| Payload (typed) | For example `latency_ms`, `jitter_ms`, `packet_loss_pct` (double) in `connectivity_events`. Optional contract fields such as the technology-specific health metrics are NULL when absent. |
| Lineage | `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` (bridge arrival time), `_kafka_key`, `_ingested_at` (when the consumer processed the record) |

`(_kafka_topic, _kafka_partition, _kafka_offset)` identifies the exact source
record. Comparing it with `event_id` tells upstream duplicates apart from
replay duplicates (see [TRADEOFFS.md → Delivery is at-least-once](TRADEOFFS.md#delivery-is-at-least-once-so-tables-can-contain-duplicates)).

### `rejected_events`

| Column | Purpose |
| --- | --- |
| `raw_value` (varbinary) | Exact Kafka value bytes. This is the lossless copy. |
| `raw_payload` (varchar) | The same value decoded as UTF-8 for reading. Invalid bytes are replaced, so it's lossy only for input that isn't valid UTF-8. |
| `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_kafka_key` | Source coordinates for finding or replaying the record |
| `rejection_reason` | Short reason, e.g. `Invalid JSON: ...`, `Schema validation error: ...`, `event_type 'x' not allowed on topic y` |
| `rejected_at` | When the record was rejected |

A record is rejected, never dropped and never allowed to crash the consumer,
when:

- it is empty;
- it isn't valid UTF-8 or JSON, or the JSON isn't an object;
- it fails its JSON Schema contract;
- it arrives on an unmapped topic;
- its `event_type` doesn't match its topic;
- its timestamp matches the contract pattern but isn't a real date (e.g. month 13).

### Physical layout and table properties

| Setting | Value | Why |
| --- | --- | --- |
| Format version | 2 (pyiceberg default) | Supports row-level deletes, which later dedup/`MERGE` work needs |
| File format | Parquet (`write.format.default`) | Columnar, and what Trino reads fastest |
| Compression | zstd (`write.parquet.compression-codec`) | Better ratio than snappy at similar decode speed |
| Target file size | 128 MiB (`write.target-file-size-bytes`) | Target for compaction. Streaming flushes write much smaller files, which `iceberg_maintenance` compacts. |
| Metadata retention | `write.metadata.delete-after-commit.enabled=true`, `previous-versions-max=100` | Ingestion commits every few seconds. Without this, the old metadata files would pile up without limit. |
| Timestamps | `timestamptz`, microseconds | Iceberg's native precision. Epoch-millisecond Kafka times are converted explicitly, so they're never misread as microseconds. |

**Partitioning choices**

- Event tables are partitioned by `day(event_timestamp)`, i.e. measurement
  time, because that's how the reports filter. At the default rate the
  busiest table gets about 10M rows a day, so daily partitions are large
  enough to avoid lots of small files and small enough to prune well. Late
  events go into the partition for their own day.
- `rejected_events` is partitioned by `day(_kafka_timestamp)`, because a
  rejected record may have no usable `event_timestamp`.
- Reference tables are not partitioned. The largest has 2,294 rows.

**How tables are created and changed.** `ensure_table()` in
[`ingestion/src/tables.py`](ingestion/src/tables.py) runs every time a writer
starts, and every step it takes is additive and idempotent:

1. create the table if it's missing;
2. add any new columns (existing columns are never dropped or retyped);
3. add the day partition field using partition evolution;
4. re-apply the table properties if they've drifted.

This means schema and layout changes reach tables that already hold data
without deleting anything. Files written before partitioning was added stay
under the old unpartitioned spec until `iceberg_maintenance` rewrites them.

## Reference data

`reference-loader` ([`ingestion/src/reference.py`](ingestion/src/reference.py))
loads the four required files from `reference-data/` (mounted read-only):

| Table | Source | Rows | Key |
| --- | --- | ---: | --- |
| `census_block_groups` | `census/v1/census_block_groups.csv` | 2,294 | `block_group_geoid` |
| `sites` | `inventory/v1/sites.csv` | 623 | `site_id` |
| `devices` | `inventory/v1/devices.csv` | 654 | `device_id` |
| `device_dependencies` | `inventory/v1/device_dependencies.csv` | 650 | `downstream_device_id` |

Each row also has `_source_file`, `_source_sha256` and `_loaded_at`. Column
types are explicit:

- `block_group_geoid` stays a string, so its leading zeros are kept.
- Booleans, dates and doubles are parsed to their proper types.
- Empty provider and speed fields become NULL.

**Validation.** All files are checked before anything is written. Any failure
means nothing is written and the loader exits with code 1. The checks are:

- **File contract:** each header matches the documented columns, and each
  SHA-256 matches its `manifest.json`.
- **Counts:** total rows per file, plus the access/infrastructure device split
  and the service/network site split, all against the manifest counts.
- **Identifiers:** required fields are non-empty, primary keys are unique,
  GEOIDs are 12 digits starting with Arkansas FIPS `05`, coordinates fall
  inside Arkansas, and measures aren't negative.
- **Relationships:**
  - `sites.block_group_geoid` → `census_block_groups`;
  - `devices.site_id` → `sites`;
  - both ends of every dependency → `devices`.
- **Inventory rules:**
  - every service site has exactly one device;
  - access devices have a provider and advertised speeds, and infrastructure
    devices don't;
  - IXP devices have no upstream, and every other device has exactly one;
  - every device's upstream chain reaches an IXP without cycles.

**Idempotency.** Each table is replaced with one atomic Iceberg `overwrite`.
The snapshot records the source file checksum and `LOADER_VERSION`. A re-run
whose source and loader are unchanged skips the table, so repeated runs don't
create new snapshots. The four tables are committed one at a time. If the
loader is interrupted partway, re-running it fixes things, because tables
that are already current are skipped.

The loader does not check that events refer to known devices; that belongs to
the `lakehouse_quality` DAG. The optional GeoJSON and Census Place enrichment
is not loaded.

## Airflow: `iceberg_maintenance`

A manually triggered DAG (`schedule=None`, `max_active_runs=1`) that inspects
small-file behaviour and compacts only when that is justified and bounded.
Its tasks run in order: `plan` → `compact` → `report`.

| Param | Default | Meaning |
| --- | --- | --- |
| `mode` | `plan` | `plan` only reads metadata and changes nothing. `apply` compacts the tables the plan marks `compact`. |
| `tables` | all 10 | Which tables to consider. Checked against an allowlist before being put into SQL. |
| `small_file_threshold_mb` | 32 | Files below this size count as small, and only these are rewritten. |
| `min_small_files` | 20 | **Justified:** a table needs at least this many small files to be compacted. |
| `max_rewrite_mb` | 2048 | **Bounded:** a table is skipped if its small files add up to more than this. |

**What each task does**

- **`plan`** reads the `$files` metadata table for each selected table. It
  reports data files, small files and their size, and the partition specs
  present. It then decides `compact` or `skip` and gives the reason.
- **`compact`** runs `ALTER TABLE … EXECUTE optimize(file_size_threshold => …)`
  on one table at a time. It only runs when `mode=apply`.
- **`report`** runs even if `compact` failed. It shows file counts before and
  after.

**Why whole tables rather than single partitions.** Trino rejects a `WHERE` on
`event_timestamp` while files from before partitioning still exist.
So the size limit applies to each table's total small-file bytes instead.
Compaction also rewrites those older files under the current day partitioning.

**Verification.** Each compaction must keep every row. If the current snapshot
is still the `replace` that `optimize` just committed, its `added-records`
must equal its `deleted-records`. If ingestion has committed an append in the
meantime, the `replace` snapshot is no longer visible. In
that case the check falls back to "row count after ≥ row count before".

**Failure and reruns**

- Each `optimize` is a single atomic commit, so a failed table is left
  unchanged.
- All selected tables are attempted before the task fails, and the error names
  every table that failed.
- Reruns are safe. The plan is recomputed each time, so tables that were
  already compacted drop below `min_small_files` and are skipped.
- Compaction can run while ingestion is writing. If the two commits conflict,
  whichever one loses retries: `optimize` on the next DAG run, and ingestion
  through its flush retry.

**Out of scope.** Snapshot expiry and orphan-file removal delete data files,
and the brief rules out destructive cleanup. Compaction deletes nothing:
replaced files stay reachable from earlier Nessie commits.

First run (2026-09-28): the plan marked the six event and reject tables for
compaction and skipped the four reference tables (1 file each). `apply` on
`connectivity_state_events`, `infrastructure_events` and `rejected_events`
rewrote about 1,730 files into 6, and each table passed the `replace`-snapshot
check.

```bash
# plan only (no changes)
docker compose exec airflow-scheduler airflow dags trigger iceberg_maintenance --conf '{"mode":"plan"}'
# compact selected tables
docker compose exec airflow-scheduler airflow dags trigger iceberg_maintenance \
  --conf '{"mode":"apply","tables":["throughput_events"]}'
```

## Airflow: `manual_sql_to_csv`

Exports one checked-in report from `solution/sql/` to CSV. It's triggered
manually, and each run exports exactly one report.

| Param | Meaning |
| --- | --- |
| `report` | One of the numbered `NN_name.sql` files. The allowed list is read from the SQL folder when the DAG is parsed, and `checks/` is excluded. |

**Safety checks before running.** The report name must be in that list, which
also blocks path tricks. The SQL must be a single statement that starts with
`SELECT` or `WITH` and contains no write or DDL keywords.

**Output files.** The CSV is written to
`solution/exports/<report>/<report>__<run_id>__try<n>.csv`, with a `.json`
metadata file next to it.

- **Bounded memory.** Rows stream from Trino in batches of
  `READYDATA_EXPORT_FETCH_SIZE` (1,000) straight to disk. Only a small summary
  goes to XCom.
- **CSV format:**
  - UTF-8 without a BOM;
  - a header row with the query's column names, which must be unique;
  - comma-separated with CRLF line endings, quoting only where needed and
    doubling embedded quotes;
  - **NULL written as an empty field** (the reports never return empty
    strings);
  - ISO 8601 timestamps, plain-notation decimals, and `true`/`false`.
- **Collisions and reruns.** Every run and every retry attempt gets its own
  file name, and publishing never replaces an existing file. The file is
  written as a hidden `.part` file and then linked into place atomically, so a
  failed attempt leaves no CSV behind.
- **Metadata and verification.** The `.json` file records:
  - the report name and the SHA-256 of its SQL;
  - the run ID and attempt number;
  - start and finish times;
  - the path inside the container and the path on the host;
  - row count, byte size and the CSV's SHA-256;
  - the column names.

  The `verify` task re-reads the published file and checks it against that
  row count and checksum.

Exports are runtime artifacts and are git-ignored (`solution/exports/*`).

```bash
docker compose exec airflow-scheduler airflow dags trigger manual_sql_to_csv --conf '{"report":"09_incident_impact.sql"}'
```

## Airflow: `lakehouse_quality`

A manually triggered, read-only DAG. Each check is one SQL query that returns
a number, which is compared with a threshold. There are two severities:

- **fail**: the data can't be trusted. The run fails.
- **warn**: something to investigate. The run still succeeds.

The `summarize` task always runs. It prints every finding as a table, and the
same findings are available in XCom.

| Param | Default | Meaning |
| --- | --- | --- |
| `window_hours` | 6 | Event checks cover records that arrived in Kafka during the last N hours |

| Check group | Severity | Checks |
| --- | --- | --- |
| Reference completeness | fail | Row counts of `sites`, `devices`, `device_dependencies` and `census_block_groups` against the supplied `manifest.json` files |
| Reference relationships | fail | Duplicate keys; orphaned site→block group, device→site and dependency→device links; non-IXP devices with no upstream |
| Event consistency | fail | Events from devices not in the inventory; events whose `site_id` doesn't match their device's site; ingestion stalled for longer than Kafka's 2-hour retention |
| Event quality | warn | Replay duplicates above 0; source duplicates above 1% (about 0.25% is normal); rejects above 1% (about 0.02% is normal); no new arrivals for more than 15 minutes; median simulator clock skew above 5 minutes; inventory access devices with no events |

**First run (2026-09-29).** All reference checks passed, and there were no
events from unknown devices or with the wrong site. It raised three warnings,
each a real problem:

- **Ingestion looked stalled (101 minutes with no new data).** The cause was
  the supplied MQTT-to-Kafka bridge. It lost its Kafka connection during a
  memory-pressure episode and never reconnected, and restarting it fixed the
  problem.
- **1,000 replay duplicates.** Kafka stopped responding for about 45 s during
  the same episodes. Ingestion's offset commits failed, so the last written
  batches were re-read. This is the at-least-once behaviour from
  TRADEOFFS.md → Delivery is at-least-once.
- **Source clock skew of 525 minutes.** This is the known simulator clock lag.

```bash
docker compose exec airflow-scheduler airflow dags trigger lakehouse_quality --conf '{"window_hours":6}'
```

The three DAGs don't depend on each other. Each reads Iceberg through Trino
and runs on its own.

## SQL reports

Ten read-only reports in [`sql/`](sql/), one per analytical perspective in the
brief. The perspective mapping, grain, thresholds, peer groups and windows are
in [sql/README.md](sql/README.md), and what each report should show is in
[sql/SPEC.md](sql/SPEC.md). Run them with `./solution/sql/run.sh`, or export one
with `./solution/airflow/run.sh export <report.sql>`.

## Trade-offs and production evolution

The design choices (with the alternatives considered), assumptions,
limitations, and how the solution would evolve into a production-grade system
are in **[TRADEOFFS.md](TRADEOFFS.md)**.

**Dependencies:** Python 3.11 with `pyiceberg[s3fs,nessie,pyiceberg-core]`,
`confluent-kafka`, `pyarrow` and `jsonschema` (ingestion); the `trino` client in
the Airflow image and the query tool. All are pinned.

## Testing

There are two test suites, and both run inside the project's own containers,
so nothing needs to be installed on your machine. Run them from the
repository root:

```bash
# Ingestion and reference loading (91 tests)
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps ingestion python -m pytest -q tests

# Airflow DAGs and SQL reports (71 tests)
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps \
  -e PYTHONPATH=/opt/airflow/dags -v "$PWD/solution/airflow/tests:/opt/solution-airflow-tests:ro" \
  --entrypoint python airflow-scheduler -m pytest -q -p no:cacheprovider /opt/solution-airflow-tests
```

Most tests are pure unit tests. The logic that decides what happens on
failure lives in plain functions, and the Kafka consumer, the Iceberg catalog
and the Trino connection are passed in as arguments, so tests can replace them
with small fakes. The main example is `src/pipeline.py`, which is separated
from `main.py` for exactly that reason. A few tests use the real contract,
reference and SQL files, and they're skipped when those aren't mounted. The
SQL report tests also skip when Trino isn't running.

| Suite / file | What it protects | Failure boundaries covered |
| --- | --- | --- |
| `ingestion/tests/test_pipeline.py` | The rule that offsets are committed only after the write | Commit only after a successful Iceberg write; failed write → pause and never commit; retry delays 5→10→20→40→60 s (capped); resume after recovery; failed commit survives (replay, no crash); shutdown flushes and commits, or doesn't commit if that final write fails; Kafka error messages are never processed as records ; rebalances: write and commit before partitions are revoked, discard instead of writing late when that fails or partitions are lost, re-pause new partitions; offsets deleted by retention reported as `DATA LOSS` |
| `ingestion/tests/test_ingest.py` | Handling each record | Bad JSON; bytes that aren't valid UTF-8 (original bytes kept losslessly); JSON that isn't an object; empty messages; event on the wrong topic; impossible dates; a write that fails partway doesn't duplicate rows; epoch milliseconds never read as microseconds |
| `ingestion/tests/test_validator.py` | Contract validation | Every supplied example passes; 12 broken events are rejected by the real contracts (missing, wrong-type or extra fields; timestamp without milliseconds; values out of range; `schema_version`; uuid format; `device_id` pattern). Also documents that the "one technology field" health rule isn't in the schema. |
| `ingestion/tests/test_reference.py` | Reference-data checks | Types, and GEOIDs keeping their leading zero; checksum, header and count mismatches; duplicate keys; orphaned links; cycles; each inventory rule (Arkansas bounds, advertised speeds, one device per service site, IXPs as the only devices with no upstream, empty required values, GEOID format, negative values); unchanged sources are skipped; the real files pass |
| `ingestion/tests/test_load_reference.py` | Loader exit codes | Validation failure → exit 1 without opening the catalog; storage down → exit 2 after `MAX_ATTEMPTS`; a temporary error → retry, then exit 0 |
| `ingestion/tests/test_tables.py` | `ensure_table` is safe to re-run | Current tables are left alone; older tables get only the missing columns, partitioning and properties; running twice changes nothing; reference tables stay unpartitioned |
| `ingestion/tests/test_schemas.py` | Arrow schemas | Column sets and order; building a table with the schema |
| `airflow/tests/test_dag_integrity.py` | The DAG files | Every DAG file loads; the three required DAGs exist, are manual-only and don't backfill; the export dropdown equals the checked-in reports; maintenance defaults to plan mode |
| `airflow/tests/test_export.py` | `manual_sql_to_csv` | Only numbered reports can run (path tricks blocked); non-read-only or multi-statement SQL is rejected, and all 10 reports pass; CSV quoting, NULLs and CRLF; duplicate columns refused; existing exports never overwritten; a failure mid-stream leaves no CSV or `.part` file; `verify` catches a modified file |
| `airflow/tests/test_quality.py` | `lakehouse_quality` | Fail vs warn; a query error counts as a failure; warnings alone don't fail the run; a check group that didn't run fails it; manifest keys map to table names; window and table coverage of the SQL |
| `airflow/tests/test_maintenance.py` | `iceberg_maintenance` | Compact only when justified and within the size limit; table allowlist; only small files rewritten; both row-preservation checks |
| `airflow/tests/test_sql_reports.py` | The brief's report rules | At least 5 reports; each plans in Trino (`LIMIT 0`, no data read) with unique, named columns; final `ORDER BY`; header states the grain |

Beyond the unit tests, the pipeline was also tested end to end:

- running the DAGs (`airflow dags test`);
- the live duplicate-origin check (`sql/checks/duplicate_origin.sql`);
- checking late and duplicate rates against the simulator's configured
  anomaly rates (see `sql/README.md`).

**Not covered by automated tests:**

- `query/query.py` and `query/scratch.py`, which are developer tools;
- behaviour with the real Kafka, Nessie and Garage services, which was
  checked by running the stack instead.

## Where things are

```text
solution/
├── README.md, SPECS.md, TRADEOFFS.md, AI_USAGE.md, BUILD_BRIEF.md
├── run.sh                      # whole-stack shortcuts (up/down/status/logs/test)
├── docker-compose.yml          # overlay: ingestion, reference-loader, query tool, Trino heap cap
├── scripts/lib.sh              # shared compose helper for the run.sh scripts
├── ingestion/                  # one image, two entry points; SPEC.md + REFERENCE_LOADER_SPEC.md
│   ├── main.py                 #   long-running Kafka -> Iceberg consumer
│   ├── load_reference.py       #   one-shot reference-data loader
│   ├── src/                    #   pipeline, ingest, validator, models, schemas, tables, reference
│   └── tests/
├── airflow/                    # SPEC.md
│   ├── Dockerfile              #   adds the pinned trino client + pytest
│   ├── dags/                   #   the three DAGs
│   │   └── lakehouse_ops/      #   their logic (export, quality, maintenance, Trino client)
│   └── tests/
├── sql/                        # the ten reports, README.md, SPEC.md
│   └── checks/                 #   read-only data checks (duplicate_origin.sql)
├── query/                      # developer query tool (SPEC.md)
├── trino/jvm.config            # Trino heap cap
├── docs/exploring.md           # hands-on Trino/Iceberg tour
└── exports/                    # generated CSVs (git-ignored)
```
