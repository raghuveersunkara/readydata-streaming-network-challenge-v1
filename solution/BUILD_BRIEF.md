# Solution: Builder's Challenge

I want to build the first version of the data foundation project. Steps include 
1. consuming data from kafka telemetry topics and ingest (validate before ingest) into iceberg tables
2. Generate SQL reports for analytical processing of this consumed data
3. Build 3 simple airflow dags for based on #1 and #2 above.

The project must be built based on the sub projects and phases listed below. 
Everything goes under `solution/` folder in the repo. The project brief is in [`../README.md`](../README.md), and the environment is described in [`../TECHNICAL_GUIDE.md`](../TECHNICAL_GUIDE.md).

## Guardrails and rules that apply to every sub project

- All code, configuration and docs go under `solution/`. Don't edit any other files. `solution/docker-compose.yml` should have all required services and should be able to run from docker containers
- The project must run with `docker compose -f docker-compose.yml -f solution/docker-compose.yml up -d --build`,  with no undocumented dependencies on the host. Tests must run inside respective containers.
- Pin every python dependency that is not a part of standard python package
- Take env settings from the supplied env.example file in the repo root:
  - Kafka: `kafka:29092`;
  - Nessie Iceberg REST catalog: `http://nessie:19120/iceberg` (the same as in
    `infrastructure/trino/catalog/iceberg.properties`);
  - Garage: `http://garage:3900`, region `garage`, keys from the
    `S3_ACCESS_KEY`/`S3_SECRET_KEY` defaults.
- No secrets should be needed beyond the local defaults
- Add unit tests for all files within each module. Make sure to use dummy data if needed.  
- To maintain code quality and easier unti testing, put code logic in plain modules that can be tested without additional service (like airfow or kafka), and
  pass dependencies in (consumer, catalog, clock, trino connector) so tests
  can use mocks.
- In iceberg tables, for timstamp fields use python's `timestamptz` which is in **microseconds**. Convert epoch millisecond values explicitly; never put integer milliseconds into a microsecond column in any of the tables.
---

## Sub project 1: streaming ingestion (`solution/ingestion`)

Create a Python service that consumes the five kafka topics under [`telemetry-and-kafka-behavior`](../TECHNICAL_GUIDE.md#telemetry-and-kafka-behavior) and writes to
iceberg using `pyiceberg` package (include the `pyiceberg-core` extra). Run it as an
always on compose service (`restart: unless-stopped`) (meaning not as an airflow dag).

### Phase 1: consume and validate

- Read the contracts in `contracts/events/` and their README file.
  - Validate with the contracts' declared draft (2020-12), and **turn on
    `format` checking**; otherwise `uuid` isn't enforced.
  - Contract files are hyphenated, but `event_type` values use underscores
    (`connectivity-metric.v1.schema.json` → `connectivity_metric`).
- Map each topic to exactly one `event_type` and table. Reject an event that
  arrives on the wrong topic.
- No single bad message should error out and stop the ingestion service. They should be rejected and added to rejection records. invalid data record includes 
    - an empty value
    - non UTF-8 bytes data bytes 
    - malformed json
    - contract failure
    - timestamp that matches the pattern but can't exist must each become a reject.
- Test with every file in `contracts/events/examples/` (all must pass), and
  with a set of broken events (each must be rejected).
- Document the topic to table mapping in `solution/README.md`.

### Phase 2: write to Iceberg

- Create the `iceberg.solution` namespace in iceberg and six tables on startup:
  `connectivity_events`, `throughput_events`, `device_health_events`,
  `connectivity_state_events`, `infrastructure_events` and `rejected_events`.
- Make sure the catalog is readable, so retry with backoff if needed (or you can follow best practice to reach the catalog).
- For event rows, keep the contract envelope, the typed metadata payload fields, and
  lineage: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`,
  `_kafka_timestamp`, `_kafka_key`, `_ingested_at`.
- For rejected rows:
  - retain the exact original bytes (`raw_value`, binary) and a readable UTF-8 copy
    (`raw_payload`);
  - the kafka topic, partition, offset, timestamp and key;
  - a short reason, and timestamp of when the record was rejected

- For better oganization, the layout of tables should follow these guideleines
  - event tables partitioned by `day(event_timestamp)`, and rejects by    `day(_kafka_timestamp)`;
  - parquet, zstd, 128 MiB target file size, and a limit on old metadata
    files since the first verison of the project has to run locally.
  - table setup must be append-only and should be safe to rerun. Add a new column if needed or missing.
- Test by running the stack, then querying the tables through trino python connector (row
  counts, lineage columns, rejects).
- Document the tables, columns and physical layout, with reasons, in
  `solution/README.md`.

### Phase 3: delivery guarantees and failures

- Buffer records, and write once `BATCH_SIZE` (default=500) records are buffered or
  `FLUSH_INTERVAL_SEC` (5 s) has passed. Commit kafka offsets only after the
  ingestion succeeds (at-least-once).
- If a write fails:
  - use backpressure, pause consumption and retry after incremental delays in 5, 10, 20, 40, 60s, capped at 60s, and resume after the first successful write.
  - don't pause polling though, we need the kafka group to be live. Pause fetching data but keep polling even when paused. 
- If one table write succeeds and another fails, clear the buffer of the
  successful one. This is to make sure the retry doesn't duplicate its rows.
- Assume a failed offset commit is a warning (and not an error)
- For unit tests, put the loop in its own module and test it with a mock consumer, writer and
  clock. The tests should include checking the order of events
<!-- - Add where duplicates come from (the source vs
  our replays, told apart by Kafka offset), and when to move to exactly-once,
  in `gotchas.md`. -->

---

## Sub-project 2: reference data (`solution/ingestion`, one-shot `reference-loader` service)

### Phase 1: validate and load

- Load `sites`, `devices`, `device_dependencies` and `census_block_groups` from
  `reference-data/`.
  - Use explicit types; geo_ids (or equivalent) stay strings, so their leading zeros are kept.
- Validate everything before writing anything:
  - headers and sha256 checksums against each `manifest.json` undre reference-data;
  - row counts, including the access/infrastructure device and
    service/network site splits;
  - keys should be unique with no rogue links
  - value rules should incldue coordinates inside Arkansas, advertised speeds only on access
    devices, one device per service site
  - for topology, every device's upstream chain reaches an IXP without cycles.
- each table is written with one atomic overwrite, and skipped when its source
  checksum and loader version haven't changed 
- Exit codes: 
  - 0 = loaded or current
  - 1 = failed validaton and no write happened
  - 2 = storage down (after exhausting retries)
- Test with a small mocked inventory data that each test breaks in one way, and
  with the real files.
- document the validation rules and idempotency in `solution/README.md`.

---

## Sub-project 3: analytical SQL (`solution/sql`)

### Phase 1: reports

- I have added 5 SQL files under `sql/` folder, add the remaining ones absed on the challenge readme. Similar to existing 5 SQL files that I've created, add one read-only query per file (`NN_name.sql`) for each of the remainnig 5 perspectives in the brief. Each file has:
  - a header comment stating the report name, grain, and if there are any assumptions made for that report
  - the final query should have unique column names;
  - a final `ORDER BY` (add all column names for safety but use discretion if needed).
- Don't add any hard coded identifiers or dates, all time windows must be relative (so that they run when the project is bootstrapped).
- Duplicates:
  - counts deduplicate on `event_id` (I've added this in report 1)
  - statistics may include duplicates
  - deduplicate join keys befpre any joins.
- test each report against real data from iceberg tables, and check the results against the
  simulator's configured anomaly rates (`generator/simulator/config.json`).
- Document the list of reports, definitions, thresholds, peer groups and
  windows in `solution/sql/README.md`.

---

## Sub-project 4: Airflow workflows (`solution/airflow`)

Put the DAG utils code in `solution/airflow/dags/lakehouse_ops/` (no airflow dependencies or
imports) and keep the DAG files for just orchestration. Assume all 3 dags are independent and manually triggered. The tasks read Iceberg through trino
using the supplied `TRINO_*` cofnig variables. Add `trino` client
to `solution/airflow/Dockerfile`.

### Phase 1: manual_sql_to_csv

- Allow only a single report for selection
- Stream rows in batches to disk. csv format with UTF-8 encoding, a header row, standard
  quoting, NULL as an empty field.
- Write to a hidden `.part` file (or other standard setup), and publish with an atomic link that never overwrites. This is to make sure file names are unique per run and per attempt. Ideally this is one of the ways to ensure safe handling of collisions, reruns, and failures.
- Write a JSON file next to the CSV (rows, bytes, SHA-256 and other metadata), and a `verify`
  task that re-reads the CSV and checks it.
- Test the failure cases, a failure partway must leave no file behind
- Document the csv formatting and the output location.

### Phase 2: iceberg_maintenance

- Plan mode, the default, reads `$files` only and changes nothing.
- Apply mode compacts a table only when it's required (at least N small
  files) and bounded (their total size under a specific limit), using Trino
  `optimize(file_size_threshold)`. Tables should compact one at a time.
- Verify each compaction kept every row. Nessie exposes only the
  current snapshot, so as a fail safe strategy, fall back to "rows after ≥ rows before" when an append competes during compaction as a race condition.
- No snapshot expiry and no orphan-file removal is required.
- Test the plan logic with unit tests. Test on the live data, plan first, then apply.
- Document the parameters, rerun and failure behaviour, and if any potential
  limitations in `solution/README.md`.



### Phase 3: lakehouse_quality

- Each check is one SQL query returning a number, compared with a threshold,
  and is either failure or a warning (fail or warn)
- For reference checks: counts against the manifests, duplicate keys, orphans, topology.
- Event checks, over a window by arrival time: unknown devices, wrong
  site, replay duplicates, source duplicate rate, reject rate, freshness, a
  stall longer than Kafka's retention, clock skew, silent devices.
- The run fails on any failure, or when a check group didn't run. Warnings
  must never fail the run.
- Test the pass/fail logic with unit tests. Run it on the live data and
  investigate every warning.
- Document the checks and their severities.

---

## Sub-project 5: operability and review

### Phase 1: runtime

- Cap trino's heap in the overlay by mounting a `jvm.config` override with
  `-Xmx2G`. The supplied config lets the heap grow to 80% of the Docker VM,
  and the VM's OOM killer then kills it.
- Add a `query` tool container (python `trino` package) in a `tools` profile
- Add a `run.sh` to each module, plus a shared `solution/scripts/lib.sh`, that
  wrap the compose commands. They must work from any directory, and nothing
  that deletes data gets a shortcut
- Document the commands in `solution/README.md`, and write a hands-on tour in
  `solution/docs/exploring.md`.

### Phase 2: tests and specs

- Focused tests for the important logic and failure boundaries, including a
  test that every DAG file loads and a smoke test of every report against
  Trino (`LIMIT 0`).
- Generate a `SPEC.md` for each module, and an index in `solution/SPECS.md`.
  Each spec must have:
  - purpose and non goals
  - interfaces and configuration
  - numbered "must" requirements, each traced to the test or live check
    that proves it;
  - known limitations if any
  - a sign off checklist table.
- Document how to run the tests in `solution/README.md`.

---

## How to work

Start by writing a specification document, and let me iterate on it before
you start implementing. Ask clarifying questions when needed, with selectable
options to keep answers simple. Write the specification as `SPECS.md` at the
root of `solution/`, with a `SPEC.md` per module. Then implement each phase one by one. For each phase, add tests it, give me the run command to execute the service, and wait for me to sign-off on that phase's work before moving onto the next phase or sub project.

For each service, add a shell file to quick run that module (or solution package as a whole)