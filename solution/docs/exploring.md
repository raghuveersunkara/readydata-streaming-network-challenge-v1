# Exploring the pipeline with Trino

A hands-on tour for someone new to Trino and Iceberg: watch events stream in,
follow one from Kafka to its table row, join to reference data, and look at
Iceberg's internals. Run every command from the repository root, with the stack
already up (see [README.md](../README.md#operating-the-solution)).

## 0. How the pieces fit together

```text
simulator ──MQTT──▶ bridge ──▶ Kafka topics ──▶ solution-ingestion ──▶ Iceberg tables
                                                (our Python consumer)       │
                                                                            │
                         Garage (S3-style storage): the actual Parquet data files
                         Nessie (catalog): "table X's current metadata is file Y"
                                                                            │
                                            Trino (SQL engine) ◀── you ─────┘
```

- **Iceberg** is a *table format*. A table is a set of Parquet data files plus
  metadata that lists which files make up the table right now. Every write
  creates a new **snapshot**, meaning a new version of that file list.
- **Nessie** is the *catalog*: the registry that points each table name at its
  current metadata.
- **Garage** stores the data files themselves (it behaves like Amazon S3).
- **Trino** is a SQL engine. It stores nothing itself. It reads the file list
  from Nessie, then reads the Parquet files from Garage.
- In Trino, names have three parts: `catalog.schema.table`, for example
  `iceberg.solution.throughput_events`.

## 1. Check the simulator's clock

The simulator sets `event_timestamp` as its start time plus an internal
counter (`time.monotonic()`) measuring how long it has run. That counter stops
while the Docker VM is paused, for example when the Mac sleeps. The simulator's
clock then falls behind real time by however long the pause lasted, and stays
behind. On 2026-09-28 a 35-minute freeze left every event about 2,033 seconds
behind.

Check the gap with the query in [step 4](#two-different-times). If it's more
than a few seconds, restart the simulator:

```bash
docker compose restart simulator
```

A restart gives the simulator new device sessions, and sequence numbers start
again from 1. The brief treats that as a normal source condition. Our pipeline
stores `event_timestamp` exactly as it arrives, so the jump stays visible in
the data. That's the kind of problem the planned event-time vs arrival-time
check in `lakehouse_quality` is meant to catch.

## 2. Open a Trino SQL prompt

```bash
docker compose exec trino trino --catalog iceberg --schema solution
```

This opens an interactive `trino>` prompt inside the Trino container. Setting
the catalog and schema up front lets you type `throughput_events` instead of
`iceberg.solution.throughput_events`.

- End every statement with `;`.
- Arrow keys give you history.
- `q` closes a long result; `quit` exits.
- To run a single query from your shell without the prompt:
  `docker compose exec trino trino --execute "SELECT ..."`.

The web UI at <http://localhost:8080> (type any username, no password) shows
running and finished queries. It isn't a SQL editor. If you'd prefer a
graphical SQL tool, DBeaver can connect to `localhost:8080` using its Trino
driver, with any username and no password.

## 3. Look around

```sql
SHOW CATALOGS;                        -- iceberg is ours; tpch/tpcds are built-in sample data
SHOW SCHEMAS FROM iceberg;            -- solution
SHOW TABLES;                          -- the 10 tables
DESCRIBE throughput_events;           -- columns and types
SHOW CREATE TABLE throughput_events;  -- also shows partitioning, file format and properties
```

In `DESCRIBE`, the contract fields come first, then the payload fields
(`download_mbps`, …), then the lineage columns that start with `_`. The
lineage columns record where each row came from in Kafka.

## 4. Watch data arrive

Use `_ingested_at` (when our consumer processed a row) to see what just
landed:

```sql
SELECT _ingested_at, event_timestamp, device_id, download_mbps, _kafka_partition, _kafka_offset
FROM throughput_events
WHERE event_timestamp > current_timestamp - INTERVAL '1' DAY   -- limits the scan to recent daily partitions
ORDER BY _ingested_at DESC
LIMIT 10;
```

Run it twice a minute apart and you'll see new rows. To watch counts grow
from a second terminal:

```bash
while true; do docker compose exec -T trino trino --execute "SELECT count(*) FROM iceberg.solution.connectivity_state_events"; sleep 15; done
```

### Two different times

`event_timestamp` is when the device measured something. `_kafka_timestamp` is
when it reached Kafka, and `_ingested_at` is when we processed it. Reports
should use `event_timestamp`. The gap between the first two should be a few
milliseconds:

```sql
SELECT date_trunc('minute', _kafka_timestamp) AS arrived,
       round(avg(to_milliseconds(_kafka_timestamp - event_timestamp)) / 1000.0, 1) AS avg_delay_seconds
FROM throughput_events
WHERE event_timestamp > current_timestamp - INTERVAL '1' DAY
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
```

A steady gap of minutes means the simulator clock drifted (see
[step 1](#1-check-the-simulators-clock)).

## 5. Follow one event from Kafka to the table

Pick a row and note its `_kafka_partition` and `_kafka_offset`:

```sql
SELECT event_id, _kafka_topic, _kafka_partition, _kafka_offset
FROM connectivity_events
WHERE event_timestamp > current_timestamp - INTERVAL '1' HOUR
LIMIT 1;
```

Then read that exact record straight from Kafka in your shell. Replace `<P>`
and `<O>` with the partition and offset from the query:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic network.connectivity --partition <P> --offset <O> --max-messages 1
```

You'll see the raw JSON that became your row. That's the lineage from the
source message to the table. Kafka keeps about two hours of data, so pick a
recent row.

## 6. Join events to the reference data

Measured speed compared with advertised speed, by technology:

```sql
SELECT d.technology,
       count(*)                                  AS tests,
       round(avg(t.download_mbps), 1)            AS avg_download,
       round(avg(d.advertised_download_mbps), 1) AS advertised
FROM throughput_events t
JOIN devices d ON t.device_id = d.device_id
WHERE t.event_timestamp > current_timestamp - INTERVAL '1' HOUR
GROUP BY 1 ORDER BY 1;
```

On the first run, fiber averaged 520 Mbps against 565 advertised, and
satellite 63 against 97. From there you can join further:

- `devices` → `sites` on `site_id`
- `sites` → `census_block_groups` on `block_group_geoid`
- `device_dependencies` links each device to the device upstream of it, ending
  at the IXP.

## 7. Look at Iceberg's internals

Iceberg exposes its metadata as extra tables. The names contain `$`, so they
must be in double quotes:

```sql
SELECT * FROM "throughput_events$partitions";         -- rows and files per day
SELECT spec_id, record_count, file_size_in_bytes, file_path
FROM "throughput_events$files" LIMIT 5;               -- the Parquet files in Garage (s3://warehouse/...)
SELECT * FROM "throughput_events$snapshots";          -- the current version
SELECT * FROM "throughput_events$properties";         -- zstd, target file size, ...
```

What to notice:

- **`$partitions`** may have a row with `event_timestamp_day = NULL`. That's
  data written before day partitioning was added.
  It disappears once `iceberg_maintenance` compacts the table.
- **`$files`** shows the small-file problem: roughly 40 KB files against a
  128 MB target. That's what `iceberg_maintenance` fixes.
- **`$snapshots`** shows only the latest snapshot, because Nessie keeps the
  history itself.
  Run it twice a minute apart: the `snapshot_id` changes as ingestion commits
  new data.

## 8. Rejected records

```sql
SELECT rejection_reason, count(*) FROM rejected_events GROUP BY 1 ORDER BY 2 DESC;
SELECT _kafka_topic, _kafka_offset, rejection_reason, raw_payload FROM rejected_events LIMIT 5;
```

`raw_payload` is readable text. `raw_value` holds the exact original bytes.

## Tips

- **Always filter on `event_timestamp`.** It lets Trino skip whole days of
  files. Add `LIMIT` when exploring.
- **Big full-table scans are slow until compaction.** Before
  `iceberg_maintenance` runs in apply mode, the big event tables have
  thousands of tiny files, and a heavy query here once coincided with Docker
  trouble. Start with the small tables, or compact first.
- **Deduplicate when counting.** About 0.2% of events arrive twice from the
  source ([TRADEOFFS.md → Delivery is at-least-once](../TRADEOFFS.md#delivery-is-at-least-once-so-tables-can-contain-duplicates)).
  For counts, use `count(DISTINCT event_id)`.
- **All times are UTC.**
