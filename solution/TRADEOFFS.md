# Trade-offs and production evolution

The main design choices in this solution, what they cost, and how the system
would evolve into a production-grade one. Operating details are in
[README.md](README.md), and the requirements are in [SPECS.md](SPECS.md).

## Design choices

| Decision | Potential alternative  | Reason |
| --- | --- | --- |
| A custom Python consumer, always on | kafka connect iceberg sink, flink, (thought of spark) | simple to configure and run |
| At-least-once, deduplicate on read | Exactly-once writes (offset watermarks), `MERGE` on `event_id` | Most duplicates come from the source anyway, and I've added dedupelication in reports (whereever necesasry) |
| Ingestion outside airflow | Frequent micro-batch DAG runs | Airflow adds latency and rebalances, not a great choice to consume streams |
| Day partitions on measurement time | Hourly, or bucketed by device | Intuition purely based on the data volume, around 10M rows a day in the biggest table; late data lands in its own day |
| Write every 5 s or 500 rows | Larger, less frequent commits | Low latency, at the cost of small files, which `iceberg_maintenance` compacts|
| Lateness measured on the source clock | Arrival time minus event time | The simulator clock lags wall time by hours after docker VM pauses |
| Trino heap capped at 2 GB | The supplied 80% of VM memory | It was repeatedly killed by the VM's OOM killer on my machine|

## Assumptions

- `event_timestamp` is the measurement time. The simulator's clock can lag
  wall time, and a restart re-syncs it.
- A duplicate is an identical copy with the same `event_id`.
- The reference inventory is a static snapshot. Each load replaces it, with no
  history kept.
- Every device's records share one Kafka partition (keyed by `device_id`), so
  offset order is arrival order per device.
- The Docker VM has about 8 GB of memory.

---

## Main trade offs

### Delivery is at-least-once, so tables can contain duplicates

Offsets are committed only after the iceberg ingesstion to make sure no record is lost. But
a crash between the write and the commit writes rows again. Duplicates have
two sources, which the lineage columns tell apart:

- producer payload has the same `event_id` at a different kafka offset. In the data, around 0.25% of events have these duplicated events
- the same kafka offset stored twice, after a crash between write commit and offset commit.

Reports count on `event_id`. `sql/checks/duplicate_origin.sql` and
`lakehouse_quality` measure both kinds. On live data every duplicate was a
source resend, until the Kafka stalls of 2026-09-29 produced 1,000 replays.

We can make the *pipeline* exactly-once (the offset watermark, or
Flink) once replays become common. Make the *data* exactly-once (`MERGE` or a
scheduled job) after downstream reads raw tables without
deduplicating.

### Ingestion does not run in Airflow

Kafka topics never end, and Airflow schedules finite tasks. A DAG that
never finishes breaks retries and timeouts. Micro-batch runs add latency, a
rebalance on every run, and a dependency on Airflow being up. So ingestion is
an always-on service, and Airflow only runs the finite DAGs (export, quality,
maintenance). Either side keeps working while the other is down.

These gaps in the ingestion needs to be handled in production 

- More than 2hours of downtime loses data. Kafka deletes records before
   they're read. It's detected by a `DATA LOSS` log on restart and a failing
   quality check, but not prevented. The fix is alerting when lag grows along with kafka retention.
- If there are several writes to iceberg tables, it would result in conflict commits. Production needs a better coordinated writes


---

### Pluggable destinations (dependency injection for destination dbs)

Ingestion logic is coded for iceberg only. The consume loop already receives its
dependencies as arguments. The remaining step is to split `IcebergIngest`
into a destination-neutral record processor, and a small `Sink` interface
(`ensure_tables`, `write`, `close`) with one implementation per destination
(Iceberg, PostgreSQL, etc). `main.py` would then pick the sink from
configuration.



### Scaling the system

Several ways (or several areas to optimize) to scale. Bounding the timewindows in reports could help with large running queries. May be pre-aggregated data would help. 

Adding kafka replication to make sure the data loss is minimal in the event of kafka broker dying. 

