# Analytical SQL Report Inventory

Read-only Trino reports over `iceberg.solution`. Each file contains one query,
has stable unique column names, documents its grain and assumptions in a
header comment, and orders its results deterministically. The perspective
numbers match the list in the [challenge README](../../README.md#3-analytical-sql).

## Reports

| File | Perspective | Grain | Purpose |
| --- | --- | --- | --- |
| [01_accepted_event_totals.sql](01_accepted_event_totals.sql) | 1. Accepted event totals by event family and inventory dimensions | Event type × provider × technology × region × infrastructure role, 6 h before the newest measurement | Duplicate-safe event counts per inventory slice |
| [02_reconciliation.sql](02_reconciliation.sql) | 2. Reconciliation of duplicates, sequence gaps, rejects, duplicate-safe counts | Kafka topic (= event family), 6 h before the newest Kafka arrival | Stored rows vs unique events, duplicate rows, missing sequence numbers, rejected rows |
| [03_device_statistics.sql](03_device_statistics.sql) | 3. Device connectivity and throughput statistics | Access device, last day | Min / median / p95 / max for latency, jitter and loss; min / p05 / median / max for download and upload |
| [04_technology_statistics.sql](04_technology_statistics.sql) | 4. The same statistics by access technology | Technology, last day | Report 3's statistics over all measurements in each technology |
| [05_underperforming_devices.sql](05_underperforming_devices.sql) | 5. Devices underperforming a peer group | Underperforming access device, last day | Device medians compared with their peer group's median |
| [06_provider_technology_cohorts.sql](06_provider_technology_cohorts.sql) | 6. Provider / technology cohort comparison | Provider × technology, last day | Download as % of advertised plan; latency and loss tails |
| [07_geographic_performance.sql](07_geographic_performance.sql) | 7. Geographic patterns with Census context | Region × inside Census urban area, last day | Performance with the median population density of the devices' block groups |
| [08_connectivity_state_availability.sql](08_connectivity_state_availability.sql) | 8. Connectivity-state duration and availability | Access device with at least one state transition, last day | Minutes online / degraded / offline, offline periods, availability % |
| [09_incident_impact.sql](09_incident_impact.sql) | 9. Shared incidents and downstream impact | Infrastructure incident, last day | Duration, impacted access devices and critical sites, devices that actually changed state |
| [10_late_records_by_measurement_time.sql](10_late_records_by_measurement_time.sql) | 10. Late records by measurement time | Measurement hour × event family, 6 h before the newest measurement | Records, late records, late %, maximum lateness |

The `checks/` folder holds data-quality checks, which are separate from the
reports. For example, [checks/duplicate_origin.sql](checks/duplicate_origin.sql)
splits duplicates into our pipeline's replays and the source's resends.

## Definitions, thresholds and peer groups

- **Duplicates.** Delivery is at-least-once, and about 0.25% of events arrive
  twice as identical copies ([TRADEOFFS.md → Delivery is at-least-once](../TRADEOFFS.md#delivery-is-at-least-once-so-tables-can-contain-duplicates)).
  - **Counts** are de-duplicated on `event_id` (reports 1 and 2). Report 1
    removes duplicates before joining the inventory, which keeps the memory
    Trino needs for the distinct count small.
  - **Statistics** (reports 3–7) don't deduplicate. A duplicate is identical,
    so min and max are unchanged, and medians and percentiles move far less
    than `approx_percentile`'s own error. Deduplicating would hold millions of
    event IDs in Trino's memory. Sample counts in those reports include
    duplicates.
- **Percentiles** use `approx_percentile`. The high tail (p95) is reported
  where high values are bad (latency, jitter, loss). The low tail (p05) is
  reported where low values are bad (throughput).
- **Download vs plan** (reports 6 and 7): `100 × download_mbps /
  advertised_download_mbps`. This makes cohorts with different plan mixes
  comparable.
- **Peer group** (report 5): same `technology` and same
  `advertised_download_mbps`. Comparing satellite with fiber, or a 300 Mbps
  plan with a 1000 Mbps plan, wouldn't be fair.
  - A device underperforms if its median download is below 70% of the peer
    median, or its median latency is above 1.5× the peer median.
  - Devices need at least 10 samples of each measure, and peer groups need at
    least 5 devices.
- **Sequence gaps** (report 2): holes inside each `(device, session)` stream's
  observed range. A simulator restart starts a new session, so its sequence
  reset isn't a gap. Rejected records also show up as holes.
- **Late** (report 10): when a record arrived, Kafka already held a record from
  the same device and family that was measured more than one reporting
  interval later. The intervals are 5 s for connectivity, 30 s for health,
  60 s for throughput, and 60 s for the event-driven families.
  - Both timestamps come from the source clock, so simulator clock lag cancels
    out.
  - The measured maximum lateness (90 s and 120 s) matches the simulator's
    configured late delay.
- **Availability** (report 8): the share of observed time not offline.
  Degraded still carries traffic.
  - Time is counted from a device's first transition, because its state
    before that is unknown.
  - The last interval is closed at the newest connectivity measurement.
- **Downstream impact** (report 9): an access device is impacted if its
  `device_dependencies` chain (access → aggregation → backhaul → IXP, at most
  3 hops) passes through the incident's source device.
  - `devices_with_state_change` confirms real impact. It counts impacted
    devices that reported a state transition between the incident's start and
    5 minutes after recovery.

## Time windows

- Reports 3–9 cover the last day of `event_timestamp` relative to
  `current_timestamp`. The simulator clock can lag wall time by hours after
  the Docker VM pauses ([docs/exploring.md](../docs/exploring.md#1-check-the-simulators-clock)).
  In that case the window covers less recent source time. Restarting the
  simulator removes the lag.
- Reports 1 and 10 anchor a 6-hour window to the newest measurement in the
  data instead.
- Report 2 uses the 6 hours before the newest Kafka arrival. It filters the
  event tables and `rejected_events` on the same arrival clock, so
  `delivered = accepted + rejected` holds.
- Reports 1 and 2 are limited to 6 hours because an exact distinct count over
  all history (about 12 million events) exceeds Trino's per-query memory
  (about 600 MB with the 2 GB heap cap).

## Running a report

```bash
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm query -f /sql/03_device_statistics.sql
```

The `query` container mounts this folder at `/sql`. Every report runs in about
1–8 seconds once the event tables are compacted (`iceberg_maintenance`). Before
compaction, the heavier reports can exceed Trino's memory limit.
