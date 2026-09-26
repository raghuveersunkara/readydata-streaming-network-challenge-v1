# Telemetry event contracts

These JSON Schema Draft 2020-12 files define the supplied source-event
interface that candidates receive at the Kafka boundary. V1 establishes
the common envelope and five active event families:

- `common-envelope.v1.schema.json` — identifiers, event time, device session,
  sequence, and payload container shared by event families.
- `connectivity-metric.v1.schema.json` — latency, jitter, and packet-loss
  measurements published by access devices.
- `throughput-test.v1.schema.json` — download and upload measurements published
  by access devices.
- `device-health.v1.schema.json` — common health values for all devices and a
  sparse technology signal for access devices.
- `connectivity-state.v1.schema.json` — access-device transitions among online,
  degraded, and offline states.
- `infrastructure-event.v1.schema.json` — shared-infrastructure incident kind,
  severity, and correlation identifier.
- `examples/` — valid examples for every family and health-payload variant.

`sequence_number` starts at one and increases within
`(device_id, device_session_id, event_type)`. A simulator restart creates a new
device session, so a sequence reset is not itself a missing or out-of-order
record. `event_timestamp` is measurement time in UTC. The candidate solution
defines any additional lineage or processing fields it needs.

`packet_loss_pct` and `signal_quality_pct` are percentages from 0 through 100,
not fractions. An access-device health payload contains exactly one of
`optical_rx_power_dbm`, `snr_db`, `signal_strength_dbm`, or
`signal_quality_pct`, according to its inventory technology. Infrastructure
health contains none of those sparse fields. Provider, technology, model,
role, advertised speed, and geography remain in the supplied inventory and are
intentionally not repeated in every event.

The supplied bridge preserves JSON bytes and identifiers while forwarding all
five event families to their documented Kafka topics. State records use the
affected access device as their key, while infrastructure records use the
source router; the dependency inventory supplies downstream relationships.
The schemas describe valid source records. Not every source delivery is
guaranteed to satisfy a schema, and the at-least-once transport can repeat a
logical event.
