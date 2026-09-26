# Event Contracts

This supplied, challenge-harness-owned area defines the machine-readable
interface between the telemetry source system and solution ingestion at the
Kafka boundary.

`events/` contains versioned JSON Schema files for the common event envelope
and event-specific payloads. The V1 contracts define `connectivity_metric`,
`throughput_test`, `device_health`, `connectivity_state`, and
`infrastructure_event` plus representative payloads for each supported shape.
The supplied bridge preserves these records byte-for-byte when forwarding them
to Kafka with `device_id` as the record key.

Generator implementation, runtime telemetry, Iceberg table definitions,
candidate modeling decisions, and solution code do not belong here. Contracts
describe source inputs without prescribing how candidates store or process
them. The schemas define valid records; not every source delivery is guaranteed
to satisfy them.
