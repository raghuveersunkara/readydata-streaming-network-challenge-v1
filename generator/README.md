# Generator

This supplied, challenge-harness-owned area contains the runtime source-data
system:

- `simulator/` — deterministic simulated network devices and MQTT publishing.
- `mqtt-kafka-bridge/` — minimal forwarding from device-oriented MQTT topics to event-oriented Kafka topics.

Solution ingestion, normalization, Iceberg writes, and analytical SQL do not
belong here. Candidates should not normally need to modify this area. The
simulator publishes periodic telemetry,
sparse network-control events, and intentional source imperfections; the bridge
forwards every payload unchanged to keyed Kafka source records.

Candidates and the simulator consume the canonical files in `reference-data/`;
normal challenge startup requires neither GIS tooling nor network access.
