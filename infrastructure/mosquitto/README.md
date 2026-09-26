# Mosquitto

Mosquitto exposes an anonymous plaintext MQTT listener on localhost:1883 for
local challenge traffic. Persistence is disabled because Kafka is the durable
candidate-facing event boundary.

This configuration is for local development only and is not a production
security model.

The supplied simulator publishes non-retained QoS 1 events to:

```text
network/{site_id}/{device_id}/connectivity
network/{site_id}/{device_id}/throughput
network/{site_id}/{device_id}/health
network/{site_id}/{device_id}/state
network/{site_id}/{device_id}/infrastructure
```

The supplied bridge subscribes to each corresponding `network/+/+/{family}`
route. Each `+` is an MQTT single-level wildcard matching one site or device
identifier.

The bridge uses a fixed persistent MQTT session. Mosquitto buffers at most
10,000 queued messages for a short bridge interruption while the broker remains
running. Broker disk persistence is intentionally disabled; Kafka is the
durable candidate-facing boundary.
