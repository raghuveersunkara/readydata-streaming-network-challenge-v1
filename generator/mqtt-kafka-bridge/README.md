# MQTT-to-Kafka bridge

This supplied service forwards five event families from Mosquitto to
candidate-facing Kafka source topics:

```text
network/+/+/connectivity -> network.connectivity
network/+/+/throughput   -> network.throughput
network/+/+/health       -> network.device_health
network/+/+/state        -> network.state
network/+/+/infrastructure -> network.infrastructure
```

For each record, the bridge writes the UTF-8 `device_id` from the MQTT topic as
the Kafka key and preserves the MQTT payload as the byte-identical Kafka value.
It adds no headers, parses no JSON, and performs no schema validation,
normalization, enrichment, or deduplication.

The bridge runs inside Docker Compose with pinned Python, Paho MQTT, and
Confluent Kafka dependencies. No host Python environment is required:

```bash
docker compose up -d
docker compose logs -f mqtt-kafka-bridge
```

Normal deployment addresses are supplied through:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MQTT_HOST` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka bootstrap address |

All route, acknowledgement, batching, queue, retry, readiness, and logging
settings are strict V1 values in `config.json`.

## Delivery behavior

The bridge acknowledges a QoS 1 MQTT message only after Kafka confirms
delivery. Its Kafka producer uses `acks=all` and idempotence, but MQTT and Kafka
cannot share one transaction. A crash after Kafka accepts a record and before
MQTT receives its acknowledgement can therefore create a duplicate. Consumers
can identify repeated logical events through the stable source `event_id`.

Kafka create time represents bridge arrival. The payload's `event_timestamp`
remains measurement time. A fixed persistent MQTT session and Mosquitto's
10,000-message in-memory queue cover short bridge restarts. Mosquitto disk
persistence remains disabled, so this local buffer does not replace Kafka
durability.

The bridge becomes ready only after all five Kafka targets are available and
MQTT has acknowledged all five subscriptions. It becomes unready during either
dependency outage, reconnects automatically, and performs a bounded Kafka
flush on SIGTERM/SIGINT.
