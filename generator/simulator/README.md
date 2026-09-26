# Network telemetry simulator

This supplied service reads the checked-in V1 inventory and publishes three
periodic telemetry families plus two sparse control families. Periodic events
include a small amount of deterministic imperfect delivery. It runs inside
Docker Compose; no host Python installation or virtual environment is required.

The simulator publishes to device-oriented MQTT topics:

```text
network/{site_id}/{device_id}/connectivity  connectivity_metric, 600 access devices, every 5 seconds
network/{site_id}/{device_id}/throughput    throughput_test, 600 access devices, every 60 seconds
network/{site_id}/{device_id}/health        device_health, all 654 devices, every 30 seconds
network/{site_id}/{device_id}/state         connectivity_state, affected access devices, event driven
network/{site_id}/{device_id}/infrastructure infrastructure_event, source router, event driven
```

Messages use QoS 1 and are not retained. The simulator uses one broker
connection while independently tracking each virtual device's schedule,
session and deterministic performance state. Scheduling and sequence numbers
are independent per event family. Infrastructure devices publish health only.

Source delivery includes a small amount of deterministic imperfection, and the
source does not label those records or simulated performance patterns. The
candidate prompt and event contracts define the observable source conditions
and required outcomes.

Performance varies across devices, geography, providers, technologies, and
shared dependencies. Observe at least ten minutes of normal runtime before
evaluating analytical output.

Normal operation starts with the repository environment:

```bash
docker compose up -d
docker compose logs -f simulator
```

Pause only telemetry generation, without stopping the rest of the environment
or deleting any volume data, with:

```bash
docker compose stop simulator
```

Kafka, Trino, Airflow, storage, and existing source records remain available.
Resume with `docker compose start simulator`. A restarted simulator creates a
new run identity and new device sessions.

Observe one source event directly from Mosquitto:

```bash
docker compose exec mosquitto \
  mosquitto_sub -h localhost -t 'network/+/+/connectivity' -C 1 -W 10
```

Runtime overrides are optional:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `SIMULATOR_SEED` | `20250401` | Stable device, schedule, and measurement variation |
| `EVENT_RATE_MULTIPLIER` | `1` | Publication-rate multiplier from `0.1` through `10` |
| `MQTT_HOST` | `mosquitto` | Broker host inside Compose |
| `MQTT_PORT` | `1883` | Broker port inside Compose |

Normal runs generate a new run identity and new device sessions at process
startup. The simulator does not persist sequence state across container
restarts. Event contracts are under `contracts/events/`.
