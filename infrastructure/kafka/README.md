# Kafka

Kafka runs as a single combined broker/controller using KRaft. It exposes
localhost:9092 to host tools and kafka:29092 to Compose services.

The harness-owned kafka-init service idempotently creates these source topics
with three partitions and replication factor one:

- network.connectivity
- network.throughput
- network.device_health
- network.state
- network.infrastructure

Candidates may create additional topics, but generator and bridge topic
contracts remain harness-owned.

All five harness-owned topics enforce two-hour retention and 15-minute segment
rolling. Cleanup is asynchronous and removes complete segments, so deletion may
lag slightly beyond two hours. The default source rate retains roughly 1.1
million records in that window. The 10x rate multiplier is intended only for
short stress tests.

The supplied bridge writes all five harness-owned topics with:

- UTF-8 `device_id` as the Kafka key.
- The byte-identical MQTT payload as the Kafka value.
- No required headers.
- Kafka create time as bridge-arrival time.
- At-least-once delivery, so a stable `event_id` may appear more than once.

Kafka preserves bridge-observed order for one device within its keyed
partition. Candidates must not depend on a specific partition number.
