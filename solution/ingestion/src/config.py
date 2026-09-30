import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "iceberg-ingestion-group")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://garage:3900")
S3_REGION = os.getenv("S3_REGION", "garage")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
CONTRACTS_DIR = os.getenv("CONTRACTS_DIR", "/app/contracts/events")
REFERENCE_DATA_DIR = os.getenv("REFERENCE_DATA_DIR", "/app/reference-data")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "5.0"))

TOPIC_TO_TABLE = {
    "network.connectivity": "connectivity_events",
    "network.throughput": "throughput_events",
    "network.device_health": "device_health_events",
    "network.state": "connectivity_state_events",
    "network.infrastructure": "infrastructure_events",
}

# The only event_type each topic may carry; anything else is rejected.
TOPIC_EVENT_TYPE = {
    "network.connectivity": "connectivity_metric",
    "network.throughput": "throughput_test",
    "network.device_health": "device_health",
    "network.state": "connectivity_state",
    "network.infrastructure": "infrastructure_event",
}
