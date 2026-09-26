# Infrastructure

This directory is owned by the challenge harness and holds service-specific configuration for the local Docker Compose environment.

Service configuration is organized into these subdirectories:

- kafka/ — Kafka topic-bootstrap ownership and connection details.
- mosquitto/ — MQTT broker configuration.
- garage/ — local S3-compatible object-storage configuration.
- iceberg/ — Nessie Iceberg REST catalog documentation.
- trino/ — Trino Iceberg REST and S3 connector configuration.
- airflow/ — base Airflow LocalExecutor environment documentation.
- postgres/ — shared metadata-database initialization for Nessie and Airflow.

Application logic, solution ingestion code, analytical SQL, and generated data
do not belong here.

Service-owned runtime state uses Docker named volumes: Kafka data, Garage
objects, Iceberg catalog state, and the Airflow metadata database.
Repository-controlled configuration and solution code use bind mounts. Garage
and catalog state must be reset together so Iceberg metadata and stored objects
cannot drift apart.

Garage-specific bootstrap and administration lives here, but application code
interacts with it through generic S3-compatible settings. The supplied Garage,
Nessie Iceberg REST, and Trino configuration is the supported local lakehouse
path.

Kafka bootstrap owns the five harness source topics: connectivity, throughput,
device health, state transitions, and infrastructure events. The supplied
bridge must be ready on all five MQTT subscriptions before it is considered
healthy.

## Internal service endpoints

- Kafka: kafka:29092
- Mosquitto: mosquitto:1883
- Garage S3 API: http://garage:3900
- Nessie Iceberg REST: http://nessie:19120/iceberg
- Trino: http://trino:8080
- PostgreSQL: metadata-db:5432
- Airflow API server: http://airflow-api-server:8080

PostgreSQL is not exposed to the host. Garage administration and RPC ports are
also internal-only.
