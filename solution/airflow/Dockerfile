# Supplied starter image for the base Airflow control plane. Add pinned task
# dependencies here when they are required by the candidate-owned DAGs.
FROM apache/airflow:3.3.0

# Trino DB-API client used by the solution DAGs, and pytest for the DAG logic
# tests. Pinning apache-airflow alongside stops pip from upgrading Airflow's
# own dependencies.
RUN pip install --no-cache-dir "apache-airflow==3.3.0" "trino==0.340.0" "pytest==9.1.1"
