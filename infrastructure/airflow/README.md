# Airflow

## TL;DR

Airflow is the supplied local workflow scheduler. PostgreSQL stores workflow
state, while candidate-owned workflows and their file interfaces live under
`solution/airflow/`. The challenge prompt defines Airflow's required outcomes;
candidates choose and justify its responsibilities within their design.

Airflow 3 runs with LocalExecutor using an API server, scheduler, and standalone
DAG processor. PostgreSQL stores Airflow metadata, and a named volume stores
logs.

The stack builds a small pinned image extension from
`solution/airflow/Dockerfile` so task dependencies are reproducible. The stable
`solution/airflow/dags`, `solution/sql`, and `reference-data` directories are
mounted read-only; only `solution/exports` is writable for generated artifacts.
Redis and Celery are intentionally omitted because this is a small
single-machine environment.

Candidate workflow implementation and operating instructions belong in
`solution/airflow/` and `solution/README.md`.
