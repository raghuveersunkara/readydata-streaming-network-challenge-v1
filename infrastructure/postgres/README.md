# Metadata PostgreSQL

One PostgreSQL container hosts separate airflow and nessie databases with
separate local-development users. The initialization script creates them only
when the metadata-db-data volume is first initialized.

PostgreSQL is harness infrastructure and is not exposed to the host or intended
as a candidate data store.
