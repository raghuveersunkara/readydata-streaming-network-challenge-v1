# Iceberg Catalog

Nessie provides the Iceberg REST catalog at http://nessie:19120/iceberg. Nessie
stores catalog state in its PostgreSQL database and is configured to use the
Garage warehouse through generic path-style S3 settings.

Catalog configuration is harness-owned. Iceberg namespaces, tables, schemas,
partitioning, and ingestion behavior belong in the solution workspace.

Garage requires Nessie's S3 chunked encoding to be disabled; the supplied
configuration already applies that setting. Nessie Iceberg REST exposes the
snapshot associated with the selected Nessie reference and commit.
