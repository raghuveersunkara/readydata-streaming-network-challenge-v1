# Trino

Trino exposes localhost:8080 and loads the Iceberg catalog from
catalog/iceberg.properties. The connector uses Nessie through Iceberg REST and
accesses Garage through Trino's native S3 filesystem with path-style access.

Static local S3 credentials are supplied through environment-variable
substitution. Garage-specific APIs do not appear in the connector contract.
The supplied catalog supports Trino reads, writes, row-level mutations, and
Iceberg data-file and manifest optimization for this local environment.
