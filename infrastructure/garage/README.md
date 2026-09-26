# Garage

Garage runs as a single-node S3-compatible object store with replication factor
one. The server automatically creates the configured access key and warehouse
bucket during its first startup. Object data and Garage metadata share the
garage-data named volume.

The committed RPC secret and default S3 credentials are intentionally
non-production values. Only the S3 API is exposed to localhost.

Garage-specific administration belongs here. Solution code should interact with
the object store through the supplied generic S3-compatible endpoint and
credentials rather than depending on Garage administration APIs.
