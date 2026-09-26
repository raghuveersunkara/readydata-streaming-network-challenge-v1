# Reference Data

This directory holds versioned static inputs supplied by the challenge harness:

- `census/v1/` — prepared public, aggregate 2020 Arkansas Census geography and
  population context.
- `inventory/v1/` — deterministic fictional sites, devices, and shared network
  dependencies derived from that geographic context.

Streaming telemetry, generated runtime state, secrets, outputs, and solution
implementation code do not belong here. Canonical files are checked in and are
not regenerated during normal startup.
