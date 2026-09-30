# Changelog

## 0.1.0

- Deterministic stale fill after invalidation and negative cache resurrection schedules.
- Bounded, binary-safe multi-connection RESP2 and non-streaming RESP3 proxy.
- Target key/anchored regex selection, duplicate detection and event barriers before fill forwarding.
- Sequential baseline, proxy wiring checks, HTTP probes and independent authoritative verification.
- PASS, FAIL, UNRESOLVED and INFRASTRUCTURE_ERROR with stable finding IDs.
- Safe human/JSON reports and monotonic event traces.
- Broken/fixed Go demos, real Redis/Valkey repetition tests and cancellation checks.
- CI and five-platform release archives with SHA256 checksums.
- Reject config/report file collisions before probes, including existing file aliases.
- Compare final HTTP status and JSON together, including stale negative-cache 404 responses.
- Reject CLIENT REPLY OFF/SKIP without blocking subsequent pipeline commands.
- Classify parent cancellation consistently as infrastructure failure at schedule waits.

Replay, JUnit, broader RESP3 modes and additional modeled schedules remain future work.
