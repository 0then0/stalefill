# Changelog

## 0.3.0

[Release notes](docs/releases/v0.3.0.md).

- Join read, miss episode and publication lifetimes independently; retain early old HTTP observations while awaiting detached fills within `scenario.timeout`.
- Join baseline miss publications before mutation and before race prepare, including the final baseline verification fill.
- Associate existing string/hash/EXEC publication boundaries using concrete key, read family, wire command identity and transaction start context across pooled connections.
- Add SF009 and reason events for competing candidates/reads/misses, incompatible publication ordering or shape, pending prior publication and unexpected early reader values.
- Record actual reader completion and numeric episode/connection evidence without inspecting cache payloads; reject early reader errors before release.
- Validate unmodified gocache lib/v4.4.0 on Redis 8.10.2 (24 complete FAIL SF001 schedules) and Valkey 9.1.2 (4 complete FAIL SF001 schedules). See the retained case evidence for scope and limits.

Version-1 configuration gains optional `read/verify.cache_publication: "none"` for explicit baseline no-fill contracts. Publication command support is unchanged. Writer cache reads after the hold retain the selected episode; derived reader errors preserve known scheduling findings. Attribution still requires an isolated target; hidden unrelated producers cannot be identified from opaque Redis traffic alone.

## 0.2.0

[Release notes](docs/releases/v0.2.0.md).

### Added

- Hash cache fills: HGET/HMGET/HGETALL miss detection and an HSET publication barrier.
- Single-key hash transactions with optional DEL/UNLINK, one or more HSET commands and optional EXPIRE/PEXPIRE. EXEC is held before publication; queued deletes do not count as writer invalidation.
- Pass-through for Lua commands whose declared keys exclude the selected cache key.
- SF008 for discarded, rejected or aborted publications, and `fill_mode` metadata in reports.
- Reproducible redis-py 8.1.0, go-redis 9.22.0 and node-redis 6.3.0 fixtures, tested in RESP2 and RESP3 on Redis 8.10.2 and Valkey 9.1.2.
- A separate client compatibility workflow required by release publication.

### Fixed

- Classify unsupported hash mutations on the selected key as SF006, including HSETEX and field-expiration commands.
- Reject noncanonical Lua key counts instead of treating invalid declarations as unrelated scripts.
- Include release notes in every binary archive and verify packaged documentation links and checksums before publication.
- Promptly cancel the waiting HTTP write when invalidation is queued behind a held fill on the same connection.
- Reject unsupported transaction shapes and publication errors without reporting false stale findings.

Version-1 configurations, string and negative-cache scenarios, existing finding IDs and five-platform archive targets remain compatible.

## 0.1.0

[Release notes](docs/releases/v0.1.0.md).

Initial release:

- Deterministic stale-fill-after-invalidation and negative-cache-resurrection scenarios.
- Bounded, binary-safe, independent-connection RESP2 and non-streaming RESP3 proxy.
- Exact key or anchored regex selection, duplicate detection and publication barriers.
- Sequential baseline, proxy wiring checks, HTTP probes and independent authoritative verification.
- PASS, FAIL, UNRESOLVED and INFRASTRUCTURE_ERROR outcomes with stable finding IDs.
- Human output and JSON reports with monotonic event traces and no raw cache or HTTP payloads.
- Vulnerable and protected Go demos, repeated real Redis/Valkey checks and cancellation coverage.
- Config/report collision protection, including existing file aliases.
- Final HTTP status/value comparison, including negative-cache 404 observations.
- Explicit rejection of CLIENT REPLY OFF/SKIP.
- CI and five-platform archives with SHA256 checksums.
