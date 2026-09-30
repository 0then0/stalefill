# StaleFill v0.1 engineering report

Local validation date: 2026-09-30. Go 1.27.1, macOS arm64; disposable Redis 7.4.11 and Valkey 8.1.10. This report describes the current implementation and the local checks below. It does not certify arbitrary applications or every Redis client.

## Architecture

The proxy retains a separate upstream TCP connection for each application connection, preserving authentication, selected database and ordinary connection state. A bounded reader queue accepts pipelined requests while a worker forwards each request and receives its response in order. A barrier on one connection does not stop other connections. Serial forwarding preserves ordering but reduces pipeline throughput.

The RESP parser preserves original binary frames. It bounds each frame at 16 MiB, aggregate counts at 65,536 entries and nesting at 32 levels. RESP2 and non-streaming RESP3 replies are supported, including HELLO maps and attributes. Push streams require a different response multiplexer and are outside v0.1.

The scheduler observes target commands and replies under a mutex. A real null GET/MGET response arms the possibility of a fill barrier. A matching SET, SETEX or PSETEX is held before forwarding. Invalidation on a different connection can then proceed. Required transitions use channels and observed events; timeouts guard execution rather than select interleavings. Duplicate pre-release fills, multiple regex-selected keys and opaque mutations make the schedule unresolved.

The HTTP runner first performs sequential prepare/read/write/authoritative/verify probes. It requires successful assertions and matching read/fill/invalidation traffic before arming a race. Prepare runs again to restore the fixture. The independent authoritative endpoint confirms the new state while the old fill is still held; the final cache-backed endpoint supplies the correctness observation. Redis values remain opaque.

The reporter snapshots monotonically numbered events and emits human output and JSON. Reports include version, outcome, finding IDs, timings and environment metadata. They exclude raw Redis values, HTTP bodies, URLs, headers, credentials and cookies. Keys are SHA256 hashes. Report replacement is atomic with mode 0600 where supported. The CLI rejects config/report collisions, including existing file aliases, before executing probes.

## Deterministic schedule

```text
baseline_started
baseline_completed
prepared
read_started
cache_miss_observed
stale_set_held
write_started
invalidation_applied
write_completed
authoritative_confirmed
stale_set_released
stale_set_completed
read_completed
verify_started
verification_stale / verification_passed
```

The write-completion and invalidation events can occur in either relative order for a different application; both are required before release. The bundled synchronous demo consistently observes invalidation before HTTP write completion. Sequence numbers, rather than elapsed milliseconds, define logical order.

## Broken and protected demos

The broken demo implements GET/miss/authoritative-read/fill and authoritative-write/DEL without a fence. Expected and observed outcome: FAIL SF001 after a full schedule. Its negative-cache variant returns the old missing marker after creation and produces SF002.

The protected demo compares cached generations with an authoritative snapshot and repairs stale entries on read. Expected and observed outcome under the same schedule: PASS. It can still physically write an old cache entry; the protection applies to the configured API observation. The authoritative lookup has a cost and is not presented as a universal mitigation.

Both implementations use real TCP and HTTP without sleeps or StaleFill hooks. Their authoritative store is an in-memory fixture, not a production database. Example real-server traces are [broken](traces/broken.json) and [fixed](traces/fixed.json).

## HTTP observation contract

The old state consists of the read probe's expected HTTP status and extracted JSON value. The new state consists of the verify probe's expected status and value. Baseline, write and authoritative probes enforce their configured statuses strictly.

After a complete race, a final observation matching the old pair produces FAIL; a match for the new pair produces PASS. A mixture of old/new status and value is inconclusive, not a correctness proof. For negative caching this supports an old `404 {"exists":false}` and new `200 {"exists":true}`. An unexpected 500, transport failure, malformed JSON or missing extraction path remains an infrastructure error. A protected in-flight read may finish with the configured new status after the held fill completes.

## Reproducibility and regression checks

Each positive/negative, broken/protected combination ran 24 times against the protocol fixture and another 24 times against each real server. Each set had zero schedule misses and identical logical event order within its combination. Expected outcomes and finding IDs were asserted on every iteration.

Additional regressions cover:

- Default, explicit, normalized, symlink and hardlink config/report collisions, including malformed input; the config remains unchanged and no HTTP probe executes.
- Stale negative-cache 404, protected 200, refreshed in-flight reads, unexpected 500 and mixed status/value pairs, on the fixture and both real servers.
- Strict baseline/write/authoritative HTTP status assertions before barrier release.
- CLIENT REPLY OFF/SKIP rejection, case-insensitive command arguments and continued pipeline operation, on the fixture and both real servers. CLIENT REPLY ON remains supported.
- Fifty cancellations before fill interception, with successful-response and connection-error completions; every result is SF100 and the listener closes.
- Cancellation while a fill is held and cancellation classification at schedule waits. Aborted held fills do not complete upstream.

## Protocol support and boundaries

Observed commands: GET, MGET, SET with options, SETEX, PSETEX, DEL, UNLINK, EXPIRE and PEXPIRE. AUTH, SELECT, HELLO 3 and ordinary request/reply commands pass through as original frames. Binary payloads, fragmented frames, connection reuse, independent concurrent connections and ordered pipelines are tested.

MULTI/EXEC, Lua and Redis Functions make scenarios unresolved conservatively. Pub/sub, MONITOR, CLIENT TRACKING and CLIENT REPLY OFF/SKIP receive explicit protocol errors. An invalidation queued behind the held fill on the same connection is unsupported. TLS, Cluster, Sentinel, inline commands, streamed RESP3, asynchronous pushes and background activity on the selected key are outside v0.1.

Common client libraries were researched but not installed as test dependencies. Current compatibility evidence comes from direct RESP traffic and the bundled demo. There is no claim of broad library interoperability or exhaustive cache consistency.

## Safety

Listen addresses require literal loopback IPs. Non-loopback upstreams require explicit remote opt-in. The proxy generates no synthetic cache mutations; configured application probes are responsible for writes, including doctor baseline and prepare. Use disposable fixtures.

Parent cancellation is an infrastructure failure; missing scheduled events under the scenario timeout are unresolved. Cancellation closes HTTP probes, client/upstream sockets and the listener. Held connections are canceled before cleanup releases their local waiters.

Tests mutate `product:42` and protocol-test keys under `stalefill:protocol:*`. Never use shared or production caches for the commands below.

## Checks performed locally

All Go commands used `GOCACHE=/private/tmp/stalefill-go-cache` and `GOMODCACHE=/private/tmp/stalefill-go-mod`. Real-server addresses below belonged to disposable containers stopped after validation.

```sh
go test ./cmd/stalefill ./internal/scenario ./internal/proxy \
  -run 'TestReportCannot|TestNegativeHTTP|TestCancellationBefore|TestReplyModes' -count=1 -v
go test ./...
go vet ./...
gofmt -l cmd internal
REDIS_TEST_ADDR=127.0.0.1:61382 go test -race ./...
REDIS_TEST_ADDR=127.0.0.1:61383 go test -race ./internal/scenario ./internal/proxy \
  -run 'TestRealRedis' -count=1 -v
```

All completed successfully; gofmt reported no files. Race detection found no data races. Real-server tests include the HTTP-status and response-mode regressions as well as the repeated demos.

`scripts/release.sh v0.1.0` also rebuilt all five local archives. SHA256SUMS, documentation links and inclusion of the README icon were checked. Archives include configuration examples and the documents linked by README; generated binaries and archives stay ignored by Git.

CI is configured for Go 1.26/1.27 and Redis/Valkey on Linux. The local run used Go 1.27.1; it does not substitute for execution of the hosted matrix. Five-platform archive building uses the existing release script and SHA256 checksums. Building local archives does not publish a GitHub Release; publication requires a version-tag push.

## Remaining scope

Replay, JUnit, broader protocol modes and additional schedules are future work. The landscape distinction is documented in [research](research.md). The current API oracle checks an explicitly configured observation; it neither intercepts database protocols nor discovers all races or proves linearizability.
