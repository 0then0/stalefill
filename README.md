![StaleFill: a paused old cache fill returning after invalidation](docs/assets/stalefill.svg)

# StaleFill

[![Go 1.26+](https://img.shields.io/badge/Go-1.26%2B-00ADD8?style=flat&logo=go&logoColor=white)](go.mod)
[![Redis / Valkey](https://img.shields.io/badge/Redis%20%2F%20Valkey-RESP2%20%2B%20RESP3%20subset-0f8b8d?style=flat)](#protocol-and-connection-scope)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-64748b?style=flat)](LICENSE)

StaleFill deterministically tests cache-aside race conditions by controlling Redis/Valkey command ordering.

It can hold an old cache fill, allow a newer authoritative write and invalidation to finish, then release the old fill and check whether stale data becomes visible again.

```text
Application  →  StaleFill RESP proxy  →  Redis / Valkey
                       ↑
               HTTP probe runner
```

An ordinary Redis client needs only its Redis address changed to the proxy address. StaleFill checks specific modeled schedules. It does not prove complete cache consistency.

## The race

```text
A: GET product:42 → MISS
A: authoritative read → V1
A: SET product:42 V1 → held BEFORE forwarding
B: authoritative write → V2
B: DEL product:42 → upstream reply received
B: HTTP write completes
   authoritative endpoint confirms V2
A: held SET forwarded → upstream reply received
   final cache-backed HTTP read
```

A vulnerable cache-aside implementation returns V1 at the last step: **FAIL SF001**. The fixed example runs the same commands and schedule, validates the cached version against an authoritative snapshot, and returns V2: **PASS**. A stale entry can still physically be written; this example protects the configured API observation and repairs the entry on read. It trades an authoritative version lookup for correctness under this schedule.

Neither demo uses sleeps or StaleFill hooks. The demo's authoritative store is in memory, not a production database. The proxy never speaks a database protocol.

## Binary archives

Archives contain the CLI, configuration examples and documentation. After extracting, run `./stalefill init` (`stalefill.exe init` on Windows), configure the Redis addresses and HTTP probes for your application, then run `doctor` and `test`. Go is required only to build from source. The bundled demo below is built from the source checkout.

## Quick start from source

Requires Go 1.26 or newer, plus a disposable Redis or Valkey instance. No third-party Go dependencies.

```sh
go build -o bin/stalefill ./cmd/stalefill
go build -o bin/stalefill-demo ./cmd/demo
./bin/stalefill init
```

Start a disposable Redis (or use your existing local test instance):

```sh
docker run --rm --name stalefill-demo-redis \
  -p 127.0.0.1:6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

In another terminal, start the broken application:

```sh
./bin/stalefill-demo --mode broken --redis 127.0.0.1:6380
```

In a third terminal:

```sh
./bin/stalefill doctor --config stalefill.json
./bin/stalefill test --config stalefill.json --report broken.json
# Exits 1: FAIL SF001: stale cache resurrection
```

Stop the application and restart it with `--mode fixed`. Repeat the same `test` command with `--report fixed.json`: it exits 0, PASS. `prepare` resets the fixture before every schedule, so repeated runs need no manual Redis cleanup.

The application must accept HTTP requests without requiring Redis during startup, or retry its initial Redis connection. The proxy listener exists only during `doctor` and `test`; no background daemon is installed.

## Configuration

Use [`examples/stalefill.json`](examples/stalefill.json), or generate it with `init`.

- `redis.listen` must be a literal loopback IP. `redis.upstream` defaults to local access; a remote host requires `allow_remote: true` in the file.
- `scenario.key` selects an exact key. Alternatively use an anchored Go RE2 `key_regex` of at most 512 bytes. A schedule matching multiple concrete keys is UNRESOLVED.
- `prepare` restores the old authoritative state and invalidates the target through the application. It runs before baseline and again before the race. StaleFill never creates Redis mutations itself.
- `read.assert` describes the old baseline API observation; `verify.assert` describes the new one. A protected in-flight read may refresh its response before it finishes. Their `json_path` must match, and their expected values must differ.
- `authoritative` independently confirms the new state while the old fill is still held. Its expectation must equal `verify.assert.equals`; its path can differ. Use a real authoritative endpoint for your application's contract.
- Every probe supports `method`, `url`, `headers`, a JSON body in `json`, and required exact `status`. Assertions support `$`, `.field`, and `[index]`. Final observations compare both status and JSON value against the configured old and new states. Transport errors, unexpected statuses, missing fields and invalid JSON never prove staleness. Numeric equality preserves large integers and treats `120` and `120.0` alike.
- `scenario.timeout` guards the entire invocation, including baseline, from 0 to 5 minutes exclusive of zero. It does not decide command ordering.

`doctor` runs the sequential `prepare → read → write → authoritative → verify` baseline. It checks listener availability, Redis reachability, HTTP assertions, and target GET/MGET, fill and invalidation traffic through the proxy. It **executes the configured application writes**, but never holds a command or runs concurrent race probes. Use disposable fixtures for both commands.

`test` refuses to arm the barrier when baseline is broken or target traffic is absent. Background activity on the selected key is not supported: isolate the fixture. More than one matching fill before release produces UNRESOLVED instead of choosing one arbitrarily.

For negative caching, use [`examples/negative.json`](examples/negative.json). It seeds a missing record, creates it in the write probe, and asserts `$.exists == true`; stale `false` produces **SF002**. For an API that returns 404 for a missing record and 200 for an existing one, set `read.status` to 404 and `verify.status` to 200. A final 404 with the configured old JSON observation is FAIL SF002 after the complete schedule. The in-flight read may finish with either the old or new configured status.

## Results and traces

```sh
stalefill test --config stalefill.json --report stalefill-repro.json
stalefill test --config stalefill.json --json
stalefill version
```

- **PASS**, exit 0: the complete schedule ran and the configured final invariant holds. For `doctor`, PASS refers only to baseline.
- **FAIL**, exit 1: the schedule ran, new authoritative state was confirmed, and final API observation matches the configured old state.
- **UNRESOLVED**, exit 2: a required event is absent, target traffic bypasses the proxy, the schedule is unsupported or ambiguous, or final value matches neither old nor new.
- **INFRASTRUCTURE_ERROR**, exit 3: invalid config, occupied listener, Redis/HTTP failure, broken baseline, or cancellation.

Stable findings: SF001 stale fill; SF002 negative cache; SF003 missing invalidation; SF004 missing proxy wiring/target traffic; SF005 incomplete schedule; SF006 unsupported/ambiguous schedule; SF007 inconclusive observation; SF100 infrastructure/baseline.

Tests write a JSON report, including failures and unresolved runs, when the report path is writable and distinct from the config. Paths that identify the same file, including existing symlink/hardlink aliases, are rejected before probes execute. Reports contain version, outcome, findings, environment, elapsed timings and monotonic sequence numbers. Keys are SHA256 hashes. Reports exclude Redis values/frames, credentials, HTTP bodies, URLs, cookies and headers. Reports are atomically written with mode 0600 where filesystem permissions are supported. Example real-server traces: [broken](docs/traces/broken.json), [fixed](docs/traces/fixed.json). A trace describes logical ordering; replay with restored application fixtures is a future milestone.

## Protocol and connection scope

Single upstream, TCP, RESP2 plus non-streaming RESP3 scalars, arrays, maps, sets and attributes. `HELLO 3`, AUTH, SELECT and unknown ordinary request/reply commands pass through as original frames. RESP payloads are binary safe and bounded at 16 MiB, nesting depth 32 and 65,536 aggregate entries (maps have two frames per entry).

Command-aware observation covers GET, MGET, SET with options, SETEX, PSETEX, DEL, UNLINK, EXPIRE and PEXPIRE. Barriers target fill forwarding and require an actual null GET/MGET reply followed by a successful matching DEL/UNLINK reply. SET NX/XX can refuse a delayed fill; correctness still comes from the final API assertion.

Connections are independent and can be reused or pooled. Pipelines preserve command/response order: the proxy serially forwards request/reply pairs on each connection, buffering at most 16 pending commands. It does not preserve pipeline throughput. An invalidation behind the held fill on the same connection is SF006; a separate application connection is required. Cross-connection ordering is precisely what the scenario controls.

MULTI/EXEC, Lua and Redis Functions make the scenario UNRESOLVED conservatively, even when they mention unrelated keys. Pub/sub, MONITOR and CLIENT TRACKING are refused with a protocol error because their unsolicited messages need a different multiplexer. CLIENT REPLY OFF/SKIP are also refused because they suppress expected responses; CLIENT REPLY ON passes through. Rejected modes do not block following pipeline commands. Inline commands, streamed RESP3, TLS, Cluster and Sentinel are outside v0.1. Broad compatibility with client libraries is not claimed.

## Safety and boundaries

Use disposable integration environments. StaleFill never issues FLUSHALL, DEL, SET or other synthetic mutations; only the configured application's probes may mutate data. Default bind and upstream are loopback. Remote upstream opt-in does not make production testing safe.

Ctrl-C and timeouts cancel HTTP probes, close client/upstream sockets and stop listeners. An aborted held fill is canceled before forwarding. Unrelated connections keep running while a target fill is held.

[Toxiproxy](https://github.com/Shopify/toxiproxy) models network conditions. [CacheProof](https://github.com/balyakin/cache-proof) models cache disposability, misses, outages and cold caches. StaleFill controls semantic command ordering and checks a stale resurrection invariant. It adds no generic toxics, load testing, DB proxy, source analysis, automatic race discovery, GUI or SaaS. See the dated [landscape research](docs/research.md).

## Development and CI

```sh
gofmt -w cmd internal
go vet ./...
go test ./...
go test -race ./...
REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
```

Without `REDIS_TEST_ADDR`, real-server tests explicitly skip; protocol/scheduler tests and 24 repetitions of each broken/fixed positive/negative demo run against a small protocol fixture. With that variable, another 24 repetitions per combination run against real Redis/Valkey. CI runs both server families and Go 1.26/1.27, then builds release archives on a version tag. See [CONTRIBUTING](CONTRIBUTING.md) and [CHANGELOG](CHANGELOG.md).

Licensed under Apache-2.0.

> Do not wait for a cache race to happen. Force the exact schedule that can make stale data win.
