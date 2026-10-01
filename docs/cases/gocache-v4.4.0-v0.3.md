# gocache lib/v4.4.0: v0.3 detached scheduling validation

On 2026-10-01, the local StaleFill v0.3 development build completed **24 of 24** independent schedules on Redis 8.10.2 and **4 of 4** on Valkey 9.1.2. All returned **FAIL SF001**: authoritative V2 was confirmed before releasing the old publication, and the final cache-backed read returned V1. Both batches had zero PASS, UNRESOLVED and INFRASTRUCTURE_ERROR outcomes.

This extends, and does not replace, the [original v0.2 case study](gocache-v4.4.0.md), which retained 12 FAIL / 12 UNRESOLVED Redis outcomes and 3 FAIL / 1 UNRESOLVED Valkey outcomes. The upstream dependency, fixture handlers and passive hooks were unchanged. No publication wait was added to HTTP GET, and no sleep or library-specific hook determined the race schedule.

## Provenance and execution

Host: macOS arm64, Go 1.27.1. Servers: disposable Linux arm64 containers, Redis `redis:8.10.2-alpine` and Valkey `valkey/valkey:9.1.2-alpine`, with persistence disabled. Actual server version output and image IDs are retained in [provenance](gocache-v4.4.0-v0.3/provenance.json), together with CLI/fixture SHA256 hashes and a digest of the local core source. This was an uncommitted development build reporting `0.3.0`, based on the recorded main commit, not a published release/tag.

The fixture still uses `github.com/eko/gocache/lib/v4 v4.4.0` and the Redis store from commit `515e65d2cd170b9ba807c64f9b86ed71f6842754`, with go-redis v9.13.0 and RESP2. Each invocation used a fresh fixture process and library instance. Previous instances completed their lifecycle Close/drain before the next began. The isolated server was reused; prepare performed ordinary application deletion. Outcomes were not retried or discarded.

The existing extended doctor preflight passed for each server. Race runs used direct application HTTP endpoints. The CLI's own baseline now joins the successful publications associated with its read and verification miss episodes before mutation and before race prepare. It observes upstream completion through the proxy; it does not inspect values or synthesize Redis commands.

The [reproduction instructions](../../integration/gocache/README.md#v03-detached-scheduling-validation) use the same driver and pinned fixture dependencies. Earlier exploratory development batches were kept separately in temporary directories; the checked-in summaries below describe the final acceptance batch only.

## Evidence

- [Redis summary](gocache-v4.4.0-v0.3/redis-summary.json): 24 independent complete FAIL SF001 schedules.
- [Valkey summary](gocache-v4.4.0-v0.3/valkey-summary.json): 4 independent complete FAIL SF001 schedules.
- [Redis representative report, run-02](gocache-v4.4.0-v0.3/redis-fail.json).
- [Valkey representative report, run-03](gocache-v4.4.0-v0.3/valkey-fail.json).

In Redis, `read_completed` precedes the publication hold in **10/24** runs and publication release in **24/24**. In Valkey, it precedes the hold in **2/4** and release in **4/4**. The event now records actual HTTP probe completion after reading/parsing the response, rather than later consumption by the runner. This directly exercises successful early readers while retaining the existing held-first path. Millisecond timings remain diagnostic; sequence numbers establish logical order.

The representative reports show:

```text
baseline miss publications completed
baseline completed → prepare → read started
confirmed target miss → HTTP old observation completed
publication held → writer started
invalidation applied + HTTP write completed
independent authoritative V2 confirmed
publication released → upstream publication completed
final cache-backed read → FAIL SF001
```

The underlying gocache setter remains detached from the HTTP context. Publications can use a different pooled connection. Numeric episode/connection metadata is retained; no raw Redis key, field, payload, HTTP body, credentials or headers enter the reports.

## Regression coverage and limits

Go tests force reader-first, held-first and synchronous lifetimes across SET, SETEX, PSETEX, HSET and supported EXEC, on matching and differing connections. They cover early reader errors while the writer waits, absent publications, multiple candidates, competing target reads/misses, incompatible shape, commands queued before a miss, transactions started before a miss, baseline joins and misses arriving after HTTP completion. Regression tests also cover negative-only caching with explicit baseline no-fill contracts, writer GET before mutation, conflicting writer publications and scheduling findings surviving derived reader errors. A detached HTTP fixture repeats both vulnerable and protected outcomes 24 times.

Validation completed: `go test ./...`, `go vet ./...`, and `go test -race ./...` with disposable real Redis and Valkey, plus the six gocache driver regression tests. Real-server tests include RESP2/RESP3 hash/EXEC and protocol behavior. The external gocache batches here use RESP2 only; no claim of an external RESP3 gocache matrix is made.

These finite batches demonstrate removal of the observed v0.2 reader-lifetime and modeled baseline-publication failures in this fixture. They do not establish universal scheduling success or correctness across other interleavings. Timings were affected by local concurrent validation work and passive hook overhead; they are not benchmarks.

Association still requires an isolated selected key with one publication producer per miss. The proxy can reject observed ambiguity but cannot identify a lone unrelated worker or hidden future retry with identical key, command shape and ordering. Joining modeled baseline publications is not proof of global application quiescence. The [architecture](../architecture.md#miss-episodes-and-detached-publication) documents this causal boundary. No upstream issue, PR, release or security report was published.
