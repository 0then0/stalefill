# Historical external validation: gocache with StaleFill v0.2

StaleFill v0.2.0 observed stale resurrection in **12 of 24** independent Redis
runs. The other **12 were UNRESOLVED**, with three distinct stopping conditions.
There were no PASS or INFRASTRUCTURE_ERROR outcomes. This is evidence of both an
observable stale-fill schedule and a limitation in v0.2's handling of detached
background publications. It is **not a deterministic 24/24 external validation**.
This historical experiment used the released v0.2.0 scheduler and unmodified
gocache. See the [v0.3 validation](gocache-v4.4.0-v0.3.md) for subsequent
detached scheduling results and the [upstream regression](gocache-upstream-regression.md)
for deterministic library tests and an experimental fix.

## Versions and provenance

Validation date: 2026-10-01. Host: macOS arm64, Go 1.27.1. Disposable backend
containers run Linux arm64, without persistence.

- Upstream: [eko/gocache](https://github.com/eko/gocache/tree/lib/v4.4.0).
- Exact tag: `lib/v4.4.0`, commit
  `515e65d2cd170b9ba807c64f9b86ed71f6842754`.
- Library dependency: `github.com/eko/gocache/lib/v4 v4.4.0`.
- Redis store from that same commit:
  `github.com/eko/gocache/store/redis/v4 v4.2.12-0.20260908213858-515e65d2cd17`.
  The store has its own module and tag series. The fixture has no `replace`
  directive.
- go-redis v9.13.0, RESP2, ordinary connection pool and retry policy.
- StaleFill: public `v0.2.0`, commit
  `e4ab5669d294f216a5bd9b5fda5dbcafdf5793fb`, built from unchanged tag source.
- Primary server: Redis **8.10.2**, `redis:8.10.2-alpine`.
- Secondary server: Valkey **9.1.2**, `valkey/valkey:9.1.2-alpine`.

[Provenance](gocache-v4.4.0/provenance.json) retains image digests, actual server
version output, binary/source SHA256 hashes and checks. All 31 downloaded
library Go files and all four Redis-store Go files were byte-compared with the
exact upstream commit archive; all matched. `go mod verify` passed. The core
`cmd`, `internal` and `go.mod` matched the v0.2.0 tag before and after validation.

## Why selected and relevant architecture

gocache is a third-party cache abstraction with a real Redis store, generics,
singleflight loader suppression and asynchronous publication. This makes it a
useful validation candidate for a command-barrier tool whose original model
expects a reader to remain active until its fill is accepted.

The following are **facts from the exact tagged source**, not runtime findings:

1. [LoadableCache.Get](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/loadable.go#L95)
   executes a `singleFlight.Do` callback. It checks temporary `setCache`, then
   the underlying cache, then calls `loadFunc`. A successful loader value is
   stored in `setCache` and sent to buffered `setChannel` (capacity 10000).
   Get returns after enqueue, without waiting for Redis publication. A full
   queue can block enqueue; this fixture uses one key and no queue saturation.
2. The [setter and setItem](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/loadable.go#L57)
   consume the queued value in a separate goroutine. `setItem` calls
   `c.Set(context.Background(), ...)`, ignores its error, then deletes the
   temporary entry **after Set returns**, including on failure. The initiating
   HTTP context does not own or cancel this publication.
3. [Delete](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/loadable.go#L152)
   deletes `setCache`, then delegates to the underlying cache. It does not remove
   queued items, cancel an executing Set or join the setter. An item already
   taken by the setter retains its own old value.
4. [Cache.Delete](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/cache.go#L90)
   delegates through the codec to
   [RedisStore.Delete](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/store/redis/redis.go#L125),
   which waits for ordinary `client.Del(...).Result()`. RedisStore.Set uses
   ordinary `client.Set(...).Err()`. This fixture sets no expiration or tags,
   yielding single-key GET, SET and DEL, without transactions or Lua.
5. There is no generation, version comparison or fencing between Get, queued
   publication and Delete. Singleflight coalesces concurrent Get callbacks for
   a key; neither Delete nor the background Set participates in that operation.
   It does not order an old publication after a newer mutation/invalidation.
6. [Close/drain](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/loadable.go#L68)
   closes `done` once and waits for the setter. The setter drains buffered items
   by invoking the same background Set operation. Close does not revoke them
   or report their individual Set failures. It is a lifecycle operation, not a
   mutation barrier. The harness calls it only after CLI completion, never from
   a read, write or prepare endpoint.

The **initial hypothesis** was that a setter already carrying V1 could publish
it after Delete completes. Temporary caching prevents some duplicate loads;
its deletion does not prove publication cancellation. The runtime findings
below test this distinction.

The tagged [loadable tests](https://github.com/eko/gocache/blob/515e65d2cd170b9ba807c64f9b86ed71f6842754/lib/cache/loadable_test.go)
cover loader coalescing, temporary storage, background setter completion and
Close. They do not exercise an executing Redis Set ordered after Delete.
They were inspected, but not run during this historical experiment.

## Harness and baseline

The separate [integration module](../../integration/gocache/README.md) uses real
`LoadableCache[string]`, `Cache[string]`, RedisStore and go-redis dependencies.
The sole key is `stalefill:gocache:item:1`. Its mutex-protected in-memory
authoritative store is independent of the cache.

- `GET /item`: ordinary LoadableCache.Get; returns `{"value":"V1"}` or V2.
- `PUT /item`: updates authoritative value, then ordinary LoadableCache.Delete.
- `GET /authoritative/item`: independent authoritative observation.

Only the Redis client's address points to StaleFill. Application GET, background
SET and invalidating DEL all use that client and pass through the proxy. No
application handler issues direct Redis DEL. Passive public go-redis hooks
record command names, request/operation IDs, success/miss/error and monotonic
timings in memory. They do not access gocache internals or block on test signals.
No application sleep, synthetic setter, publication wait inside GET or
StaleFill-specific synchronization was added. Socket and process deadlines
bound failures only.

The **first plain doctor invocation**, using direct application URLs, was
[PASS](gocache-v4.4.0/first-doctor.json). Before race testing, an additional
sequential doctor preflight checked actual async publication and cache reuse.
An external baseline-only observer occupied the doctor's HTTP write probe:
it used read-only upstream Redis GET to observe V1 publication, repeated normal
application GET until the client hook showed a Redis hit without another loader
call, then invoked ordinary application PUT V2 and independently checked that
the Redis entry was absent. Authoritative V2 and the doctor's subsequent
cache-backed V2 observation passed. The observer never runs in the application
or in any race probe. See [baseline doctor](gocache-v4.4.0/baseline-doctor.json)
and baseline flags in [the Redis matrix](gocache-v4.4.0/redis-summary.json).

Every race invocation also ran v0.2's built-in direct baseline successfully.
That baseline checks HTTP observations and observed traffic, but does not drain
the final verification read's detached publication before prepare. A PASS
baseline therefore does not guarantee that no publication remains pending.

## Attempted schedule and Redis results

The ordinary v0.2 `stale_fill_after_invalidation` configuration selected the
exact key and a 10-second safety timeout. Read asserted V1; verify and the
independent authoritative probe asserted V2. The intended schedule was:

```text
cache miss -> load V1 -> hold SET V1 before forwarding
write authoritative V2 -> LoadableCache.Delete -> Redis DEL completes
HTTP write completes -> independent authoritative V2 confirmed
release old SET -> Redis accepts it -> final cache-backed GET
```

24 CLI invocations used 24 fresh fixture processes, authoritative stores,
LoadableCache instances, temporary maps, setters and client pools. Each previous
process completed Close/drain before the next started. The disposable Redis
server was reused; ordinary prepare reset V1 and deleted the key. No outcomes
were retried or discarded. All 24 reached `read_started`.

- **FAIL: 12**, all SF001, final stage `FAIL / verification_stale`.
- **PASS: 0**.
- **UNRESOLVED: 12**, all SF005, split below.
- **INFRASTRUCTURE_ERROR: 0**.

The 12 complete schedules held exactly one string fill and verified stale V1
after authoritative V2 and completed invalidation. Each final GET hit Redis
without another loader call. This rules out temporary `setCache` as the source
of these final stale responses: Delete had cleared it and final reads reached
Redis. There were no complete schedules yielding PASS.

The 12 incomplete schedules must not be pooled as one cause:

1. **8 at `STALE_SET_HELD / stale_set_held`**: a race miss and held SET appeared,
   but no `write_started` event followed. The early `readDone` branch in the
   unchanged runner was selected; HTTP probe completion ended the scenario even
   though the final trace contained a held background fill.
2. **1 at `CACHE_MISS_OBSERVED / cache_miss_observed`**: reader/loader completed
   before the barrier became held. The background SET attempt appears in
   passive application metadata, but the CLI trace records no held publication.
   It was not kept alive with a synthetic reader wait.
3. **3 at `READ_STARTED / read_started`**: the race reader hit Redis and never
   loaded or produced a race fill. Baseline verification had queued its own
   detached publication, which survived prepare's Delete and left the key
   populated. This exposes missing publication quiescence at the baseline to
   prepare boundary; it is distinct from reader completion during a race miss.

[Redis summary](gocache-v4.4.0/redis-summary.json) records every run number,
outcome, finding, stopping stage, barrier/writer progress, diagnosis and observed
timings. Retained raw CLI traces include
[FAIL, run 5](gocache-v4.4.0/redis-fail.json),
[held UNRESOLVED, run 2](gocache-v4.4.0/redis-held-unresolved.json),
[miss UNRESOLVED, run 15](gocache-v4.4.0/redis-miss-unresolved.json), and
[no-miss UNRESOLVED, run 1](gocache-v4.4.0/redis-no-miss-unresolved.json).
Each has an `-app.json` sidecar with passive metadata captured after CLI return.
CLI reports are unchanged. Neither CLI nor application traces retain Redis
values/frames, raw keys, HTTP bodies, credentials or headers.

## Representative timeline and measurement limits

Redis run 5 completed the schedule and returned FAIL SF001. Application times
are microseconds from fixture startup; StaleFill times use its separate clock.
The clocks are not numerically merged.

```text
App 46962 us: Redis GET miss; loader takes authoritative V1 snapshot
App 46966 us: LoadableCache.Get returns
App 46970 us: HTTP GET handler returns
App 46985 us: background go-redis SET attempt begins
SF  seq 6, 16 ms: SET observed and held before forwarding
App 47106 us: writer updates authoritative state to V2
App 47107 us: ordinary Delete starts Redis DEL
SF  seq 8, 17 ms: invalidation applied
App 47610 us: DEL succeeds; LoadableCache.Delete returns
SF  seq 9, 17 ms: HTTP write completed
SF seq 10, 17 ms: independent authoritative V2 confirmed
SF seq 11, 17 ms: old SET released
App 48232 us: old SET succeeds
SF seq 12, 18 ms: old SET completed
SF seq 13, 18 ms: runner consumes reader result, records read_completed
App 48723 us: final GET is a Redis hit, with no loader
SF seq 15, 18 ms: final observation matches old V1, FAIL SF001
```

The old SET starts **15 us after HTTP handler return** in this run. Across all
21 Redis race readers that loaded, that interval was **9–22 us**, and the marker
immediately before loadFunc return to SET attempt interval was **14–31 us**.
The 12 completed race SET attempts lasted **966–1610 us** through client
completion. These are diagnostic measurements, not throughput benchmarks.
Run 5's held-to-release trace difference is 1 ms, at millisecond resolution;
it is not an exact network hold duration. Its writer DEL attempt-to-success
interval is 503 us.

`http_get_handler_returned` measures the server handler boundary, not when a
client consumes the body. `redis_started` measures the public go-redis hook,
before connection acquisition/network write; the StaleFill barrier event is
the independent evidence of actual command observation. In early UNRESOLVED
paths, source inspection establishes actual HTTP probe completion: `readDone`
is sent only after the probe reads and parses the body. No exact client-complete
timestamp was added to v0.2. Conversely, its late `read_completed` trace event
timestamps consumption by the schedule after release, **not** the original
HTTP request lifetime. The handler's early return is observable even in FAIL
runs; it is not correct to claim that all readers waited for the fill.

## Valkey cross-check

The identical fixture and direct race configuration ran on actual Valkey 9.1.2
after its sequential baseline passed. Four independent runs produced:
**FAIL 3, UNRESOLVED 1, PASS 0, INFRASTRUCTURE_ERROR 0**. The unresolved case
stopped at held SET without starting the writer; the three complete schedules
returned SF001. See [Valkey summary](gocache-v4.4.0/valkey-summary.json),
[representative FAIL](gocache-v4.4.0/valkey-fail.json) and
[held UNRESOLVED](gocache-v4.4.0/valkey-held-unresolved.json).

Both servers exhibit the same completed stale schedule and reader-lifetime
limitation. Different small-sample proportions do not establish a Redis/Valkey
behavioral difference; acceptance also depends on Go/HTTP/connection scheduling.

## v0.2 scheduling limitation

The decisive implementation is the barrier wait's `select` in
[the v0.2 scheduler source](https://github.com/0then0/stalefill/blob/e4ab5669d294f216a5bd9b5fda5dbcafdf5793fb/internal/scenario/run.go). It races receipt
of `readDone` against `heldDone`. A successful early reader result still leads
to SF005, without a write. If the held branch wins, the existing command barrier
can finish a correct controlled schedule despite the detached HTTP lifetime.
There is no request-to-fill association supplied by the library, and baseline
traffic checks do not establish absence of pending background publications.

Thus the answer is qualified: **v0.2 can hold and verify this asynchronous fill
when its existing barrier wait wins, but cannot reliably schedule it independently
of HTTP request completion or isolate it from baseline publications.** The
experiment observed stale resurrection in complete schedules; it does not
justify a blanket upstream bug claim or a 24/24 deterministic reproduction
claim. It also does not establish consistency outside the tested schedule.
Passive metadata collection has some overhead and can perturb scheduling;
the observed proportions are measurements of this environment, not portable
probabilities or guaranteed results of a later run.

StaleFill v0.3 subsequently retained successful early reader completion while
awaiting a bounded publication candidate and joined modeled baseline miss
publications before prepare. The [v0.3 case study](gocache-v4.4.0-v0.3.md)
records complete schedules for this same fixture. Its association limits still
require an isolated selected key and a single publication producer per miss.

Validation performed: fixture build, `go mod verify`, fixture `go vet ./...`,
root `go test ./...`, two baseline modes, 24 real Redis race runs and four Valkey
runs, plus source/trace checks described above. Core real-server tests skipped
in the root test command because `REDIS_TEST_ADDR` was not set; the external
experiment itself used real servers. Full ephemeral driver artifacts are
recreated by [the reproduction instructions](../../integration/gocache/README.md).
