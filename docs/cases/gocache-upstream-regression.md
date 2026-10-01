# gocache: deterministic regression and experimental fix

`eko/gocache` `LoadableCache` can restore a stale value after a completed
Delete: a preceding Get loads V1, its background Set is delayed, an application
updates authoritative state to V2 and deletes the cache entry, then the old
Set publishes V1. A subsequent cache-backed Get returns V1.

This case provides a deterministic regression test, a local experimental fix,
and Redis/Valkey validation. It follows the [v0.3 scheduling validation](gocache-v4.4.0-v0.3.md).
The [reproduction guide](../../integration/gocache/upstream/README.md) explains
how to run the tests using isolated copies of the upstream source.

## Versions and upstream status

The target is `github.com/eko/gocache/lib/v4 v4.4.0`, commit
`515e65d2cd170b9ba807c64f9b86ed71f6842754`, with Redis store
`v4.2.12-0.20260908213858-515e65d2cd17` and go-redis `v9.13.0`.
Dependencies remain pinned in the [fixture module](../../integration/gocache/README.md).

On 2026-10-01, the inspected upstream
[master](https://github.com/eko/gocache/tree/400151c70f7054ff89b5a51d076a247d52b8984b)
was `400151c70f7054ff89b5a51d076a247d52b8984b`. Its `loadable.go`, existing
loadable tests, and library manifest matched the pinned version; the new
regression tests failed on both. Searches of issues and PRs for `loadable`,
`delete`, and `stale`, together with the file history, found no fix for this
ordering. This describes the inspected snapshot, not future upstream changes.

[#251](https://github.com/eko/gocache/issues/251) and
[#260](https://github.com/eko/gocache/pull/260) address repeated loading through
a temporary cache. [#271](https://github.com/eko/gocache/issues/271) explains
removal of temporary values by Delete/Clear; that removal does not cancel
queued writes or coordinate an underlying Set already in flight.
[#176](https://github.com/eko/gocache/issues/176) concerns Ristretto's buffered
visibility, a different behavior from the immediate-effect fake and Redis
used here.

The upstream README documents asynchronous publication without promising
strong consistency. The demonstrated concern is narrower: completed
cache-aside invalidation can be undone by work from an earlier read.

## Deterministic regression

The [test source](../../integration/gocache/testdata/upstream/loadable_regression_test.go)
uses the existing `CacheInterface` and standard-library `testing/synctest`.
The primary test holds a background Set inside a controlled underlying cache,
updates authoritative state, starts Delete, releases Set, and checks the next
Get. It requires no Redis server, StaleFill, timing sleeps, or stress retries.

On the original library, Delete completes before Set is released and the
next Get returns V1. On the experimental implementation, Delete waits for
Set; the test releases Set before awaiting Delete and checks the same V2
invariant. Deadline guards bound failures, and cleanup releases the gates
and joins Close/drain. A future implementation using a blocking Mutex would
need adapted orchestration because synctest does not treat Mutex waits as
durable blocking.

Three additional confirmed cases cover queued publication after Delete,
queued V1 overwriting an explicit Set(V2), and a loader that captures V1
before Delete but finishes afterwards. All four cases fail on the original
and inspected master, and pass on the experimental copy under `-race`.
These failures are logical ordering errors; the new tests emitted no Go
race-detector warnings.

## Experimental fix and its scope

The [patch](../../integration/gocache/upstream/experimental-fix.patch) changes
only upstream `lib/cache/loadable.go`. Per-key generations suppress superseded
loads and queued publications. A per-key gate is held through the entire
underlying Set/Delete call, closing the window that a generation check before
Set alone would leave open. Explicit Set participates in the same ordering.

The guarantee applies to one LoadableCache instance with stable normalized
keys. It orders underlying calls; the tested visible invariant additionally
requires their effects to be applied before successful return. Redis and the
controlled fake satisfy that condition in these tests. Other instances,
direct store users, buffered backends such as ChainCache, and ambiguous remote
errors are outside the guarantee. There is no distributed locking or new API.

Delete latency includes a same-key in-flight Set; waiting mutations can be
canceled through their context. Unrelated keys do not wait on that gate,
although the existing single background setter retains its FIFO queue.
Temporary cache hits remain available during publication.

This is not a general read-freshness guarantee: singleflight is not forgotten
on mutation, so a later Get can still join an older unfinished load. The
regression invariant waits for that prior load/publication to finish.
Clear/Invalidate and arbitrary concurrent Get/Close behavior are not repaired.
Normal Close/drain cleanup with quiesced producers is covered. Mutation errors
propagate, but an attempted mutation can discard old local publication state
even if the underlying operation fails.

## Real-server results

Validation used disposable loopback containers without persistence on
2026-10-01. All runs used RESP2 and fresh application/CLI processes.

- **Original, Redis 8.10.2:** 3 FAIL SF001; 0 PASS, UNRESOLVED, or infrastructure
  errors. All three schedules completed.
- **Original, Valkey 9.1.2:** 3 FAIL SF001; 0 PASS, UNRESOLVED, or infrastructure
  errors. All three schedules completed.
- **Experimental local dependency, Redis:** 3 UNRESOLVED SF009; 0 PASS, FAIL,
  or infrastructure errors. None completed the schedule.

In the experimental runs, Delete cannot finish while StaleFill holds Set.
The client's normal two-second socket timeout/retry introduces another
publication candidate; reports record `multiple_publication_candidates` and
`write_probe_failed`. No completed invalidation/release/verification sequence
was observed. These UNRESOLVED outcomes are retained and do not prove library
correctness. The scheduler, oracle, attribution rules, and client retry policy
were unchanged.

A [separate real-Redis integration test](../../integration/gocache/testdata/upstream/redis_integration_test.go)
releases Set before awaiting Delete, permitting serialization. It passes under
`-race`, observes successful `SET(V1) → DEL → SET(V2)`, confirms V2 in Redis,
and checks a final Get without another loader call. It invokes the same
loader/update/Delete/read flow directly through Go calls, without the HTTP
scheduler. This is a library invariant check on a local modified dependency,
not a StaleFill PASS or a released upstream fix.

## Evidence and verification

[The complete run summary](gocache-upstream-regression/summary.json) retains
all nine outcomes, including all three unresolved runs, baseline results,
findings, stages, and timings. No outcomes were retried or discarded.
[Provenance](gocache-upstream-regression/provenance.json) records source and
binary hashes, dependency identities, server versions/image IDs, and checks.
Representative reports are available for
[Redis original](gocache-upstream-regression/redis-original.json),
[Valkey original](gocache-upstream-regression/valkey-original.json), and
[Redis experimental](gocache-upstream-regression/redis-experimental.json).

Retained test logs cover [original regressions](gocache-upstream-regression/original-regression.log),
[current-master regressions](gocache-upstream-regression/current-regression.log),
[fixed regressions](gocache-upstream-regression/fixed-regression.log),
[the fixed library suite](gocache-upstream-regression/fixed-lib.log), and
[the real-Redis invariant](gocache-upstream-regression/fixed-redis-invariant.log).
The fixed library's full race suite and `go vet` passed, as did StaleFill's
real-server race checks, fixture module verification, and nine Python harness
tests in normal and optimized mode.

A separate [original-library suite run](gocache-upstream-regression/original-existing-lib.log),
excluding the new tests, failed in unchanged `TestLoadableGetTwice`. Its mock
always returns a miss even after Set; a background publication completing
between the two Gets exposes another loader call. This existing-suite failure
is retained separately from the deterministic new regression.

[SHA256SUMS](gocache-upstream-regression/SHA256SUMS) covers the compact evidence
set. Machine-local source prefixes in logs are normalized to `<upstream>`.
These finite local checks establish the stated behavior, not universal cache
correctness, independent adoption, or maintainer confirmation.
