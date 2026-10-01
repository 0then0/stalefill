# gocache external validation

This fixture uses `eko/gocache` **lib/v4.4.0**, unmodified, with the Redis store
from the same commit, `515e65d2cd170b9ba807c64f9b86ed71f6842754`. The store is a
separate upstream Go module, so its version is a commit-derived pseudo-version.
There are no `replace` directives or vendored patches. The root StaleFill module
has no new dependencies.

`GET /item` calls `LoadableCache[string].Get`. Its loader reads a separate,
mutex-protected authoritative value, initially V1. `PUT /item` accepts
`{"value":"V1"}` or `{"value":"V2"}`, updates that store, then calls ordinary
`LoadableCache.Delete`. `GET /authoritative/item` bypasses the cache. `/health`
and `/diagnostics` expose readiness and passive metadata.

The single go-redis client connects only to the StaleFill proxy, including the
upstream library's background setter. RESP2 is explicit for the recorded run;
`--resp 3` is also available. The client uses its normal pool and retry policy;
two-second socket deadlines are safety guards. No loader, reader, writer or
prepare handler waits for publication. `Close` drains only during process
shutdown, after the CLI invocation. The client hook records metadata in memory;
it neither intercepts values nor coordinates or rewrites commands. Metadata
collection has some overhead and can perturb local scheduling.

## Reproduce

Run from a source checkout of StaleFill v0.3.0 or newer. Requirements: Go
1.26+, Python 3.10+, Docker, and a disposable local server.

```sh
mkdir -p bin
go build -o bin/stalefill ./cmd/stalefill
(cd integration/gocache && go mod verify && go build -o ../../bin/gocache-fixture .)

docker run -d --rm --name stalefill-gocache-redis \
  -p 127.0.0.1::6379 redis:8.10.2-alpine \
  redis-server --save '' --appendonly no
docker exec stalefill-gocache-redis redis-server --version
docker port stalefill-gocache-redis

# Substitute the actual loopback port printed above.
python3 integration/gocache/validate.py \
  --upstream 127.0.0.1:PORT --server 'Redis 8.10.2' \
  --binary bin/stalefill --fixture bin/gocache-fixture \
  --output bin/gocache-redis-validation --runs 3 --resp 2 --expect-sf001

docker stop stalefill-gocache-redis
```

Dependencies stay in the fixture module and its checked-in `go.sum`; the Go
build downloads missing modules normally. The output directory must be new
so earlier evidence is preserved. For Valkey, substitute
`valkey/valkey:9.1.2-alpine`, `valkey-server`, its actual port and version,
and a new output directory.

`--expect-sf001` makes this a positive control of StaleFill's detector: every
requested run must finish the complete schedule with `FAIL SF001`, including
stable miss and publication attribution. PASS, UNRESOLVED, infrastructure
errors and incomplete batches fail validation. Reports are retained before
this check. Omit the option when exploring another implementation whose
outcome is unknown. Three runs check startup, baseline publication joins and
cross-run cleanup; they are not a statistical estimate.

## Baseline and retained artifacts

The driver first runs a sequential doctor preflight. An external HTTP
write-probe observer uses read-only upstream Redis observations to await the
initial background publication, repeats ordinary application GET until a
Redis hit occurs without loading, then executes ordinary PUT V2 and checks
deletion. The doctor's final GET must return V2. Failed preflight stops the
batch. These observations run only during baseline, outside the application.

Each race uses direct application HTTP URLs, the CLI's own baseline, and a
fresh application and CLI process. Previous applications complete Close/drain
before another starts. The disposable server is reused; prepare resets the
selected key through ordinary application deletion. There are no retries of
outcomes or race sleeps. The driver waits for startup readiness before
invoking the CLI. StaleFill v0.3 joins modeled baseline miss publications
before mutation and prepare and records early race reader completion.

Output includes every CLI report and configuration, passive application
metadata after CLI return and shutdown, stderr, doctor evidence and a summary
of outcomes, schedule progress and timings. Diagnostic transport/JSON failures
are recorded separately and preserve the CLI verdict. Unexpected errors
propagate; partial summaries retain received verdicts and distinguish requested
runs from actual fixture processes. Baseline checks also work under Python `-O`.

Client-hook SET attempts do not timestamp wire arrival. HTTP handler return
does not timestamp client body consumption. Diagnostic timings are not
benchmarks. See the [historical v0.2 case](../../docs/cases/gocache-v4.4.0.md)
for its early-reader and baseline-publication limitations, and the
[v0.3 validation](../../docs/cases/gocache-v4.4.0-v0.3.md) for detached scheduling
results. Reproducing v0.2 behavior requires a v0.2 binary built from that tag;
building the current checkout produces the current scheduler.

## Harness regression tests and CI

These standard-library tests need no Redis server or extra dependencies:

```sh
python3 -m unittest discover -s integration/gocache -p 'test_*.py'
```

They exercise the baseline observer with replaced transport/process I/O,
required operations under optimized Python, result retention after diagnostic
failures or interrupted batches, and the positive control's acceptance rules.
CI runs the same pinned unmodified fixture against disposable Redis with
`--runs 3 --resp 2 --expect-sf001` and retains all reports as artifacts.

## Upstream regression and experimental fix

The [case study](../../docs/cases/gocache-upstream-regression.md) records a
deterministic library regression, a local experimental patch and real-server
results. The [upstream reproduction guide](upstream/README.md) explains how
to run the tests in isolated copies of the pinned source. Test sources live
under `testdata/`, which Go excludes from normal package discovery.

The experimental patch serializes Delete with an in-flight Set. Delete cannot
finish while StaleFill holds that Set, so its retained StaleFill batch is
UNRESOLVED. Correctness evidence for the patch comes from the library invariant
tests and a separate Redis integration check that releases Set before awaiting
Delete. Do not use `--expect-sf001` to evaluate the experimental fix.
