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

Requires Go 1.26+, Python 3.10+, Docker, and a disposable local server. Run from
the StaleFill repository root. Use an unchanged v0.2.0 checkout or public binary;
the runner refuses to interpret broken baseline as a completed race.

```sh
git diff --exit-code v0.2.0 -- cmd internal go.mod
mkdir -p bin
go build -o bin/stalefill-v0.2.0 ./cmd/stalefill
(cd integration/gocache && go mod download && go mod verify && go build -o ../../bin/gocache-fixture .)

docker run -d --rm --name stalefill-gocache-redis \
  -p 127.0.0.1::6379 redis:8.10.2-alpine \
  redis-server --save '' --appendonly no
docker exec stalefill-gocache-redis redis-server --version
docker port stalefill-gocache-redis

# Substitute the actual loopback port printed above.
python3 integration/gocache/validate.py \
  --upstream 127.0.0.1:PORT --server 'Redis 8.10.2' \
  --binary bin/stalefill-v0.2.0 --fixture bin/gocache-fixture \
  --output bin/gocache-redis-validation --runs 24

docker stop stalefill-gocache-redis
```

Go dependencies stay in this fixture module and its checked-in `go.sum`. If the
normal Go cache is unwritable, set `GOPATH` and `GOCACHE` to disposable writable
directories before building. No dependency synchronization of other fixtures
is needed. The output directory must be new, to preserve earlier evidence.

The driver first runs an extended **sequential doctor preflight**. An external
HTTP write-probe observer waits for the initial background publication using
read-only upstream Redis observations, repeats ordinary application GET until
an actual Redis hit occurs without a loader call, then executes ordinary PUT V2
and checks deletion. These baseline observations do not run inside the
application and never run during the race. The doctor's final GET must return
V2. A failed preflight stops all race invocations.

Every subsequent `test` uses the normal v0.2 configuration with **direct**
application URLs and its own built-in baseline. Each run starts a new application
and CLI process. A previous application's `Close`/drain finishes before another
starts. The isolated Redis server is reused; ordinary prepare deletes the one
selected key. There are no retries of outcomes or sleeps during the race. The
only driver sleep waits for startup readiness, before invoking the CLI.

Artifacts include every CLI report/config, passive application events captured
after CLI return and after shutdown, stderr, the doctor evidence and a summary
with four outcome counts, last schedule stage, per-run diagnosis and timings.
Optional diagnostic transport/JSON failures are recorded separately and do not
replace the CLI verdict or stop subsequent runs. Unexpected errors propagate
with their traceback; a partial summary retains every received CLI verdict.
`requested_runs` and the actual fixture-process count distinguish interrupted
batches. Baseline checks use explicit conditions, including under Python `-O`;
only expected baseline and transport/JSON errors become observer HTTP failures.
SET attempts are client-hook observations, not wire-arrival timestamps. HTTP
handler return is not claimed to timestamp client body consumption. Read the
case study for how the unchanged runner's early-return branch establishes that
the HTTP probe completed in UNRESOLVED cases.

For the secondary server, use `valkey/valkey:9.1.2-alpine` and `valkey-server`,
verify its actual version, then run the same driver with a new output directory
and `--server 'Valkey 9.1.2' --runs 4`. Neither distribution is expected to remain
constant: this experiment exposes nondeterministic scheduling acceptance.

See [the case study](../../docs/cases/gocache-v4.4.0.md) and its retained evidence.

## Harness regression tests

These standard-library tests need no Redis server or additional dependencies:

```sh
python3 -m unittest discover -s integration/gocache -p 'test_*.py'
```

They exercise the actual baseline observer with replaced transport/process I/O,
check required operations in a separate optimized Python process, and verify
result retention after diagnostic failures or interrupted batches.

## v0.3 detached scheduling validation

The same fixture and driver can validate the local v0.3 implementation. Build
from the current checkout, preserving the pinned dependencies and leaving the
upstream library and application handlers unchanged:

```sh
go build -o bin/stalefill-v0.3.0 ./cmd/stalefill
(cd integration/gocache && go build -o ../../bin/gocache-fixture .)
python3 integration/gocache/validate.py \
  --upstream 127.0.0.1:PORT --server 'Redis 8.10.2' \
  --binary bin/stalefill-v0.3.0 --fixture bin/gocache-fixture \
  --output bin/gocache-v03-redis-validation --runs 24
```

Use a new output directory and the disposable server setup above. For Valkey,
substitute its actual port/server version and use four independent runs. The
extended doctor preflight remains a sequential observer; race probes still use
direct application URLs without sleeps, synchronization hooks or retries of
outcomes. The v0.3 CLI additionally joins its own baseline miss publications
before mutation/prepare, and retains the race reader's early old observation.

The [v0.3 case study](../../docs/cases/gocache-v4.4.0-v0.3.md) records completed
schedules and actual reader completion order. The original v0.2 case study and
its retained artifacts remain unchanged.
