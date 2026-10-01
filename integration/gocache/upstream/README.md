# Reproducing the gocache publication regression

These tests target `eko/gocache` `lib/v4.4.0`, commit
`515e65d2cd170b9ba807c64f9b86ed71f6842754`. They demonstrate stale publication
after Delete and check a local experimental fix. See the
[case study](../../../docs/cases/gocache-upstream-regression.md) for results
and the patch's consistency boundary.

Requirements: a POSIX shell, Go 1.26+, curl, tar, and patch. Tests use upstream's
existing dependency manifests; no dependency additions are required. The Go
sources live in `integration/gocache/testdata/upstream/`, which Go excludes
from normal package discovery.

## Deterministic test

Run from the StaleFill repository root. Download the pinned source into a
new temporary directory and copy the test into both original and experimental
versions:

```sh
review_repo=$PWD
review_root=$(mktemp -d /tmp/stalefill-gocache-review.XXXXXX)
mkdir "$review_root/original"
curl --fail --location \
  https://codeload.github.com/eko/gocache/tar.gz/515e65d2cd170b9ba807c64f9b86ed71f6842754 \
  --output "$review_root/source.tar.gz"
tar -xzf "$review_root/source.tar.gz" --strip-components=1 -C "$review_root/original"
cp -R "$review_root/original" "$review_root/fixed"
cp integration/gocache/testdata/upstream/loadable_regression_test.go \
  "$review_root/original/lib/cache/"
cp integration/gocache/testdata/upstream/loadable_regression_test.go \
  "$review_root/fixed/lib/cache/"

# Expected exit 1: post-Delete Get returns V1 instead of authoritative V2.
(cd "$review_root/original/lib" && go test -mod=readonly -race -v -timeout 30s \
  -run '^TestLoadableDeleteDoesNotResurrectInFlightPublication$' ./cache)

patch -d "$review_root/fixed" -p1 \
  --input "$review_repo/integration/gocache/upstream/experimental-fix.patch"
# Expected exit 0: all four regression cases pass.
(cd "$review_root/fixed/lib" && go test -mod=readonly -race -v -timeout 30s \
  -run 'TestLoadable(DeleteDoesNot|MutationDoesNot)' ./cache)
(cd "$review_root/fixed/lib" && go vet -mod=readonly ./...)
(cd "$review_root/fixed/lib" && go test -mod=readonly -race -timeout 120s ./...)
```

The first command is an intentionally failing reproducer; it should not be
used as a success-only shell step. The additional tests cover queued Delete,
queued Set(V2), and an overlapping loader. Cleanup releases controlled gates
and waits for Close/drain. Synctest observes the candidate's channel waits;
Mutex-based implementations need adapted orchestration.

For an existing pinned upstream checkout, copying just
`loadable_regression_test.go` into `lib/cache/` and running the first test is
sufficient. StaleFill and a Redis server are not needed for that test.

## Real-Redis integration check

Create an isolated copy of the fixture module and explicitly replace only its
local gocache library dependency:

```sh
mkdir "$review_root/fixture-fixed"
cp integration/gocache/main.go integration/gocache/go.mod integration/gocache/go.sum \
  "$review_root/fixture-fixed/"
printf '\nreplace github.com/eko/gocache/lib/v4 => %s\n' "$review_root/fixed/lib" \
  >> "$review_root/fixture-fixed/go.mod"
cp integration/gocache/testdata/upstream/redis_integration_test.go \
  "$review_root/fixture-fixed/"

# Substitute the port of a disposable local Redis server.
(cd "$review_root/fixture-fixed" && REDIS_TEST_ADDR=127.0.0.1:PORT \
  go test -mod=readonly -race -v -timeout 30s -run TestLoadableRedisPostDelete .)
```

The [fixture guide](../README.md#reproduce) explains disposable Redis setup.
This check releases old Set while Delete can be pending, then awaits Delete.
It checks actual `SET → DEL → SET` completion, V2 in Redis, and a final cache
hit without reloading. A test PASS here is separate from StaleFill's schedule
verdict. The experimental dependency is not a released upstream fix.

After inspection, remove the temporary source and fixture copies:

```sh
rm -rf "$review_root"
```
