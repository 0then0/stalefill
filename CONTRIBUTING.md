# Contributing

StaleFill's core is a dependency-free Go module. Contributions should preserve deterministic scheduling, binary-safe forwarding, report privacy and cancellation behavior.

## Build from source

Use Go 1.26 or newer from a source checkout:

```sh
go build -o bin/stalefill ./cmd/stalefill
go build -o bin/stalefill-demo ./cmd/demo
```

The [quick start](README.md#quick-start-from-source) runs the vulnerable and protected demo applications. Real-client fixtures have separate dependency manifests under `integration/`; follow [client compatibility](docs/client-compatibility.md) to install and validate them.

## Run checks

```sh
gofmt -w cmd internal
go vet ./...
go test ./...
go test -race ./...
REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
```

Without `REDIS_TEST_ADDR`, tests that require a real server skip explicitly; protocol-fixture and scheduler tests still run. Set it to a disposable Redis or Valkey instance to enable real-server coverage. Tests mutate `product:42` and keys under `stalefill:protocol:*`. Client fixtures also use `lock:product:42`. Do not use a shared cache.

Python fixture error-handling tests run with `integration/redis-py/.venv/bin/python -m unittest discover -s integration -p 'test_*.py'` after installing the pinned fixture dependencies.

Core CI checks Go 1.26 and 1.27 against Redis and Valkey. The separate client workflow checks all three pinned clients in RESP2 and RESP3, with 24 repetitions per broken/protected combination. The gocache positive control runs three complete `FAIL SF001` schedules against an unmodified pinned library and retains its reports. See the [fixture guide](integration/gocache/README.md#harness-regression-tests-and-ci) for its dependency-free Python tests and reproduction commands. Run the checks relevant to your change and include their results in the pull request.

## Scheduling and regression tests

A new schedule must define observable transitions, an HTTP correctness observation, timeout outcomes, and vulnerable and protected examples. Use events to select interleavings; sleeps must not determine transitions. Add regression tests for meaningful behavior and retain explicit unresolved outcomes for unsupported or ambiguous traffic.

See [architecture](docs/architecture.md) for publication boundaries and transaction handling. Keep raw keys, cache contents, HTTP payloads, credentials, Lua source and ARGV out of reports and committed traces. Traces are diagnostic metadata, not replayable application snapshots.

Keep changes focused. Discuss the concrete need before adding a dependency or architectural boundary. Use Conventional Commits in English. Contributions use the repository's Apache-2.0 license.

## Release packaging

After the required checks pass, build the current version's archives:

```sh
scripts/release.sh v0.3.1
python3 scripts/verify_release.py dist
```

The script writes Linux amd64/arm64, macOS amd64/arm64 and Windows amd64 archives plus `SHA256SUMS` to `dist/`. Archives contain the CLI, configuration examples and public documentation. Demo and client fixture programs are built from the source checkout.

Building archives does not publish a release. The version-tag release workflow requires client compatibility and core real-server checks before building archives and publishing a GitHub Release. Keep the CLI version, changelog, release notes and packaging script aligned when preparing a new version.
