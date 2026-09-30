# Contributing

Use Go 1.26 or newer. There are no external Go modules to install. Keep changes focused on deterministic cache-aside schedules and maintain binary-safe forwarding, secret-free reports and cancellation behavior.

Run `gofmt -w cmd internal`, `go vet ./...`, `go test ./...`, and `go test -race ./...` before submitting a change. Set `REDIS_TEST_ADDR` to a disposable real Redis or Valkey instance to enable real-server tests. These tests mutate the demo key `product:42` and protocol-test keys under `stalefill:protocol:*`; never point them at a shared or production cache.

Every new schedule must specify observable transitions, an API consistency oracle, timeout outcomes and at least one vulnerable and one protected implementation. Protect a credible behavior with a regression test. Sleeps must not decide schedule transitions.

Use Conventional Commits in English. The repository uses Apache-2.0. Do not add dependencies or architectural boundaries without discussing the concrete need.

Release maintainers run `scripts/release.sh v0.1.0` after checks pass. A pushed version tag runs the release workflow, which verifies Redis/Valkey tests, creates five archives and SHA256SUMS, and publishes a GitHub Release. A trace is diagnostic metadata, not a replayable application snapshot; do not commit private configs or payloads.
