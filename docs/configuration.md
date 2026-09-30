# Configuring an application

StaleFill needs a selected cache key, HTTP endpoints that read and change its authoritative state, and an independent endpoint that confirms the new state without using the selected cache entry. Run it against an isolated integration fixture.

## Connect the application

1. Start Redis or Valkey at `redis.upstream`.
2. Configure the application's Redis client to use `redis.listen` instead of the server address. Reader and writer operations need independent Redis connections; an ordinary connection pool can provide them.
3. Expose the five HTTP operations below and make `prepare` repeatable.
4. Run `stalefill doctor --config stalefill.json` to check the sequential baseline, then `stalefill test --config stalefill.json` to run the race.

The proxy runs only during `doctor` and `test`. Your application must start without an immediately available Redis connection, or retry that connection when the proxy starts.

## File format and Redis addresses

Configuration files contain one JSON object with `version: 1`. Unknown fields are rejected. The same configuration works for supported string, hash and transactional hash fills.

Use [`examples/stalefill.json`](../examples/stalefill.json), or generate it with `init`.

- `redis.listen` must be a literal loopback IP. `redis.upstream` defaults to local access; a remote host requires `allow_remote: true` in the file.

## Scenario selection

- `scenario.key` selects an exact key. Alternatively use an anchored Go RE2 `key_regex` of at most 512 bytes. A schedule matching multiple concrete keys is UNRESOLVED.
- `scenario.type` is `stale_fill_after_invalidation` for old data returning after an update, or `negative_cache_resurrection` for an old missing-record observation returning after creation.
- `scenario.timeout` is a Go duration such as `10s`. It must be greater than zero and at most `5m`; it bounds the entire invocation, including baseline.

## HTTP probes

- `prepare` restores the old authoritative state and invalidates the target through the application. It runs before baseline and again before the race. StaleFill never creates Redis mutations itself.
- `write` changes the authoritative state and invalidates the selected cache key through the application. Its successful HTTP response must complete before the held fill is released.
- `read.assert` describes the old baseline API observation; `verify.assert` describes the new one. A protected in-flight read may refresh its response before it finishes. Their `json_path` must match, and their expected values must differ.
- `authoritative` independently confirms the new state while the old fill is still held. Its expectation must equal `verify.assert.equals`; its path can differ. Use a real authoritative endpoint for your application's contract.
- Every probe supports `method`, `url`, `headers`, a JSON body in `json`, and required exact `status`. Assertions support `$`, `.field`, and `[index]`. Final observations compare both status and JSON value against the configured old and new states. Transport errors, unexpected statuses, missing fields and invalid JSON never prove staleness. Numeric equality preserves large integers and treats `120` and `120.0` alike.

## Baseline checks

`doctor` runs the sequential `prepare → read → write → authoritative → verify` baseline. It checks listener availability, Redis reachability, HTTP assertions, and target string/hash reads, fill and invalidation traffic through the proxy. It **executes the configured application writes**, but never holds a command or runs concurrent race probes. Use disposable fixtures for both commands.

`test` refuses to arm the barrier when baseline is broken or target traffic is absent. Background activity on the selected key is not supported: isolate the fixture. More than one matching fill before release produces UNRESOLVED instead of choosing one arbitrarily.

## Negative caching

For negative caching, use [`examples/negative.json`](../examples/negative.json). It seeds a missing record, creates it in the write probe, and asserts `$.exists == true`; stale `false` produces **SF002**. For an API that returns 404 for a missing record and 200 for an existing one, set `read.status` to 404 and `verify.status` to 200. A final 404 with the configured old JSON observation is FAIL SF002 after the complete schedule. The in-flight read may finish with either the old or new configured status.

## Choosing the HTTP observation

Assert a value that captures the application's correctness requirement, such as a record version or an existence flag. `read` defines the old state and `verify` defines the new state. `authoritative` must confirm that same new value through a cache-independent path.

For example, the bundled positive-cache configuration changes `$.version` from `1` to `2`. The negative-cache configuration changes `$.exists` from `false` to `true`. A successful final read must match both the expected status and extracted value. A mixed old/new pair is inconclusive.

`prepare` and `write` require successful status checks and may also include assertions. Assertions are required for `read`, `authoritative` and `verify`. URLs must use HTTP or HTTPS without embedded user credentials. Use `headers` when authentication is required and keep private configuration files out of version control.

See [supported command patterns](architecture.md#supported-cache-fills) before connecting an application, and [results](results.md) to interpret incomplete or unsupported schedules.
