# Commands and results

## CLI commands

`init` creates a version-1 configuration and refuses to overwrite an existing file. `doctor` checks the sequential baseline. `test` runs that baseline followed by the controlled race. `version` prints the CLI version.

Both `doctor` and `test` accept `--config`, `--report` and `--json`. The default paths are `stalefill.json` and `stalefill-repro.json`. `--json` changes stdout to JSON and still writes the report file.

## Outcomes and reports

```sh
stalefill test --config stalefill.json --report stalefill-repro.json
stalefill test --config stalefill.json --json
stalefill version
```

- **PASS**, exit 0: the complete schedule ran and the configured final invariant holds. For `doctor`, PASS refers only to baseline.
- **FAIL**, exit 1: the schedule ran, new authoritative state was confirmed, and final API observation matches the configured old state.
- **UNRESOLVED**, exit 2: a required event is absent, target traffic bypasses the proxy, the schedule is unsupported or ambiguous, or final value matches neither old nor new.
- **INFRASTRUCTURE_ERROR**, exit 3: invalid config, occupied listener, Redis/HTTP failure, broken baseline, or cancellation.

Stable findings: SF001 stale fill; SF002 negative cache; SF003 missing invalidation; SF004 missing proxy wiring/target traffic; SF005 incomplete schedule; SF006 unsupported/ambiguous schedule; SF007 inconclusive observation; SF008 discarded/rejected/aborted publication; SF009 ambiguous publication attribution; SF100 infrastructure/baseline.

## Report files

Both `doctor` and `test` write a JSON report, including failed and unresolved runs, when the report path is writable and distinct from the config. Config/report collisions, including existing symlink and hardlink aliases, are rejected before probes execute. Reports are replaced atomically with mode 0600 where filesystem permissions are supported.

A report includes the CLI version, outcome, findings, environment metadata, elapsed timings and ordered events. The selected key is represented by a SHA256 hash. `fill_mode`, when present, is `string`, `hash` or `transactional_hash`. Events may include invocation-local numeric miss episode and miss/fill connection identifiers.

Reports exclude Redis values and frames, credentials, HTTP bodies, URLs, cookies and headers. A trace records logical ordering; it is not a replayable application snapshot.

## Diagnosing an unresolved run

- **SF003:** check that the writer invalidates the selected key and that the invalidation completes on an independent connection.
- **SF004:** check the application's Redis address and key selection. Target operations must pass through the proxy.
- **SF005:** inspect the last trace event, application logs and timeout. A missing event prevents completion of the schedule.
- **SF006:** compare the application's commands with the [supported patterns](architecture.md#supported-cache-fills). Also check for multiple selected keys.
- **SF007:** compare the final status/value pair with the configured old and new observations.
- **SF008:** check transaction replies and application errors for a discarded, rejected or aborted fill.
- **SF009:** inspect the reason event for competing reads/misses, multiple publication candidates, an incompatible key/type/order, a transaction started before the miss, an unexpected early reader value, or a publication violating a baseline `cache_publication: "none"` contract. Isolate the publication producer; a TCP connection match alone is insufficient.

For **SF100**, resolve the reported configuration, baseline, connection or HTTP failure before interpreting consistency. A PASS applies only to the configured observation under the completed schedule; it does not establish correctness for other interleavings.

## Reading a trace

Events have monotonically increasing sequence numbers. Use those numbers to establish logical order; elapsed timings are diagnostic. The key barriers are a miss, a held fill, completed invalidation and write, authoritative confirmation, released and completed fill, and final verification. Write completion and invalidation may appear in either order, but both must precede release. `read_completed` timestamps actual probe completion and may precede a held publication; it does not end the miss episode. Baseline publication completion events identify the joins before mutation/prepare. `baseline_no_publication` records closure under an explicit no-fill fixture contract. Reader HTTP errors after a scheduler refusal retain the scheduling finding; independently observed Redis failures remain SF100.

Example reports are provided for string [broken](traces/broken.json) and [protected](traces/fixed.json) applications, and transactional hash [broken](traces/transactional-broken.json) and [protected](traces/transactional-fixed.json) applications.
