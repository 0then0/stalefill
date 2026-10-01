# Client compatibility

The StaleFill 0.2.0 compatibility checks below were completed on 2026-09-30 with Go 1.27.1, Python 3.13.15 and Node 24.21.0 on macOS arm64. Tested servers: Redis 8.10.2 (`redis:8.10.2-alpine`) and Valkey 9.1.2 (`valkey/valkey:9.1.2-alpine`), disposable, persistence disabled. These results describe the included fixtures, not exhaustive support for each client's APIs.

## Tested clients

Each fixture implements the equivalent official-style pattern: HGETALL, a token-owned Lua lock on `lock:product:42`, an authoritative snapshot, MULTI / DEL / HSET / EXPIRE / EXEC, lock release, and independent authoritative-write / DEL. Cache data is a hash with a five-second TTL. No sleeps or StaleFill hooks coordinate the race. A local in-memory authoritative store and HTTP endpoint provide the oracle.

- **redis-py 8.1.0**, pinned in `integration/redis-py/requirements.txt`. Uses the ordinary connection pool and transactional `pipeline()`. Registered scripts exercise EVALSHA, SCRIPT LOAD and retry behavior. Tested in RESP2 and RESP3; installed client default is RESP3.
- **go-redis 9.22.0**, pinned in a separate `integration/go-redis/go.mod` / `go.sum`. Uses the ordinary connection pool, `TxPipeline` and `Script.Run` (EVALSHA with EVAL fallback). Tested in RESP2 and RESP3; client default is RESP3. The fixture imports go-redis, not StaleFill's RESP client.
- **node-redis 6.3.0**, pinned in `integration/node-redis/package.json` / `package-lock.json`. Uses `createClientPool`, `pool.execute`, `multi().exec()` and EVAL. A read owns a pooled connection through publication; the writer can obtain another connection. Tested in RESP2 and RESP3; the installed 6.3.0 default is RESP3.

For **every client on each server in each RESP mode**:

- Broken implementation: **24 / 24 FAIL SF001**.
- Protected implementation: **24 / 24 PASS**.
- Schedule misses: **0**. Logical event order is identical across all 24 repetitions within each combination.

This is 576 transactional schedules: 3 clients × 2 servers × 2 protocols × 2 modes × 24. A further 192 standalone hash schedules use redis-py HSET / EXPIRE without MULTI, again 24 / 24 expected results for each server/protocol/mode and zero schedule misses. The original Go demo separately preserves positive and negative-cache coverage with 24 repetitions per combination on both servers and the protocol fixture.

Protected mode compares the cache version against an authoritative snapshot before serving it and repairs a stale hash on read. It protects the configured HTTP observation; the held old transaction can still physically publish stale bytes. This trades an authoritative lookup for the invariant and is not a universal mitigation.

## Connection and error handling

HGETALL returns an empty array in RESP2 and an empty map in RESP3 on both tested servers. Ordinary client handshake settings, connection pools, pipelined transactions and reconnect behavior between CLI invocations are retained. HELLO, CLIENT SETINFO, CLIENT SETNAME and SELECT pass through. AUTH pass-through is covered by the local protocol fixture; authenticated real-server sessions were not separately tested.

Real-server regressions cover the EXEC forwarding barrier, fill-internal DEL isolation, ordered OK/QUEUED/aggregate replies, a trapped same-connection invalidation, disconnect isolation, cancellation of a held EXEC, DISCARD, queue errors, runtime EXEC errors and a real WATCH abort. Target Lua remains SF006 even on an error response; unrelated NOSCRIPT is left to the client's normal script fallback. Reports expose only logical events, command names, a target hash and a bounded fill-mode enum.

## Reproduce from a source checkout

The following commands use a POSIX shell and require Go 1.26+, Python 3.13, Node 24 and npm. Start a disposable Redis instance using the [quick start](../README.md#quick-start-from-source), or start Valkey with `valkey/valkey:9.1.2-alpine` and `valkey-server --save '' --appendonly no`. Fixtures need their own installed dependencies; the core module has none. Use a disposable server because these commands mutate `product:42`, `lock:product:42` and protocol-test keys. With an existing Python environment, use its interpreter instead of creating another environment.

```sh
go build -o bin/stalefill ./cmd/stalefill
(cd integration/go-redis && go build -o ../../bin/go-redis-fixture .)
python3 -m venv integration/redis-py/.venv
integration/redis-py/.venv/bin/pip install -r integration/redis-py/requirements.txt
(cd integration/node-redis && npm ci --ignore-scripts)

python3 integration/validate.py --upstream 127.0.0.1:6379 \
  --server Redis-8.10.2 --python integration/redis-py/.venv/bin/python
python3 integration/validate.py --upstream 127.0.0.1:6379 \
  --server Redis-8.10.2 --python integration/redis-py/.venv/bin/python \
  --clients redis-py --fill single
```

Repeat on Valkey with its address and `--server Valkey-9.1.2`. The runner keeps a fixture process alive for 24 invocations, checks expected exit codes and finding IDs, and compares logical ordering. It stops at the first failed outcome; it does not turn infrastructure failures into successful repetition evidence. Optional `--output` writes the summary, and `--trace-dir` saves sanitized transactional traces. `--artifact-dir` retains every CLI report, stderr, configuration and fixture log in a new directory, including failed runs. CI uploads these artifacts for each server even when validation fails; trace mismatch errors include both event lists.

## Scope of the compatibility checks

These results cover the bundled cache-aside fixture paths, including ordinary pooling, handshakes, transactional pipelines and unrelated Lua locks. They do not cover every client API, real-server authentication, maintenance pushes, alternate hash schemas, real databases or a third-party application's full invariant.

Protocol and scheduling restrictions apply even when a client version is listed here. See [architecture and supported behavior](architecture.md) for the transaction whitelist, miss detection rules and unsupported connection modes.

The fixtures follow patterns described in the official Redis [redis-py](https://redis.io/docs/latest/develop/use-cases/cache-aside/redis-py/), [go-redis](https://redis.io/docs/latest/develop/use-cases/cache-aside/go/) and [node-redis](https://redis.io/docs/latest/develop/use-cases/cache-aside/nodejs/) guides.
