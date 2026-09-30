# Landscape and name check

Research date: 2026-09-30. This records a bounded search, not a claim that no similar code exists anywhere. Research preceded implementation; the specific CacheProof repository was located during subsequent source verification after exact-name searches surfaced unrelated namesakes.

## Product overlap

[Shopify Toxiproxy](https://github.com/Shopify/toxiproxy) is a mature TCP network-condition proxy with latency, disconnect and other toxics. It can delay a connection but does not expose Redis-key-aware stale-fill orchestration and an application consistency oracle. StaleFill therefore adds no network toxic framework.

[CacheProof, balyakin/cache-proof](https://github.com/balyakin/cache-proof) uses a Redis-aware proxy and HTTP probes, so the transport and packaging overlap. Its documented scenarios are baseline, deterministic random misses, Redis unavailable and cold cache; policy findings cover TTL, value size, commands and bypassed proxy. Its question is whether the application's cache is disposable. StaleFill instead holds a real old fill before forwarding, waits for a successful invalidation plus completed authoritative write, releases the fill and compares final API observations. No CacheProof outage/cold-cache/TTL scenarios are implemented.

[RedFI](https://openfip.github.io/redfi/) and [red-monkey](https://github.com/Toyota-Connected-India/red-monkey) understand command types, but document delay, empty/error replies and disconnections rather than event-gated cross-connection fill/invalidation schedules with an API oracle. Semantic interception alone is not the distinction; the forced ordering and final assertion are.

[Jedis-Mock](https://github.com/fppt/jedis-mock) offers command interception, test-proxy mode and an extensive Redis/Valkey mock. It is a useful foundation for bespoke tests, not a documented standalone HTTP-probe stale-resurrection scenario product. It is the closest adjacent building block found. We do not claim that its interceptor cannot be programmed to implement the same schedule.

[Microsoft Coyote](https://github.com/microsoft/coyote) systematically explores and replays concurrent .NET programs using instrumentation. StaleFill does not instrument application concurrency or enumerate schedules: it controls a known Redis boundary schedule from outside the application.

No mature tool with practically the entire requested product model was found in the examined sources. This justified proceeding with a small independent v0.1.

## Protocol and clients

The [Redis RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) distinguishes ordinary request/reply framing from RESP3 pushes and streaming types. StaleFill preserves bounded frames, supports non-streaming RESP3 replies for modern handshakes and explicitly excludes asynchronous modes.

The [Redis pipelining documentation](https://redis.io/docs/latest/develop/using-commands/pipelining/) explains clients sending several commands before receiving replies. This drove a reader/worker split and bounded queues: all ordinary commands and replies stay ordered on each connection, and a held command cannot block another connection. Serial forwarding reduces pipeline throughput but changes neither ordinary reply framing nor command order.

The [go-redis repository](https://github.com/redis/go-redis) documents protocol selection and modern handshakes. The [official redis-py cache-aside example](https://redis.io/docs/latest/develop/use-cases/cache-aside/redis-py/) uses HGETALL/HSET, Lua-backed single-flight and transactional pipelines. It was evaluated as a real-world candidate but deliberately not rewritten to fit v0.1: its cache path falls outside the supported GET/SET scenario. No third-party application's race outcome or broad client interoperability is claimed. The bundled integration uses real TCP, HTTP and Redis/Valkey with an in-memory authoritative store.

## Go and name

The official [Go download feed](https://go.dev/dl/?mode=json) returned Go 1.27.1 and Go 1.26.8; the local toolchain is Go 1.27.1. [Go release policy](https://go.dev/doc/devel/release) supports the two latest major releases. Minimum Go 1.26.0 keeps the project on a supported line without requiring new 1.27 APIs. Standard library TCP, context, HTTP, JSON, regexp, channels and the race detector suffice; no dependency is introduced.

Exact StaleFill searches in general web, GitHub repository search and pkg.go.dev did not reveal a mature conflicting developer tool. GitHub's repository search returned the pre-existing empty target `0then0/stalefill`, which is already this workspace's origin. The Go module therefore uses `github.com/0then0/stalefill`; no second repository or guessed owner is created. Searches for CacheProof initially returned unrelated AST/build-cache projects; the Redis tool is named `cache-proof` on GitHub.

## Scope decision

StaleFill combines command-aware interception, event barriers, configured HTTP orchestration and an observable stale-state assertion. It does not add load benchmarks, outage simulation, cache monitoring, database interception, comprehensive protocol emulation or exhaustive consistency proofs. Additional mitigation classes (generation fencing, versioned keys, write-through and authoritative version checks) are context-dependent, not universal prescriptions.
