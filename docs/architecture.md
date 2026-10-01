# Architecture and supported behavior

StaleFill combines a Redis/Valkey TCP proxy, an HTTP probe runner, an event scheduler and a reporter. The application uses its ordinary Redis client with the proxy address. Redis executes the application's original commands; the proxy controls when a selected publication is forwarded.

## Execution model

Before the race, the runner performs `prepare → read → write → authoritative → verify` sequentially. It checks HTTP assertions and observes the selected key's read, fill and invalidation through the proxy. After each baseline read/verify, it joins any observed miss with its successful upstream publication reply before moving on. This includes the final verification fill before the next prepare. An explicit `read/verify.cache_publication: "none"` fixture contract closes a baseline miss without a fill; an observed conflicting publication is SF009. This supports negative-only caching without inferring quiescence from HTTP completion. A missing expected publication expires as UNRESOLVED SF005; conflicting association evidence is SF009. A failed HTTP baseline prevents the race from starting.

After preparing the fixture again, the runner starts the reader. A cache miss enables interception of the subsequent old fill. With that fill held, it starts the writer and waits for all three release conditions:

- An independent DEL/UNLINK has received its upstream reply.
- The HTTP write has completed successfully.
- The independent authoritative probe confirms the new state.

The proxy then forwards the held fill. The reader may complete before the fill is held, while it is held, or after release. Its result is retained and consumed once. Once publication and the reader complete, the final cache-backed HTTP probe checks the configured observation. Transitions depend on observed events, not sleeps. Timeouts bound the invocation.

Each application connection has its own upstream connection, preserving authentication and selected database. Holding a fill on one connection allows the writer to proceed on another. Cancellation closes probes, sockets and the listener; a canceled held fill is not forwarded during cleanup.

## Miss episodes and detached publication

Read operation, miss/load episode and cache publication have independent lifetimes. A `MissEpisode` records a confirmed miss for one concrete key, string/hash read family, miss connection, publication command identity and completion flags. The proxy assigns a per-connection command sequence so a previously queued publication cannot acquire attribution merely by reaching the forwarding worker later. Transactions must start after the episode's miss, not just reach EXEC after it.

A successful early HTTP reader must match the configured old status/value. Its completion is recorded when the HTTP probe reads/parses the response, independently of runner consumption. It does not end the association window or change a held barrier's scheduling state. Early transport/status failures stop scheduling; an unexpected early value is SF009. A protected synchronous reader can still refresh after release.

Association uses the exact selected key, preceding confirmed miss, compatible publication shape, command ordering and a single uncontested candidate. A different pooled connection is allowed. Events include numeric `miss_episode`, `miss_connection` and `fill_connection` metadata; these identifiers are local to the invocation and contain no payloads. Competing target reads/misses before the hold, multiple publication boundaries or a publication without a compatible episode produce UNRESOLVED SF009, with a specific event reason. After the hold, writer cache lookups do not enroll in the reader episode, while additional publications still produce SF009. Reader HTTP errors following a known scheduler refusal preserve its finding; independently observed Redis errors remain SF100. Unsupported commands remain SF006; rejected/discarded EXEC remains SF008.

The existing `scenario.timeout` bounds baseline joins and detached association together with the rest of the invocation. Completion never renews the deadline. Absence of a publication remains UNRESOLVED SF005 at timeout, never PASS. A completed reader without a preceding confirmed target miss is SF005 immediately: a later unrelated miss cannot acquire its episode. No polling sleep or additional timeout setting is used.

This model requires an isolated target with one publication producer per modeled miss. Joining the observed baseline publications closes their modeled lifetime before prepare; it does not prove that an arbitrary application has no hidden worker or future retry. A lone unrelated write with the same key/shape/order is indistinguishable without application causality metadata. Do not use such traffic as an attributed fixture. The proxy detects observed conflicts conservatively and never inspects cache values to infer ownership.

## Supported cache fills

```text
String:            GET/MGET miss → SET/SETEX/PSETEX [held]
Hash:              HGET/HMGET/HGETALL miss → HSET [held]
Hash transaction:  miss → MULTI → [DEL|UNLINK] HSET+ [EXPIRE|PEXPIRE]
                        → EXEC [held]
```

Brackets denote optional commands and `HSET+` means one or more HSET commands. Transaction commands must address the same selected key. An optional delete comes first, HSET commands are contiguous, and an optional positive TTL comes last without conditional options. A following TTL command on a standalone HSET passes through normally.

## Protocol and connection limits

Single upstream, TCP, RESP2 plus non-streaming RESP3 scalars, arrays, maps, sets and attributes. `HELLO 3`, AUTH, SELECT and unknown ordinary request/reply commands pass through as original frames. RESP payloads are binary safe and bounded at 16 MiB, nesting depth 32 and 65,536 aggregate entries (maps have two frames per entry).

Command-aware observation covers GET/MGET, HGET/HMGET/HGETALL, SET with options, SETEX/PSETEX, HSET, DEL/UNLINK and EXPIRE/PEXPIRE. A GET/HGET null, an all-null HMGET projection, a selected MGET null, or an empty HGETALL array (RESP2) / map (RESP3) establishes the supported miss. Field names and contents remain opaque; HMGET with a partial hit does not establish a miss.

Standalone HSET is a publication boundary; a following EXPIRE is forwarded normally. Transaction support is limited to `[DEL|UNLINK] HSET+ [EXPIRE|PEXPIRE]`, all on one key, with positive TTL and no TTL options. MULTI and QUEUED replies are checked per connection; EXEC must return a matching successful aggregate before completion is claimed. Transaction retention is capped at 256 commands / 16 MiB of wire frames. DISCARD and aborted/error EXEC produce UNRESOLVED, never a stale finding. WATCH passes through; its null EXEC abort is recognized without modeling optimistic transactions. For SET NX/XX, the final HTTP observation determines correctness even when Redis declines the conditional write.

Connections are independent and can be reused or pooled. Pipelines preserve command/response order: the proxy serially forwards request/reply pairs on each connection, buffering at most 16 pending commands. It does not preserve pipeline throughput. An invalidation behind the held fill on the same connection is SF006; a separate application connection is required. Cross-connection ordering is precisely what the scenario controls.

Lua EVAL/EVALSHA (including read-only variants) passes through when the selected key is absent from valid declared KEYS. Target Lua and malformed key declarations are SF006. Script source, ARGV and script SHA are never interpreted. Lua locks on unrelated keys therefore work without a Lua engine. Redis Functions remain unsupported. HSETEX, hash-field expiration mutations and other unsupported target hash writes remain UNRESOLVED. Non-transactional DEL/HSET publication, multi-key hash transactions, unknown transaction operations, multiple fills and ambiguous regex targets remain UNRESOLVED.

HELLO, AUTH, SELECT, CLIENT SETINFO and CLIENT SETNAME pass through. Pub/sub, MONITOR and CLIENT TRACKING are refused with a protocol error because their unsolicited messages need a different multiplexer. CLIENT REPLY OFF/SKIP are also refused; CLIENT REPLY ON passes through. Inline commands, streamed RESP3, asynchronous pushes, TLS, Cluster and Sentinel remain outside scope. Tested client versions and fixture coverage are listed in [client compatibility](client-compatibility.md).

## Transaction handling

The request reader captures a bounded, immutable snapshot of transaction commands at EXEC or DISCARD. The forwarding worker checks the server's MULTI and QUEUED replies before treating EXEC as a supported publication. Queued reads, writes and deletes do not count as applied operations, so a reader's queued replacement DEL cannot satisfy the writer's invalidation barrier.

A transactional publication completes only after a non-null EXEC aggregate with the expected successful integer replies. Malformed transaction boundaries retain the server's transaction state when appropriate; EXECABORT clears it. The proxy does not simulate Redis transactions or synthesize their replies.

## HTTP correctness boundary

Redis fields, values and serialized records remain opaque. The configured HTTP status/value pairs define old and new application states. StaleFill confirms the new authoritative state before release and reports stale behavior only after the supported schedule completes and the final API observation matches the old state.

The protected examples validate cached versions against an authoritative snapshot and repair stale entries on read. They can physically publish an old entry while still protecting the HTTP observation. Their in-memory authoritative store illustrates the contract; it is not a production database or a universal mitigation.

## Integration scope

Use disposable fixtures with isolated selected keys. The configured probes execute application writes, including during `doctor`. StaleFill creates no synthetic Redis mutations. Listener addresses are restricted to literal loopback IPs; remote upstreams require explicit opt-in.

The tool checks one modeled schedule and one configured observation. It does not discover races, intercept database protocols, enumerate interleavings, prove linearizability, replay traces, simulate cache outages or perform load testing. See [client compatibility](client-compatibility.md) for tested versions and [configuration](configuration.md) for application requirements.
