# Architecture and supported behavior

StaleFill combines a Redis/Valkey TCP proxy, an HTTP probe runner, an event scheduler and a reporter. The application uses its ordinary Redis client with the proxy address. Redis executes the application's original commands; the proxy controls when a selected publication is forwarded.

## Execution model

Before the race, the runner performs `prepare → read → write → authoritative → verify` sequentially. It checks HTTP assertions and observes the selected key's read, fill and invalidation through the proxy. A failed baseline prevents the race from starting.

After preparing the fixture again, the runner starts the reader. A cache miss enables interception of the subsequent old fill. With that fill held, it starts the writer and waits for all three release conditions:

- An independent DEL/UNLINK has received its upstream reply.
- The HTTP write has completed successfully.
- The independent authoritative probe confirms the new state.

The proxy then forwards the held fill. Once publication and the reader complete, the final cache-backed HTTP probe checks the configured observation. Transitions depend on observed events, not sleeps. Timeouts bound the invocation.

Each application connection has its own upstream connection, preserving authentication and selected database. Holding a fill on one connection allows the writer to proceed on another. Cancellation closes probes, sockets and the listener; a canceled held fill is not forwarded during cleanup.

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
