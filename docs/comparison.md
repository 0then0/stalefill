# StaleFill and related testing tools

StaleFill tests a specific cache-aside consistency question: can a delayed old fill become visible after a newer authoritative write and cache invalidation? It controls Redis command ordering and checks an application HTTP observation.

## Network fault testing

[Toxiproxy](https://github.com/Shopify/toxiproxy) introduces TCP network conditions such as latency and disconnects. These tests exercise failure handling and resilience. StaleFill instead waits for semantic events on a selected Redis key before releasing a command.

## Cache disposability testing

[CacheProof](https://github.com/balyakin/cache-proof) uses a Redis-aware proxy and HTTP probes to exercise cache misses, unavailability and cold caches. Those scenarios check whether an application can operate when its cache is disposable. StaleFill checks the ordering of fill and invalidation when Redis remains available.

## Custom Redis tests

[Jedis-Mock](https://github.com/fppt/jedis-mock) provides Redis mocking and command interception for custom tests. StaleFill provides a predefined stale-fill schedule, HTTP orchestration and explicit outcomes against a real Redis or Valkey upstream. A custom interceptor can implement application-specific scenarios beyond StaleFill's supported patterns.

## Instrumented concurrency testing

[Microsoft Coyote](https://github.com/microsoft/coyote) explores concurrent .NET programs through instrumentation. StaleFill operates outside the application and controls a known cache boundary schedule. It does not enumerate application interleavings.

These approaches answer different questions and can be used together. See [supported behavior](architecture.md) to determine whether an application's cache path fits StaleFill's model.
