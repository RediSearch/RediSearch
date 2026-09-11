# Reply-buffer integration (MOD-18503)

Search serializes pipeline rows immediately into Redis reply buffers. A single
`SearchResult` serves the whole row loop: clearing it releases field values but
retains row capacity. The reply wrapper also retains its collection stack and
string scratch across rows. Query reply state holds serialized bytes and final
metadata instead of an array of retained result objects.

Background SEARCH, AGGREGATE, and HYBRID share that loop. Foreground execution
keeps its separate streaming path and command-local result buffering for
FAIL/OOM discard semantics. It never creates a Redis reply buffer.
Coordinator SEARCH still needs its ranking heap to determine global result order;
it serializes and releases ranked rows during reduction. Reply callbacks assemble
response envelopes, timeout warnings, and profile information around the buffered
rows. The representation and ordering of client-visible RESP2/RESP3 replies remain
unchanged.

## Redis dependency

This integration requires both APIs from
[redis/redis#15775](https://github.com/redis/redis/pull/15775), with blocked-client ownership:

```c
RedisModuleCtx *RedisModule_GetReplyBufferContext(RedisModuleBlockedClient *bc);
int RedisModule_ReplyWithBufferedReply(RedisModuleCtx *destination,
                                       RedisModuleCtx *buffer);
```

The blocked client owns buffers through its final callbacks. Redis controls
buffer allocation and cleanup; Search frees only the wrapper's scratch. Creation
requires the Redis lock; serialization and moves follow the destination's normal
threading rules. Every moved fragment consists of complete elements with no open
postponed collections. A successful move empties the source and updates wrapper
counts together.

The API is unreleased. Search rejects loading when either API is absent, and the
shared CI dependency builds upstream commit `52d8aad57586b0d5072c23ff2980c2739a42155c`,
including PR, merge-queue, and periodic validation, manual tests, and benchmarks.
There is no fallback for older cores. Upstream approval, merge, and a supported
packaged core version remain prerequisites for landing this change.

## Lifetime and timeout ownership

Each blocked query or cursor read creates a new buffer on the main thread using
that cycle's client protocol. A cursor never reuses the previous client's Redis buffer.

FAIL timeout callbacks return the error without reading the worker's buffer. The
worker can finish cleanup because Redis retains the buffer until the blocked
handle's final cleanup. Normal completion moves the rows once, and discarded
buffers are released by Redis.

RETURN_STRICT retains the existing claim/completion handshake. After the worker
finishes, eligible pipeline suffixes are drained directly into the same serialized
buffer, preserving prefix order and the remaining result budget. This change does
not implement the concurrent drain protocol or remove timeout-callback waits;
that dependency remains tracked by MOD-17486. It introduces no per-row lock or
separate allocation ownership protocol.

## Validation

Search adds value-reuse, protocol-changing cursor, and timeout/disconnect tests
inside open reply collections. Existing timeout,
profile, cursor, hybrid, disconnect, and index-drop suites exercise the shared
paths. PR CI must include standalone, coordinator, and sanitizer lanes against the
pinned Redis revision. Performance comparisons and flamegraph evidence required
for graduation of MOD-18503 remain separate from the original PoC artifacts.
