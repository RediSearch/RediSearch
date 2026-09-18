# Reply-buffer integration (MOD-18503)

Search serializes pipeline rows immediately into Redis reply buffers. A single
`SearchResult` serves the whole row loop: clearing it releases field values but
retains row capacity. The reply wrapper also retains its collection stack and
string scratch across rows. Query reply state holds serialized bytes and final
metadata instead of an array of retained result objects.

SEARCH, AGGREGATE, and HYBRID share that loop unconditionally: every cycle,
foreground or background, serializes into a reply buffer through the same
`Pipeline_SerializeResults`, and finalizes with the same O(1)
`RedisModule_Reply_Buffered` move — a background cycle uses the persistent
per-cycle buffer created at block time and defers the move to its reply
callback; a foreground cycle creates a transient buffer inline, moves it, and
frees it before returning. There is no separate `SearchResult**`
array-buffering path left for FAIL/ReturnStrict/OOM-Fail discard semantics —
those policies discard by not moving the buffer, the same mechanism used for a
mid-stream error either way.
Coordinator SEARCH still needs its ranking heap to determine global result order;
it serializes and releases ranked rows during reduction. Reply callbacks assemble
response envelopes, timeout warnings, and profile information around the buffered
rows. The representation and ordering of client-visible RESP2/RESP3 replies remain
unchanged.

## Redis dependency

This integration requires both APIs from
[redis/redis#15775](https://github.com/redis/redis/pull/15775), with module ownership:

```c
RedisModuleCtx *RedisModule_CreateReplyBufferContext(RedisModuleCtx *ctx);
int RedisModule_ReplyWithBufferedReply(RedisModuleCtx *destination,
                                       RedisModuleCtx *buffer);
```

Buffers are module-owned, not blocked-client-owned: `ctx` can be any context with
a module and a reply target (a command context, a blocked-client callback
context, a thread-safe context bound to a blocked client, or another buffer), and
Redis never frees the result on its own — the module must call
`RedisModule_FreeThreadSafeContext()` explicitly, and a live buffer blocks module
unload. Creation and freeing both require the server lock (the main thread, or a
worker holding the GIL); serialization and moves follow the destination's normal
threading rules. Every moved fragment consists of complete elements with no open
postponed collections. A successful move empties the source and updates wrapper
counts together, but does not free it — the source buffer is still reusable, and
still owned by whoever created it.

Search follows the same "capture under the GIL at block time, release from
free_privdata on the main thread" pattern it already uses for `argv`/`MRCtx`:
buffers are created from the command's own `ctx` right after
`RedisModule_BlockClient` (still on the main thread, before any worker sees the
request), and freed explicitly from each request kind's free_privdata callback
—`QueryRequest_OnFree` for AREQ/HYBRID/cursor cycles, `DistSearchFreePrivData`
for coordinator SEARCH. The latter reads and frees the buffer *before* releasing
its own `MRCtx` reference, because `MRCtx`'s internal refcount teardown (which
the reducer and fan-out-dispatch jobs also hold references into) is not otherwise
guaranteed to run on the main thread — only the request's original blocked-client
reference is.

The API is unreleased. Search rejects loading when either API is absent, and the
shared CI dependency builds upstream commit `7ade4235dd405c3bdbd609739bf761910db6848d`,
including PR, merge-queue, and periodic validation, manual tests, and benchmarks.
There is no fallback for older cores. Upstream approval, merge, and a supported
packaged core version remain prerequisites for landing this change.

## Lifetime and timeout ownership

Each blocked query or cursor read creates a new buffer on the main thread using
that cycle's client protocol. A cursor never reuses the previous client's Redis buffer.

FAIL timeout callbacks return the error without reading the worker's buffer. The
worker can finish cleanup because the buffer stays valid — module-owned, not tied
to the blocked handle — until the request's free_privdata callback explicitly
frees it. Normal completion moves the rows once; discarded buffers are freed
unmoved, from the same free_privdata callback.

Coordinator SEARCH stops serializing discarded replies between complete rows after
a FAIL timeout or disconnect, so the reducer completion wait does not serialize the
remaining payloads. RETURN_STRICT still serializes the retained ranked results.
The terminal background path always unblocks the Redis handle, including after a
timeout or disconnect, so the request's free_privdata callback runs and explicitly
releases its reply buffer.

RETURN_STRICT retains the existing claim/completion handshake. After the worker
finishes, eligible pipeline suffixes are drained directly into the same serialized
buffer, preserving prefix order and the remaining result budget. This change does
not implement the concurrent drain protocol or remove timeout-callback waits;
that dependency remains tracked by MOD-17486. It introduces no per-row lock or
separate allocation ownership protocol.
HYBRID keeps its existing completed-row prefix on timeout; its tail is not drained
by the timeout callback.

## Validation

Search adds value-reuse, protocol-changing cursor, and timeout/disconnect tests
inside open reply collections. Existing timeout,
profile, cursor, hybrid, disconnect, and index-drop suites exercise the shared
paths. PR CI must include standalone, coordinator, and sanitizer lanes against the
pinned Redis revision. Performance comparisons and flamegraph evidence required
for graduation of MOD-18503 remain separate from the original PoC artifacts.
