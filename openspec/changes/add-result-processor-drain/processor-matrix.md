# ResultProcessor Drain implementation matrix

## Status and common rules

This matrix is the implementation plan for the one-processor-per-PR stack. It
records the expected behavior after inspecting the current `Next` state
machines. Details may change inside a processor PR, but a deviation from the
shared rules belongs in the design review.

Common rules:

- Drain continues the result sequence and calls Drain, never `Next`, upstream.
- A result returned from the pipeline entry is complete enough to serialize.
- The in-flight `Next` call keeps any result it already claimed; omitting that
  result from a partial timeout reply is allowed, but leaking it is not.
- Drain never waits for the in-flight call or for background work. It may take
  short local locks to claim state.
- Once a processor returns drain EOF, it cannot later expose another drain
  result. Background producers must observe a local stop/handoff state.
- Drain does not inspect or mutate the `Next` function pointer. Existing
  `Next`-only phase switching remains unless a processor's Drain handoff needs
  shared phase state now.
- Existing implementations stay in their current language. The Rust counter is
  the only production ResultProcessor already implemented in Rust.

`Error` below means the processor has a real path that can produce or propagate
an execution error while creating a serializable drained result. This is why the
API audit recommends an explicit drain error status.

## Production processors

| PR / processor | Current role | Proposed Drain behavior | Local handoff and synchronization | Error and focused tests |
| --- | --- | --- | --- | --- |
| `RP_INDEX` | Source over the query iterator; optional async disk metadata queue | Return EOF without advancing/revalidating the iterator, submitting I/O, polling, or waiting. A result becomes serializable only after `Next` claims it, so iterator and async-read state remain on that path. | No handoff state is needed: Drain always returns EOF and the orchestration contract prevents a new `Next` call. Existing `Next` and async cleanup remain unchanged. | No new Drain error. Test repeated Drain while `Next` is parked inside the iterator and verify the in-flight call still owns its completion. |
| `RP_NETWORK` | Source over shard replies and cursors | Yield rows from the currently admitted reply and replies already queued in the channel; never wait for another reply. Reuse the existing drain-only channel behavior, but make row/reply ownership explicit. | Protect the current reply, row index, and transition to nonblocking channel consumption with a short local lock/state handoff. Cursor cleanup remains deferred to normal request cleanup. | Can observe shard/protocol errors. Test timeout before a reply, between row claim and conversion, queued empty/error replies, RESP2/RESP3, and cursors. |
| `RP_LOADER` | Synchronous field loader | Pull through upstream Drain, load fields under the caller's existing keyspace/GIL context, skip invalid documents, and return only emittable rows. | Loader configuration is immutable. Drain does not update `skippedResults`; if correct reply counts require that write, stop for design review. | Load failures are currently converted to skipped rows, not returned errors. Test deleted/re-indexed documents and explicit/all-fields loading. |
| `RP_SAFE_LOADER` | Buffer, acquire the GIL, load, then yield | Claim any buffered batch without waiting for the background `Next`; load the claimed rows using the drainer's already-held keyspace/GIL context, then yield them. After the batch, continue through upstream Drain. Never enter the background GIL handshake or lock a thread-safe context from Drain. | Protect buffer indices and ownership transfer with a short local handoff. Keep existing `Next` phase switching unless Drain proves it needs a shared phase field. The in-flight loader either owns its current batch or loses the handoff before taking the GIL. | Loading failures remain skipped rows. Test every phase, especially background waiting before/for/while holding the GIL gate and Drain running on the GIL-owning thread. |
| `RP_DISK_ASYNC_LOADER` | External Rust async disk field loader | Yield only rows whose required fields are already loaded and ready. Cancel/abandon pending reads without waiting. An upstream-drained row that would require new disk I/O is omitted rather than returned incomplete. | Provider adds a terminal handoff covering ready, pending, running, and unscheduled work. Buffer ownership must be protected independently of worker progress. | Provider read failures follow its existing row-drop/error policy; confirm during paired change. Test all job states and provider/core teardown ordering. |
| `RP_SCORER` | Scores and may filter rows | Pull through upstream Drain and run the same scoring/filter logic before returning a row. | Scoring context and score-explain scratch state need local synchronization; never hold it across upstream Drain. Drain does not update shared counts or `minScore`; return to design review if the output contract requires either write. | Propagates upstream errors. Test filtered scores, explain-score ownership, and concurrent scorer scratch use. |
| `RP_METRICS` | Writes iterator metrics into the row | Pull through upstream Drain and apply metrics exactly as in `Next`. | No mutable processor state; no local lock expected. | Propagates upstream errors. Compare drained and normal metric rows. |
| `RP_KEY_NAME_LOADER` | Writes the document key into the row | Pull through upstream Drain and write the key. | Immutable local state; no local lock expected. | Propagates upstream errors. Test document metadata ownership and key value lifetime. |
| `RP_VECTOR_NORMALIZER` | Streaming vector-score transform | Pull through upstream Drain and normalize the score and score field. | Immutable normalization configuration; no local lock expected. | Propagates upstream errors. Compare normal and drained scores, including missing/non-numeric fields. |
| `RP_HIGHLIGHTER` | Streaming highlight/summarize transform | Pull through upstream Drain and highlight using the result's retained index data. Drain must not rewind or read the root iterator concurrently; if retained index data is unavailable, preserve the existing no-highlight behavior. | Configuration is immutable. Temporary fragment state remains call-local. | Propagates upstream errors. Test owned index-result retention, missing index data, JSON/hash fields, and concurrent root `Next`. |
| `RP_PROJECTOR` | Evaluates and writes an expression | Pull through upstream Drain and evaluate/write the expression using the same semantics as `Next`. | Protect mutable evaluator scratch (`ExprEval`, cached value, block allocator) locally, without holding the lock across upstream Drain. | Expression evaluation can return an execution error. Test success, evaluation failure, and concurrent scratch ownership. |
| `RP_FILTER` | Evaluates an expression and discards false rows | Repeatedly pull through upstream Drain until a row passes or Drain terminates; clear rejected rows without updating shared query counts. | Same evaluator synchronization as projector. Return to design review if reply semantics cannot tolerate leaving shared counts unchanged. | Expression evaluation can return an execution error. Test several rejected rows, error after rejection, and EOF. |
| `RP_PROFILE` | Times and counts an upstream processor | Time upstream Drain and count each Drain call using concurrency-safe counters, then propagate its status. | Do not hold a profile lock across upstream Drain; atomically accumulate elapsed time/count afterward. | Propagates upstream errors. Test concurrent `Next`/Drain accounting without lost increments. |
| `RP_COUNTER` | Rust sink that consumes all upstream rows | Consume upstream Drain results, count and destroy each one, then return EOF. It intentionally does not yield rows. | Convert the count to concurrency-safe interior state under the shared-access Rust trait. | Propagates an upstream error if the final API supports it. Test concurrent count increments and exactly-once result destruction. |
| `RP_PAGER_LIMITER` | Skips offset rows and limits output | Apply the remaining offset and limit while pulling through upstream Drain. Skipped rows are cleared; yielded rows consume the remaining budget. | Reserve/commit offset and limit under short synchronization without holding it across upstream Drain. Keep existing `Next` phase switching unless the handoff requires otherwise. | Propagates upstream errors. Test handoff in skip and limit phases, zero limit, and an in-flight reserved row. |
| `RP_SORTER` | Top-N buffering reducer | Atomically stop accumulation, finalize the current heap as the partial sorted set, and yield it in normal order. Do not pull more upstream results. This replaces the current timeout-aware `Next` transition. | Add only the local handoff needed to protect heap/pooled-result ownership; existing `Next` phase switching may remain. An in-flight upstream result is omitted unless it was inserted before the handoff. | Preserve a previously observed terminal error if applicable. Test empty/partial/full heaps, exchange-min race, Return/ReturnStrict, ordering, and pooled-result cleanup. |
| `RP_MAX_SCORE_NORMALIZER` | Buffers all rows and normalizes by the maximum | Stop accumulation, freeze the current maximum and pool, then normalize and yield the partial set. Do not pull upstream after handoff. | Add short pool/max ownership synchronization; refactor the `Next` phase only if required by that handoff. | Preserve a previously observed terminal error if applicable. Test empty/partial pool, zero maximum, score-field rewrite, and insertion race. |
| `RP_GROUP` | Buffers reducer state by group and finalizes groups | Stop accumulation and finalize/yield the groups built before the handoff. Do not pull more upstream rows. A row still owned by in-flight `Next` may be omitted. | Protect hash/reducer mutation and the finalization claim; never finalize a reducer instance concurrently with `Add`. Add shared phase state only if this cannot be expressed by ownership handoff alone. | Finalization is not status-returning, but an upstream/accumulation error may already exist. Test empty/partial groups, reducer mutation race, group limits, arrays, and exactly-once `Finalize`/cleanup. |
| `RP_DEPLETER` | Synchronously buffers an upstream before yielding | Yield already buffered rows first, then transparently continue through upstream Drain. It must never run the existing synchronous `Next` depletion loop from Drain. | Use a short result-array ownership handoff; keep existing `Next` phase switching unless Drain must share it. | Propagates stored or upstream errors. Test Drain before depletion, during append, during yield, and after a stored timeout/error. |
| `RP_SAFE_DEPLETER` | Background depletion into a shared result array | Claim and yield results already buffered, signal the background producer to stop publishing, then continue through upstream Drain. Never wait on the condition variable or for an unscheduled/running job. | Extend the existing shared mutex/state with a monotonic drain/cancel handoff. Background appends must become protected; job completion may occur later but cannot publish after drain EOF. | Existing lock-acquisition/background errors must be propagated or recorded. Test unscheduled, queued, waiting for index lock, running, completed, and competing depleters. |
| `RP_HYBRID_MERGER` | Fan-in, deduplication, and hybrid score calculation | Drain every upstream nonblockingly, translate/store each ready row, then freeze the partial dictionary and yield merged results. Use stable upstream order and preserved per-source rank state for deterministic RRF behavior. | Explicit accumulate/yield/drained phase. Protect dictionary, iterator, per-upstream return/rank state, and finalization; never hold the merger lock across an upstream Drain call. | Upstreams can error and merge currently has an error path. Test independent upstream handoffs, duplicate keys, partial source coverage, RRF ranks, linear scoring, and error precedence. |

## Debug processors

| PR / processor | Proposed Drain behavior | Synchronization and focused tests |
| --- | --- | --- |
| `RP_TIMEOUT` | Bypass timeout injection and delegate to upstream Drain. The real timeout is already the reason Drain is running. | Protect/reset the debug counter only if cursor reuse requires it. Test Drain before and after the injected count. |
| `RP_CRASH` | Delegate to upstream Drain without injecting a new crash. Crash injection remains a `Next` behavior. | Test that Drain can pass a concurrently blocked/crashing debug position without touching its state. |
| `RP_CRASH_IN_RUST` | Same as `RP_CRASH`; no new Rust panic/crash is triggered by Drain. | Include the Rust FFI/default-propagation path in layout and concurrency tests. |
| `RP_PAUSE` | Delegate to upstream Drain without pausing. This processor is specifically useful for parking the in-flight `Next` while testing Drain. | Test Drain while `Next` is parked before and after the processor, then release `Next` and verify ownership cleanup. |

## Mutable QueryProcessingCtx state

Processor-local synchronization does not protect concurrent mutations made by
different levels of the chain. The audit found these shared
`QueryProcessingCtx` fields in active processor paths:

- `totalResults` and `skippedResults`;
- `minScore`;
- `resultLimit`;
- `err` / `QueryError`;
- `queryGILTime` and per-processor `rpGILTime`;
- hybrid subquery return-code storage.

The initial implementation keeps Drain away from these mutable fields rather
than adding a second query-level synchronization protocol. The blocked-client
and QueryRequest lifetime mechanisms own result publication and eventual
handoff. If a processor cannot implement correct serializable Drain behavior
without one of these writes, its PR records the concrete case and returns to
design review before adding shared synchronization.
