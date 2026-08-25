# Design: result-processor drain API

## Status

The PR stack, testing strategy, result semantics, and concurrency envelope are
agreed. The repository audit produced concrete API, Rust, ABI, and
per-processor recommendations. The base implementation now provides the draft
three-state status and shared Rust entry model for review; the contract is not
yet marked approved.

## Agreed constraints

- Drain is built for the timeout flip and is thread-safe from its first version.
- Pipeline lifetime is guaranteed externally.
- At most one thread executes the `Next` chain while one other thread drains.
- Drain is an alternate result-producing path. It yields results into
  caller-provided `SearchResult` storage using the same ownership convention as
  `Next`.
- Results yielded by Drain are suitable for serialization. The caller may use
  them for both return-on-timeout and return-strict behavior, or discard them
  when the selected policy does not return partial results.
- Drain continues the current output sequence: it does not restart the
  processor or re-yield results already transferred through `Next`.
- Drain itself does not serialize a client reply. The caller owns each yielded
  result and decides how to publish or discard it.
- A result is released exactly once, whether it is yielded through `Next`,
  yielded through Drain, discarded during depletion, or released by `Free`.
- The API supports linear AREQ pipelines and the HybridRequest fan-in shape.
- The C and Rust APIs expose the same contract, allowing a processor to migrate
  without changing its drain semantics or adding a permanent C adapter.
- No timeout policy behavior changes as part of MOD-17482.

## Concurrency model

The proposed safety boundary has two participants:

1. one query thread with at most one active call anywhere in the `Next` chain;
2. one timeout thread driving Drain from the pipeline entry.

No processor lock may be held across blocking I/O or an upstream call. Each
stateful processor needs a short critical section or atomic ownership handoff
for its local mutable state. Stateless processors may already satisfy the
contract without additional synchronization.

Every result belongs to exactly one path:

- a result claimed by `Next` before the drain handoff remains owned by `Next`;
- a result still buffered at the handoff is eligible for Drain;
- no new `Next` call starts after the handoff;
- an already in-flight `Next` may finish, but its result is not part of Drain
  and the caller may discard it from a partial timeout reply.

Omitting that one in-flight result from a partial timeout reply is acceptable.
It must nevertheless be destroyed exactly once. The ownership handoff must be
made concrete per processor, especially where `Next` blocks or temporarily owns
a result outside a protected buffer.

Drain must not wait for the query thread, a scheduled background job, blocking
I/O, a condition variable, or a global interpreter/runtime lock. It may use
short processor-local critical sections, assuming low contention, to claim
ready state or request cancellation. In particular, the timeout thread must not
depend on whether background work is unscheduled, blocked, running, or complete.

## Result-yielding API recommendation

Drain needs a caller-provided `SearchResult`, like `Next`. A dedicated status
type is preferred over `REDISMODULE_OK` / `REDISMODULE_ERR`: neither outcome is
an error, and the return value needs to distinguish a populated result from an
exhausted drain.

The initial discussion preferred a two-state result/EOF enum. The implementation
audit found real error-producing paths in processors that must create normal
serializable results while draining:

- projectors and filters can fail expression evaluation;
- network draining can encounter a shard/protocol error;
- hybrid fan-in can receive upstream errors and has a merge error path;
- depleters can already hold an upstream or background-depletion error.

Suppressing those failures would make Drain observably different from `Next`.
The audit therefore recommends a dedicated three-state enum:

```c
typedef enum {
  RP_DRAIN_OK,
  RP_DRAIN_EOF,
  RP_DRAIN_ERROR,
} RPDrainStatus;

RPDrainStatus ResultProcessor_Drain(ResultProcessor *self, SearchResult *result);
```

`RP_DRAIN_OK` transfers a populated result to the caller. `RP_DRAIN_EOF` is
terminal: subsequent calls also return EOF, and async work cannot later publish
a new drain result. An already in-flight `Next` remains independently owned and
is the only exception to global exhaustion.

Drain never returns timeout, paused, depleting, or pending: timeout is the
reason the caller selected Drain, and Drain cannot wait for future progress.
Both EOF and ERROR terminate the drain sequence. The owning query's existing
timeout/error policy decides which condition has reply precedence.

There is no separate Drain output-parameter convention: implementations and
callers follow the existing `Next` convention for `SearchResult` initialization,
population, transfer, and cleanup.

## C dispatcher and ABI recommendation

Add the Drain function pointer after `Free` in `ResultProcessor`. Keeping the
existing field offsets minimizes source and layout churn, although every
producer of the full struct must still be rebuilt.

All calls go through a public `ResultProcessor_Drain(rp, result)` dispatcher;
processors do not call `rp->Drain` directly. A null Drain pointer invokes the
temporary default behavior: recurse through the dispatcher to `upstream`, or
return EOF at a source. This lets the API PR compile before the specialized
processor PRs land. Each processor PR replaces the temporary default with an
explicit implementation or an explicit decision that propagation is correct.

Drain does not use `Next` function-pointer identity as synchronization state and
does not modify `Next`. Existing processors may continue changing `base.Next`
inside the single `Next` flow when Drain neither reads nor depends on that
field. A processor refactors its phase into shared local state only when its
Drain handoff actually requires that state; other vtable cleanup is deferred to
the later Rust migration.

`RP_DISK_ASYNC_LOADER` is constructed outside this repository and embeds the
complete base struct. There is no versioned, mixed-binary ResultProcessor ABI:
the core and provider are built from matching headers. Its provider update must
therefore land in lockstep with the API layout change; appending the field does
not make an old provider binary safe to drain.

## Processing semantics

A result yielded from the pipeline entry through Drain is a normal serializable
pipeline result. Each processor applies the semantics necessary at its output
boundary, including transformation, filtering, loading, paging, or partial
finalization. It may not blindly forward an upstream drain result when doing so
would expose a result that its `Next` path would not expose.

Drain also owns timeout-specific transitions that currently lengthen the `Next`
unwind. For example, the sorter currently observes timeout configuration,
switches independently from accumulation to yield, emits its buffered results
until EOF, and only then returns timeout. Its Drain implementation will perform
that transition and yield the partial sorted results directly, allowing the
concurrent `Next` chain to fold sooner.

For each processor the implementation review must therefore answer:

- Does Drain yield local buffered results, call upstream Drain, or both?
- If it calls upstream Drain, which parts of its normal `Next` transformation
  must also run on the drained result?
- Which local state is shared with concurrent `Next`, and where is ownership
  linearized?
- What makes repeated Drain calls and later `Free` safe?

The default drain behavior can only be used where forwarding preserves the
semantic form of the result expected at the pipeline entry.

The orchestration layer prevents new `Next` calls after the handoff. Individual
processors do not need to enforce a permanent post-Drain `Next` prohibition
unless their implementation requires it for safety; the API does not currently
promise behavior for a newly started `Next` after terminal Drain.

## Request synchronization boundary

The first implementation avoids introducing query-level synchronization into
the ResultProcessor API. Drain implementations should avoid mutable
`QueryProcessingCtx` bookkeeping (`totalResults`, `skippedResults`, `minScore`,
`resultLimit`, profiling totals, and `QueryError`) wherever possible. Stable
configuration may still be read.

The blocked-client mechanism already owns the timeout latch, exclusive result-
production transition, reply storage, wake-up, and callback lifetime. The
ongoing QueryRequest lifetime refactor is also moving the active pipeline entry
from the legacy `QueryProcessingCtx.endProc` slot into QueryRequest. Drain will
align with those mechanisms rather than adding an independent query-level
handoff or lifetime protocol in MOD-17482.

In particular:

- `ResultProcessor_Drain` operates only on the processor chain and caller-owned
  result storage;
- the caller owns the result budget and reply bookkeeping;
- Drain does not mutate shared `resultLimit`;
- processor implementations keep result ownership and phase synchronization
  local;
- the later timeout-flip integration decides when the blocked-client owner may
  serialize Drain results and how late `Next` bookkeeping is excluded.

If a processor cannot preserve serializable semantics without mutating shared
query state, its PR stops and brings that concrete case back to design review.
The implementation stack is intentionally being used to discover those cases
instead of committing the base API to speculative cross-chain synchronization.

## Rust migration compatibility

This API change is a prerequisite for ongoing ResultProcessor migrations to
Rust. The API PR must make concurrency sound in the Rust abstraction rather
than exposing Drain only in the C vtable.

The current Rust FFI thunk obtains mutable access to the entire
`ResultProcessorWrapper` and calls `ResultProcessor::next(&mut self, ...)`.
Calling a concurrent Drain thunk in the same way would create overlapping Rust
mutable references, even when the concrete processor protects its state with
fine-grained synchronization. The API PR must remove that whole-processor
exclusive-borrow assumption from concurrent entry points. The recommended
shape is conceptually:

```rust
pub trait ResultProcessor: Sync {
    fn next(&self, cx: Context<'_>, result: &mut SearchResult<'_>)
        -> Result<Option<()>, Error>;

    fn drain(&self, cx: DrainContext<'_>, result: &mut SearchResult<'_>)
        -> Result<Option<()>, DrainError>;
}
```

The exact aliases and lifetimes can follow existing crate style. The important
properties are shared processor access and a Drain error type that cannot
represent timeout or pending. Under this model:

- `Next` and Drain enter the concrete processor through shared access;
- mutable processor state uses local interior synchronization appropriate to
  that state;
- the Rust processor type satisfies the thread-safety bounds required for the
  two concurrent entry points;
- `Context` and `Upstream` do not claim exclusive access to the shared header
  for the duration of either call;
- `Free` remains externally serialized after both calls complete, as guaranteed
  by the pipeline lifetime contract.

The current Rust `Context::subtract_total_results` implementation mutates the
parent counter through a raw pointer and documents the chain as single-threaded.
Drain must not call it concurrently. If a future Rust processor needs a drain-
path count adjustment, that concrete migration will first need an API aligned
with QueryRequest ownership.

The wrapper must not make the Rust implementation safe by holding one mutex
across an entire `Next` or Drain call. Such a mutex would make Drain depend on a
possibly blocked query thread and violate the non-blocking contract.

The C enum, function pointer, generated bindings, Rust trait, FFI thunks, and
layout assertions land together in the API PR. Pure Rust contract tests should
exercise concurrent `Next` and Drain without passing through a C-only helper.

Changing the receiver to shared access requires a mechanical update to the
already-Rust counter so the workspace continues to compile; its count becomes
interior concurrency-safe state. That enabling change belongs to the API PR.
The counter-specific Drain semantics and focused tests remain in its processor
PR.

Every processor PR implements Drain in that processor's current language. The
already-Rust counter remains in Rust; this stack does not migrate any additional
processor from C to Rust. Rust readiness is an API acceptance criterion, while
actual migrations remain separate follow-up work after the stack lands.

## PR stack

The implementation is a linear stack. Each ResultProcessor type has its own PR
so its concurrency and ownership rules can be reviewed independently.
The detailed behavior, handoff, error, and test plan for each PR is in
[the processor matrix](processor-matrix.md).

1. `guyav-mod-17482-rp-api`
2. `guyav-mod-17482-rp-index`
3. `guyav-mod-17482-rp-network`
4. `guyav-mod-17482-rp-loader`
5. `guyav-mod-17482-rp-safe-loader`
6. `guyav-mod-17482-rp-disk-async-loader`
7. `guyav-mod-17482-rp-scorer`
8. `guyav-mod-17482-rp-metrics`
9. `guyav-mod-17482-rp-key-name-loader`
10. `guyav-mod-17482-rp-vector-normalizer`
11. `guyav-mod-17482-rp-highlighter`
12. `guyav-mod-17482-rp-projector`
13. `guyav-mod-17482-rp-filter`
14. `guyav-mod-17482-rp-profile`
15. `guyav-mod-17482-rp-counter`
16. `guyav-mod-17482-rp-pager`
17. `guyav-mod-17482-rp-sorter`
18. `guyav-mod-17482-rp-max-score-normalizer`
19. `guyav-mod-17482-rp-group`
20. `guyav-mod-17482-rp-depleter`
21. `guyav-mod-17482-rp-safe-depleter`
22. `guyav-mod-17482-rp-hybrid-merger`
23. `guyav-mod-17482-rp-timeout`
24. `guyav-mod-17482-rp-crash`
25. `guyav-mod-17482-rp-crash-rust`
26. `guyav-mod-17482-rp-pause`
27. `guyav-mod-17482-rp-drain-integration`

The API PR owns the C/Rust vtable layout, dispatcher, Rust trait and FFI safety
model, common concurrency primitives, and synthetic contract tests. The final
integration PR invokes Drain from an existing controlled path and must preserve
replies byte-for-byte.

## Testing strategy

Every processor PR covers the states relevant to that processor, including:

- Drain before the first `Next`;
- Drain concurrent with `Next` at each mutable-state handoff;
- Drain after EOF and after an execution error;
- repeated Drain calls;
- completion of an already in-flight `Next` after the drain handoff;
- Drain followed by `Free`, with ownership counters proving exactly-once cleanup;
- race-oriented tests proving that every result remains singly owned and no
  result is duplicated, leaked, or released twice between `Next` and Drain.

Tests may observe that a result owned by an in-flight `Next` is intentionally
omitted from a partial timeout reply. They must still prove its eventual cleanup.

Stateful and asynchronous processors also cover not-started, blocked, running,
and completed work. Hybrid tests cover foreground and background sub-pipelines,
fan-out to every upstream, and independent completion order.

Final verification includes the C/C++ unit suite, Rust result-processor and FFI
tests, aggregate and hybrid Python suites in standalone and cluster modes, and
ASAN coverage for stateful processors.

## Open decisions

1. Approve the audit recommendation to add `RP_DRAIN_ERROR`, and finalize the
   three status names and representation.
2. Review the implemented shared-access Rust trait and restricted
   `DrainContext` shape used to make concurrent entry sound without coarse
   locking or mutable query bookkeeping.
3. Confirm the paired provider landing/build plan for the external disk async
   loader.

Per-processor handoff mechanisms are implementation decisions to document and
test in their respective PRs, rather than unresolved shared API semantics.

## External dependency

`RP_DISK_ASYNC_LOADER` is implemented by the disk provider rather than in this
repository. Its PR needs a paired provider change and compatibility plan for the
extended ResultProcessor vtable.
