# Result processor Drain API (MOD-17482)

## Proposal and scope

Timeout replies need a way to recover valid, already available pipeline results
without waiting for background execution to finish. Today processors such as the
sorter mix timeout handling and buffered yielding into Next. A separate Drain
traversal lets Next unwind promptly and makes result recovery compositional.

This document records the maintainer-discussed design for
[MOD-17482](https://redislabs.atlassian.net/browse/MOD-17482) and the
[base API PR](https://github.com/RediSearch/RediSearch/pull/11145).
It consolidates the proposal, design, implementation plan and behavior delta;
the base PR is the public review surface. It does not claim approval of every
processor implementation or completion of query-level integration.

The base API is internal-only: no command, reply format, persistence format or
timeout policy changes. Each processor remains in its current language. The API
must support later C-to-Rust migration without requiring an additional ownership
redesign; performing those migrations is out of scope.

## Contract and behavior delta

The callback contract is defined on `ResultProcessor::Drain` in
[`result_processor.h`](../../src/result_processor.h). Every constructor supplies
a callback, including external providers. Unsupported processors explicitly use
`RPDrain_EOF`; chain insertion neither probes nor repairs the callback.

The query owner repeatedly invokes the final processor's Drain. It does not
bypass barriers or search upstream for a buffer. OK transfers a serializable
result using Next's ownership conventions. EOF and ERROR terminate that drain
sequence: callers stop, including after taking an error diagnostic. Subsequent
calls are outside the contract, so a persistent error latch is not required.

| Timeout policy | Intended caller behavior |
| --- | --- |
| RETURN-STRICT | Main publishes timeout, closes reply admission, then drains while one Next chain may remain active. |
| RETURN | The executing thread drains inline only after Next fully unwinds. |
| FAIL | Return the timeout error without draining, including for cleanup. |

Only RETURN-STRICT exercises concurrent Next/Drain. Each linear chain has one
drainer and at most one Next executor; different hybrid subchains may have their
own jobs. The caller guarantees chain and payload lifetimes until all borrowers
finish and uses distinct initialized output storage. Free is quiescent.

An in-flight row may be omitted from partial results, but cannot be duplicated,
published after takeover, or freed twice. The timeout flag is cancellation, not
a mutation guard: a worker can pass a check and then block before committing.
Correctness cannot depend on prompt timeout detection or job scheduling.

## Ownership and synchronization design

Keep immutable configuration, Next-private scratch, Drain-private scratch and
published state separate. Transfer ownership with a short local phase/cursor or
pointer update. Once claimed, a row belongs exclusively to its consumer.
Late Next completion rejects and cleans its own unpublished row.

Contention is expected to be very low; the normal path has no contention and
should incur minimal overhead. Use policy-specific paths where useful so RETURN
and FAIL avoid synchronization needed only for STRICT. Atomic counters still
publish progress per consumed row, not only when Next completes.

Short spin guards are allowed for published ownership state. Do not hold them
across upstream calls, allocation, conversion, cleanup, arbitrary callbacks,
I/O, condition waits, the GIL or another RP's guard. A guard can be delayed by OS
scheduling; this is not a wait-free or hard-real-time latency guarantee.
Drain must not wait for a background job to start, finish or acquire the GIL.

Next-only vtable phase changes can remain unless concurrent Drain needs the same
field. Rust accesses stable C header fields through raw field projections rather
than borrowing a whole header that C may mutate. Shared Rust processor entry
uses interior mutability where needed; the restricted DrainContext/DrainUpstream
cannot reopen Next or borrow live query bookkeeping.

## Processor semantics and PR plan

Implement one processor per PR above the base interface. Tests accompany each
layer; default EOF initialization in the base is not a full implementation.

| Processor or category | Drain behavior / planned review unit |
| --- | --- |
| Base API | C callback/statuses, constructor initialization, Rust bridge and Counter interior mutability. |
| RPNet | Claim available owned reply rows; never fetch or await another shard/cursor batch. Protect shared lookup mutation with short local guards. |
| Plain loader | Normal loading in sequential RETURN only; BG construction promotes it to safe loader before publication. |
| Safe loader | Yield only unclaimed rows from a completely loaded published batch. Unloaded/private batches produce EOF; never load or refill upstream. |
| Sorter | Close accumulation and yield its committed heap in normal order, without upstream refill. |
| Metrics, key-name loader, vector normalizer | Separate PRs applying row-local transformations to upstream Drain results. |
| Pager | Apply committed OFFSET progress and conservatively reserve LIMIT capacity for in-flight output. |
| Highlighter | Transform retained row data without iterator reads or rewinds. |
| Max-score normalizer | Take committed valid pool/max state and yield locally. |
| Counter | Consume upstream Drain, clearing rows and publishing atomic progress. |
| Projector and filter | Separate PRs with drain-private evaluation scratch and diagnostics. |
| Profile | Transparent Drain and safe completed-sample publication; incomplete debug statistics must not block production progress. |
| Plain and safe depleters | Separate PRs taking committed buffered rows independently of job/condition waits. |
| Hybrid merger | Yield committed valid rows; no live dictionary iteration or upstream refill from Drain. |
| Debug crash, Rust crash, pause, timeout | Separate transparent Drain callbacks that do not execute Next-only debug actions. |
| INDEX, scorer, grouper | EOF barriers: no iterator advancement, shared scorer mutation, or partial-group finalization. |
| Enterprise disk loader | Separate compatibility PR first; actual ready-row draining remains deferred. |

An accumulator stops at its own local state even when upstream has more buffered
rows. For example, sorter -> safe loader can yield nothing if output fields
have not been loaded. A downstream accumulator can still return completed group
rows even though grouper itself is an EOF barrier.

## Alternatives and integration prerequisites

Waiting for the whole Next chain is rejected: the job may be unscheduled,
blocked on I/O, or waiting for the GIL held by the drainer. A whole-RP lock has
the same problem. Timeout polling alone cannot serialize ownership. Broad
query-level ownership machinery is not introduced by this stack; integration
must use the blocked-client and query-lifetime refactors.

Before activating nonwaiting STRICT Drain, the query owner must close reply
prefix admission, reject late Next output, publish immutable reply metadata and
chain configuration, retain all job borrows, and serialize only caller-owned
state. Live Next error, result-limit and profiling fields are not reply snapshots.
Cursor-cycle policy selection changes only at quiescent boundaries. RETURN
activation also replaces legacy timeout yielding inside Next; FAIL never drains.
These are follow-up tasks under MOD-17482, not behavior enabled by the base PR.

The C struct and Rust trait change together. Old provider binaries are not
compatible. Enterprise must build against the matching RediSearch revision and
adapt its Rust loader to shared entry with default EOF Drain. This compatibility
change must accompany adoption of the base API; it does not enable disk draining.

## Verification requirements

Base tests cover constructor callbacks without chain insertion, preservation
through insertion, C/Rust layout and status translation, and Rust borrowing
constraints. Each processor adds result/status parity and exactly-once ownership
coverage, including rejected late rows, undrained cleanup and caller-owned output.

Concurrency tests park Next outside guards, exercise pre-publication and
already-claimed states, and require Drain to complete without releasing the
worker. Cover all timeout policies, EOF/errors, LIMIT/OFFSET, nested accumulators,
cursor cycles and unscheduled jobs where applicable. Validate standalone and
coordinator builds and relevant behavioral flows. Use sanitizers and Miri for
ownership/FFI paths; any retained performance benchmarks must be proper
CI-runnable microbenchmarks, not timing assertions in C++ unit tests.
