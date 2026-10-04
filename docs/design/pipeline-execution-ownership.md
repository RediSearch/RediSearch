# Pipeline execution ownership and Drain

Design for [MOD-17482](https://redislabs.atlassian.net/browse/MOD-17482).
This is the target contract for the alternative ownership stack. Individual PRs
must state which parts they implement; this document does not certify unlanded
code. The existing concurrent Next/Drain stack remains a separate alternative.

## Purpose and scope

Recover valid buffered results on timeout while keeping ordinary pipeline
execution sequential. STRICT must be able to reply when its worker has not
started or is blocked on the GIL, an external lock, or input readiness. Requiring
that worker to wake before replying can deadlock or delay the main thread.

One execution owner protects the pipeline rather than synchronizing every RP
operation. Ownership changes at execution and blocking boundaries, with no new
per-row ownership atomic operation. Existing timeout polling remains necessary.
This also permits ordinary mutable RP state during incremental Rust migration.

No new command, timeout policy, or persistence format is introduced. The dedicated
enterprise disk-loader Drain implementation remains deferred; its adapter must
continue compiling and its unsupported Drain returns EOF without waiting for I/O.

## Execution domain

An execution domain contains one pipeline and its mutable processing inputs:
RP state, lookup structure, counters, errors, profiles, result budgets, and reply
publication. Its parent processing context references the ownership gate. Request
and blocked-client ownership continue to control lifetime independently.

Immutable configuration may be shared. Independent producers and transport
mailboxes retain their own synchronization. Hybrid inputs and the merger tail
are separate domains; a tail gate does not protect a running input's metadata.
Each domain admits at most one executor. Redis serializes main-thread callbacks.

A safe depleter straddles this boundary: its consumer-side RP and outer profile
wrapper belong to the tail context; producer execution uses the upstream RP's
context. Output publication is a separately synchronized mailbox. A consumer waiting
for publication releases tail ownership and must regain it before changing its
yield phase or consuming output. A tail gate never grants access to unfinished
producer rows, errors, counters, or profiles.

Producer recovery publishes output without claiming the producer job finished.
Lifetime joins still wait for the actual job-completion signal. Normal completion
publishes both facts together; timeout recovery can publish output while a losing
worker remains parked. That worker may later release private resources and signal
job completion, but cannot modify recovered output or diagnostics.

Coordinator hybrid construction runs before tail publication. Recovery finding
no published context must not inspect parsed flags or partial pipelines; its
caller builds the empty reply from construction-safe command metadata. Once
published, timeout excludes the tail, recovers each producer under its own gate,
then drains and serializes the tail without nesting execution gates.

The tail collector publishes its actual reply-buffer slot and immutable reply
configuration before calling Next. A timeout owns that slot, appends Drain output,
and snapshots input visibility for synchronous reply/profile serialization. Worker
frames that lose admission must not replace the recovered reply. Producer joins
belong to background lifetime cleanup before UnblockClient, never the timeout
callback or the tail ownership critical section.

The C implementation uses a mutex. BG uses try-lock for admission; main may wait
for active execution to release the mutex. The existing request timeout is the
only cancellation signal: there is no separate drainRequested flag. A private
worker bookkeeping bit may describe its suspended frames, but cannot authorize
main-thread access or replace the mutex.

| Boundary | Required action |
| --- | --- |
| BG admission | Check timeout, try-lock, check timeout again. Reject on timeout or failed acquisition. |
| Ordinary Next loop | Keep ownership across rows; poll timeout at existing execution checkpoints. |
| Before a blocking operation | Commit consistent state, end conflicting accesses, release ownership, then wait using private resources. |
| Natural wake | Release external locks needed by Drain; reacquire admission before touching domain state. |
| Denied admission/resume | Dispose private resources, unwind without domain writes, then release the job's lifetime reference. |
| STRICT callback | Set timeout, acquire ownership, recover and reply using existing reply arbitration, then release ownership. |
| RETURN timeout | Unwind Next completely and Drain on the same thread. No STRICT gate operations. |
| FAIL timeout | Return the existing timeout error and never invoke Drain, including during cleanup. |

STRICT timeout terminates the cursor. RETURN timeout preserves the cursor using
the existing processor continuation semantics. A timed-out sorter or maximum-score
normalizer remains in yield: later reads consume its remaining committed output,
then reach EOF, without accumulating more input or replaying drained rows. Other
processors retain their ordinary continuation. Normal exhaustion closes the cursor;
timeout alone does not unconditionally close a RETURN cursor.

Mutex release/acquire publishes state. The post-acquisition timeout check rejects
a worker that acquires after main has drained and unlocked, even when timeout
uses a relaxed atomic access. Timeout alone does not publish RP state.

The gate must not be held while waiting for the GIL, external locks, transport,
or another execution domain. A worker holding the GIL may try admission, but
must release the GIL on rejection. Main must release mailbox/publication locks
before waiting for the gate. Domain gates are never nested: signal timeout on
the required domains first, then access each independently.

A parked worker need not run for main to acquire ownership. An active worker
must reach a safe release point; scheduler stalls or long uninterruptible work
can delay it. There is no hard wall-clock bound. Drain and serialization can also
take time proportional to buffered input.

## Borrow and wait boundary

Ordinary contention must not require folding the Next chain. At a blocking site,
the C caller publishes consistent recovery state, releases execution ownership,
waits using privately captured resources, and reacquires ownership in the same
frame. Successful admission continues normal execution. Failed admission cleans
up private resources and returns the existing timeout result through Next; no new
suspension result is required by this target design.

Acquiring the GIL does not permit skipping the timeout check: it excludes a
concurrent timeout callback, but the callback may already have drained and replied
while the worker was waiting. After failed admission, all surviving frames must
fold without accessing recovered pipeline state, including temporary budgets,
profile bookkeeping, and reply buffers. The execution driver must not unlock a
gate that the worker no longer owns.

Rust migration must not force full-chain suspension into the C design. Shared
references with narrowly encapsulated `UnsafeCell` access under ownership are a
possible future representation; the full Rust model is deferred. This does not
permit existing `&mut` references to overlap main-thread recovery today. Current
mixed-language call paths must end or avoid conflicting references before scoped
release is enabled. The mutex alone does not make overlapping references sound.

Next and Drain run under driver-created execution access to the parent and chain.
Upstream calls use that access without acquiring another lock. C access is opaque
and carries debug domain/ownership checks; only the admitted driver creates it.

The C bridge borrows this token through `QueryProcessingCtx.executionAccess`
for the duration of a segment and clears it before unlocking. Only blocking
boundaries consult it; passing an ordinary row does not touch the gate. Existing
Rust entry points must avoid holding exclusive references across upstream calls
that can release ownership. The Counter C-entry adapter uses raw handles across
those calls and limits references to operations within an admitted interval.

Profiling wrappers publish their active intervals before calling upstream.
After successful readmission, the original call finishes its interval normally.
If timeout takes ownership, recovery closes it instead; the losing worker cannot
subsequently change the reported time. Waiting and completion work remain part
of cumulative profiles without adding normal-path synchronization.
Profile output counts successful rows explicitly: recovery may inspect an
unentered wrapper or call Drain after a terminal Next, so subtracting one assumed
terminal invocation is not valid. Producer timing starts under its own ownership
and can be completed by recovery without waiting for the producer job to finish.

Before releasing ownership, every accepted payload is either committed in
drainable domain state or exclusively private to the waiting frame. Recovery
derives its remaining output budget from the published prefix and original chunk
limit, not a parked ancestor's temporary upstream budget. A losing
worker cannot restore an old budget, append a profile sample, change callbacks,
or overwrite an error after main has taken ownership.

Scoped parking requires an audit of every surviving caller and pointer. A lock,
raw pointer, Pin, or retained request lifetime does not make overlapping Rust
references safe. Rust mutation guards must end before ownership release, and
values crossing threads still require valid Send and destructor-affinity proofs.
The eventual Rust representation may use shared parent references and narrowly
encapsulated interior mutation under the execution guard. No per-row lock,
allocation, reference count, or async runtime is required by this model.

## Result recovery contract

Every RP constructor installs a Drain callback, using immediate EOF when
unsupported. Callers invoke the final RP directly and repeat while it returns
OK. Drain returns only OK, EOF, or ERROR. EOF and ERROR terminate the sequence;
the caller must stop, including after taking a diagnostic. A persistent error
latch is not required. There is no generic fallback to Next.

OK transfers an initialized, ordinary serializable SearchResult under Next's
ownership conventions. Drain traverses the constructed pipeline and preserves
its transformations, filtering, OFFSET, LIMIT, and ordering. It never searches
for an upstream accumulator to bypass a downstream semantic boundary.

Recovery prioritizes short timeout handling: stop at the first accumulator with
usable buffered output and do not replenish it after draining that output. An
initially empty accumulator may follow upstream Drain to find ready results.
This exception does not permit loading unfinished batches, advancing iterators,
or waiting for additional work.
Each RP implements this boundary locally in its Drain behavior. The execution
context supplies ownership only; it does not inspect RP buffers or choose a
pipeline-wide stopping point.

| Processor category | Drain behavior |
| --- | --- |
| Index source | EOF; never advance or revalidate an iterator. |
| Network source | Consume a finite set of already-available replies; never request or wait for another batch. |
| Sorter | In accum, drain the existing heap; only an initially empty heap consumes upstream Drain before yielding. Recovery commits the yield phase for both APIs. Later RETURN cursor reads yield only the remaining heap; empty yield returns EOF, never a refill or a third phase. |
| Safe loader | Yield remaining rows of a fully loaded batch; an unfinished batch yields EOF. No loading or upstream calls. |
| Grouper | EOF; never finalize partial groups. Completed groups already buffered downstream remain eligible. |
| Transparent transform/filter/pager | Drain upstream and apply ordinary row semantics using exclusively owned state. |
| Hybrid merger | Drain eligible input mailboxes without waiting, preserve each input's rank/window progress, then score and yield the committed union. Once yielding has begun, do not replenish inputs. |
| Depleter | Yield its own buffered rows without replenishment; only an initially empty buffer may recover upstream Drain. Safe-depleter recovery happens under producer ownership before output publication. |
| Other accumulators | Yield only committed, semantically valid local output; otherwise EOF. |

Plain loaders can perform normal sequential loading on RETURN; BG construction
must promote them to safe loaders before STRICT execution. A scorer may drain
only when its transformation and all reachable callback state are safe under
the ownership contract; opaque external state is not covered merely by the gate.
No generic scorer/provider capability redesign is part of this stack.

Next folds promptly on timeout. Timeout-driven sorter yielding moves to Drain;
ordinary EOF yielding remains valid. No RP needs a concurrent Next/Drain mutex
or atomic counter solely for this contract. Locks protecting independent
producers, lookup users in other domains, or lifetime machinery remain necessary.

## Reply and lifetime ownership

Preserve the already-published ordered reply prefix and append eligible Drain
output using the remaining original output budget. Do not reapply OFFSET or use
a suspended wrapper's temporary resultLimit. Reply buffers, where available,
must obey their existing row-publication and reply-ownership rules.

Within the admitted domain, Drain can use coherent counters, errors and profile
state directly. Independent hybrid input metadata is readable only after output
publication, through normal completion or separate producer-domain recovery;
unpublished input profiles must not be read live. Immutable configuration outcomes
needed by replies, such as timeout capping, belong to the parent request rather
than producer execution flags. Reply-buffer serialization can cache these from
the parent without reading an unpublished producer. This applies to RETURN replies
as well as STRICT callbacks: a folded tail does not imply all producers finished.
Partial counts describe safe progress and need not equal a completed query.
Existing completed-EOF/error precedence and exactly-one reply arbitration remain.

Rows crossing a release boundary own their mutable payloads. Borrowed immutable
data must outlive every reader. Iterator scratch, mutable RP buffers, and network
envelopes cannot escape merely because the request is retained. Use existing
move/materialization rules rather than cloning every row. In-flight private
rows may be omitted, but every payload is released exactly once without duplicate
output. There may be unfinished work in more than one nested accumulator.

Initialization must publish either a ready pipeline or an explicit no-pipeline
reply path before a timeout can inspect it. Main may reply before BG finishes.
The blocked-client reference keeps the request alive until BG calls UnblockClient;
there must be no request access afterward. Independent child jobs require their
own existing lifetime references. Free waits for all borrowers, and cursor rearm
requires the old cycle to be quiescent before timeout or ownership is reset.

RPNet separates transport delivery from execution ownership. Late replies may
be accepted or discarded consistently with transport cleanup; Drain never waits
for arrivals. A future FT.SEARCH coordinator reducer RP follows the same rule
and reduces already-received input, including input outside its heap.

## Alternatives and evidence

The existing completion lock plus wake-abort requires special handling for each
new blocking state. Concurrent Next/Drain permits earlier recovery while BG is
active, but requires RP and reachable-state publication throughout the pipeline.
Published ownership moves synchronization to admission and wait boundaries and
preserves exclusive mutation. Its principal audit burden is wait publication and
denied-resume cleanup.

The October 4 comparison used equivalent release builds on master `6bc5765abf`,
four rotating rounds, and an identical-baseline control. Across 480 normal-path
samples, median per-shape CPU/query deltas were +0.32% FAIL, +0.81% RETURN,
and +0.28% STRICT for ownership, versus +4.38%, +3.55%, and +3.97% for
concurrent Next/Drain. The common contention filter retained 77/120 groups;
standalone loader and no-worker coverage remained noisy. Several worker sorter
shapes retained roughly 1.6–2.1% overhead. A 96-sample forced-inlining experiment
did not reliably improve it and was not adopted. These are whole-pipeline
observations, not attribution of sorter or synchronization costs.

All 64 loaded STRICT samples passed generator-validity and correctness checks,
with comparable full-result throughput and overlapping latency ranges. Different
partial counts and row availability prevent treating timeout output as identical.
Coverage was aggregation sorter/loader/grouper workloads on one host with one/two
shards, not exhaustive SEARCH, vector, hybrid, or enterprise coverage. The evidence
supports the architecture choice, not universal zero overhead or a hard latency
bound; later integration changes still require appropriate validation.

## Reply-buffer integration and merge order

Execution ownership and reply storage are separate responsibilities. The request
cycle owns both resources; the gate authorizes pipeline/reply mutation but neither
extends request lifetime nor replaces reply arbitration. Allocate before dispatch,
and release only at cycle teardown after all job borrowers have finished. A cursor
must not retain an old cycle's gate, access token, or Redis reply-buffer context.
Independent hybrid producers retain separate domains and lifetime completion.

The ownership stack can land without Redis reply-buffer APIs. Prefer landing it
first, then integrate MOD-18503's storage/serialization change against this protocol.
The buffer change also requires upstream Redis API availability and a supported
packaged core. If buffers land first, preserve their storage while replacing the
legacy completion/wake-abort handoff; neither ordering permits silently restoring
that handoff for migrated ownership domains.

The combined implementation must preserve these boundaries:

- Next and row serialization run within the same admitted segment. Complete a
  RESP row and commit its count/budget before publishing a recoverable prefix.
  Do not release ownership with an unfinished row or a conflicting serialization
  borrow into pipeline state.
- STRICT takes execution ownership and appends Drain output to that prefix;
  it never waits for worker completion merely to access the buffer. RETURN folds
  Next and then uses the same serialization logic for Drain on its own thread.
  FAIL never drains and may discard without accessing a worker-mutated buffer.
- Cache immutable serialization options from request-owned configuration. Capture
  mutable totals, errors, and profile visibility only under the relevant domain's
  ownership or producer publication, after the appropriate Next/Drain sequence.
- Denied readmission cannot reset, replace, or free the published buffer. Final
  reply transfer happens once, and buffer destruction remains cycle-owned.

No per-row publication mutex is required when serialization stays inside the
execution domain. Existing completion synchronization remains necessary for
unmigrated coordinator SEARCH and disk-loader paths; output recovery must not
be confused with job completion. Converting the SEARCH reducer into an RP is a
separate follow-up, not implied by AGGREGATE/RPNet coverage.

Combining the changes requires parked-worker timeout tests during Next and at
serialization boundaries, RESP2/RESP3 prefix/count/error checks, cursor rearm and
disconnect cleanup, hybrid producer visibility, and fresh whole-query performance
measurements. The current ownership benchmarks do not measure reply-buffer costs.

## Implementation and acceptance

Review boundaries follow dependencies, with tests in the layer changing behavior:

1. Sequential Drain API and access contracts, constructor defaults, Rust/FFI and
   enterprise compatibility.
2. Compositional result recovery for row processors and accumulators, preserving
   source barriers, result ownership, and timeout/error precedence.
3. Execution gate, wait-boundary publication, admission and denied-resume paths,
   with scoped borrowing that permits same-frame waits.
4. Standalone request/cursor policy integration and ordered reply publication.
5. Network and hybrid domains, independent producer metadata, and late completion.
6. Final integration verification and equivalent performance measurements.

Inventory each wait's protected state, surviving aliases, private payload,
external locks, losing-unwind actions, and regression test. Cover queued, active,
GIL/spec-lock/input-blocked and completed BG states; cursor rearm; independent
hybrid producers; and late network replies. Tests must prove main replies while
a deliberately parked worker remains parked, then prove its late completion
cannot mutate the drained domain and all payloads are cleaned exactly once.

Validate representative standalone, coordinator, cursor, PROFILE and hybrid
pipelines under all three policies. Run relevant C/Rust/flow tests, build, lint,
and sanitizer checks. Compare equivalent release builds on a common base with
identical-build controls, no concurrent builds, and explicit invalid-sample
exclusions. Measure normal CPU/query, throughput and latency first; then timeout
acquisition, Drain, serialization, reply completion, partial quality and eventual
BG cleanup. Report remaining gaps rather than interpreting omitted work as speed.
