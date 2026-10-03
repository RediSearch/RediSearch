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

## Borrow and suspension boundary

Next and Drain receive driver-created execution access to the parent and chain.
Upstream calls reborrow that access without acquiring another lock. Drain access
exposes upstream Drain, not upstream Next. C access is opaque and carries debug
domain/ownership checks; only the admitted driver creates it.

Before releasing ownership, every accepted payload is either committed in
drainable domain state or exclusively private to the pending operation. Temporary
budgets are restored, or represented as consistent resumable state. A losing
worker cannot restore an old budget, append a profile sample, change callbacks,
or overwrite an error after main has taken ownership.

Scoped parking inside a C Next call is valid only with an audit of every
surviving caller and pointer. A Rust mutable borrow into the pipeline cannot
survive such parking. A lock, raw pointer, Pin, or retained request lifetime does
not make that aliasing safe. Where a live Rust borrow would cross a blocking
site, execution must use the following segment boundary:

1. The blocking source prepares an owned pending operation and returns an
   internal suspension outcome. Wrappers preserve their progress and propagate
   suspension while still owning the domain.
2. The driver regains control, ending RP/parent/upstream borrows, then releases
   the gate and performs the wait through independently owned handles.
3. The driver attempts admission. On success it commits the private completion
   and resumes; otherwise it disposes the completion without entering the chain.

Suspension is internal execution control flow, not EOF, timeout, or a Drain
status. Accumulators must preserve progress across suspension. Rust processors
use exclusive mutable access and do not need Sync solely for Drain. Values
crossing threads still require valid Send and destructor-affinity proofs.
The eventual Rust driver can hold a standard mutex guard across a segment and
lend disjoint processor/parent/upstream references. No per-row lock, allocation,
reference count, or async runtime is required by this model.

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

| Processor category | Drain behavior |
| --- | --- |
| Index source | EOF; never advance or revalidate an iterator. |
| Network source | Consume a finite set of already-available replies; never request or wait for another batch. |
| Sorter | Yield its committed heap in normal order without replenishing from upstream. |
| Safe loader | Yield remaining rows of a fully loaded batch; an unfinished batch yields EOF. No loading or upstream calls. |
| Grouper | EOF; never finalize partial groups. Completed groups already buffered downstream remain eligible. |
| Transparent transform/filter/pager | Drain upstream and apply ordinary row semantics using exclusively owned state. |
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
state directly. Independent hybrid input metadata is readable only after its
completion publication; incomplete input profiles must not be read live.
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
preserves exclusive mutation. Its principal audit burden is suspension and
denied-resume cleanup.

Local comparison supports this architecture choice: across the measured workload
medians, ownership added about 0.1–1.3% CPU/query versus about 4.9–5.9% for the
concurrent candidate. Individual sorter workloads regressed materially. Profiling
identified a numeric comparator hotspot; a diagnostic fast path removed the
measured regression without changing ownership. The baseline did not receive
that optimization, so optimized parity remains unproven. Loaded tests also
differed in partial-result quality. These observations select a direction; final
integration still requires equivalent performance and correctness validation.

## Implementation and acceptance

Review boundaries follow dependencies, with tests in the layer changing behavior:

1. Sequential Drain API and access contracts, constructor defaults, Rust/FFI and
   enterprise compatibility.
2. Compositional result recovery for row processors and accumulators, preserving
   source barriers, result ownership, and timeout/error precedence.
3. Execution gate, wait-boundary publication, admission and denied-resume paths,
   with the Rust-safe suspension boundary wherever required.
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
