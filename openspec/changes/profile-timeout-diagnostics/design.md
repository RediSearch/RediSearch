# Signal execution timeout, then finish profiling

## Execution and ownership

A separate Redis module timer is armed when a PROFILE + FAIL client is blocked. The blocked-client automatic deadline is zero. The existing blocked-client timeout callback remains available for an explicit `CLIENT UNBLOCK ... TIMEOUT`, which retains cancellation/error semantics.

The timer signals an execution flag without setting the cancellation flag. Iterators, result processors, row buffering, and the final buffering check observe execution timeout. Lifecycle bailouts and profile collection observe cancellation. Queued requests still enter their normal worker flow to construct diagnostics, with execution already stopped.

The blocked-client private data owns the timer and retains its request. Timer registration, firing, and stopping run on the main thread; free-data cleanup stops an armed timer before releasing request ownership. Workers never manipulate Redis timers.

For distributed AGGREGATE, RPNet execution waits observe the execution flag. Profile collection switches to observing cancellation, drains the remaining shard replies, and uses the existing internal cursor PROFILE cleanup flow. Both signals wake a registered channel. For distributed SEARCH, fan-out continues collecting all shard replies; the reducer stops processing rows and retains the shard profiles.

## Comparison

| Behavior | Regular FAIL with blocked-client timeout | Older cooperative PROFILE + FAIL | This PROFILE + FAIL draft |
| --- | --- | --- | --- |
| Deadline detection | Redis blocked-client callback | Execution checks the clock | Redis module timer signals execution |
| Client reply at deadline | Immediate timeout error from callback | None | None |
| Result rows after timeout | Entire reply discarded | Buffered rows discarded | Buffered rows discarded |
| Diagnostics | No profile when the callback wins | Normal profile with warnings | Normal profile with warnings |
| Time in worker queue | Can cause immediate error | Depends on where the branch starts its execution clock | Can signal timeout; worker still finalizes a profile |
| Reply/profile collection after deadline | Client already received an error | May continue past the deadline | May continue past the deadline |

Unlike switching to RETURN, the draft retains FAIL buffering and row-discard semantics. Measurements reflect work performed up to the observed stop; the exact cutoff can differ from clock polling because timer callbacks run on the Redis event loop. A timer is a stop request, not thread preemption: long operations finish until their next check. Once execution has finished, encoding/profile collection does not retroactively invalidate its result decision.

## Limits and compatibility

A shard that never replies can prolong collection indefinitely. There is no second hard deadline or bounded grace period in this draft. Disconnect and explicit client unblocking remain cancellation paths, not diagnostic timeouts. Existing non-timeout errors retain their normal error behavior.

Consumers that expected a top-level timeout error from PROFILE + FAIL will instead receive the established profile envelope. They must inspect timeout warnings. Ordinary SEARCH/AGGREGATE, RETURN, RETURN-STRICT, and HYBRID retain their public contracts. Inline execution without a blockable client retains cooperative clock checks.

## Validation

Tests hold workers or shard replies at debug synchronization points and invoke the same timer callback through a debug-only hook, selecting a specific blocked client. This separates deadline behavior from machine speed. A timer counter supports a real timer-expiration case as well. Local builds/tests are intentionally omitted at the requester's direction; CI is the validation authority for this draft.
