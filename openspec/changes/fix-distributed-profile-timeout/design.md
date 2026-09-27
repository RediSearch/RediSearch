# Bounded profile drain under RETURN-STRICT

## Ownership and placement

`printAggProfile` is called on the main thread when the coordinator uses STRICT's
stored-result reply path. The background worker has handed off the pipeline before
serialization. The MR channel has a single consumer; I/O callbacks only enqueue
replies. A snapshot of the queue length therefore guarantees that this consumer
can pop that many replies without waiting for more.

For STRICT, bound the existing profile-drain loop by the queue length observed at
entry. Stop earlier if `getNextReply` reports EOF. Each call consumes at most one
queued reply and retains its existing profile extraction and cleanup behavior.
The finite bound also prevents concurrently arriving replies from prolonging the
main-thread drain. Other timeout policies retain their existing drain behavior.

No new wait, completion signal, timeout-policy override, or shard-collection
mechanism is introduced. Existing request ownership and iterator teardown handle
late replies. Global ON_TIMEOUT and TIMEOUT and the request timeout remain intact.

## User-visible behavior

STRICT profiling returns the shard profiles collected by the pipeline and the
bounded drain. It does not guarantee all shard profiles, including when LIMIT
finishes a query early without timing out. The existing incomplete-profile log
remains. Query result semantics, command syntax, and RESP2/RESP3 shapes do not
change. This applies to full and LIMITED distributed aggregate profiles.

## Alternatives

A request-local fallback to RETURN avoids the STRICT callback but changes timeout
semantics. Keeping STRICT and bounding the drain is narrower.

Skipping the drain altogether discards profile information already buffered.
Waiting for all shard profiles can block the main thread and is outside scope.
The existing `drainOnly` flag alone is insufficient on normal completion: its
nonblocking pop behavior currently relies on the timeout's abort flag being set.

## Branch scope

Master is the implementation target. The refreshed refs inspected on 2026-09-27
contain STRICT and the affected profile path in `8.6-rse`, `8.8`, `8.8-rse`, and
`8.10`. Their actual labels are `backport 8.6-rse`, `backport 8.8`,
`backport 8.8-rse`, and `backport 8.10`. Releases `2.6`, `2.8`, `2.10`, `8.0`,
`8.2`, `8.4`, and `8.6` do not contain STRICT. Backport review must confirm the
single-consumer handoff on each affected release.

## Validation

RESP2/RESP3 cluster tests cover full and LIMITED profiles under explicit STRICT,
with disabled and generous finite timeouts, early LIMIT and full aggregation.
Bound client reads independently of query timeout. Check query results, profile
envelopes, PING on every shard, and unchanged ON_TIMEOUT/TIMEOUT configuration.
No test should require complete profiles after early termination.

Deterministic cases hold shard workers paused and park the coordinator just before
result handoff. They check both an empty channel with pending shards and a queued
profile with another shard still pending. Completion, the exact available profile
count, and PING responsiveness are asserted before releasing the paused shards.
