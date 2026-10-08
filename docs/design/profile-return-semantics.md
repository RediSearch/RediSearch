# PROFILE with RETURN execution semantics

Draft alternative for [MOD-18986](https://redislabs.atlassian.net/browse/MOD-18986),
based on the Redis Search 8.10 branch.

## Proposal

A blocked-client FAIL timeout currently replaces FT.PROFILE output with a bare
timeout error. This alternative treats profiling as a diagnostic execution:
when the configured timeout policy is FAIL, the individual profiled request
executes with RETURN. The configured policy is never changed globally.

This differs from the ticket's original proposal: partial result rows are
retained, rather than discarded to reproduce the earlier FAIL result body.
The profile envelope and timeout diagnostics are preserved. This is a draft
design choice, not a claim of exact compatibility with historical FAIL.

## Behavior

The override covers FT.PROFILE SEARCH, AGGREGATE, and HYBRID, including LIMITED,
standalone execution, cluster dispatch, and internal shard requests. RETURN and
RETURN-STRICT policies keep their existing timeout semantics. Ordinary commands
continue to use their configured policy.

The configured query TIMEOUT remains in force for cooperative execution checks.
PROFILE under FAIL has no blocked-client deadline or timeout reply callback.
CLIENT UNBLOCK TIMEOUT therefore cannot force a timeout reply for that request.
Client disconnect still cancels work and wakes registered shard-reply waits.

SEARCH and AGGREGATE retain their existing envelopes:

- RESP2: `[results, profile]`.
- RESP3: `{Results: results, Profile: profile}`.

HYBRID retains its existing result map, warnings, and profile section in each
protocol. No new public fields or syntax are introduced. Timeout reporting uses
the existing RETURN warnings and per-stage profiling.

## Comparison with FAIL

| Aspect | Ordinary FAIL with blocked-client timeout | Earlier cooperative FAIL profile | This alternative |
| --- | --- | --- | --- |
| Client reply at timeout | Bare timeout error | Profile with timeout warning | RETURN profile with timeout warning |
| Partial result rows | Discarded | Buffered rows discarded on execution timeout | Retained according to RETURN |
| Worker queue and encoding | Blocked-client deadline remains active | No hard end-to-end bound | No blocked-client deadline; RETURN checks govern execution |
| Profile collection | May be cancelled when the error reply wins | Can finish after TIMEOUT | Can finish after TIMEOUT |
| Result buffering | Full chunk before success is serialized | FAIL buffering remains | RETURN may serialize incrementally |
| Late expression errors | Can replace the complete buffered chunk | FAIL buffering remains | Inherits RETURN behavior after serialization starts |
| Measurements | Actual FAIL execution | Actual cooperative FAIL execution | Diagnostic RETURN execution, not a faithful measurement of FAIL buffering |

The configured OOM policy is unchanged. In particular, OOM FAIL still requires
full-chunk buffering; converting the timeout policy does not remove that
requirement. Consequently, RETURN does not guarantee partial rows in every
pipeline or OOM configuration.

Coordinator sorting, merging, and post-processing can consume shard partial
results that FAIL would have discarded. Counts, scores, ordering, processor
counters, memory use, and timings can therefore differ from a FAIL execution.
This matters even when both executions eventually return a profile.

Distributed RETURN has existing protocol differences. For example, aggregate
RESP3 recognizes shard timeout warnings and can stop consuming result rows early,
while RESP2 may continue reading internal cursor chunks. This alternative
inherits those differences; it does not claim to align the two protocols' rows.

A delayed or nonresponsive shard can prolong profile collection beyond TIMEOUT.
This approach provides no hard end-to-end deadline. A cancellation is distinct
from a cooperative timeout: cancellation terminates profile collection, whereas
a cooperative timeout preserves the diagnostic reply.

## Design

Resolve the timeout policy before choosing callbacks and before deriving pipeline
timeout behavior. Each shard independently applies the same rule when it parses
a profiled request. The coordinator captures the effective policy at dispatch,
so a subsequent CONFIG SET cannot turn its profile back into a FAIL execution.

SEARCH reduction uses the captured effective policy when interpreting shard
errors. AGGREGATE and HYBRID use their existing RETURN execution, serialization,
warnings, and cursor mechanisms. The existing profile printers provide the
desired framing without translating an already-sent timeout error.

Disconnect callbacks remain installed for profiled requests. Aggregate profile
collection recognizes the cancellation flag under RETURN as well as FAIL, while
RETURN-STRICT keeps its existing main-thread drain behavior.

## Validation and implementation checklist

- [x] Resolve PROFILE FAIL to RETURN before coordinator/shard dispatch.
- [x] Propagate the effective policy into hybrid subquery and tail pipelines.
- [x] Keep ordinary FAIL and RETURN-STRICT callback selection unchanged.
- [x] Add RESP2/RESP3 parity checks against actual RETURN, including deterministic
      execution timeouts, partial rows, LIMITED, and internal shard cursors.
- [x] Update blocked-client tests to distinguish profiling from ordinary FAIL.
- [x] Retain disconnect coverage while waiting for profile replies.
- [ ] Obtain passing CI build, lint, standalone, and coordinator results.
- [ ] Review and accept the compatibility differences above.

No tests or builds were run locally, as requested. CI is the execution evidence;
the assertions added here are not a claim that those scenarios have passed.
Coverage and sanitizer lanes are not enabled by the ordinary 8.10 draft workflow.
