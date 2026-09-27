# Distributed aggregate profile timeout policy

Proposed behavior delta; pending maintainer approval.

## Effective policy

When `FT.PROFILE ... AGGREGATE` executes through the multi-shard coordinator and
the configured `ON_TIMEOUT` is `RETURN-STRICT`, the coordinator MUST use `RETURN`
for that request. This applies to both full and `LIMITED` profiles.

The effective policy MUST be selected before registering blocked-client callbacks
and MUST remain consistent through pipeline execution and any cursor continuation.
Profile serialization MUST NOT use STRICT's main-thread result-ready callback.

The request MUST retain its effective TIMEOUT value and existing caps. RETURN's
cooperative timeout handling applies; the strict deadline guarantee does not.
Existing RETURN behavior determines results, timeout warnings, and available
profile information. This change adds no guarantee that every shard's profile
will be collected and adds no profile-completion wait.

## Isolation and compatibility

The fallback MUST NOT change global `ON_TIMEOUT` or `TIMEOUT` configuration.
Unrelated requests MUST retain their configured policy. Standalone profiling,
single-shard local execution, `FT.PROFILE SEARCH`, ordinary `FT.AGGREGATE`, and
requests configured with RETURN or FAIL retain their existing behavior.

Command syntax and RESP2/RESP3 reply shapes remain unchanged.

## Acceptance scenarios

1. Under configured RETURN-STRICT, a distributed aggregate profile completes with
   correct query results and its existing profile envelope on a healthy cluster;
   subsequent PING commands on every shard succeed.
2. The same holds for LIMITED profiles and both RESP2 and RESP3 clients.
3. The same holds with disabled timeout and a generous finite timeout, without
   introducing a test expectation that depends on query execution speed.
4. Reading global ON_TIMEOUT and TIMEOUT before and after these requests yields
   the same values; the fallback applies only to the affected request.
