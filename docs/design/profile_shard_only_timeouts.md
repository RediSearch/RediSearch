# Shard-only hard timeouts for PROFILE AGGREGATE

Status: draft experiment for [MOD-18986](https://redislabs.atlassian.net/browse/MOD-18986),
based on the 8.10 release branch. This implements the shard-only option selected
for evaluation, rather than the ticket's proposed restoration of cooperative
coordinator deadline checks.

## Scope and behavior

For distributed `FT.PROFILE ... AGGREGATE` under `ON_TIMEOUT FAIL`, disable the
coordinator's automatic blocked-client deadline. Preserve the configured query
timeout, the FAIL policy, and shard timeout handling. The coordinator still
buffers query results and uses the existing profile serializer.

A shard timeout encountered by the result pipeline discards the coordinator's
buffered rows and becomes a coordinator profile warning. The coordinator then
collects the remaining shard profile replies. A shard whose hard timeout already
replied contributes a timeout error entry to `Profile.Shards`, not fabricated
iterator timings or counters. Responsive shards contribute their actual profiles.
RESP2 retains its `[results, profile]` envelope; RESP3 retains `Results` and
`Profile`, with a timeout warning in `Results.warning` when execution observes it.

The timeout callback remains installed so explicit `CLIENT UNBLOCK ... TIMEOUT`
still cancels the request and returns an error. Disconnect handling also remains
active, including waking a worker blocked on shard replies and releasing its
request. Removing the automatic timer does not remove cancellation.

This change does not affect ordinary AGGREGATE, PROFILE SEARCH/HYBRID, RETURN,
RETURN-STRICT, standalone execution, or the single-shard shortcut. Shard hard
timeouts apply where shard execution uses a blocked client; inline shard execution
retains its existing cooperative checks.

## Comparison

| Property | Ordinary FAIL with blocked-client timeouts | Older cooperative PROFILE + FAIL | This draft |
| --- | --- | --- | --- |
| Client response on execution timeout | Top-level timeout error | Profile envelope with timeout warnings | Profile envelope with warnings and available shard diagnostics |
| Coordinator time budget | Automatic blocked-client deadline | Execution checks the clock cooperatively | No coordinator deadline or cooperative clock checks under FAIL |
| Coordinator queue exceeding TIMEOUT | Can reply before worker execution | No automatic blocked-client timeout reply | Waits for the worker; elapsed dispatch time still reduces the shard budget |
| Timed-out shard's measurements | No profile returned to the user | Measurements up to the cooperative stopping point, when available | Hard timeout can replace the entire shard profile with an error entry |
| Buffered rows when the pipeline observes a timeout | Discarded | Discarded | Discarded |
| Time spent finishing profile collection | Client can already have received an error | May exceed TIMEOUT | May exceed TIMEOUT |

The older cooperative column describes the historical profiling behavior, not
ordinary non-profile FAIL: an ordinary FAIL command returned a timeout error
even before blocked-client timeouts existed.

## Limits of this experiment

- Coordinator-only processing and reply encoding may exceed TIMEOUT and still
  succeed if no shard timeout reaches the result pipeline. This is a deliberate
  difference from restoring cooperative coordinator FAIL checks.
- Shard execution stops at different points under hard and cooperative timeouts.
  The returned measurements are real but can be incomplete; the response is not
  a reconstruction of the profile the older execution path would have produced.
- A timeout found only while collecting remaining profiles after the result
  limit has been satisfied remains in the shard-profile list. Existing reply
  ordering does not retract rows or warnings already serialized before that
  collection phase.
- A stalled or unreachable shard can prolong collection. Shard timeout callbacks
  cannot guarantee that a reply reaches the coordinator, and this draft adds no
  end-to-end deadline or bounded profile-collection period.
- No coordinator timeout warning is invented merely because wall-clock TIMEOUT
  elapsed. Timeout warnings reflect the shard/pipeline information actually
  observed.
- No new coordinator-to-shard arguments or reply formats are introduced. The
  user-visible compatibility change is that this profiled timeout can produce a
  profile envelope instead of raising a top-level error.

## Validation plan

Local test execution is intentionally omitted; CI owns runtime validation.

- RESP2 and RESP3: hold a profile in the coordinator queue beyond an ordinary
  FAIL command's real deadline, then verify the profile returns shard timeout
  errors and a coordinator warning after dispatch.
- RESP2 and RESP3: hold profile reply encoding beyond that deadline, then verify
  successful rows and profiles still return without a synthetic timeout warning.
- One or all shards: invoke hard-timeout callbacks at a shard execution barrier;
  verify empty result rows, error entries for timed-out shards, a coordinator
  warning, request/cursor cleanup, and warning/error accounting. Exercise both
  WITHCOUNT and the regular distributed pipeline.
- Retain explicit coordinator timeout/disconnect regressions, including
  cancellation during profile collection and before fan-out. Remove only the
  expectation that PROFILE AGGREGATE's automatic coordinator deadline cancels
  reply encoding.

Basic standalone and coordinator CI tests run for draft PRs. The release branch's
coverage and sanitizer lanes require a non-draft PR plus their enforcement labels;
they are not implied by a passing draft run.
