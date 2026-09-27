# Request-local fallback

Status: proposed; maintainer review required before implementation.

## Placement and ownership

On master, `DistAggregateCommandImp` in `src/module.c` identifies profiling and
routes single-shard requests locally before allocating the distributed request.
`AREQ_New` snapshots global request configuration into the request shell.

For the distributed profile request only, change that shell's policy from STRICT
to RETURN before `QueryRequestTimeout_UpdateConfig`, deadline initialization, and
blocked-client callback selection. Background compilation retains the request
configuration, so pipeline execution and callback selection use the same policy.
Applying the fallback only in `AREQ_Compile` would be too late: the main-thread
reply and timeout callbacks would already have been selected.

Reuse existing RETURN serialization on the coordinator worker. `printAggProfile`
may consume pending replies through the existing path; this change neither adds
collection machinery nor strengthens its completeness guarantees. The fallback
removes STRICT's result-ready handoff to main-thread profile serialization.

No persistent state, command syntax, reply shape, or internal protocol changes
are proposed. Shards keep their existing execution policy. The coordinator uses
existing RETURN handling for shard results and timeout warnings.

## Boundaries

- Preserve the effective per-query TIMEOUT, including existing foreground caps.
- Preserve global ON_TIMEOUT and TIMEOUT, including concurrent requests' settings.
- Keep ordinary FT.AGGREGATE, FT.PROFILE SEARCH, standalone profiling, and the
  single-shard local route on their existing policy paths.
- Cover full and LIMITED profiles with the same dispatch decision.
- Existing debug-command policy restrictions remain unchanged.
- Ensure any profile cursor lifecycle uses the effective request policy.
- Document the loss of STRICT's deadline behavior for distributed aggregate
  profiling; a new wire warning is not proposed.

## Alternatives

Waiting for every profile envelope before signaling results adds synchronization
and a stronger collection contract. It is outside the requested scope.

Changing global ON_TIMEOUT affects unrelated requests and creates a race with
configuration updates. A request-local snapshot avoids that problem.

Changing policy only during background compilation leaves dispatch callbacks
inconsistent with execution. The main-thread snapshot is the decision point.

## Branch scope

Audited against refreshed remote refs on 2026-09-27; master baseline is
`811145e9c`. Master is the implementation target.

| Release | STRICT and affected profile path | Backport label |
| --- | --- | --- |
| 8.6-rse | Present | `backport 8.6-rse` |
| 8.8 | Present | `backport 8.8` |
| 8.8-rse | Present | `backport 8.8-rse` |
| 8.10 | Present | `backport 8.10` |
| 2.6, 2.8, 2.10, 8.0, 8.2, 8.4, 8.6 | STRICT absent | None for this fix |

Older affected branches use `CoordRequestCtx` rather than master's request shell.
Their adaptations must preserve the same effective policy across dispatch and
background execution; a conflict-free cherry-pick alone is insufficient evidence.
Labels are intended for the implementation PR once the behavior is approved.

## Validation plan

Add focused cluster flow coverage in `test_profile.py` for RESP2 and RESP3, full
and LIMITED profiles, under configured RETURN-STRICT. Use a small deterministic
data set and bounded client operations. Verify command completion, results, the
profile envelope, PING responsiveness on every shard, and unchanged global
ON_TIMEOUT and TIMEOUT. Do not assert that every shard profile must be present.

Exercise disabled timeout and a generous finite timeout without relying on
host-speed-dependent timeout races. Cover policy selection directly where the
existing test infrastructure permits it; confirm unaffected commands retain their
policy. Use existing profile/count regressions alongside the focused tests on
the fixed build. Run the repository build, C unit tests, behavioral verification,
and cluster checks sequentially with retained logs. Review and CI remain required
before the implementation PR is ready for handoff.
