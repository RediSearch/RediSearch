---
name: review-enterprise-flow-tests
description: Review or implement Redis Enterprise re-tests and CI workflow changes. Use when working on `re-tests/`, Enterprise lifecycle/profile fixtures, Redis Enterprise integration tests, compatibility coverage, or GitHub Actions that run Redis Enterprise tests for PR/nightly validation.
---

# Review Enterprise Flow Tests

Use this skill for Redis Enterprise flow-test changes, especially `re-tests/`
fixtures, lifecycle events, survival scenarios, database profiles, and workflows
that run Enterprise tests.

Treat the checks below as pre-review implementation guidance and as a focused
review checklist.

First apply [`/review-changes`](../review-changes/SKILL.md) for review target
handling, duplicate-comment checks, finding quality, nit policy, PR description
checks, generic test coverage-reduction policy, and output format. Then apply
the Enterprise-specific checks below.

## First Pass

- Identify whether the change touches a workflow, fixture/plugin registration,
  database/profile setup, lifecycle event, survival/noise scenario, or assertion
  oracle.
- Identify the Enterprise test scenario matrix affected by the change: Redis
  version, Search module source, topology, DB profile, lifecycle event, auth/TLS
  mode, or query/index type.

## Enterprise Workflow Checks

- Never run PR-controlled `re-tests` code with secrets, cloud credentials, OIDC
  roles, Docker logins, private checkout material, or deploy tokens already
  exposed. Gate untrusted PR triggers before credentialed steps, or run them only
  on trusted events.
- For `workflow_dispatch` inputs, remember defaults do not automatically apply
  to other events. Add explicit env fallbacks for `pull_request`, `push`, and
  scheduled paths.
- Keep PR and nightly workflows separate when their scope differs. Use explicit
  markers or test subsets instead of one overloaded workflow with ambiguous
  defaults.
- Keep image tags, node counts, RAMP source selection, and feature-set defaults
  defined in one place or wired through shared env variables. The workflow,
  conftest defaults, and README must agree.
- If an S3/RAMP path downloads artifacts, verify the workflow configures the
  correct AWS credentials for that path, not only credentials for unrelated
  setup such as sccache.
- Ensure log collection follows configurable env/container names and captures
  all nodes in multi-node clusters.
- Remove development-only branch triggers, debug comments, and stale TODOs before
  merge.

## Pytest Fixtures And Imports

- Shared fixtures must be discoverable from the actual pytest invocation. Put
  shared fixtures in `conftest.py` or register fixture modules with
  `pytest_plugins`; importing constants from a fixture module is not enough to
  register its fixtures.
- Do not make shared fixtures depend on module-local fixtures. If a fixture such
  as `db` needs `search_module`, that dependency must be shared as well.
- Avoid plain `import conftest`; use package-qualified helpers or move reusable
  helper code out of conftest files.
- Align `pyproject.toml` tooling targets with the CI Python runtime before adding
  lint or type-check gates. Do not enable strict checks unless the existing tree
  is already compliant or the gate is scoped to compliant files.
- Register cleanup as soon as resources can exist. If deploy or DB creation can
  partially succeed before raising, finalizers must still destroy the cluster or
  delete any created databases.
- Continue cleanup after one delete failure and surface unrecoverable cleanup
  failures instead of only logging them.

## Enterprise API Semantics

- Do not assume OSS Redis commands are valid against Redis Enterprise database
  endpoints. For RE-managed settings such as persistence, memory quota, eviction
  policy, and TLS/auth, assert BDB metadata, rladmin state, or shard-level state
  rather than `CONFIG GET`.
- Do not create or delete Enterprise ACL users with database-level `ACL SETUSER`
  or `ACL DELUSER`. Use the RE access-control helpers for users, roles, ACLs,
  and database role assignment.
- In bundled-module mode, resolve the Search module by semantic Redis version and
  feature set. Do not create a DB with an empty module list when tests require
  `FT.*` commands.
- For in-place upgrades, distinguish Redis-version upgrades from Search module
  swaps. A custom module may already be loaded while the Redis version still
  needs to upgrade.
- Compare Redis versions semantically, usually at major/minor granularity, when
  RE exposes patch/build suffixes in one source but normalized values elsewhere.
- Assert the post-upgrade Redis and Search module versions that the test claims
  to exercise.

## Lifecycle And Concurrency

- Replace sleeps with deterministic convergence checks when possible: shard
  counts, slot distribution, module version, lifecycle completion, index
  readiness, GC counters, or other state-specific signals.
- Expected lifecycle transients must be narrow and lifecycle-specific. Do not
  swallow broad `Exception` or every `ResponseError`; distinguish transient
  proxy/connection errors from real command/query failures.
- Background readers, writers, and validators must report unexpected exceptions.
  A test that ignores worker errors can pass without validating the failure
  window it was written for.
- Always stop and join worker threads in `finally` blocks. After a timed join,
  assert no worker is still alive.
- Validate after the lifecycle completes, not only before or during it.
- If a tolerated transient can interrupt a multi-step updater, restore or clean
  transient keys before final validation.
- When forcing GC, verify the force operation actually succeeded unless the test
  deliberately and narrowly tolerates that failure.
- For replica-of tests, wait for the RediSearch index on the replica before
  searching. Link sync does not guarantee index build completion.
- For ASM tests, build migration plans after enabling flexible shards and ASM
  tuning. Do not cache plans before the lifecycle mutates cluster settings.

## Assertion Quality

- Apply the coverage-reduction rule from `/review-changes` especially strictly
  to Enterprise scenario matrices. Removing a lifecycle event, database profile,
  topology, auth/TLS mode, Redis/Search version, query/index type, or assertion
  needs explicit rationale and reviewer approval.
- Assertions must prove the specific axis under test. Positive defaults are weak
  oracles: for example, `memory_size > 0` does not prove a quota overlay applied
  if the default DB is already positive, and a default eviction policy does not
  prove an eviction-profile override.
- Prefer non-default profile values, exact expected metadata, or explicit
  postcondition checks over assertions that would pass without the new config.
- Tests that claim search survival should verify content and result identities,
  not only nonzero counts.
- If updates are meant to exercise TEXT/TAG/vector membership, update documents
  across those indexed terms; do not repeatedly update only documents that share
  the same term bucket.
- Check Redis command return values when they reveal data loss. For example,
  `HSET` on an existing hash should not silently recreate a lost document.
- For cursor tests, use a cursor count that forces actual follow-up reads and
  assert expected read counts or uniqueness when that is the behavior under test.
- For vector reload/upgrade tests, preserve and compare vector index parameters
  such as HNSW `M`, `EF_CONSTRUCTION`, and `EF_RUNTIME` when those parameters are
  part of the contract.
- For TAG filters containing punctuation such as `sci-fi`, escape according to
  RediSearch TAG query syntax in every SEARCH or VSIM filter path.
- Handle the RESP protocol shape used by the existing test client. Do not parse
  only RESP3 maps if the suite commonly runs through RESP2 list replies.

## Xfail, Skip, And Comments

- Prefer `pytest.mark.xfail(..., raises=AssertionError)` or a narrower exception
  when the known failure is an assertion mismatch. A broad xfail hides setup,
  Redis, rladmin, provisioning, and import regressions.
- Keep new assertions outside an existing xfailed path when they are meant to
  protect different behavior. Otherwise unrelated failures become expected.
- Remove stale skips/xfails once the underlying issue has been fixed or prove the
  failure still reproduces.
- Do not place raw Jira/PR/conversation context in source comments unless it is a
  real tracked follow-up such as `TODO(<ticket-id>)`. Put narrative ticket context
  in the PR description or commit message.
- Avoid duplicating helper functions that already exist under `re-tests/utils`
  or rl-automation helpers. Search first, then add one shared helper if needed.

## Review Output

Use the output format from `/review-changes`. Add Enterprise-specific findings
only when they identify a distinct workflow, fixture, lifecycle, API semantic,
coverage, or assertion-quality problem.
