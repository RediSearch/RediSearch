# Contributing a Feature Behind the Unstable-Features Flag

RediSearch ships a runtime feature gate, `ENABLE_UNSTABLE_FEATURES`, that is **off by default**. Code behind that gate is invisible to every user who has not deliberately turned it on.

That gate exists to make a trade possible. Normally a new `FT.*` command, or a new option on an existing one, goes through the [spec-driven workflow](CONTRIBUTING-specs.md): a proposal and a design reviewed by the Search team and by product before you write code, because anything that reaches a default-on release is something Redis has to support, document, and keep working for years. That review might be slow, and for an idea whose value is not yet proven it is often slower than the idea deserves.

If you instead land your feature **behind the gate**, the review narrows sharply. Maintainers still check that your code is safe, but they do not have to agree that the feature is the right long-term API, because nothing is committed: the surface is off by default, carries no compatibility promise, and can be changed or removed at any time. Once the feature has proven useful and correct, a follow-up PR removes the gate and *that* is when the full design and product review happens — with a working implementation and real users to point at.

This document is the contract for that path. Follow it and your PR should be a short review. Break the parts marked as requirements and your PR becomes a normal spec-driven change, because the gate stops protecting users.

## Is this path right for your change?

Use the gate when you are adding **new** surface whose value is not yet established:

- a new `FT.*` command;
- a new option, argument, reducer, or aggregation function on an existing command;
- a new scorer, tokenizer, or query-expansion behavior that a user opts into explicitly;
- an alternative execution strategy that a user must ask for by name.

Do **not** use the gate — these need the full workflow regardless, because a runtime flag cannot make them safe:

- anything that changes the **persistence or replication format**: new RDB fields, new AOF or replicated command shapes, changed index-spec serialization. A user can turn the flag off, or restart onto a build where your code is gone; data written while it was on must still load. See [Requirement 3](#3-do-not-touch-persisted-or-replicated-state).
- anything that changes **existing** behavior — different results, different errors, different defaults for queries that work today.
- a bug fix, refactor, or performance change. These are not features; use the normal PR flow in [`CONTRIBUTING.md`](../CONTRIBUTING.md). Never gate a bug fix.
- removing or deprecating existing surface.

If you are unsure, open a GitHub issue and ask. A one-line answer from a maintainer is cheaper than a rewritten PR.

## The flag

One boolean, `RSGlobalConfig.enableUnstableFeatures` (`src/config.h`), default `false`. It is reachable through two equivalent surfaces that read and write the same field:

| Surface | Name | Example |
| --- | --- | --- |
| Redis config | `search-enable-unstable-features` | `CONFIG SET search-enable-unstable-features yes` |
| `FT.CONFIG` | `ENABLE_UNSTABLE_FEATURES` | `FT.CONFIG SET ENABLE_UNSTABLE_FEATURES true` |

It can also be set at startup, in `redis.conf` as `search-enable-unstable-features yes`, or as a module argument: `loadmodule redisearch.so ENABLE_UNSTABLE_FEATURES TRUE`.

Four properties of the flag matter when you design against it:

**It is mutable at runtime.** Neither registration marks it immutable, so a user can flip it without restarting, and the change takes effect on the next command. Your gate check therefore has to be evaluated per request, not cached at load time.

**It is process-scoped, not database-scoped.** `RSGlobalConfig` is a single global per `redis-server` process. It is not per logical DB (`SELECT` changes nothing), not per index, and not per connection. In a cluster it must be in effect on **every shard** — `FT.CONFIG SET` is handled locally and is not fanned out, so in Open Source cluster mode the operator issues it per shard, while in Redis Enterprise and Redis Cloud the database configuration mechanism applies it to all of the database's shards. A half-configured cluster will behave inconsistently, and that is the operator's problem, not something your code should try to paper over.

**It is not persistent on its own, and how you persist it depends on the deployment.** A bare `CONFIG SET` changes the running process only, and is lost on restart.

- **Redis Open Source 8** — run `CONFIG REWRITE` if you want the setting preserved across a restart, or set it up front in `redis.conf` or as a module argument.
- **Redis Enterprise and Redis Cloud** — use the database configuration API or another supported database-management mechanism. A `CONFIG SET` issued directly against a shard may be temporary: the shard configuration is owned by the control plane and can be regenerated when a shard restarts or fails over, discarding the local change. Settings that must survive have to be stored in the database configuration.

This matters when you write reproduction steps or test instructions for your feature. "Run `CONFIG SET` and try it" is fine for a local build, but do not present it as the way to enable the flag in a managed deployment — an operator following that advice can see the feature silently switch off after a shard restart.

**It is shared by all unstable features.** There is no per-feature sub-flag. Turning it on turns on *everything* currently gated. Two consequences: your feature must not interfere with other gated features, and you cannot offer users a way to opt into yours alone. If your feature genuinely needs independent control, say so in your issue — that is a maintainer decision.

## Requirements

These are the conditions that let a maintainer approve your PR without a product review. Each one exists so that a user who leaves the flag off is provably unaffected.

### 1. Off by default, with zero observable difference when off

Do not change the default. With the flag off, your change must be undetectable: same results, same errors, same `FT.INFO` output, same `COMMAND DOCS`-visible behavior for existing commands, same profile output. If a reviewer can tell your PR is present without enabling the flag, the gate is in the wrong place.

### 2. Runtime gate, not a compile-time one

Gate on the config value. Do not use `#ifdef`, a build flag, or `DEFINE_COMMAND`'s `shouldRegister` field. The same binary has to be testable both ways, and CI must exercise both paths — a compile-time gate makes your code untested in the shipping build. (`shouldRegister` is additionally wrong because it is evaluated once at load time and cannot follow a runtime-mutable config.)

### 3. Do not touch persisted or replicated state

Gated code must not write anything that outlives the flag being on: no new RDB or AOF content, no new index-spec fields, no changed replicated command payloads, no new keys in the keyspace with formats you might revise. The reason is asymmetric failure — enabling a flag is reversible, but data written under it is not. A user who tries your feature, turns the flag off, and restarts must end up with a loadable, correct index.

Read-path and query-path features fit this constraint naturally. If yours cannot, it needs the full spec workflow, including a review of the on-disk format and a plan for what happens to data written by an early version.

### 4. No cost when off

The gate must be a plain branch on the global boolean. Do not allocate, register callbacks, spawn threads, take locks, grow structs, or add hooks to hot paths unconditionally "so it's ready" when the flag is on. A user with the flag off should pay nothing measurable.

### 5. Keep the code separable

Put the feature in its own file or files where the codebase allows it, and keep the gate check in one place. Both graduation and removal should be a small, mechanical diff. If your feature is threaded through six shared functions with six separate flag checks, neither outcome is cheap, and reviewers will ask you to restructure.

### 6. One gate, checked in one place, on every path

If your feature is reachable from more than one entry point — typically shard-local execution *and* the coordinator's query rewriting in a cluster — put the check inside the single shared parse function both paths call, so they reject identically. Do not duplicate the check per call site; duplicated gates drift.

### 7. Fail with an actionable error

When the flag is off, reject the request with an error that names the feature and tells the user exactly how to enable it. Do not silently ignore the argument, and do not fall back to different behavior.

### 8. Normal quality bar still applies

The gate lowers the bar for *product* review, not for engineering review. Your code still needs the project's formatting and lint standards, correct `rm_malloc`/`rm_free` usage, `// SAFETY:` comments on Rust `unsafe`, no compiler warnings, clean sanitizer runs, and tests (below). Memory-safety bugs, crashes, and unbounded allocation are just as unacceptable behind a flag as in front of one — an operator who enables the flag on a production shard is still a real user.

## How to gate the code

### A new option, argument, reducer, or function

Check at **parse time**, as early as possible, before you allocate or open keys. This is how the `COLLECT` reducer was gated for its first two releases:

```c
bool CollectArgs_Parse(const ReducerOptions *options, CollectArgs *out) {
  if (!RSGlobalConfig.enableUnstableFeatures) {
    QueryError_SetError(options->status, QUERY_ERROR_CODE_INVAL,
      "`COLLECT` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off. "
      "Enable it with `CONFIG SET search-enable-unstable-features yes`");
    return false;
  }
  /* ... normal parsing ... */
}
```

Two things to copy from that example. The check is in the shared parse helper, so the reducer factory and the coordinator's distribution rewriter both reject at the same point (Requirement 6). And the error message names the feature *and* the command that fixes it (Requirement 7).

### A new `FT.*` command

Register the command **unconditionally** in `src/module.c`, and check the flag as the first statement of the handler:

```c
static int MyFeatureCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (!RSGlobalConfig.enableUnstableFeatures) {
    return RedisModule_ReplyWithError(ctx,
      "ERR FT.MYFEATURE is an unstable feature. Enable it with "
      "`CONFIG SET search-enable-unstable-features yes`");
  }
  /* ... */
}
```

If the command has both a coordinator entry point and a shard-local one, add a small static helper in your own file and call it from both, rather than copying the check.

While the command is gated, register it with no command-info callback — `NULL, NONE` in the `DEFINE_COMMAND` row, as several existing commands do — so you are not publishing argument metadata for a surface that may change. Adding an entry to `commands.json` and regenerating `src/command_info/command_info.c` (via `srcutil/gen_command_info.py`) is part of graduation, not of the initial PR.

The command will still appear in `COMMAND` output while disabled. That is expected and accepted: command registration happens once at module load, so a runtime-mutable flag cannot hide it. Gating behavior is the goal; hiding existence is not.

## Tests

A gated feature is not exempt from testing — it is exactly as testable as any other, since the flag is runtime-settable. Your PR needs three things.

**Tests with the flag on**, covering the feature itself. In Python, use the helper in `tests/pytests/common.py`, which sets the config on every shard so the same test works in standalone and cluster mode:

```python
from common import *

def test_my_feature():
    env = Env(protocol=3)
    enable_unstable_features(env)
    # ... exercise the feature ...
```

**One test with the flag off**, asserting the feature is rejected. This is the test that proves the gate works, and it is the one reviewers will look for first:

```python
def test_my_feature_requires_unstable_features():
    env = Env()
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect('FT.AGGREGATE', 'idx', '*', ...).error().contains('MYFEATURE')
```

**Cluster coverage**, if your feature has a coordinator path. Gate bugs concentrate there, because the coordinator and the shards parse the same query separately.

For C++ unit tests, set the field directly in the fixture and restore it in teardown, so you do not leak state into other tests:

```cpp
void SetUp() override {
  previousEnableUnstableFeatures = RSGlobalConfig.enableUnstableFeatures;
  RSGlobalConfig.enableUnstableFeatures = true;
}
void TearDown() override {
  RSGlobalConfig.enableUnstableFeatures = previousEnableUnstableFeatures;
}
```

See [`CONTRIBUTING.md`](../CONTRIBUTING.md#testing-requirements) for how to run each suite.

## Documentation and release notes

While your feature is gated, it is **not** part of the public API, and it should not be documented as if it were:

- Do not add it to the redis.io documentation. That happens at graduation.
- Do document it in your PR description and in code comments: what it does, why the design is what it is, what you know is incomplete, and what you would want to change before it graduates. The next person to touch this will be reading that, possibly a year later.
- Choose the release-notes checkbox that matches. A gated feature is generally not a user-facing release note; if you are unsure, check the box that produces no note and say in the PR that the feature is gated. A maintainer will correct it if needed.
- State the limited-support status plainly wherever you *do* describe it — including in any blog post, issue comment, or example you publish. Unstable means: off by default, no compatibility guarantee, may change shape or be removed in any release, and not covered by Redis support commitments.

## PR checklist

Copy this into your pull-request description and tick it honestly. It is what a reviewer will walk through.

```
Unstable-feature gate
- [ ] Flag default is unchanged (off); no observable difference with the flag off
- [ ] Gate is a runtime check on RSGlobalConfig.enableUnstableFeatures, not a compile-time or registration-time gate
- [ ] No new persisted, replicated, or on-disk state
- [ ] No unconditional allocation, callback registration, locking, or hot-path cost
- [ ] Feature code is separable; graduation or removal is a small diff
- [ ] Single gate check, shared by all entry points including the coordinator path
- [ ] Rejection error names the feature and the command that enables it
- [ ] Tests with the flag on
- [ ] A test asserting rejection with the flag off
- [ ] Cluster tests, if the feature has a coordinator path
- [ ] Not added to public (redis.io) documentation
- [ ] Design intent and known gaps written down in the PR and/or code comments
```

## Graduating out of the gate

Graduation is a separate PR, and it is where the review you skipped happens. Expect it to be the harder review of the two — that is the point of the deal.

A feature is a candidate when it has been in at least one release behind the gate, its API has stopped changing, it has real users or a real internal consumer asking for it, its test coverage is genuinely complete rather than gate-focused, and no correctness or performance issues are outstanding. At that point open an issue proposing graduation, and expect to produce the spec artifacts described in [`CONTRIBUTING-specs.md`](CONTRIBUTING-specs.md) — now with an implementation to describe, which makes them much easier to write.

The code change itself is mechanical. `git show 66978e2386` (PR #10377, which graduated `COLLECT`) is the reference:

1. Delete the gate check and any comment that documented it.
2. Delete the `enable_unstable_features(env)` calls from the Python tests, and the fixture toggling from the C++ tests. Do not replace them with anything — with the calls gone, every existing test now runs with the flag off by default, so the whole suite implicitly proves the feature works without it.
3. Delete the flag-off rejection test; it is asserting behavior that no longer exists.
4. Add the public documentation, the `commands.json` entry and regenerated command info if it is a command, and the release note.
5. Leave the flag itself alone. It is shared; other features are still behind it.

## If it does not work out

Some gated features will not graduate, and that is a normal outcome rather than a failure. Because nothing was promised to users, removal is a straightforward deletion of the feature code, its gate, and its tests — no deprecation cycle, no compatibility shim. Maintainers may propose removing a gated feature that has sat unused, or that has become a maintenance cost, and will normally open an issue and give you a chance to make the case for keeping it first.

## See also

- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — setup, coding standards, PR workflow, how to run the test suites.
- [`CONTRIBUTING-specs.md`](CONTRIBUTING-specs.md) — the spec-driven workflow, required for graduation and for anything this path excludes.
- `src/config.c`, `src/config.h` — the flag's registration and definition.
- `git show 910e9c4e48` and `git show 66978e2386` — the `COLLECT` reducer arriving behind the gate, and graduating out of it.
