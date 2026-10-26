---
name: write-flow-tests
description: Guidelines for writing Python flow tests (end-to-end behavioral tests). Use this when writing new Python tests in tests/pytests/, and as the review criteria when reviewing changes to them.
---

# Writing Python Flow Tests

Guidelines for writing new end-to-end Python flow tests in `tests/pytests/`.

## Framework

Tests use the `RLTest` framework. The typical pattern is:
1. Create an index with `env.expect('FT.CREATE', ...).ok()`
2. Load data with `conn.execute_command('HSET', ...)`
3. Assert query results with `env.cmd(...)` or `env.expect(...)`

## Finding where to add tests

- Search existing test files in `tests/pytests/` for related functionality using Grep
  (function names, command names, feature names).
- Determine whether an existing test can be extended or a new test is needed.
- Look at nearby tests in the same file for style and patterns specific to that file.

## Test function signature

- Accept `env` as a parameter when the test works with the default environment (dialect 2 on CI):
  ```python
  def testMyFeature(env):
  ```
- Only create a custom `Env()` when you need specific settings that differ from the default, such as:
  - `protocol=3` (to access `res['warning']` dicts)
  - `DEFAULT_DIALECT 1` (to test legacy behavior)
  - Other non-default `moduleArgs`
- Always document *why* a custom `Env()` is needed if it's not obvious.

## Cluster considerations

- Add `@skip(cluster=True)` to tests that don't exercise cluster-specific behavior. This avoids redundant test runs.
- If a test does need to run in cluster mode, use `{hash_tag}` key prefixes (e.g., `{doc}:1`) to ensure keys land on the same shard.

## Index creation

- Use `env.expect('FT.CREATE', ...).ok()` for creating indexes — not `conn.execute_command('FT.CREATE', ...)`.
- Reserve `conn` (`getConnectionByEnv(env)`) for key-write commands like `HSET`, `DEL`, etc.

## Waiting for index

- **Data inserted before `FT.CREATE`**: Background indexing is activated. Call `waitForIndex(env, 'idx')` after `FT.CREATE` to wait for the backfill to complete before querying.
- **Data inserted after `FT.CREATE`**: Each `HSET`/`JSON.SET` is immediately acknowledged by the index — no `waitForIndex` needed.
- Do not call `waitForIndex` right after `FT.CREATE` with no pre-existing data — there is nothing to wait for.

## Assertions

- **Compare the full result** when the response is deterministic and small:
  ```python
  # Good — full result comparison
  res = env.cmd('FT.SEARCH', 'idx', '@t:{al*}', 'NOCONTENT')
  env.assertEqual(res, [1, 'doc1'])

  # Good — empty result
  res = env.cmd('FT.SEARCH', 'idx', '@t:{a*}', 'NOCONTENT')
  env.assertEqual(res, [0])
  ```
- **Add `message=res`** when the assertion checks only part of the result (e.g., `assertGreaterEqual`), so failures show the actual response:
  ```python
  env.assertGreaterEqual(res[0], 9, message=res)
  ```
- Use `env.expect(...).error().contains('...')` for error-path tests.
- Use `env.assertContains(...)` for checking substrings in responses (e.g., warning messages, explain output).

## Determinism

A test must pass, or fail, for the same reason on every machine. Anything host-speed
dependent — a wall-clock `TIMEOUT`, a sleep, a race with background indexing or GC — should
prefer a deterministic hook instead; grep `_FT.DEBUG` in `tests/pytests/` for what exists,
and check the one you pick reaches the code path under test.

## Main-thread liveness under worker locks

When a change adds or alters a worker path that holds a lock the main thread also takes (the GIL,
the IndexSpec lock, or a disk-mode lock), test that the main thread is not stuck behind it:

- Park the worker inside the lock window with `debug_cmd(), 'SYNC_POINT', 'ARM', <name>, <ms>`
  (add a `SyncPoint_Wait` there if none exists), and poll `SYNC_POINT IS_WAITING <name>` until it
  parks. The park must release itself — a main thread blocked on the lock can never run a
  `SIGNAL` — so pass `auto_release_ms`, or release through a `SyncPoint_WaitUntil` predicate.
- Issue the main-thread command from a thread and `join` it with a timeout shorter than the
  auto-release, so a blocked command fails the assertion instead of passing once the worker lets
  go: `PING` catches a held GIL; a write to a key the index covers catches a spec or disk lock the
  write should not wait on.
- `SYNC_POINT SIGNAL <name>` and `CLEAR` in a `finally`. Sync points exist only in `ENABLE_ASSERT`
  builds: guard a test function that takes `env` with `@require_enable_assert`, and a class or a
  test that builds its own `Env` with `skipIfNoEnableAssert(env)`.

`TestNoDeadlockQueryWithConcurrentWriter` in `test_blocked_client_timeout.py` follows this pattern,
releasing through a `SyncPoint_WaitUntil` predicate.

## Deprecated commands

- Do not use `FT.ADD` — use `HSET` via `conn.execute_command('HSET', ...)` instead.
- `FT.ADD` does not work in cluster mode and is deprecated.

## Test structure

- Include a docstring explaining what code path or behavior the test exercises.
- Keep tests focused — one test per code path or behavior.
- Restore global config changes (e.g., `MAXPREFIXEXPANSIONS`) at the end of the test — tests in a file share one server.
