---
name: code-review
description: Review C code changes for correctness, safety, and style. Use this when you want to review C code changes or PRs.
---

# C Code Review

Review C code changes for memory safety, thread safety, Redis Module API correctness, and project conventions.

## Arguments

The input specifies what to review. Exactly one of the following forms:

**Changesets:**
- `<commit>` or `<commit1>..<commit2>`: Git commit(s) — uses `git diff` / `git show`.
- `pr:<number>`: GitHub pull request — fetches the PR diff.

**Source files or directories:**
- `<path>`: Path to a C file or directory.
- `<path1> <path2>`: Multiple files or directories.

If a path doesn't include `src/`, assume it to be in the `src/` directory.
E.g. `concurrent_ctx` becomes `src/concurrent_ctx`.
If a path points to a directory, review all `.c` and `.h` files in that directory (recursively).

**No argument:** default to reviewing uncommitted working-tree changes (`git diff`).

## Instructions

### 1. Collect the code to review

**When reviewing a changeset** (commits or PR), obtain the full diff of C files:

```bash
# Single commit
git show <commit> -- '*.c' '*.h'

# Commit range
git diff <commit1>..<commit2> -- '*.c' '*.h'
```

**For a GitHub PR** (`pr:<number>`):

```bash
git fetch origin refs/pull/<number>/head
git diff origin/master...FETCH_HEAD -- '*.c' '*.h'
```

Read the full source of every C file that was added or significantly modified so that you
have complete context (not just the diff hunks).

**When reviewing source files or directories**, read the full source of every `.c` and `.h`
file and review them in their entirety.

### 2. Review checklist

Run every check below on the changed C code. For each violation found, record:
- **File and line** (or line range)
- **Rule** that is violated
- **Explanation** of the issue
- **Suggested fix**

#### 2a. Memory safety

- Every `rm_malloc` / `rm_calloc` / `rm_realloc` has a matching `rm_free` on all code paths,
  including error paths.
- The `goto cleanup` pattern is used correctly: all resources allocated before the `goto` are
  freed in the cleanup label, and resources not yet allocated are initialized to `NULL` so
  that `rm_free(NULL)` is safe.
- No use-after-free: freed pointers are not accessed afterward.
- No double-free: pointers are set to `NULL` after free if they might be freed again in cleanup.
- Buffer operations (`Buffer_Write`, `Buffer_Read`) check capacity before writing.
- Return values of allocation functions are checked (non-NULL).

#### 2b. Thread safety

- Shared mutable state is accessed under appropriate locks.
- `ConcurrentSearchCtx` / `ConcurrentCTX` is used correctly for thread handoff.
- No TOCTOU (time-of-check-time-of-use) races on index state.
- `GIL` (Global Interpreter Lock) assumptions are documented when relevant.
- Atomic operations are used for lock-free counters where appropriate.

#### 2c. Redis Module API usage

- `RedisModule_*` functions must be called with the Redis global lock (GIL) held.
  The GIL can be acquired via `RedisModule_ThreadSafeContextLock` (blocking) or
  `RedisModule_ThreadSafeContextTryLock` (non-blocking; must check return value
  and skip the API call on failure). Code running on worker threads (after
  `ConcurrentSearchCtx` releases the lock) must not call Redis Module API
  functions until the GIL is re-acquired.
- `RedisModuleCtx` is not used after the command handler returns or after the
  blocked client is freed.
- `RedisModule_AutoMemory` scope is understood: auto-freed objects must not be
  manually freed (and vice versa).
- `RedisModule_ReplyWith*` calls match the expected RESP protocol for the command.
- Blocked client callbacks (`RedisModule_BlockClient` / `UnblockClient`) correctly
  handle client disconnection (the disconnect callback must clean up resources).
- `RedisModule_CreateString` / `RedisModule_FreeString` are paired correctly, accounting
  for `AutoMemory`.

#### 2d. Error handling

- All error paths clean up resources before returning.
- Functions returning `int` use `REDISMODULE_OK` / `REDISMODULE_ERR` consistently.
- Error messages passed to `RedisModule_ReplyWithError` are descriptive.
- No silent failures (errors are either propagated or logged).

#### 2e. Serialization and RDB compatibility

Only applies when changes touch `src/rdb.c` or serialization logic:
- New fields are added behind a version check (`if (version >= X)`).
- The encoding version is bumped when the format changes.
- Deserialization handles both old and new formats.
- No breaking changes to existing serialized data.

#### 2f. Integer safety

- Casts between `size_t`, `int`, `t_docId` (uint64_t), `t_fieldId` (uint16_t) are
  checked for truncation or sign issues.
- Loop counters use appropriate types for the range of iteration.
- Arithmetic that might overflow is guarded.

#### 2g. Null pointer safety

- Pointers from Redis API calls (`RedisModule_OpenKey`, `RedisModule_CallReplyStringPtr`, etc.)
  are checked for NULL before use.
- Function parameters documented as nullable are checked.
- Struct member access through pointers validates the pointer first.

#### 2h. Style and conventions

- Code follows `.clang-format` conventions (2-space indent, 100-col limit, attached braces).
- Public functions use `ModuleName_FunctionName` naming.
- License header is present on new files.
- No commented-out code left in the diff.
- No `TODO` or `FIXME` comments without a tracking issue reference.

#### 2i. PR description

Only applies when reviewing a PR (not files or commits directly):
- Exactly one release notes checkbox is checked (`This PR requires release notes`
  or `This PR does not require release notes`). CI will fail if neither or both
  are checked.
- If the PR has user-facing impact (new commands, changed behavior, bug fixes
  affecting users), it should check "requires release notes."

### 3. Emit the report

Present findings grouped by check (2a through 2i). For each group, list the
violations or state "No issues found."

At the end, provide a summary:
- Total number of violations by severity (blocking vs. suggestion).
- Whether the change is **ready to merge** or **needs revision**.

Blocking violations: any issue in 2a, 2b, 2c, 2d, 2e, or 2f.
Suggestions: issues in 2g (null safety), 2h (style), and 2i (PR description).
