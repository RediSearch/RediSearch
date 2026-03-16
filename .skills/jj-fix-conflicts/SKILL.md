---
name: jj-fix-conflicts
description: Fix merge conflicts in a jj change and its ancestors, starting from the oldest conflict. Use this when a jj change or its ancestors have conflicts that need resolving.
---

# Fix jj Conflicts

Resolve all conflicts in a set of changes, starting from the oldest, then verify the build.

If `$ARGUMENTS` is empty, default to `-b @`.

## Input Modes

`$ARGUMENTS` accepts one of three flags, similar to `jj rebase`:

| Flag | Meaning | Revset |
|------|---------|--------|
| `-s <rev>` / `--source <rev>` | The revision and all its descendants | `<rev>::` |
| `-b <rev>` / `--branch <rev>` | The whole branch (everything not on trunk, plus descendants) | `(trunk()..<rev>)::` |
| `-r <revs>` / `--revisions <revs>` | Only the specified revisions (no implicit ancestors/descendants) | `<revs>` |

If no flag is provided, treat the argument as `-s <rev>` (source mode) by default.

Parse the flag and compute the **target revset** accordingly. All subsequent steps use this revset.

## Table of Contents

1. [Identify Conflicted Changes](#1-identify-conflicted-changes)
2. [Resolve Conflicts Bottom-Up](#2-resolve-conflicts-bottom-up)
3. [Verify Build](#3-verify-build)
4. [Summary and Cleanup](#4-summary-and-cleanup)

---

## 1. Identify Conflicted Changes

Find all conflicted changes within the target revset, ordered from oldest to newest:

```bash
jj log -r '(<target-revset>) & conflicts()'
```

**Important:** In `jj log` output, the graph is displayed with the **newest** changes at the **top** and the **oldest** at the **bottom**. The oldest (ancestor) change is the one closest to the bottom of the output. Always process conflicts starting from the bottom of the log output (oldest) upward.

If no conflicts are found, report this and stop.

List the conflicted changes with their descriptions and ask the user to confirm before proceeding.

---

## 2. Resolve Conflicts Bottom-Up

Process each conflicted change **from oldest to newest**. For each conflicted change:

### 2.1. Inspect the Conflict

```bash
jj log -r <conflicted-change>
jj diff -r <conflicted-change>
```

Examine the conflict markers in the affected files. Understand what each side of the conflict contributes.

**Check the trunk state of conflicted files.** For each file with conflict markers, compare against trunk to understand what the current baseline looks like:

```bash
jj file show -r 'trunk()' <conflicted-file>
```

This is critical for rebase conflicts: code may appear in the conflict's rebase destination but have since been removed on trunk. If code exists in the conflict but not on trunk, it was deleted upstream and should **not** be kept in the resolution.

### 2.2. Create a Fix Change

Create a new change directly after the conflicted change:

```bash
jj new -A <conflicted-change>
```

This inserts a new change between the conflicted change and its children, so the fix will propagate to all descendants.

### 2.3. Resolve the Conflicts

For each conflicted file:

1. Read the file to see the conflict markers (`<<<<<<<`, `%%%%%%%`, `>>>>>>>` or `<<<<<<<`, `+++++++`, `-------`, `>>>>>>>`).
2. Understand what each side intended.
3. Write the correct merged content using the Edit or Write tool.

**Conflict marker format in jj:** jj uses a different conflict format than git. Conflicts may appear as:
- **Diff-style:** `<<<<<<<` / `%%%%%%%` (diff from base) / `+++++++` (other side) / `>>>>>>>`
- **Snapshot-style:** `<<<<<<<` / `-------` (base) / `+++++++` (side 1) / `+++++++` (side 2) / `>>>>>>>`

When resolving:
- Combine the intent of both sides where possible.
- If one side deletes code the other side modifies, prefer the modification unless it's clearly stale.
- If both sides add different code, include both additions in a logical order.
- Remove all conflict markers completely.
- **If unsure about the correct resolution**, show the conflicting sides to the user and ask how to resolve before proceeding. Do not guess.

### 2.4. Describe the Fix

```bash
jj describe -m "CONFLICT FIX: <short description of what was conflicted>"
```

### 2.5. Verify Build After Each Fix

Determine the scope of the conflict:

- **Rust-only conflict** (all conflicted files are under `src/redisearch_rs/`):
  ```bash
  ./build.sh FORCE RUN_RUST_TESTS
  ```
  Then format:
  ```bash
  cd src/redisearch_rs && cargo fmt
  ```

- **C code or mixed conflict** (any file outside `src/redisearch_rs/`):
  ```bash
  ./build.sh FORCE RUN_UNIT_TESTS
  ```

If the build fails:
- Read the errors.
- Fix the issues in the current fix change.
- Re-run the build until it passes.

### 2.6. Continue to Next Conflict

After the fix change is verified, move on to the next conflicted change (in chronological order). The fix may have already resolved downstream conflicts, so check:

```bash
jj log -r '(<target-revset>) & conflicts()'
```

If a previously conflicted change is no longer conflicted, skip it.

---

## 3. Verify Build

After all conflicts are resolved, verify the final state builds:

```bash
jj log -r '(<target-revset>) & conflicts()'
```

This should show no conflicts. If conflicts remain, return to [Step 2](#2-resolve-conflicts-bottom-up).

Then verify the build at the tip of the target revset:

```bash
jj new <tip-of-target-revset>
```

- If any Rust files were touched:
  ```bash
  cd src/redisearch_rs && cargo check && cargo fmt
  ```
- If any C files were touched:
  ```bash
  ./build.sh FORCE
  ```

---

## 4. Summary and Cleanup

Present a summary to the user:

> **Conflicts resolved:**
> | Change | Files | Resolution |
> |--------|-------|------------|
> | `<id>` (`<description>`) | `file1`, `file2` | <brief description of how it was resolved> |
> | ... | ... | ... |

Then ask the user:

> Would you like me to squash the fix changes into their respective parents, or leave them as separate changes for your review?

- **If squash:** For each fix change, squash it into the conflicted change it fixes.
  Use `-u` to keep the destination's description and avoid opening an editor:
  ```bash
  jj squash --from <fix-change> --into <conflicted-change> -u
  ```

- **If leave:** Do nothing — the user will review and decide.

---

## Key Commands Reference

| Command | Purpose |
|---|---|
| `jj log -r '(<revset>) & conflicts()'` | Find all conflicted changes in a revset |
| `jj new -A <change>` | Insert a new change after the given change |
| `jj diff -r <change>` | Show what a change modifies |
| `jj describe -r <change> -m "..."` | Set a change's description |
| `jj squash --from <src> --into <dst> -u` | Squash one change into another (keeps destination description) |

---

## Rules

- **Never use interactive commands** (`-i` flags, editor-opening commands).
- **Always verify the build** after each conflict resolution.
- **Process oldest conflicts first** — fixing an ancestor may resolve descendant conflicts.
- **Re-check remaining conflicts** after each fix to avoid unnecessary work.
- **Format Rust code** (`cargo fmt`) after resolving Rust conflicts.
