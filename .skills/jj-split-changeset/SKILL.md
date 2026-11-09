---
name: jj-split-changeset
description: Split a jj (Jujutsu) changeset into smaller, focused changesets. Use when asked to break up a large changeset, split commits, reorganize changes across revisions, or create stacked PRs from a single changeset. Covers operation-log-based rollback, file-path and hunk-level splitting without interactive commands.
---

# Splitting a jj Changeset

Split the changeset `$ARGUMENTS` into smaller, focused units — safely, efficiently, and with user involvement at the right moments.

If `$ARGUMENTS` is empty, ask the user which revset to split before proceeding.

## Table of Contents

1. [Core Safety Principle: Record the Operation First](#1-core-safety-principle-record-the-operation-first)
2. [Workflow Overview](#2-workflow-overview)
3. [Inspect the Changeset](#3-inspect-the-changeset)
4. [Plan with the User](#4-plan-with-the-user)
   - 4.1 [How to group the changes](#41-how-to-group-the-changes)
   - 4.2 [What description each changeset should get](#42-what-description-each-changeset-should-get)
   - 4.3 [How to validate each changeset](#43-how-to-validate-each-changeset)
5. [Record the Repo State](#5-record-the-repo-state)
6. [Split the Changeset](#6-split-the-changeset)
   - 6.1 [File-Path Split](#61-file-path-split-when-each-file-belongs-to-one-group)
   - 6.2 [Hunk-Level Split](#62-hunk-level-split-when-a-file-has-mixed-changes)
   - 6.3 [Ordering matters](#63-ordering-matters)
7. [Set Descriptions](#7-set-descriptions)
8. [Verify the Split](#8-verify-the-split)
   - 8.1 [Verify completeness](#81-verify-completeness--no-changes-lost)
   - 8.2 [Verify individual changesets](#82-verify-individual-changesets--stat-review)
   - 8.3 [Run user-defined validation](#83-run-user-defined-validation)
9. [Rebase Dependents](#9-rebase-dependents-if-needed)
10. [Clean Up](#10-clean-up)
11. [Key Commands Reference](#11-key-commands-reference)
12. [When to Prompt the User](#12-when-to-prompt-the-user)
13. [Limitations](#13-limitations)

---

## 1. Core Safety Principle: Record the Operation First

**Before modifying anything, record the current operation ID and the original commit ID** ([Step 5](#5-record-the-repo-state)). jj logs every repo mutation in its operation log, so the whole split can be rolled back in one step by restoring that operation — no need to duplicate the changeset. The original commit ID stays resolvable after the change has been rewritten, which is what makes the completeness check in [8.1](#81-verify-completeness--no-changes-lost) possible.

If anything goes wrong — a botched split, a failed validation the user wants to start over from, an unexpected state — **ask the user**, then roll back:

```bash
jj op restore <op-id>
```

This restores the **whole repo view** as of `<op-id>`, not just this split. The operation log is shared by every workspace of the repo, so it also reverts anything another workspace or agent did since — their commits, descriptions, bookmarks, even workspaces they added — and, by default, the remote-tracking bookmarks, which breaks a later push if something was pushed or fetched in between. Before restoring, list the operations since `<op-id>`. Pass `--at-op=@ --ignore-working-copy` so the inspection does not itself snapshot the working copy or reconcile divergent operations, which would add an operation to the list being audited:

```bash
jj op log --at-op=@ --ignore-working-copy --no-graph \
  -T 'id.short() ++ " " ++ time.start() ++ " " ++ description ++ "\n"' | sed '/^<op-id>/q'
```

If every operation is one you ran for the split, `jj op restore <op-id>` is safe. Otherwise, tell the user which operations are not yours and, rather than restoring, revert only your own, newest first:

```bash
jj op revert <your-op-id>
```

If a push or fetch happened since `<op-id>`, restore with `--what repo` to leave the remote-tracking bookmarks alone.

This principle applies to every step below.

---

## 2. Workflow Overview

1. **Inspect** — understand what's in the changeset
2. **Plan with the user** — agree on groupings, descriptions, and validation criteria
3. **Record** — note the operation ID and original commit ID
4. **Split** — file-path-based or hunk-level
5. **Describe** — set meaningful descriptions on each resulting changeset
6. **Verify** — confirm no changes were lost, then run user-defined validation
7. **Rebase dependents** — ask the user if anything needs rebasing (hunk-level splits only)
8. **Clean up** — abandon the original (hunk-level splits only)

---

## 3. Inspect the Changeset

Before doing anything, understand what you're working with.

```bash
# Show the full diff with file names and stats
jj diff -r <revset> --stat
jj diff -r <revset> --git    # Unified format: easier to read hunks from than the default color-words view

# Show the changeset description and parents
jj log -r <revset>
```

Summarize for the user:
- How many files are changed
- Which files are logically related
- Whether there are clear groupings (e.g., "refactor" vs "feature" vs "tests")

---

## 4. Plan with the User

**Always ask the user three things before proceeding.**

### 4.1. How to group the changes

Present the file list and your suggested groupings, but let the user decide.

Example prompt:

> This changeset touches 8 files. I see what looks like three logical groups:
> 1. **Refactor**: `src/foo.rs`, `src/bar.rs` (signature changes)
> 2. **Feature**: `src/new_thing.rs`, `src/lib.rs` (new functionality)
> 3. **Tests**: `tests/test_new_thing.rs`, `tests/test_foo.rs`
>
> Does this grouping look right? Would you like to split differently?

**Mixed hunks within a single file:** If a file has changes belonging to different logical groups, flag this to the user. The agent can handle hunk-level splits without interactivity — see [6.2 Hunk-Level Split](#62-hunk-level-split-when-a-file-has-mixed-changes). Present the hunks from the file and ask the user which hunks belong to which group.

### 4.2. What description each changeset should get

Ask for commit messages. Suggest defaults based on the groupings, but let the user confirm or override.

### 4.3. How to validate each changeset

Ask the user how to verify that each split changeset is correct **beyond** diffstat review. Examples:

> How should I validate each split changeset? Some options:
> - **Build check**: `cargo build` / `cargo check` after each split
> - **Test suite**: `cargo test` (all tests, or a specific subset?)
> - **Lint/format**: `cargo clippy`, `cargo fmt --check`
> - **Diff review only**: just show me the diffs and I'll eyeball them
>
> You can specify different validation per group (e.g., "run tests for the feature changeset, diff review only for the refactor").

Store the validation plan — you'll execute it in [Step 8.3](#83-run-user-defined-validation).

---

## 5. Record the Repo State

First check the working copy (`@`), and note where it sits so it can be put back at the end ([Step 10](#10-clean-up)):

```bash
jj st
jj log -r @ --no-graph -T 'change_id.short() ++ " " ++ description.first_line() ++ "\n"'
```

If `@` has uncommitted changes that are unrelated to the split, ask the user what to do with them **before recording anything**: the `jj new` calls below move `@` away and leave those changes behind on the old stack, and any jj command snapshots them into `@` — into the original itself when `@` is the changeset being split.

Then record:

```bash
jj op log -n1 --no-graph -T 'id.short() ++ "\n"'                # <op-id>
jj log -r <revset> --no-graph -T 'commit_id.short() ++ "\n"'     # <orig-commit>
```

Use the **commit ID** (not the change ID) to refer to the original state later: once the split rewrites the change, its change ID points at a rewritten commit, while `<orig-commit>` keeps pointing at the pre-split content.

---

## 6. Split the Changeset

Do **not** use `jj split -i` (interactive mode) — it opens an editor, which doesn't work in an agent context. Use either file-path-based splitting or the manual reconstruction approach below.

### 6.1. File-Path Split (When Each File Belongs to One Group)

`jj split -r <rev> -m "<desc>" <paths...>` divides a changeset into two:
- **First** changeset: contains only the changes to the specified paths, gets `<desc>`, and **keeps the original change ID**
- **Second** changeset: contains everything else, keeps the original description, gets a **new change ID**, and receives the original's bookmarks and descendants

Always pass `-m`: without it, `jj split` opens an editor for the first changeset's description.

To split into more than two groups, run `jj split` repeatedly on the remainder.

```bash
# First split: extract the refactor files from the original (say: abc)
jj split -r abc -m "refactor: update foo and bar signatures" src/foo.rs src/bar.rs
# Output lists "Selected changes" (abc) and "Remaining changes" — say it's def.

# Second split: extract the feature files from the remainder
jj split -r def -m "feat: add new_thing implementation" src/new_thing.rs src/lib.rs
# Now you have three changesets: refactor, feature, tests
```

### 6.2. Hunk-Level Split (When a File Has Mixed Changes)

When a single file contains hunks belonging to different logical groups, you cannot use `jj split -r <rev> <path>` because it moves the entire file. Instead, construct each changeset manually by creating empty changesets and writing the desired file contents into them.

**Strategy:** For each group, create a new empty changeset off the parent, then populate it — using `jj restore --from <orig-commit>` for whole-file inclusions and direct file writes for partial-file inclusions. The original changeset is only read from, and is abandoned once the split is verified ([Step 10](#10-clean-up)).

Always read the original through `<orig-commit>` recorded in [Step 5](#5-record-the-repo-state), never through the revset the user gave: a position-dependent revset such as `@` moves with each `jj new`, and would then point at the empty changeset being built.

```bash
# <orig-commit> is the original's commit ID, recorded in Step 5

# Identify the parent of the original
jj log -r '<orig-commit>-' --no-graph -T 'change_id.short() ++ "\n"'
# Say the parent is: parent

# --- Group 1: refactor (whole file src/bar.rs + some hunks from src/foo.rs) ---

# Create an empty changeset on the parent
jj new <parent>
jj log -r @ --no-graph -T 'change_id.short() ++ "\n"'
# Now the working copy is a new empty changeset — say it's g1

# Restore whole files that belong entirely to this group
jj restore --from <orig-commit> src/bar.rs

# For src/foo.rs, only some hunks belong here.
# 1. Read the file at the parent state (the "before")
# 2. Read the full diff from the original to understand all hunks
# 3. Apply only the desired hunks to produce the correct file content
# 4. Write the result directly
jj diff -r <orig-commit> --git src/foo.rs   # Examine hunks, decide which belong to group 1
# Write the file with only the group-1 changes applied:
cat > src/foo.rs << 'EOF'
... file contents with only the refactor hunks applied ...
EOF

# --- Group 2: feature (rest of src/foo.rs + src/new_thing.rs + src/lib.rs) ---

jj new <g1>
jj log -r @ --no-graph -T 'change_id.short() ++ "\n"'
# New empty changeset — say it's g2

# Restore whole files
jj restore --from <orig-commit> src/new_thing.rs src/lib.rs

# For src/foo.rs, apply the remaining hunks (the ones NOT in group 1).
# The starting state is g1's version of foo.rs (which already has the refactor hunks).
# Write the final version that includes both refactor + feature hunks:
cat > src/foo.rs << 'EOF'
... file contents with the feature hunks applied on top of g1 ...
EOF

# --- Group 3: tests (remaining whole files) ---

jj new <g2>
jj restore --from <orig-commit> tests/test_new_thing.rs tests/test_foo.rs
```

**How to produce the partial file contents:**

1. Run `jj diff -r <orig-commit> --git <path>` to see all hunks for the file.
2. Read the file at the parent state: `jj file show -r <parent> <path>`.
3. Determine which hunks belong to the current group (from the plan agreed with the user in [Step 4.1](#41-how-to-group-the-changes)).
4. Apply only those hunks to the parent-state content to produce the desired file.
5. Write the result to the working copy.

This is more work than file-path splitting, but it gives full hunk-level control without any interactive commands.

### 6.3. Ordering Matters

Think about the **dependency order** of the groups. If the feature changes depend on the refactor, extract the refactor first so it becomes the parent of the feature changeset. Both `jj split` and the manual approach create a parent→child chain.

---

## 7. Set Descriptions

Apply the descriptions agreed in [Step 4.2](#42-what-description-each-changeset-should-get):

```bash
jj describe -r <revset1> -m "refactor: update foo and bar signatures"
jj describe -r <revset2> -m "feat: add new_thing implementation"
jj describe -r <revset3> -m "test: add tests for new_thing and updated foo"
```

Describing a changeset rewrites it and rebases its descendants, so their **commit** IDs change. Keep referring to the split changesets by change ID.

---

## 8. Verify the Split

Verification has two parts: **completeness** (no changes lost) and **correctness** (each changeset is valid on its own).

### 8.1. Verify Completeness — No Changes Lost

The split chain starts from the original's parent, so the last split changeset must end up with the same tree as the original:

```bash
jj diff --from <orig-commit> --to <last-split-revset>
```

Use the original **commit ID** recorded in [Step 5](#5-record-the-repo-state): after a `jj split`, the original change ID resolves to the rewritten (partial) commit.

If the split is correct, this produces **no output**. Any output means changes were lost or duplicated.

Do not use `jj interdiff` for this check: it also compares descriptions, so once the descriptions differ (after [Step 7](#7-set-descriptions), or always for a hunk-level split) a correct split still prints a `JJ-COMMIT-DESCRIPTION` diff. Do not concatenate the per-changeset diffs and `diff` them against the original's either: file order and hunk boundaries differ, so a correct split shows spurious differences.

### 8.2. Verify Individual Changesets — Stat Review

Show the user the stat summary of each changeset:

```bash
jj diff -r <revset1> --stat
jj diff -r <revset2> --stat
jj diff -r <revset3> --stat
```

### 8.3. Run User-Defined Validation

Execute the validation plan from [Step 4.3](#43-how-to-validate-each-changeset). For each changeset, check out the state at that revision and run the agreed checks:

```bash
# Example: validate that the refactor changeset builds
jj new <revset1>
cargo check
# ... then validate the next changeset, etc.
```

If any validation fails, report the failure to the user and ask how to proceed — do not abandon the original or continue automatically. One option to offer is rolling back to the recorded state and redoing the split differently — see [Step 1](#1-core-safety-principle-record-the-operation-first) for the checks to make before restoring.

Present a summary of all verification results and ask the user to confirm before proceeding.

---

## 9. Rebase Dependents (If Needed)

After a file-path split ([6.1](#61-file-path-split-when-each-file-belongs-to-one-group)), `jj split` already rebased the original's descendants onto the last split changeset — check with `jj log` that this is where the user wants them, and skip the rest of this step otherwise.

After a hunk-level split ([6.2](#62-hunk-level-split-when-a-file-has-mixed-changes)), the dependents still sit on the original. **Only after all verification passes and the user has confirmed**, check whether other changesets depend on the original:

```bash
jj log -r '<orig-commit>+'
```

If there are dependents, ask the user which split changeset they should be rebased onto:

> The following changesets are children of the original:
> - `xyz` ("add benchmarks")
>
> Which split changeset should they be rebased onto?
> 1. `<revset1>` — "refactor: update foo and bar signatures"
> 2. `<revset2>` — "feat: add new_thing implementation"
> 3. `<revset3>` — "test: add tests for new_thing and updated foo"

Then rebase as directed:

```bash
jj rebase -s <dependent> -o <new-parent>
```

---

## 10. Clean Up

After a file-path split there is nothing to abandon: the original change ID stays on the first split changeset, while its bookmarks and descendants move to the last one ([6.1](#61-file-path-split-when-each-file-belongs-to-one-group)). Only the working copy needs restoring (below).

After a hunk-level split, the original's change ID disappears with it, and `jj abandon` **deletes** bookmarks pointing at it. Check for bookmarks first:

```bash
jj bookmark list -r <orig-commit>
```

If there are any (e.g. a PR branch), ask the user which split changeset each should move to — usually the last one — and move it. The move is sideways, so it needs `--allow-backwards`:

```bash
jj bookmark set <name> -r <new-target> --allow-backwards
```

Then, once everything is verified, abandon the original — but only once `jj log -r '<orig-commit>+'` shows no children left. Refer to it by `<orig-commit>`, not by the revset the user gave: if that was a bookmark name, the bookmark now points at a split changeset, and abandoning it would drop part of the split. `jj abandon` moves any remaining child onto the original's parent, where it loses the original's changes and may conflict or stop building; if the user declined to rebase a child in [Step 9](#9-rebase-dependents-if-needed), ask again before abandoning.

```bash
jj abandon <orig-commit>
```

Finally, put the working copy back where it was before the split (noted in [Step 5](#5-record-the-repo-state)); the validation in [8.3](#83-run-user-defined-validation) leaves it on the last checked changeset.

- If `@` was the changeset being split, it now corresponds to the whole stack: edit the **last** split changeset. Do not `jj edit` the recorded change ID — after a file-path split it holds only the first group, and after a hunk-level split it was abandoned.
- Otherwise (`@` was an empty child of the original, a dependent, or elsewhere), go back to the recorded change if it still exists — a child of the original was moved onto the split stack with the other dependents ([Step 9](#9-rebase-dependents-if-needed)), so editing it restores the user's stack without adding a new head. jj abandons a working-copy change that is empty and undescribed as soon as `@` moves away, so if `jj log -r <change-id>` reports that the revision does not exist, create a new empty change on the last split changeset instead.

```bash
jj edit <last-split>      # @ was the original
jj edit <change-id>       # @ was elsewhere and the change still exists
jj new <last-split>       # @ was an empty child that jj has since abandoned
```

If the user is unhappy with the final result, the recorded operation can still bring back the pre-split state, subject to the checks in [Step 1](#1-core-safety-principle-record-the-operation-first).

---

## 11. Key Commands Reference

| Command | Purpose |
|---|---|
| `jj op log -n1 --no-graph -T 'id.short() ++ "\n"'` | Record the current operation ID before modifying anything |
| `jj op restore <op-id>` | Roll the whole repo view back to the recorded state (see [Step 1](#1-core-safety-principle-record-the-operation-first)) |
| `jj op revert <op-id>` | Undo a single operation, leaving later unrelated ones in place |
| `jj split -r <revset> -m "<desc>" <paths...>` | Split by file paths (specified paths → first changeset, rest → second) |
| `jj restore --from <src> [paths...]` | Copy file states from one changeset into the current working changeset |
| `jj diff --from <a> --to <b>` | Compare two trees (empty = identical) |
| `jj file show -r <revset> <path>` | Print a file's contents at a given revision |
| `jj bookmark list -r <revset>` | List bookmarks pointing at a revision |
| `jj bookmark set <name> -r <revset> --allow-backwards` | Move a bookmark sideways onto a split changeset |
| `jj describe -r <revset> -m "..."` | Set a changeset's description |
| `jj abandon <revset>` | Abandon a changeset (only after verifying a hunk-level split) |
| `jj rebase -s <src> -o <dest>` | Rebase a changeset and its descendants onto a new parent |
| `jj diff -r <revset> --stat` | Show what a changeset changes (summary) |
| `jj diff -r <revset> --git` | Show full diff of a changeset in unified format |
| `jj log -r <revset>` | Show changeset metadata |
| `jj new <revset>` | Create a new empty changeset as a child |

---

## 12. When to Prompt the User

**Always ask before:**
- Deciding how to group changes ([4.1](#41-how-to-group-the-changes))
- Setting commit descriptions ([4.2](#42-what-description-each-changeset-should-get))
- Defining validation criteria for each changeset ([4.3](#43-how-to-validate-each-changeset))
- Proceeding after any validation failure ([8.3](#83-run-user-defined-validation))
- Rebasing dependent changesets ([9](#9-rebase-dependents-if-needed))
- Abandoning the original changeset ([10](#10-clean-up))
- Moving bookmarks off the original changeset ([10](#10-clean-up))
- Restoring a recorded operation with `jj op restore`

**Don't ask, just do:**
- `jj op log` — recording or listing operations only snapshots the working copy
- `jj diff --stat` / `jj diff` — read-only inspection
- `jj log` — read-only inspection
- `jj diff --from/--to` — read-only verification
- Running the agreed validation commands ([8.3](#83-run-user-defined-validation))

---

## 13. Limitations

- **No interactive commands.** Never use `jj split -i` or any command that opens an editor. Use file-path-based `jj split` for whole-file splits and the manual `jj new` + `jj restore` + file-write approach for hunk-level splits.
- **Hunk-level splits require care.** When manually applying hunks, verify the resulting file contents are correct. A mistake here is harder to spot than a wrong file-level grouping. The completeness check in [8.1](#81-verify-completeness--no-changes-lost) will catch lost or duplicated changes.
