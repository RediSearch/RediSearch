---
name: jj-split-changeset
description: Split a jj (Jujutsu) changeset into smaller, focused changesets. Use when asked to break up a large changeset, split commits, reorganize changes across revisions, or create stacked PRs from a single changeset. Covers safe duplication-based workflows, file-path and hunk-level splitting without interactive commands.
---

# Splitting a jj Changeset

Split the changeset `$ARGUMENTS` into smaller, focused units — safely, efficiently, and with user involvement at the right moments.

If `$ARGUMENTS` is empty, ask the user which revset to split before proceeding.

## Table of Contents

1. [Core Safety Principle: Duplicate First](#1-core-safety-principle-duplicate-first)
2. [Workflow Overview](#2-workflow-overview)
3. [Inspect the Changeset](#3-inspect-the-changeset)
4. [Plan with the User](#4-plan-with-the-user)
   - 4.1 [How to group the changes](#41-how-to-group-the-changes)
   - 4.2 [What description each changeset should get](#42-what-description-each-changeset-should-get)
   - 4.3 [How to validate each changeset](#43-how-to-validate-each-changeset)
5. [Duplicate the Changeset](#5-duplicate-the-changeset)
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

## 1. Core Safety Principle: Duplicate First

**Never edit the original changeset directly.** Always duplicate it first, then work on the duplicate. This gives you a free undo — the original remains untouched until you're confident the split is correct.

```bash
# ALWAYS start here
jj duplicate <revset>       # Creates an identical copy; prints the new change ID
```

Only after the split is complete and verified should the original be abandoned:

```bash
jj abandon <original-revset>
```

This principle applies to every step below.

---

## 2. Workflow Overview

1. **Inspect** — understand what's in the changeset
2. **Plan with the user** — agree on groupings, descriptions, and validation criteria
3. **Duplicate** — create a safe working copy
4. **Split** — file-path-based or hunk-level
5. **Describe** — set meaningful descriptions on each resulting changeset
6. **Verify** — confirm no changes were lost, then run user-defined validation
7. **Rebase dependents** — ask the user if anything needs rebasing
8. **Clean up** — abandon the original

---

## 3. Inspect the Changeset

Before doing anything, understand what you're working with.

```bash
# Show the full diff with file names and stats
jj diff -r <revset> --stat
jj diff -r <revset>

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

## 5. Duplicate the Changeset

```bash
jj duplicate <revset>
# Note the new change ID from the output — this is your working copy
```

All subsequent operations target the **duplicate**, not the original.

---

## 6. Split the Changeset

Do **not** use `jj split -i` (interactive mode) — it opens an editor, which doesn't work in an agent context. Use either file-path-based splitting or the manual reconstruction approach below.

### 6.1. File-Path Split (When Each File Belongs to One Group)

`jj split -r <rev> <paths...>` divides a changeset into two:
- **First** changeset: contains only the changes to the specified paths
- **Second** changeset: contains everything else

To split into more than two groups, run `jj split` repeatedly on the remainder.

```bash
jj duplicate <original>
# Say this produces change ID: abc

# First split: extract the refactor files
jj split -r abc src/foo.rs src/bar.rs
# Output tells you the two new change IDs.
# The "remainder" changeset (everything except foo+bar) — say it's def.

# Second split: extract the feature files from the remainder
jj split -r def src/new_thing.rs src/lib.rs
# Now you have three changesets: refactor, feature, tests
```

### 6.2. Hunk-Level Split (When a File Has Mixed Changes)

When a single file contains hunks belonging to different logical groups, you cannot use `jj split -r <rev> <path>` because it moves the entire file. Instead, construct each changeset manually by creating empty changesets and writing the desired file contents into them.

**Strategy:** For each group, create a new empty changeset off the parent, then populate it — using `jj restore --from <duplicate>` for whole-file inclusions and direct file writes for partial-file inclusions.

```bash
jj duplicate <original>
# Say this produces change ID: dup

# Identify the parent of the duplicate
jj log -r 'dup-' --no-graph -T 'change_id'
# Say the parent is: parent

# --- Group 1: refactor (whole file src/bar.rs + some hunks from src/foo.rs) ---

# Create an empty changeset on the parent
jj new <parent>
# Now the working copy is a new empty changeset — say it's g1

# Restore whole files that belong entirely to this group
jj restore --from <dup> src/bar.rs

# For src/foo.rs, only some hunks belong here.
# 1. Read the file at the parent state (the "before")
# 2. Read the full diff from the duplicate to understand all hunks
# 3. Apply only the desired hunks to produce the correct file content
# 4. Write the result directly
jj diff -r <dup> src/foo.rs   # Examine hunks, decide which belong to group 1
# Write the file with only the group-1 changes applied:
cat > src/foo.rs << 'EOF'
... file contents with only the refactor hunks applied ...
EOF

# --- Group 2: feature (rest of src/foo.rs + src/new_thing.rs + src/lib.rs) ---

jj new <g1>
# New empty changeset — say it's g2

# Restore whole files
jj restore --from <dup> src/new_thing.rs src/lib.rs

# For src/foo.rs, apply the remaining hunks (the ones NOT in group 1).
# The starting state is g1's version of foo.rs (which already has the refactor hunks).
# Write the final version that includes both refactor + feature hunks:
cat > src/foo.rs << 'EOF'
... file contents with the feature hunks applied on top of g1 ...
EOF

# --- Group 3: tests (remaining whole files) ---

jj new <g2>
jj restore --from <dup> tests/test_new_thing.rs tests/test_foo.rs
```

**How to produce the partial file contents:**

1. Run `jj diff -r <dup> <path>` to see all hunks for the file.
2. Read the file at the parent state: `jj cat -r <parent> <path>`.
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

---

## 8. Verify the Split

Verification has two parts: **completeness** (no changes lost) and **correctness** (each changeset is valid on its own).

### 8.1. Verify Completeness — No Changes Lost

Use `jj interdiff` to confirm the combined result of the split matches the original:

```bash
jj interdiff --from <original-revset> --to <last-split-revset>
```

If the split is correct, this produces **no output** (empty diff). Any output means changes were lost or duplicated.

If `jj interdiff` is not available in your version, fall back to comparing raw diffs:

```bash
diff <(jj diff -r <original-revset>) <(jj diff -r <first-split> && jj diff -r <second-split> && jj diff -r <third-split>)
```

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

If any validation fails, report the failure to the user and ask how to proceed — do not abandon the original or continue automatically.

Present a summary of all verification results and ask the user to confirm before proceeding.

---

## 9. Rebase Dependents (If Needed)

**Only after all verification passes and the user has confirmed**, check whether other changesets depend on the original:

```bash
jj log -r '<original-revset>+'
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

Once everything is verified and dependents are handled:

```bash
jj abandon <original-revset>
```

---

## 11. Key Commands Reference

| Command | Purpose |
|---|---|
| `jj duplicate <revset>` | Create a safe copy before any destructive operation |
| `jj split -r <revset> <paths...>` | Split by file paths (specified paths → first changeset, rest → second) |
| `jj restore --from <src> [paths...]` | Copy file states from one changeset into the current working changeset |
| `jj interdiff --from <a> --to <b>` | Show diff between two changesets (empty = identical) |
| `jj cat -r <revset> <path>` | Print a file's contents at a given revision |
| `jj describe -r <revset> -m "..."` | Set a changeset's description |
| `jj abandon <revset>` | Abandon a changeset (only after verifying the split) |
| `jj rebase -s <src> -o <dest>` | Rebase a changeset and its descendants onto a new parent |
| `jj diff -r <revset> --stat` | Show what a changeset changes (summary) |
| `jj diff -r <revset>` | Show full diff of a changeset |
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

**Don't ask, just do:**
- `jj duplicate` — always safe
- `jj diff --stat` / `jj diff` — read-only inspection
- `jj log` — read-only inspection
- `jj interdiff` — read-only verification
- Running the agreed validation commands ([8.3](#83-run-user-defined-validation))

---

## 13. Limitations

- **No interactive commands.** Never use `jj split -i` or any command that opens an editor. Use file-path-based `jj split` for whole-file splits and the manual `jj new` + `jj restore` + file-write approach for hunk-level splits.
- **Hunk-level splits require care.** When manually applying hunks, verify the resulting file contents are correct. A mistake here is harder to spot than a wrong file-level grouping. The completeness check in [8.1](#81-verify-completeness--no-changes-lost) will catch lost or duplicated changes.
