---
name: pr-backport
description: Backport a merged PR to a release branch. Use this when you need to cherry-pick a fix or feature into an older branch.
---

# PR Backport

Backport a merged PR to one or more release branches.

## Arguments

`$ARGUMENTS` should contain:
- The PR number or commit SHA(s) to backport
- The target branch(es) (e.g., `2.10`, `8.2`, `8.6-rse`, `8.8`)

Example: `/pr-backport pr:8774 8.6-rse 8.2 2.10`

## Instructions

### 1. Identify the source commit

PRs are squash-merged via the merge queue, so each PR lands as a single commit on master.
Find that commit:

```bash
gh pr view <number> --json mergeCommit
```

Or search the log:
```bash
git log --oneline --grep="#<number>" master
```

Read the PR description to understand the change and any compatibility considerations.

### 2. Set up worktrees and backport branches

For each target branch, create a worktree if one doesn't already exist, then create
a dedicated backport branch. Do not cherry-pick directly on the release branch.
The worktree keeps each target release checkout isolated; the dedicated backport
branch is what makes the final push and PR safe.

```bash
git fetch origin <branch>
git worktree add .worktrees/backport-<branch> origin/<branch>
cd .worktrees/backport-<branch>
git checkout -b backport/pr-<number>-to-<branch>
```

If a worktree already exists for that branch, reuse it after confirming it is clean:

```bash
cd .worktrees/backport-<branch>
git status --short
git fetch origin <branch>
git checkout -b backport/pr-<number>-to-<branch> origin/<branch>
```

If the backport branch already exists, check it out instead and verify it is based
on the updated target branch before cherry-picking.

### 3. Cherry-pick

For each target release line, start with the newest version closest to master and
work backward. For example, process `8.8` before `8.6`, `8.6` before `8.4`,
and `8.4` before `8.2`.

Treat same-version variants such as `8.6` and `8.6-rse` as peers; process them
in whichever order is more practical for the backport or follows team convention.

```bash
cd .worktrees/backport-<branch>
git branch --show-current  # should be backport/pr-<number>-to-<branch>
git cherry-pick <sha>
```

If cherry-pick produces conflicts:
- Read the conflict markers carefully.
- Understand what changed on the target branch vs. master since the PR was merged.
- Resolve the conflicts, preserving the intent of the original fix.
- Common conflict sources:
  - Code that was refactored differently on the target branch.
  - Features that don't exist on the target branch (remove references to them).
  - Config options or struct fields added after the branch point.

### 4. Verify compatibility

After resolving conflicts, check for issues specific to backports:

**RDB serialization**: if the PR touches `src/rdb.c` or serialization code, verify that
the encoding version on the target branch is compatible. The target branch may have a
different RDB version than master.

**API surface**: if the PR adds new Redis commands or command arguments, verify that the
target branch supports them. Some features may not exist on older branches and the
backport should only include the bug fix, not the new feature.

**Config**: if the PR adds or modifies config options in `src/config.c`, verify that the
config parameter exists on the target branch.

### 5. Build and test

```bash
cd .worktrees/backport-<branch>
./build.sh FORCE
./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```

If the original PR includes specific test files, run those:

```bash
./build.sh RUN_PYTEST TEST=<test_file>
```

### 6. Create the backport PR

```bash
cd .worktrees/backport-<branch>
git push -u origin backport/pr-<number>-to-<branch>
gh pr create \
  --base <branch> \
  --head backport/pr-<number>-to-<branch> \
  --title "[<branch>] <original PR title>" \
  --body "Backport of #<original PR number> to <branch>.

## Original PR
<link to original PR>

## Changes from original
<describe any modifications needed for the backport, or 'Clean cherry-pick, no changes needed.'>
"
```

### 7. Multi-branch backports

When backporting to multiple branches, use the same newest-to-oldest release-line
order described above. Conflicts tend to be simpler on newer branches, and the
resolution strategy from a newer branch often applies to older branches too.

Report a summary for each target branch:
- Clean cherry-pick or conflicts resolved
- Build status
- Test status
- PR link
