# Auto-backport a merged PR

You are running inside GitHub Actions to backport a merged RediSearch PR to one or
more release branches. The triggering workflow has already:

- Checked out the repository at master with full history (`fetch-depth: 0`).
- Configured `git` with a committer identity for the bot.
- Set `GH_TOKEN` so `gh` and `git push` are authenticated with write access to
  this repo's contents and pull requests.
- Written a small context file describing what to backport.

You do not need to install tools, switch accounts, or configure credentials.

## Read the context file

The workflow writes a small JSON file outside the working tree (in `$RUNNER_TEMP`)
and passes its absolute path through the environment variable
`$BACKPORT_CONTEXT_FILE`. **Read it from there**, not from the repo:

```bash
cat "$BACKPORT_CONTEXT_FILE"
```

Keeping the file out of the working tree is deliberate — it ensures the
`git add -A` you run during cherry-pick conflict resolution (see below) cannot
accidentally stage CI metadata into the backport commit. Do not copy the file
into the repo for any reason.

The file looks like:

```json
{
  "pr": 8774,
  "sha": "1a2b3c4d5e6f7890...",
  "title": "[MOD-15720] fix fork-GC crash on empty tag value with WITHSUFFIXTRIE",
  "url": "https://github.com/RediSearch/RediSearch/pull/8774",
  "targets": ["8.8", "8.6-rse", "8.6", "8.2"]
}
```

Fields:

- `pr` — the original merged PR number.
- `sha` — the commit on master to cherry-pick (the squash-merge commit).
- `title` — the original PR title, to reuse in backport PR titles.
- `url` — the original PR URL.
- `targets` — the list of release branches to backport to, already deduplicated.

If the file is missing or malformed, or `$BACKPORT_CONTEXT_FILE` is empty, stop
and print a one-line error. Do not push anything.

## How this differs from the manual backport skill

The manual `pr-backport` workflow that humans use locally is in
`.skills/pr-backport/SKILL.md`. Read it if you need additional background on the
project's conflict patterns. This automated flow is different in three ways:

- **No worktrees.** You operate in a single CI checkout; `git checkout -B` switches
  between target branches in place.
- **No local build or tests.** The backport PR's own CI runs the full build and
  test matrix. Do not invoke `./build.sh`, `cargo`, `make`, or any test runner.
- **Multi-branch in one run.** You process every target in `targets` sequentially
  so the context you build up on a newer branch (the PR's intent, the conflict
  resolutions you chose) carries over to older branches.

## Plan

1. Read the context file.
2. Read the original PR description: `gh pr view <pr> --json title,body,labels,files`.
   Look for compatibility-sensitive areas before touching any branch:
   - `src/rdb.c` or serialization → RDB version may differ on older branches.
   - `src/config.c` → config options may not exist on older branches.
   - New commands or command args in `src/module.c` → may not exist on older branches.
   - Test files (`tests/pytests/`, `tests/cpptests/`) → append-heavy, expect conflicts
     with unrelated tests that landed on master in the meantime.
3. Sort `targets` newest-to-oldest by release line (e.g. `8.8` before `8.6` before
   `8.4` before `8.2`). Same-version variants such as `8.6` and `8.6-rse` are peers
   — process them adjacent to each other in either order.
4. For each target in that order, perform the per-branch loop below.
5. At the end, print the **final summary** in the exact format shown at the bottom
   of this prompt so the workflow log captures it.

## Per-branch loop

Process each target **independently**. If one target fails unexpectedly (a push
is rejected, `gh` errors out, the branch is missing), record it in the summary
and move on to the next — never let one target abort the rest of the run.

For each `TARGET` in the ordered list, first run two pre-flight checks:

```bash
BRANCH="backport-agent/pr-${PR}-to-${TARGET}"

# 1) The target branch must exist. Targets supplied via `/backport-agent <list>`
#    are user-typed and may be wrong; a missing branch must not abort the run.
if ! git ls-remote --exit-code --heads origin "${TARGET}" >/dev/null 2>&1; then
  # Record `skipped — no such branch ${TARGET}` in the summary and continue
  # to the next target. Do not push or open anything.
  continue
fi

# 2) Idempotency. This workflow can fire more than once for the same
#    (PR, target) — e.g. a `backport-<branch>-agent` label removed and re-added
#    after merge, or a `/backport-agent <target>` comment for a target already
#    handled. If an open backport PR already exists for this head branch, do not
#    push or re-open it.
existing=$(gh pr list --head "${BRANCH}" --state open --json url --jq '.[0].url // empty')
if [ -n "${existing}" ]; then
  # Record `already open — ${existing}` in the summary and continue to the
  # next target. Do not force-push over an existing backport PR.
  continue
fi
```

Then cherry-pick onto a fresh branch:

```bash
git fetch origin "${TARGET}"
git checkout -B "${BRANCH}" "origin/${TARGET}"
git cherry-pick "${SHA}"
```

> **Squash-merge assumption.** RediSearch squash-merges PRs, so `sha` is the
> single squashed commit on master with one parent — a plain
> `git cherry-pick "${SHA}"` applies it directly, which is what this flow
> expects and handles. If a target ever resolves to a *true merge commit*
> (multiple parents), `git cherry-pick` will refuse with
> `is a merge but no -m option was given`. Do **not** guess a parent with `-m`:
> treat that as a non-mechanical case, `git cherry-pick --abort`, and skip the
> target for manual backport (record it in the summary).

If the cherry-pick succeeds cleanly:

- Push the branch and open the backport PR (see "Open the PR" below).
- Note "clean" in your running summary.

If the cherry-pick reports conflicts:

- For each conflicted file:
  1. Read the conflict markers and surrounding context.
  2. Look at what changed on the target branch vs. master since the PR was merged:
     `git log --oneline origin/${TARGET}..origin/master -- <path>`
  3. Look at exactly what the original PR added or changed in this file:
     `git show ${SHA} -- <path>`
  4. Resolve preserving the **intent** of the original change. Common patterns:
     - Code refactored differently on the target branch — adapt the fix to the
       target-branch shape.
     - Features that don't exist on the target branch — drop references to them.
     - Config options or struct fields added after the branch point — drop the
       config-related lines but keep the underlying fix.
     - **Append-heavy test files**: conflicts there usually contain *other* tests
       that landed on master in the meantime. **Only keep test additions that
       belong to this PR.** Verify against `git show ${SHA} -- <test_file>`.
  5. Record the resolution as one entry for the Conflict Log: file path, a short
     description of what differed, the choice made, and the rationale.
- Once every file is resolved: `git add -A && git cherry-pick --continue`.
- Push the branch and open the backport PR with the Conflict Log in the body.

If you cannot confidently resolve a conflict (genuinely ambiguous semantics, a
feature removed in a non-mechanical way, a dependent change you don't understand):

```bash
git cherry-pick --abort
```

Do **not** push a branch or open a PR for this target. Record it as
`skipped — manual backport required (<short reason>)` in the summary.

## Open the PR

```bash
git push -u origin "${BRANCH}"
gh pr create \
  --base "${TARGET}" \
  --head "${BRANCH}" \
  --title "[${TARGET}] <original PR title>" \
  --body-file <(cat <<'EOF'
... see body template below ...
EOF
)
```

After creating the PR, set labels with `gh pr edit "<new-pr-url>" --add-label "<label>"`.
All labels you need already exist in the repo — do **not** run `gh label create`
(the token cannot create label definitions, and labels are provisioned ahead of time):

- Add `auto-backport` to every PR you open.
- Add `auto-backport-conflicts` to PRs where you resolved any conflict.
- Copy labels from the original PR **except** any matching `^backport-.+-agent$` or
  `^backport .+$` — propagating those would re-fire the backport workflows.

## PR body template

```markdown
Backport of #<pr> to `<target>`.

## Original PR
- Title: <original title>
- Link: <original url>
- Merge commit: `<sha>`

## Cherry-pick result
<one of>:
- Clean cherry-pick — no conflicts.
- Resolved N conflict(s) — see Conflict Log below.

## Conflict Log
<only present if N > 0. One entry per conflicted hunk:>

### `<path>:<line-range>`
- **Conflict:** <what the two sides were doing differently>
- **Why it conflicted:** <what changed on `<target>` vs master since the PR was merged>
- **Resolution:** <what was kept, dropped, or merged>
- **Rationale:** <why this preserves the intent of the original change>

## Release notes
<copy the release-notes checkbox state from the original PR body — exactly one must be checked>

- [ ] This PR requires release notes
- [ ] This PR does not require release notes

🤖 Generated by the auto-backport workflow.
```

Keep the Conflict Log honest. If a resolution is uncertain, say so — reviewers will
look. It is better to flag a hunk as "best-effort, please verify" than to claim
confidence you don't have.

## Final summary

After processing all targets, print exactly this block (one row per target, in the
order you processed them):

```
Auto-backport summary for PR #<pr> (<sha>):

  <target>      <status>      <pr-url-or-reason>
```

Examples:

```
Auto-backport summary for PR #8774 (1a2b3c4):

  8.8           clean         https://github.com/RediSearch/RediSearch/pull/9999
  8.6-rse       conflicts(2)  https://github.com/RediSearch/RediSearch/pull/10000
  8.6           conflicts(2)  https://github.com/RediSearch/RediSearch/pull/10001
  8.2           skipped       Non-mechanical conflict in src/rdb.c — manual backport required
```

Targets stopped by a pre-flight check use the same `skipped` status with the
reason in the last column, e.g. `skipped  already open — <url>` or
`skipped  no such branch foo`.

Do not invent PR URLs you did not create. If you skipped a target, do not push a
branch or open a PR for it; the summary line is all the workflow needs to surface
the gap to a human.

## Guardrails

- Do **not** force-push, amend, or rewrite history on any `origin/<target>` branch.
  Only push to `backport-agent/...` branches you create.
- Do **not** push or open PRs for targets you skipped.
- Do **not** copy `backport-<branch>-agent` or `backport <branch>` labels from the
  original PR. They would re-trigger the backport workflows.
- Do **not** invent the original PR's release-notes selection — read the original
  PR body and copy the exact checkbox state.
- Do **not** run `./build.sh`, `cargo`, `make`, or any test runner. The backport
  PR's own CI is the source of truth.
- Do **not** modify files outside what the cherry-pick / conflict resolution
  produces. The backport must be a faithful port of the original commit.
