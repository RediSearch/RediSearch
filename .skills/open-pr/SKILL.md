---
name: open-pr
description: Open a GitHub pull request for RediSearch. Use whenever you are asked to open a PR.
---

# Open PR

Use this workflow whenever you are asked to open a GitHub pull request for
RediSearch.

## Not this skill

- **Backporting a merged PR to a release branch** — use
  [/pr-backport](../pr-backport/SKILL.md) instead. It has its own title format and
  worktree setup.

## Workflow

1. Inspect the repository state and active VCS. Prefer `jj` when a `.jj/` directory is
   present, and fall back to Git otherwise — `.jj/` is untracked, so a colocated
   workspace has it while a fresh clone, a CI checkout, or a `git worktree` created
   outside jj does not.
2. Inspect the revision stack that will be included in the PR.
3. Inspect bookmarks or branches and remotes.
4. If the working copy is dirty, the stack is mixed, or the target history is
   already under review, load and follow the
   [/commit-guidelines](../commit-guidelines/SKILL.md) skill before
   continuing.
5. Compose the title and body from the
   [PR template](../../.github/PULL_REQUEST_TEMPLATE.md). See *Title and body* below for
   the rules. The release-notes check reads the PR body, so the deadline is creating the
   PR in step 7; a later `gh pr edit` re-triggers it.
6. Push the review head to the correct remote.
   - Under `jj`: create or update a bookmark pointing at the intended review tip first,
     then push that bookmark — `jj git push` has nothing to push without one. Follow the
     naming convention already in use (`jj bookmark list`).
   - Under Git: push the branch with `git push -u origin <branch>`.
7. Open a **draft** PR with `gh`.
8. Concurrently, using sub-agents:
   1. Verify the final PR body, title, base, and head.
   2. Load and follow the [/verify](../verify/SKILL.md) skill.
      If verification fails, provide the parent agent with a report so
      that it can address the issues.
   3. Spawn a reviewer prompted with
      [/adversarial-review](../adversarial-review/SKILL.md)'s template. Send the
      template, not the skill — that file is for you, not for the reviewer.

   Only the `/verify` agent may run `./build.sh`, `make`, or `cargo`; the metadata and
   review agents must stay read-only. See `AGENTS.md` § *Do not run build/test/lint
   commands in parallel* for why concurrent invocations cannot work.
9. Present the adversarial review findings to the user, per that skill's *Output*
   section.
10. Iterate on the outcomes of the previous steps according to the user's
    direction until verification succeeds, the findings have been addressed or
    dismissed, and any resulting changes have been pushed and passed the
    verification and metadata checks in step 8. Re-run the adversarial review on the
    updated PR under the conditions its *Workflow* section gives.

    [/commit-guidelines](../commit-guidelines/SKILL.md) governs whether you may rewrite
    history while iterating. Being a draft does not by itself grant that permission — a
    human comment on a draft revokes it.
11. Monitor the CI run triggered by the previous push **in the background** — this
    repo's pipeline takes a long time, so do not block on it. Arm a background monitor
    that emits one event per check as it lands and exits once the run completes, then
    carry on with other work until the notifications arrive:

    ```bash
    prev="" errs=0
    draft=$(gh pr view <number> --json isDraft --jq .isDraft 2>/dev/null)
    while true; do
      s=$(gh pr checks <number> --json name,bucket 2>/dev/null)
      # Trust the payload, not the exit code: gh returns 1 both for "a check failed"
      # and for "no such PR / not authenticated / network down".
      if ! jq -e 'type=="array"' <<<"$s" >/dev/null 2>&1; then
        errs=$((errs+1))
        [ "$errs" -ge 5 ] && { echo "MONITOR ABORTED: no usable payload from gh"; exit 2; }
        sleep 30; continue
      fi
      errs=0
      cur=$(jq -r '.[] | select(.bucket!="pending") | "\(.name): \(.bucket)"' <<<"$s" | LC_ALL=C sort)
      comm -13 <(echo "$prev") <(echo "$cur")
      prev=$cur
      if jq -e 'length > 0 and all(.bucket!="pending")' <<<"$s" >/dev/null; then
        if jq -e 'all(.bucket=="pass" or .bucket=="skipping")' <<<"$s" >/dev/null; then
          if [ "$draft" = "true" ]; then
            echo "DRAFT RUN GREEN — coverage/sanitize/miri are gated on non-draft and"
            echo "have NOT run yet. Re-check after marking the PR ready."
            exit 0
          fi
          echo "CI GREEN"; exit 0
        fi
        echo "CI NOT GREEN:"
        jq -r '.[]|select(.bucket!="pass" and .bucket!="skipping")|"  \(.name): \(.bucket)"' <<<"$s"
        exit 1
      fi
      sleep 30
    done
    ```

    Four things in there are load-bearing:

    - **Gate on the payload, not the exit code.** `gh pr checks` exits 8 while checks are
      pending and 1 when one has failed — but also 1 when the PR does not exist, auth has
      expired, or the network is down, with empty stdout. Branching on the exit code
      cannot tell those apart, and on empty input the completion test never fires, so the
      monitor loops forever emitting nothing.
    - **Exit non-zero when the run is not green**, so the outcome is in the exit status
      rather than only in prose an agent may not re-read. Only `fail` and `cancel` are
      failures.
    - **`skipping` is two different things.** A job skipped because
      `check-what-changed` found nothing relevant has legitimately passed. A job skipped
      because the PR is a draft has *not run yet* — `coverage`, `sanitize` and `miri` are
      gated on `!draft` in `event-pull_request.yml` and only fire on `ready_for_review`.
      Both land in the same bucket, so a draft run that is "all pass or skipping" proves
      much less than it appears to; that is why the snippet reports the draft case
      differently.
    - **Emit every terminal bucket**, not just `pass`: silence is indistinguishable from
      "still running".

    CI must succeed. If not, failures must be triaged and addressed. Check whether a
    failure is a known flake before treating it as caused by this change:
    [/report-flaky-test](../report-flaky-test/SKILL.md) and
    [/investigate-flaky-test](../investigate-flaky-test/SKILL.md) cover that path.
12. Mark the PR as "ready to review".
13. Re-arm the same monitor. Marking ready triggers a fresh run, and for a change
    touching code or tests that run is the first to execute `coverage`, `sanitize` and
    `miri`. **That** run is the one that has to be green — the draft run in step 11 is
    early feedback, not the gate. Skip this step only when step 11 reported no
    draft-gated jobs, i.e. nothing code- or test-related changed.

    PRs land through a merge queue (`.github/workflows/event-merge-to-queue.yml`), which
    runs its own validation on the way in, so a green run here is the handoff point — not
    the merge.


## Title and body

**Title.** For PRs to `master` or another primary target branch, use
`[MOD-xyz] concise user-facing summary` when a Jira ticket exists. If no ticket is known,
ask the user whether one should be opened before choosing the title. When release notes
are required, the title must describe the **user impact** — that is what the release notes
are generated from.

**Body.** Use `.github/PULL_REQUEST_TEMPLATE.md` and keep every template section,
including ones that do not apply.

**Release notes.** Exactly one of these must be ticked — CI enforces it and will fail the
PR otherwise:

```
- [x] This PR requires release notes
- [ ] This PR does not require release notes
```

Tick "requires" for user-facing changes: new commands, behavior changes, bug fixes,
performance improvements. Tick "does not require" for internal-only changes: refactoring,
CI, tests, documentation.

## Verify after creation

After creating the PR, inspect it with `gh pr view` and confirm:

- title matches repo style
- base branch is correct
- head branch or bookmark is correct
- body follows the PR template, with exactly one release-notes checkbox ticked
- all intended commits are included

If the body does not match what you requested, fix it immediately instead of
assuming the create or edit step worked.

## Output

Report the PR URL.
