---
name: close-pr
description: Close or clean up a GitHub pull request for RediSearch. Use when asked to close a PR, delete a PR branch, sanitize mistaken PR metadata, or clean up a mistaken or unwanted PR whose diff should not remain visible.
---

# Close PR

Use this workflow before closing a PR, deleting its branch, sanitizing its title or body, or
cleaning up a mistaken/unwanted PR.

## Cleanup Of Mistaken Or Unwanted PRs

Use this when a PR was opened against the wrong repository, wrong target, wrong head branch,
or with content that should not remain visible in the PR diff.

1. Pause before closing the PR, deleting the branch, sanitizing metadata, or editing history.
   Decide whether the PR should preserve its current review history.
2. If the PR has legitimate human review discussion, do not rewrite it casually. Follow
   [/commit-guidelines](../commit-guidelines/SKILL.md) before any branch rewrite decision.
3. If the PR is mistaken, noisy, or contains content that should not remain visible in the
   PR diff, force-push the head branch to a harmless commit first. Usually this is the base
   branch tip.
4. Verify in GitHub that the PR diff is empty or otherwise no longer shows the unwanted
   content.
5. Only after that verification, close the PR and sanitize title, body, or comments if
   needed.
6. Delete the branch only after verifying the PR no longer shows the unwanted diff.
7. If the PR was already closed and GitHub still shows the old diff, do not assume a normal
   force-push can fix it. Hidden `refs/pull/*` refs are generally read-only to normal users;
   escalate to repository admins or GitHub Support if the stale diff must be purged.

## Ordinary Closure

For a normal PR close where the diff does not need cleanup:

1. Verify the repository, PR number, base, head branch, and reason for closure.
2. Check whether branch deletion is requested or expected.
3. Close the PR.
4. Delete the head branch only when it is safe, owned by this work, and no other open PR
   or active branch depends on it.
5. Report the final PR state and whether the branch still exists.
