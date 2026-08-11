---
name: close-pr
description: Close or clean up a GitHub pull request for RediSearch. Use when asked to close a PR, choose between ordinary closure and cleanup closure, delete a PR branch, sanitize mistaken PR metadata, or clean up a mistaken or unwanted PR whose diff should not remain visible.
---

# Close PR

Use this workflow before closing a PR or deleting its branch.

## Choose Closure Mode

Preserve PR history by default. Closing a PR does not normally require cleanup.

Use ordinary closure unless the user, author, maintainer, or repository owner explicitly
wants the PR diff, metadata, or branch state cleaned before closure.

Use cleanup closure only when the PR is mistaken or unwanted in a way that should not keep
its current visible diff or metadata. Examples include a PR opened against the wrong public
repository, a wrong base/head that creates a misleading diff, accidentally included
unrelated or sensitive content, or an explicit request to remove or sanitize the visible PR
state before closing.

If unsure, use ordinary closure or ask before cleanup. Cleanup is exceptional and requires a
specific reason or approval.

## Ordinary Closure

Use this for normal PR closure, such as superseded, abandoned, rejected, duplicate, replaced,
or no-longer-needed PRs where the commits, review, and discussion should remain visible.

1. Verify the repository, PR number, base, head branch, and reason for closure.
2. Do not force-push, sanitize metadata, or remove commits from the visible PR diff.
3. Close the PR.
4. Delete the head branch only when deletion is safe, expected, and does not affect another
   open PR or active branch.
5. Report the final PR state and whether the branch still exists.

## Cleanup Closure

Use this when a PR was opened against the wrong repository, wrong target, wrong head branch,
or with content that should not remain visible in the PR diff or metadata.

1. Pause before closing the PR, deleting the branch, sanitizing metadata, or editing history.
   Confirm the specific cleanup reason and whether the PR should preserve its current
   review history.
2. If the PR has legitimate human review discussion, do not rewrite it casually. Ask for
   explicit approval and follow [/commit-guidelines](../commit-guidelines/SKILL.md) before
   any branch rewrite decision.
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
