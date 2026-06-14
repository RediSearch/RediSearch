---
name: branch-out-release
description: Create a new RediSearch public release branch from master or prepare branch-out changes. Use when branching a version line such as 8.10 from master, setting initial version.h, copying GitHub branch protection, adapting release-branch CI gates, triggering validation, or documenting follow-up master-to-release sync policy.
---

# Branch Out Release

Use this for RediSearch OSS release branch creation from `master`.

## Workflow

1. Resolve current state.
   - Fetch `origin/master` and the latest release branch used as the template, usually the previous line such as `8.8`.
   - Verify the target branch does not already exist:
     `git ls-remote --heads origin <target-branch>`.
   - Check current `master` validation before branch-out. Report the latest `Nightly Flow` status and distinguish infra failures from product/test failures.
   - Use a clean worktree instead of the user's dirty checkout.

2. Create the official branch.
   - Branch from the selected `origin/master` SHA.
   - Set `src/version.h` to the requested initial milestone. For `8.10`, this was `8.9.80`.
   - Commit with a clear message such as `Initial 8.10 branch`.
   - Push the new branch before applying branch protection.

3. Add branch protection.
   - Copy settings from the previous public release branch, not from memory.
   - Create a new exact-pattern rule for the new branch, for example `8.10`.
   - Do not modify the source branch rule and do not use a wildcard rule.
   - Explicitly set every relevant value, including false/null-like settings, so GitHub does not fall back to new-rule defaults.
   - Preserve required status check bindings. In this repo, `pr-validation` is bound to the GitHub Actions app and `license/cla` is unbound.
   - Read back both source and target protection rules and compare. Only ids, URLs, pattern, and matching refs should differ.

4. Enable merge queue.
   - Treat merge queue as a separate configuration surface from classic branch protection.
   - Read the previous release branch queue with `repository.mergeQueue(branch: "<source-branch>")`.
   - Enable a queue for the new branch only, using a branch-scoped rule or settings entry for `refs/heads/<target-branch>`.
   - Copy all queue parameters from the source branch. For `8.8` to initial `8.10`, the effective values were `mergeMethod=SQUASH`, `mergingStrategy=ALLGREEN`, `maximumEntriesToBuild=5`, `maximumEntriesToMerge=5`, `minimumEntriesToMerge=1`, `minimumEntriesToMergeWaitTime=300`, and `checkResponseTimeout=21600`.
   - Read back `repository.mergeQueue(branch: "<target-branch>")` and compare. Only ids, URLs, labels/names, target refs, and current queue entries should differ.

5. Adapt release-branch CI.
   - Do this as a normal PR after protection is enabled.
   - Compare the new branch against the previous release branch under `.github/workflows/`.
   - The important release-branch CI invariants are:
     - PR flow stays lightweight: Ubuntu quick tests by default. Coverage and sanitize are label-conditioned: they run only when a non-draft PR workflow event sees `enforce:coverage` or `enforce:sanitize`.
       Adding those labels alone may not start a run; add the label before a push/update or rerun the workflow.
     - Merge queue is the release gate: full supported platform/architecture matrix, full tests, coverage/sanitize, fail-fast, separate coordinator jobs.
     - Nightly is less critical than master: full matrix but `QUICK=1`, pinned Redis SHA, not Redis `unstable`.
   - Pin Redis to the current release-branch SHA until there is a better release/pre-release line. For `8.8` and the initial `8.10` branch-out, this SHA was `3cd464263b03b425ffae2e23db24df3dc9346871`.
   - Check whether `RedisJSON/RedisJSON` has a matching release branch:
     `git ls-remote --heads git@github.com:RedisJSON/RedisJSON.git <target-branch>`.
     Use the matching branch for PR/MQ only if it exists. Otherwise keep `master` and add a TODO in the workflow.
   - Preserve newer master CI infrastructure unless it conflicts with the release-branch gating policy.

6. Validate.
   - Inspect diffs for `8.8`, `master`, and the new branch so release-only CI behavior is intentional.
   - Manually trigger `Nightly Flow` on the new branch if needed:
     `gh workflow run event-nightly.yml --repo RediSearch/RediSearch --ref <target-branch>`.
   - Remember that scheduled workflows run from the default branch; use manual dispatch for release branch nightlies unless a dedicated schedule exists.

7. Future sync policy.
   - Prefer normal merges from `master` into the release branch while the release branch is still tracking master closely.
   - Do not force-push or overwrite the official release branch.
   - During master-to-release merges, preserve release-only files such as `src/version.h` and release-branch CI differences.
   - Do not merge release branch version changes back into `master`.

## PR Notes

When opening PRs, use `.github/PULL_REQUEST_TEMPLATE.md`.

- Branch creation/version or release CI changes are user/release-facing enough to require release notes unless the release manager says otherwise.
- Skill-only documentation on `master` is internal-only and normally does not require release notes.
