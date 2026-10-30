# Fast-track approval

Run **Actions → Request Fast-Track Approval** from `master` with your PR number.
The PR must target `master`, be ready for review, and name exactly one Jira ticket
in its title. That ticket must carry `ai-fast-track`. Only the PR author, with
repository write access, can request approval.

The policy, script, tests, and setup instructions live in
[redisearch-ci-common](https://github.com/redislabsdev/redisearch-ci-common/blob/v1/scripts/fast_track/README.md).
The thin caller pins both the reusable workflow and its script to `v1`.
Publish the shared release before merging this caller migration.

This is a policy approval. Required CI, reviews, and merge-queue rules still apply.
