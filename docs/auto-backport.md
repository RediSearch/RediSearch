# Automatic backports

The unified workflow first runs `korthout/backport-action`. A clean backport uses
no model calls. Only targets with reproduced cherry-pick conflicts enter the
Codex fallback. A deterministic finalizer reports every target in one comment
on the source PR, including when the action or agent fails.

## Labels and commands

| Interaction on the source PR | Behavior |
|---|---|
| Merge with `backport 8.6` | Backport to `8.6`, with automatic conflict fallback. |
| Add `backport 8.6` after merge | Process the PR's current backport labels. |
| `/backport` | Process targets from all current backport labels. |
| `/backport 8.6 8.2` | Process exactly these targets, overriding labels for this run. |
| `/backport >= 8.6` | Process every registered active release line at or above 8.6. |
| `/backport >= 8.6 2.10` | Union the version expansion and explicit targets. |
| `/backport-agent …` | Compatibility alias for `/backport …`, including action-first execution. |

Lists accept spaces or commas. Only the first comment line contains targets.
Version floors use numeric ordering and include registered variants such as
`8.6-rse`; they do not discover arbitrary branches from the remote. Explicit
release-shaped branch names can target branches outside the active registry.
Malformed targets and empty version expansions are reported without silently
falling back to labels. Valid sibling targets still run.

Labels applied before merge take effect at merge. Creation commands only operate
on merged PRs. Commands and post-merge label events require repository write,
maintain, or admin permission; unrelated comments and bot comments do not invoke
the agent. Removing a label does not cancel running work or close a backport PR.

Repeat `/backport <targets>` to retry unfinished targets. Existing open or merged
backport PRs are reused, including historical classic branch names. A closed,
unmerged backport requires human intervention; automation does not reopen it or
replace its history.

## Conflict fallback and CI repair

All new backport branches use `backport-agent/pr-<source PR>-to-<target>` so the
same follow-up commands work regardless of which engine created the PR:

- `/backport-fix [context]` on the **backport PR** opts into diagnosing its
  failed CI and addressing relevant reviewer feedback.
- `/backport-context <text>` supplies context for that CI repair flow.

`/backport-agent-fix` and `/backport-agent-context` remain compatibility aliases.

CI repair remains opt-in. Creation never starts a model merely because a newly
created PR later fails CI.

Before fallback, the deterministic collector reconciles action results with
GitHub and reproduces failures against fetched target revisions. It uses the
same ordered commit selection as the pinned action: squash commits, rebased
commit ranges, or original PR commits, skipping merge commits. Empty picks are
already applied; remaining commits must still be checked. When skipping those
empty commits leaves a clean remainder, the collector preserves it and publishes
it without a model call. Its commit ID is recorded before the agent runs and
verified at publication.

Confirmed conflicting targets are processed together, newest first, in one
agent invocation. Missing branches, fetch failures, and failures that reproduce
as clean cherry-picks are reported for retry without model calls. This avoids
spending tokens on publication permissions and infrastructure failures. Clean
backports of fork-sourced PRs can use the action; conflicts from these PRs still
require manual backporting, preserving the existing agent eligibility rule.

Clean PRs keep the classic action's author review request and auto-merge setting.
Recovered conflict PRs request the original author but do not enable auto-merge.
Both paths copy non-trigger labels and add `auto-backport`; recovered conflicts
also receive `auto-backport-conflicts` and a reviewer-facing conflict log.

## Reporting and failure recovery

There is one summary comment **per invocation**. The action starts a progress
comment with `comment_style: summary`. The finalizer locates that comment using
the authenticated App bot identity, run URL, and a snapshot of pre-existing
comment IDs, then replaces its body with the combined results. It never parses
comment prose to decide which branches succeeded. If the action did not create
a comment, the finalizer creates one. A hidden run-and-attempt marker makes
repeating finalization idempotent. The pinned action cannot suppress its own
comments; after an unsuccessful classic attempt, the workflow immediately
changes its comment to an in-progress message while triage and fallback run.

Agent timeouts, invalid manifests, unresolved conflicts, publication failures,
and omitted targets appear as failures alongside successful PR links. A target
that advanced during resolution must be retried against its new revision. The
summary step succeeds when the comment is posted. A separate result-check step
fails the job while a requested backport remains unfinished. A final
GitHub outage leaves the report and results in the workflow artifact instead of
silently treating a failed comment write as success.

Runs serialize per source PR without cancelling active work. GitHub's
[`queue: max`](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
retains up to 100 pending runs rather than replacing the pending request. Runs
beyond GitHub's queue limit are cancelled and need to be retriggered.

## Maintenance and validation

`task-backport_pr.yml` is the only creation event entry point. The separate
agent-create workflow has been removed. Only `backport <branch>` labels select
targets; legacy `backport-<branch>-agent` labels are ignored and can be deleted.
The `/backport-agent` command remains an alias for `/backport`.
The source of truth for active release lines is `.github/release-branches.json`.

`resolve_create.py` authorizes requests. `unified.py` owns deduplication,
triage, manifest application, and final reporting. `apply_create.py` supplies
validated publication and PR/summary templates. If the pinned backport action
changes its merge-selection algorithm or summary format, update the collector
and its parity tests together.

The classic App token is explicitly revoked and its checkout removed before
Codex starts. The agent has no GitHub token or command network. Publishing
scripts are restored from the trusted checkout, agent clone configuration is
sanitized, and a fresh scoped App token is minted only afterward. Agent output
cannot add targets or replace successful deterministic results.

Run the stdlib suite locally (no GitHub writes or model calls):

```bash
python3 -m unittest discover -s scripts/auto_backport/tests -v
```

The suite includes real temporary git repositories for conflict and empty-pick
behavior. The workflow test path filter covers the unified entry point. Before
production rollout, exercise clean, mixed-conflict, and repeated requests in a
repository with the App and agent secrets configured.
