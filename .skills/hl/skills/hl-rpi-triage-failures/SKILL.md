---
name: hl-rpi-triage-failures
description: Classify every failure from a validation or CI run as caused by this change, pre-existing, flaky, or unknown, with evidence from the revision before the change. Use it from /hl:hl-rpi-deliver and /hl:hl-rpi-drive-ci before anything is fixed.
---

# Triage Failures

A failure is not automatically the change's. Fixing a failure the change did not cause
buries somebody else's bug in this PR; waving through one it did cause ships it. Triage
settles which is which **before anything is fixed**, and fixes nothing itself.

Run it as a fresh sub-agent, so the evidence is gathered by someone with no stake in the
answer. Give it the failure source (a saved log path, or the PR and failed CI jobs), the
base revision the change sits on, and the task directory.

## Verdicts

Classify **every** failure listed — not a selection:

| Verdict | Means | Evidence required |
|---|---|---|
| `mine` | the change plausibly caused it | the code path linking the change to the failure |
| `pre-existing` | it fails the same way without the change | a run at the base revision, failing the same way |
| `flaky` | it fails intermittently, independent of the change | repeated runs, or an existing flaky-test ticket |
| `unknown` | none of the above could be established | what was tried |

`unknown` is not `pre-existing`. Do not guess between them: anything that looks unrelated
is checked at the base revision. Say what you ran, not what you assume.

## Checking the base revision

Never check out another revision in the working copy under triage. Use a temporary
worktree at the base, run the one failing test there, and remove it afterwards:

```bash
git worktree add --detach "$SCRATCH/triage-base" <base>
git -C "$SCRATCH/triage-base" submodule update --init --recursive
# build and run only the failing test there, e.g.
#   ./build.sh RUN_PYTEST TEST=<file>:<test>   (from inside the worktree)
git worktree remove --force "$SCRATCH/triage-base"
```

A cluster failure is checked against a cluster run at the base
(`REDIS_STANDALONE=0 SHARDS=3`), not a standalone one: a test that passes standalone and
fails across shards is not thereby the change's.

Builds and tests in the worktree still contend for the machine; run them only while
nothing else in this checkout is building, per `CLAUDE.md`.

For `flaky`, look for an existing ticket and recommend
[/report-flaky-test](../../../report-flaky-test/SKILL.md) for one nobody has reported. Do not
file it from here.

## Answer

Write `delivery/triage-<source>-<n>.md` in the task directory and answer with:

- one row per failure: test, verdict, evidence, ticket if any;
- `all-pre-existing`: true only when every failure is `pre-existing` or `flaky` with
  evidence — never set for a failure you could not attribute;
- a recommendation: fix (listing the `mine` failures), report flakes, or escalate.

## What the caller does with it

- Every failure `pre-existing` or `flaky` → proceed; the triage file goes into the final
  report and the PR description.
- Any `mine` → the change has to be fixed; the caller says how.
- Any `unknown` → stop and hand it to the user.
