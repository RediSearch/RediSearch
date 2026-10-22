---
name: hl-rpi-deliver
description: Take an RPI task whose design the user has approved to an open pull request without stopping — implement every outline phase, validate, triage failures, review until settled, revalidate, and open the PR. Use it in place of the implement phase (`implement-outline`, `/rpi implement`) in this repository, after the approval gate and structure outline in /hl:hl-rpi-redisearch.
---

# Deliver an Approved Design

From the approved design to an open pull request, unattended. The user approved one
design; the outline written after it is how to build that design, and everything here is
held to both. It stops — rather than improvises — when either turns out to be wrong, and
when the two disagree, the approved design wins.

This session is the orchestrator. Implementation, review, revision and triage each run
in their own fresh sub-agent, and this session keeps only their answers: that is what
keeps its context small enough to last the whole delivery. Only one agent at a time runs
`./build.sh`, `make` or `cargo` (see `CLAUDE.md`).

Report at every step boundary as [/hl:hl-rpi-redisearch](../hl-rpi-redisearch/SKILL.md) §
*Reporting progress* says.

## 1. Check the approval

Read `delivery/approval.md` in the task directory. Stop, saying which check failed, unless:

- it exists — no approval record means no approval, whatever the conversation suggests;
- every document it lists still has the recorded sha256 (an edit after approval means the
  user approved something else);
- a structure outline exists, written after the approval;
- the working copy is clean and its parent's tree is the recorded `base-tree` (an edit
  made while waiting at the gate was never part of what was approved).

## 2. Implement the outline, phase by phase

For each unfinished phase, launch the RPI outline implementer (`rpi:outline-implementer-agent`,
or a general sub-agent told to follow the outline when that agent is not available) with the document
paths, the phase number, and these additions:

```text
RediSearch rules for this phase:
- Follow the outline and the approved design; where they disagree, the design wins. If
  either is wrong, stop and report the mismatch; do not redesign — that would bypass the
  approval.
- Do not pause for manual verification; list manual items in your answer instead.
- Commit at each checkpoint following /commit-guidelines; document per /docs-guidelines and
  /rust-docs-guidelines. For a port, /port-c-module §3–§8 is the authority, and an API
  difference you cannot bridge (§5) is a stop, not a workaround.
- Run `make generate-rust-headers` when you change cheadergen-relevant Rust, and commit
  the headers with that change.
- Run only the phase's narrow checks. Do not run the full build or the full suites.
- If the outline belongs to the first subtask of a larger plan, work left to a later
  subtask is not yours to start, however small it looks.
- If the work ended up in one revision, split it with /jj-split-changeset before you
  answer.
Answer with: commits (id and message), files changed, tests added, checks run with their
results, anything not verified, deviations from the outline.
```

After each phase, confirm the commits exist and the outline's automated items are checked.
A phase that reports a mismatch or a failed check **stops the delivery**: report it, and
the user settles it with the iterate-structure-outline or iterate-implementation phase.

### The tests-first phase

For a port or bug fix, Phase 1 is tests only. Once it is committed:

1. For a bug fix, run the new test and confirm it fails **for the stated reason**. Quote
   the failure in `delivery/tests.md`. A test never seen failing is not evidence the bug
   exists — if it passes, or fails for another reason, stop.
2. Run [/hl:hl-rpi-review-loop](../hl-rpi-review-loop/SKILL.md) on subject `tests`.
3. For a bug fix whose test the review revised, run it again and confirm it still fails for
   the stated reason.

### Features

Before validating a feature, confirm from the diff that it adds tests: a path under
`tests/`, or an added test declaration (`#[test]`, `#[rstest]`, `def test_`, `TEST(`,
`TEST_F(`…). A feature is not done until what it added is covered. If none, stop.

## 3. Validate

Run the full checks for everything the change touches, **unfiltered**, in this order, each
saved to a log in `delivery/` with the `tee` pattern from `CLAUDE.md`:

```bash
make fmt CHECK=1                    # Rust only
jj diff --git --context 0 --from <approved base> --to @ | clang-format-diff -p1
./build.sh DEBUG=1 TESTS=1 ENABLE_ASSERT=1
./build.sh RUN_UNIT_TESTS DEBUG=1 ENABLE_ASSERT=1
make rust-tests
make lint
./build.sh RUN_PYTEST DEBUG=1 ENABLE_ASSERT=1
make rust-tests RUN_MIRI=1          # last; CI gates on it
```

Add the cluster suite when the change touches `src/coord/` — decide from the diff
(`jj diff --name-only -r '<base>..@'`), not from what an agent said it changed:

```bash
./build.sh RUN_PYTEST DEBUG=1 ENABLE_ASSERT=1 REDIS_STANDALONE=0 SHARDS=3
```

The C format check covers only the lines the change touches, from the approved base rather
than one revision: many C files already violate `.clang-format`, so a whole-file check fails
on today's tree, and a per-revision one passes vacuously once the work is committed. Any
output from it is a failure.

**A failed build stops the chain.** The C suites after it would run against a stale or
missing module, and their results say nothing about the change. Triage the build failure on
its own (*When something fails*); when it is not the change's, find a configuration that
builds — for example a release build with `ENABLE_ASSERT=1` when only the debug build trips
a toolchain warning — record the substitution in `delivery/handoff.md`, and run the whole
chain again with it. Do not run the chain from a script that carries on past a failed step.

A suite that ran zero tests has not passed: check each log for a non-zero test count.
Defer to [/verify](../../../verify/SKILL.md) for anything this list does not cover, such as a
memory-safety fix's sanitizer repro.

### When something fails

Run [/hl:hl-rpi-triage-failures](../hl-rpi-triage-failures/SKILL.md) on the logs. Then:

- every failure `pre-existing` or `flaky` → continue;
- any `unknown` → stop;
- any `mine` → launch a fresh implementer with the triage and the outline to fix them, as
  checkpoint fixups, not a new design. Validate again. After two fix rounds that still
  leave a `mine`, stop.

## 4. Review

Start only once §3 has passed. A review of a tree that validation then sends back for a
fix reviews code that no longer exists, and has to run again on the fixed one.

Run [/hl:hl-rpi-review-loop](../hl-rpi-review-loop/SKILL.md) on subject `code`, over the range from
the approved base to the tip. If it does not settle, stop.

## 5. Revalidate

If the review loop changed the tree, run §3 again, triage included. The PR must carry a
tree that was both reviewed and fully validated after the review.

Record that tree in `delivery/handoff.md`: the tip commit, its tree id, the review round
that settled it, and the validation logs that followed.

## 6. Open the pull request

Before anything is pushed:

- the working copy is clean — anything uncommitted would be pushed unreviewed;
- the tip's tree is the one recorded in `delivery/handoff.md`. Commits may have been
  reordered or reworded since; the content may not have changed.

The PR carries this task's commits — the approved base to the tip — and nothing below them.
If the approved base is not on the PR's target branch (for instance the checkout sat on a
local tooling change when the task started), move the task's commits onto the target
before pushing:

```bash
jj rebase -s <first task commit> -d <target>@origin
```

A conflict stops the delivery. Otherwise the content check becomes a diff check: the
task's diff over the target (`jj diff --git --from <target>@origin --to <new tip>`) must be
identical to the reviewed diff over the approved base. Revalidate (§3) unless every commit
left behind is outside the build — documentation, skills, agent configuration.

Then follow [/open-pr](../../../open-pr/SKILL.md) up to and including creating the PR, and
stop there. Its later steps are already done or are not delivery's: §3–§5 verified and
reviewed this exact tree, presenting findings and waiting for direction would put a stop
in an unattended flow, and CI and the Codex review are
[/hl:hl-rpi-drive-ci](../hl-rpi-drive-ci/SKILL.md)'s (§7). Open it with:

- the title `[<ticket>] <summary>` when `delivery/task.yaml` or `approval.md` has a ticket, a plain title otherwise
  — never invent a ticket id; a wrong one is worse than none;
- a description that, for a split task, says this is the first subtask of several, what it
  covers and what follows;
- the manual-verification notes from the outline and implementers, the refuted review
  findings, and any pre-existing failures from triage, for the reviewers.

The workflow documents are not part of the PR: they live in the ignored task directory.

After it opens, confirm with `gh pr view <url> --json headRefOid,url` that the PR is in
this repository and its head is the commit that was pushed. Record the URL in
`delivery/handoff.md`.

## 7. Hand over to CI

Run [/hl:hl-rpi-drive-ci](../hl-rpi-drive-ci/SKILL.md): its first pass, then its watch, until
the PR is ready or it escalates.

## Final report

One summary: PR link, what was built, validation results, review rounds with their
outcome, refuted findings, pre-existing failures, and manual checks left for reviewers.
When the delivery stopped instead, name the step, quote the reason, and say which RPI phase
picks it up.
