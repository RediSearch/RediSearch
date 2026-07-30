---
name: swamp-implement-task
description: End-to-end flow for delivering a change — reviewed tests first, reviewed design, implementation, validation, adversarial review, then a PR driven to green. Use this when asked to port a C module to Rust, fix a bug, or add a feature, and the work is expected to end in a merged pull request.
---

# Implement a Task

The whole path from a task description to a green pull request, as swamp workflows.

**The workflows are the flow.** The ordering, the gates, the iteration caps and the
escalation rules live in `workflows/workflow-implement-task.yaml` and the phases it
calls — not here. Each phase spawns a headless agent that must answer in a fixed
schema, so a gate reads a field rather than a paragraph. This file is how to drive
it; `swamp workflow get <name>` is where the reasoning for any particular step is
written down, next to the step.

Use the narrower skills directly when you only need one step. This is for the whole
journey.

## Running it

swamp's files live under `swamp/`, and swamp only looks *upward* for them — so
every command below needs to be told where they are. Export it once and the rest
of this file works as written; see *Where swamp lives in this repository* in
`AGENTS.md`.

```bash
export SWAMP_REPO_DIR="$PWD/swamp"   # from the repository root; once per shell
swamp workflow run implement-task --input task="<what to do>" --input taskType=port
```

`taskType` is `port`, `bugfix` or `feature`, and decides whether tests are written
before the change and whether the scope analysis runs.

The run **suspends once**, at the one decision that changes what gets built: is
this the right design to build. After that it runs through implementation,
validation, review and the pull request without stopping, so pass `ticket` at the
start or when resuming the gate — there is no later prompt asking for it.

```bash
swamp workflow approvals                                   # what is waiting
swamp workflow approve implement-task design-approved
# Resume with the design you just approved — the gate prints this line with the
# path filled in, so copy it from there. Resuming without it stops at the next
# step: nothing would tie the implementation to the design you read, and the pin
# is what makes the approval mean that document.
swamp workflow resume  implement-task \
  --input expectedPlanPath=<the path the gate printed> --input ticket=MOD-1234
```

Resuming does not re-run the phase that already completed. Add `--server wss://…`
to any of these to drive a central swamp from elsewhere.

## Watching a run

A full run is hours, so start it detached and watch the log — not as a foreground
command. Anything that kills the invoking process kills the run with it, and losing
a phase forty minutes in costs the whole forty minutes:

```bash
export SWAMP_REPO_DIR="$PWD/swamp"
nohup swamp workflow run implement-task --input task="…" > run.log 2>&1 &
```

**Strip the colour before matching anything.** swamp colours each word separately,
so escape sequences sit *between* the words, and the phrases you would naturally
grep for do not exist as literal text:

```console
$ grep -c 'Completed workflow' run.log
0
$ grep -a 'Completed workflow' run.log | cat -v
… ^[[1m^[[32mCompleted^[[39m^[[22m workflow implement-task succeeded …
```

A filter written against the rendered text therefore matches nothing, and silence
from a watcher is indistinguishable from a run still going. This is how a finished
run goes unnoticed for half an hour. Pipe through `sed` first:

```bash
tail -f run.log | sed -u 's/\x1b\[[0-9;]*m//g' | grep --line-buffered -E \
  'done (implement|validate|review-code|revalidate)|(Completed|Failed) workflow'
```

Two more things worth knowing when writing that filter:

- **Anchor on the gutter (`│`) for step events.** The suites print their own output
  into the same log, and a C++ suite has hundreds of test *names* containing
  `Error` and `failed`. Only swamp's own lines carry the gutter.
- **Match the failure words too, not just the success ones.** `(Completed|Failed)`,
  not `Completed` — a run that dies on an assertion never prints the word you were
  waiting for.

**Stop the watcher when the run ends.** `tail -f` does not exit because the thing
it was watching finished — nothing connects the two — so a watcher left running
outlives its phase and goes on watching a file nobody writes to any more. The flow
is several phases long and each one invites its own watcher, so they accumulate
quietly: four of them, one per phase, the oldest nearly nineteen hours old, is a
real outcome of not tidying up.

Prefer a watcher that ends on its own, so there is nothing to remember:

```bash
# exits when the run does, and says which way it went
tail -f --pid=$RUN_PID run.log | sed -u 's/\x1b\[[0-9;]*m//g' | grep --line-buffered -E \
  'done (implement|validate|review-code|revalidate)|(Completed|Failed) workflow'
```

`--pid` makes `tail` exit once that process is gone, which is exactly the lifetime
wanted. Failing that, kill it explicitly at the end of the phase. An agent driving
this flow should treat the watcher as part of the step it belongs to: armed when the
step starts, stopped when the step's outcome has been read.

Two optional inputs are worth knowing; the rest are documented in the workflow.

```bash
--input measureCoverage:json=true   # measure coverage of the code being replaced
                                    # (an instrumented build + the whole Python
                                    # suite; off by default). The files come from
                                    # the analysis phase, so name none.
--input specApproved:json=true      # this change already went through the repo's
                                    # spec-driven workflow and maintainer review
```

## Reporting progress

The person who asked for this cannot see the run. A phase is up to an hour, a full
run is several, and from the outside a working run and a wedged one look identical
— so **say something at every step boundary**, unprompted, and keep it short.

A step boundary is a phase finishing, a gate opening, or anything failing. At each
one, post a few lines: what just happened, what it produced, what happens next.
Three or four lines is right; the detail is in the data and the reader can ask.

Build it from the structured output rather than from the log. That is what the
schemas are for — every phase answers in fixed fields, so the summary is a lookup
rather than an interpretation:

```bash
swamp data get task-analyst      analysis       --json | jq .content  # scope
swamp data get task-tests        tests          --json | jq .content  # tests written
swamp data get task-design-agent design         --json | jq .content  # the design
swamp data get task-implementer  implementation --json | jq .content  # what was built
swamp data get validation-triage triage         --json | jq .content  # failure verdicts
swamp data get task-pr-agent     pullrequest    --json | jq .content  # the pull request
```

Triage is one instance per suite and per side of the review, so that two verdicts
recorded in one run cannot be read through each other — `revalidation-triage` for
the `verify` run after the review, and `cluster-validation-triage` and
`cluster-revalidation-triage` for the two `verify-cluster` runs:

```bash
swamp data get cluster-validation-triage triage --json | jq .content  # cluster verdicts
```

The reviews are one instance per loop, named after what they review — so
`tests-reviewer`, `design-reviewer` and `code-reviewer` for the `review` spec, and
the matching `*-reviser` for `revision`:

```bash
swamp data get code-reviewer review   --json | jq .content  # findings, and clean
swamp data get code-reviser  revision --json | jq .content  # fixed or refuted, each
```

Every one of them carries `summary`, `blockers`, and `agent.costUsd`. Lead with the
summary, name the blockers if there are any, and give the cost — it is the number
people are most surprised by and the one nothing else reports.

Two rules about how it is written:

- **Report the verdict, not your reading of it.** If a phase came back
  `succeeded: false`, say so and quote its blockers. A phase that failed and was
  described as "mostly done" is worse than no report, because it spends the
  reader's trust on the next one.
- **A gate is a request, so make it actionable.** Say what is being decided, where
  to read what it is deciding on, and give the exact command — including the
  `expectedPlanPath` the design gate prints, which is what pins the approval to the
  design that was approved.

When a run fails, name the step and quote the assert's message. The messages are
written to be read on their own and say what to do next; "the workflow failed" says
none of it.

## The phases

| Workflow | What it does |
|---|---|
| `implement-task` | The whole flow, and the design gate. Start here. |
| `phase-design` | Scope analysis, tests first, design, each reviewed |
| `phase-implement` | Implement, validate, triage failures, review, revalidate |
| `phase-pr` | The hand-off on its own, with its own gate |
| `phase-ci` | One pass at driving an open pull request to green |
| `review-loop` | Review and revise until clean, shared by the phases above |

`phase-design` and `phase-implement` can be run directly when you only want that
phase. They carry no gates of their own — a gate at the end of a phase suspends
after everything it could have stopped has already happened, so the gate lives in
the parent, between the phases. `phase-pr` is the exception and keeps its own:
run it after `phase-implement` when you want to look at the commits before
anything becomes visible outside this machine, which `implement-task` no longer
stops for.

`phase-ci` is one pass, not a loop: running it again is the next iteration. That is
what makes it safe to schedule under `swamp serve`.

Re-entering a failed run is the one thing the phases do better than the parent.
`resume --from` targets steps in the run's own DAG, and a whole phase is a single
step in the parent, so resume the phase rather than replaying its agent:

```bash
swamp workflow resume phase-implement --from validate
```

## What stops a run

These are enforced, not advisory. Each fails with the reasoning on record.

- A **resume of the design gate without `expectedPlanPath`** — the pin is what
  ties the implementation to the design that was approved, so without it the run
  stops before any agent starts.
- A design flagged as a **large change** — the repo wants a GitHub issue and the
  spec-driven workflow first, and no approval prompt here substitutes for
  maintainer review.
- A **bugfix whose test was never seen failing**. That failing run is the whole
  claim the bug is real, and it can only be made before the fix exists.
- **Validation failures this change is answerable for.** Failures are triaged
  against the revision before the change first; `unknown` is not `pre-existing`.
- **A tree the review never saw.** The hand-off is given the tree hash of the
  review the gate accepted — the one with a full validation after it, not merely
  the newest clean one — and refuses to run against anything else, so an edit
  committed while the approval gate is suspended stops it. Reorganising the
  commits does not: the check is on content, not on commit ids. It also refuses
  a dirty checkout, because the hand-off commits what it finds and a tree hash
  says nothing about what is uncommitted.
- **A review loop that will not converge**, and a CI pass that keeps trying without
  getting anywhere. Both are capped; the caps are inputs with documented defaults.

## Where the detail lives

The agents load these, and they are the authority on how the work is done. Nothing
about them is restated here, so that there is one copy to keep right:

- [`port-c-module`](../port-c-module/SKILL.md) — C→Rust ports, start to finish
- [`write-flow-tests`](../write-flow-tests/SKILL.md), [`write-rust-tests`](../write-rust-tests/SKILL.md)
- [`adversarial-review`](../adversarial-review/SKILL.md), [`code-review`](../code-review/SKILL.md), [`rust-review`](../rust-review/SKILL.md)
- [`docs-guidelines`](../docs-guidelines/SKILL.md), [`rust-docs-guidelines`](../rust-docs-guidelines/SKILL.md)
- [`commit-guidelines`](../commit-guidelines/SKILL.md), [`jj-split-changeset`](../jj-split-changeset/SKILL.md), [`jj-fix-conflicts`](../jj-fix-conflicts/SKILL.md)
- [`open-pr`](../open-pr/SKILL.md), [`report-flaky-test`](../report-flaky-test/SKILL.md)

## Doing it by hand

The flow predates the workflows and still works read as prose: analyse the scope,
write the tests, design and review it, get it approved, implement against the
approved design, validate, review adversarially until nothing is unresolved,
re-validate, open the pull request, drive CI to green. The phase descriptions in
each workflow say why each step is shaped the way it is — `swamp workflow get
phase-implement` reads as the argument for that phase.

## Known gaps

- **No branch is created.** A colocated `jj` checkout sits on a detached HEAD, and
  nothing in the flow makes a bookmark, so `open-pr` has nothing to push. Create one
  before the hand-off.
- **Nothing notifies you.** A run that finishes, or suspends at a gate, tells no
  one. Poll `swamp workflow approvals`, or watch the log as *Watching a run* above
  describes — carefully, because a filter that silently matches nothing looks
  exactly like a run that is still going.
- **No isolation.** Work happens in the current checkout. `CLAUDE.md` wants a
  dedicated worktree when it is dirty or on an unrelated branch, and a central
  server needs one per concurrent task — the phases contend on the checkout and the
  Cargo build-directory lock.
- **Performance is not covered.** `port-c-module` §4 asks for criterion benchmarks
  on performance-sensitive code, but nothing captures a baseline before the change
  or compares after, and there is no swamp model for benchmarks yet. A port can be
  correct and still regress throughput.
