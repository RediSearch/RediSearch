---
name: hl-rpi-review-loop
description: Review-and-revise loop used by the RPI delivery flow — an isolated reviewer reports, a separate reviser fixes or refutes each finding with evidence, until nothing blocking remains or three rounds have passed. Use it from /hl:hl-rpi-redisearch and /hl:hl-rpi-deliver; for a single review on demand use /adversarial-review instead.
---

# Review Loop

One review is a report. This loop turns reports into a settled change: findings are fixed
or refuted with evidence, the change is reviewed again, and the loop ends when nothing
blocking remains — or stops and asks a person when it will not converge.

## Subjects

| Subject | What is reviewed | Reviewer brief |
|---|---|---|
| `docs` | the design documents, before the approval gate | *Document reviewer* below |
| `tests` | the tests-only checkpoint (outline Phase 1) | [/adversarial-review](../../../adversarial-review/SKILL.md) template, on that commit |
| `code` | the whole change, base to tip | [/adversarial-review](../../../adversarial-review/SKILL.md) template, plus *Design conformance* below |

## One round

1. **Review.** Launch a fresh sub-agent with the subject's brief. It reports and changes
   nothing. Record the report in `delivery/review-<subject>-<round>.md`.
2. **Check the reviewer stayed read-only.** If the working copy changed during the review
   (`jj status` / `git status` against what it was before), discard the report, undo the
   writes, and run the round again.
3. **Decide whether it is settled** (see *Settled*). If so, stop.
4. **Revise.** Launch a *different* fresh sub-agent with the findings to resolve (see
   *What the orchestrator decides*) and the reviser brief below. Record its answer beside
   the review.
5. Go to the next round, up to three.

The reviewer and the reviser are never the session that wrote the subject, and never each
other: a review resumed from the authoring session is not independent.

### What the orchestrator decides

The session running the loop routes findings; it does not judge them. A new finding goes to
the reviser — every one, the ones that look wrong included — except:

- a finding that changes a choice the user settled, which goes to the user;
- a repeated finding (below), which may be settled without the reviser.

In particular the orchestrator never refutes a finding itself, however plainly the design
or a repository rule answers it. The orchestrator is the one session that knows why the
change looks the way it does, which is exactly the context that makes a refutation feel
obvious; the reviser has to show the evidence. When a round has a revision, its
non-blocking findings go to the reviser too, which may resolve them or leave them with a
reason; when a round is settled, they go into the final report as they are.

### Repeated findings

Follow-up rounds are blind, as [/adversarial-review](../../../adversarial-review/SKILL.md)
requires: each is prompted exactly like the first, without the earlier findings or their
resolutions. That means a refuted finding can come back. Before sending findings to the
reviser, compare them with earlier rounds' resolutions:

- a finding already **refuted** whose new report brings no evidence against the refutation
  is settled — do not send it again; preference is not a finding;
- one whose new report contradicts the refutation's evidence is a **dispute** between
  reviewer and reviser, and goes to the user (see *Not converging*).

### Settled

A round is settled when the reviewer managed to review, and no finding remaining is
**blocking**. A finding is blocking when it states a concrete failure — input, state or
interleaving, and the wrong result — or when the reviewer says it could not determine
whether something is a problem. Style, naming and preference findings are not blocking;
they are listed in the final report, not looped on. A blocking finding stops counting only
once a reviser has resolved it, or as a repeat of one already settled — never because the
orchestrator judged it wrong.

### Not converging

After the third round, if blocking findings remain, **stop** and hand the user the
unresolved findings, the reviser's answers, and any disputes. A finding that survived three
rounds is a disagreement a person has to settle, not a reason for a fourth attempt.

## Resolving findings without asking first

[/adversarial-review](../../../adversarial-review/SKILL.md) says not to address findings until
the user directs it. In this flow, the user's approval of the design is that direction for
the `tests` and `code` subjects; for `docs`, the user reads the result at the gate. Either way, every finding and its resolution goes into the report the user sees —
nothing is dismissed silently.

## Reviser brief

Send this, with the findings and the subject filled in:

```text
You are resolving review findings on [subject]. The task directory is [path]; read its
numbered workflow documents for what was agreed.

Resolve every finding below: either fix it, or refute it with evidence — a quoted line,
a command you ran and its output, a test that covers the case. Agreement is not a
resolution, and "minor" is not a refutation.

Commit each fix into the checkpoint it belongs to rather than appending a single
"address review" commit, following /commit-guidelines. Keep tests-only checkpoints free of
implementation. Do not run the full build or test suites; narrow checks only.

Answer with one entry per finding: the finding, `fixed` or `refuted`, and the evidence.

[findings]
```

For `docs`, the reviser edits the documents in place instead of committing, and a finding
that changes a settled design choice is not resolved by the reviser: it goes to the user at
the gate.

## Document reviewer

```text
Review the design in [task directory] as an independent reviewer. Read the numbered workflow documents and the code they cite. You have no other
context and should not ask for any. You are read-only: change nothing.

Check:
- Both levels: the system design, and the program design — types, signatures, file layout,
  call stacks. A design agreed at the architecture level and vague about the code is the one
  that produces arguments in code review.
- Facts: every claim about current code is true of the code as it is today.
- Scope: if there is an overall plan, each subtask is reviewable alone, the first is
  implementable without any of the others, and together they add up to the task.
- Size: whether the change is a new FT.* command or option, a new field or index type, a
  behaviour or persistence-format change, or a cross-cutting C/Rust refactor. If so, report
  it as blocking: it needs docs/CONTRIBUTING-specs.md first.
- Tests: whether the test plan would fail if the behaviour were removed, and for a bug fix,
  whether the test fails for the bug's reason rather than another.

Report findings sorted by severity, each with the document and section, the problem, and
what goes wrong if it is built as written.
```

## Design conformance

For the `code` subject only, alongside the adversarial reviewer and kept separate from it
(the adversarial reviewer must not see the design — that is authoring context): launch a
fresh read-only sub-agent with the outline and design paths and the change's revision
range, asking it to report every place the code departs from what was approved, and every
outline item not delivered. Its departures are blocking findings in the same round.
