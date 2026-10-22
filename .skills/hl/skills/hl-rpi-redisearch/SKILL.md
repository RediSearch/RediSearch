---
name: hl-rpi-redisearch
description: RediSearch rules for every phase of HumanLayer's Research-Plan-Implement workflow — the riptide-rpi skills (create-research-questions, create-research, create-design-discussion, create-tdd, create-structure-outline, implement-outline and their iterate-* variants) or the /rpi skill. Load it whenever you run any RPI phase in this repository, before the phase's own process.
---

# RediSearch Rules for RPI

HumanLayer's RPI skills own the phases, their documents and their templates. This skill
adds what RediSearch needs on top: the scope and split judgement, tests first, the
large-change refusal, reviewed designs, one approval, and an unattended delivery from that
approval to a pull request. Where the two disagree, this skill wins for this repository.

The phases come either from the `riptide-rpi` plugin that HumanLayer's daemon loads into
its sessions (one skill per phase: `create-tdd`, `create-structure-outline`, …), or from
the single [`/rpi`](https://github.com/humanlayer/skills/tree/main/plugins/rpi) skill
(`/rpi make a TDD …`). Neither is vendored here. This file names phases, not commands:
use whichever form the session has.

## The flow

```text
research questions → research → TDD (or design discussion) → design review loop
   → approval gate → structure outline → /hl:hl-rpi-deliver → /hl:hl-rpi-drive-ci
```

- **TDD is the default design step** for ports and bug fixes; a design discussion fits a
  feature with a few clear options. A PRD is only worth it for new user-facing surface,
  which is a large change anyway (see *Design*).
- **The design is what the user approves**; everything after it runs without stopping.
- **The structure outline is mandatory.** It is written after the approval, and it is what
  [/hl:hl-rpi-deliver](../hl-rpi-deliver/SKILL.md) drives phase by phase. Do not offer
  "implement directly" from research or design.
- Use one task directory per task and one checkout per task. Two tasks sharing a checkout
  collide on the working copy and on the Cargo build lock; use
  [/jj-workspace](../../../jj-workspace/SKILL.md) for the second one.

## Task type

Record it in `delivery/task.yaml` in the task directory when the task starts, next to the
optional Jira ticket (a file of its own, so it does not depend on either RPI flavour's
document formats):

```yaml
task-type: port        # port | bugfix | feature
ticket: MOD-1234       # or omit; never invent one
spec-approved: false   # true only once docs/CONTRIBUTING-specs.md review has happened
```

Ask for the task type if the request does not make it obvious. It changes:

| | port | bugfix | feature |
|---|---|---|---|
| Scope and split judgement in the design | yes | yes | yes, briefly |
| Outline Phase 1 is tests only | yes, scoped to the functions being ported | yes, a test that fails for the stated reason | no — but some phase must add tests |
| Test must be *seen failing* before the fix | — | yes, output quoted | — |
| Authority on how | [/port-c-module](../../../port-c-module/SKILL.md) | — | — |

## Research questions and research

Research stays descriptive, as RPI requires. Make sure it establishes the facts the
scope judgement needs:

- the files the change lives in, as real repository-relative paths — never a guessed path,
  never a file listed because it seems related;
- for a port, what [/port-c-module](../../../port-c-module/SKILL.md) §1 asks for: the C files
  being replaced, the functions in scope, their dependencies and callers, existing tests;
- for a bug, where the bug lives and which tests touch it;
- whether `src/coord/` is involved, since that decides whether the cluster suite runs.

**Coverage**, only when the user asks for it (it is an instrumented build and the whole
Python suite): run [/check-flow-coverage](../../../check-flow-coverage/SKILL.md) on the C files
being replaced and record the uncovered lines in the research document. It is a floor for
the tests, not a checklist — unreachable defensive code does not need a test.

## Design (TDD or design discussion)

Add a **Scope** section before anything else and settle it with the user first:

- **Can this land as one reviewable pull request?** It cannot when something has to happen
  before the change proper — C that needs reshaping, an oversized module that needs
  splitting, an API that needs getters where callers reach into fields today — or when it
  touches enough independent surface that one PR could not be reviewed. Reshaping C before
  porting it is a task of its own, not a preamble to this one.
- When it cannot, write a separate `NN-overall-plan-<description>.md` naming the ordered
  subtasks, what each delivers and why that order, and design **only the first subtask**.
  The first subtask must be implementable and reviewable without any of the others,
  against the code as it is today. Splitting is not the same as a large change.

**Large changes stop here.** If the design is a new `FT.*` command or option, a new field
or index type, a behaviour or persistence-format change, or a cross-cutting C/Rust
refactor, it needs the spec-driven workflow in `docs/CONTRIBUTING-specs.md` (GitHub issue,
maintainer review) before code. Unless the request records `spec-approved: true`, stop and
tell the user; no approval inside this flow substitutes for maintainer review.

The TDD's two levels are what the code review will later hold the change to, so keep the
**Program Design** concrete: call-stack trees (diff syntax for changed orchestration),
file-tree diffs marking new, modified and deleted files, and the types and signatures of
the key new items. For a port, this is [/port-c-module](../../../port-c-module/SKILL.md) §2's
porting plan, including whether C must be reshaped first and every deliberate departure
from the C.

Add a **Test Plan**: which tests, in which suite, asserting what. A test whose assertion
would still hold with the behaviour removed is not a test of it. For a bug, the test that
must fail before the fix, and why it fails.

**Review the design before the gate.** Once the user has settled the design's last
section, run [/hl:hl-rpi-review-loop](../hl-rpi-review-loop/SKILL.md) over the design
documents (subject `docs`). Bring any finding that would change a choice the user settled
back to the user rather than letting the reviser decide it.

## The approval gate

When the review has settled, present the gate:

- what is being approved: the design documents, and — when split — that this starts the
  first subtask only;
- the review's outcome, including any finding that was refuted rather than fixed;
- that approval starts the structure outline, implementation, full validation, code
  review and **opening the pull request without any further stop**;
- a request for the Jira ticket if `delivery/task.yaml` has none.

**Never approve this gate on the user's behalf.** Starting the flow, asking for
implementation, or approving a section of the design is not approval of the design. Stop
and wait for a new, explicit user message approving it.

When that message arrives, write `delivery/approval.md` in the task directory, then go
straight on to the structure outline:

```markdown
---
approved-at: <ISO timestamp>
base: <commit id the design was written against>   # jj: @- with @ empty; git: HEAD
base-tree: <tree id of base>
documents:                                           # the approved design documents
  <path>: <sha256>
ticket: <MOD-1234 or empty>
---

> <the user's approval message, quoted verbatim>
```

## Structure outline

Shape the phases so `/hl:hl-rpi-deliver` can run them unattended:

- **Phase 1 is tests only** for a port or bug fix, committed on their own with no
  implementation. For a bug, its automated verification is "the new test fails, for the
  stated reason" — the outline says what failure output is expected.
- Each later phase ends in one or a few checkpoint commits per
  [/commit-guidelines](../../../commit-guidelines/SKILL.md). For a port, follow
  [/port-c-module](../../../port-c-module/SKILL.md) §3–§8: crate skeleton, pure Rust with tests
  and docs, FFI wrapper with its regenerated headers, C repointed and deleted.
- For a feature, some phase adds tests for what it adds.
- **Automated Verification uses narrow commands** — `cargo nextest run -p <crate>`,
  `./build.sh RUN_UNIT_TESTS TEST=<name>`, `./build.sh RUN_PYTEST TEST=<file>:<test>`. The
  full suites run once, in `/hl:hl-rpi-deliver`, after all phases.
- **Manual Verification must not block.** Delivery is unattended, so anything that needs
  human judgement becomes a note for the PR's reviewers, or a reason to rethink the phase.
- When split, the outline covers the first subtask only. Work left to a later subtask is
  not this outline's, however small.
- **The outline implements the approved design, nothing more and nothing less.** Nobody
  approves it, so it cannot carry a decision of its own: if writing it shows the design is
  wrong or incomplete, stop and take that back to the user, whose answer changes the design
  and needs a new approval.

## Delivery starts on its own

Once the outline is written, run [/hl:hl-rpi-deliver](../hl-rpi-deliver/SKILL.md) on the
task directory straight away — no review, no second approval. Skip the phase's usual
hand-off (suggesting a worktree or a new session, asking the user to read the outline).
It replaces the implement phase (`implement-outline`, `/rpi implement`) for this
repository: a request to implement an approved task directory goes to
`/hl:hl-rpi-deliver` too.

## After delivery

- Feedback before the PR exists goes through the iterate-implementation phase, and then back
  through `/hl:hl-rpi-deliver` from its validation step — the change has to be validated and
  reviewed again before anything is pushed.
- Once the PR is open, CI results and review comments are
  [/hl:hl-rpi-drive-ci](../hl-rpi-drive-ci/SKILL.md)'s.

## Artifacts

The task directory is the one the RPI phase uses: `.humanlayer/tasks/<task>` under
HumanLayer's daemon (a symlink into its artifact store — list it with `ls -La`, since Glob
and Grep do not follow it), or `.agents/artifacts/<task>` under `/rpi`. Both are ignored;
neither ever goes into a commit or a PR. `delivery/` inside it holds this flow's own records
(task type, approval, review rounds, triage, hand-off); the RPI implementer reads the
numbered workflow documents, not that subdirectory.

## Reporting progress

The user cannot see a long run from the outside, and a working run looks the same as a
wedged one. At every phase end, gate, or failure, post three or four lines: what happened,
what it produced, what is next.

- **Report the verdict, not your reading of it.** A step that failed is reported as failed,
  with its blockers quoted — never as "mostly done".
- **A gate is a request**: say what is being decided, where to read it, and what reply is
  expected.
