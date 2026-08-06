---
name: adversarial-review
description: Commission an independent review of a change from an isolated reviewer, before opening or updating a PR or on demand by the user. For the agent requesting the review — if you were spawned to review something, you are the reviewer and this is not your skill.
---

# Adversarial Review

An independent, skeptical review pass over the final state of a change. It **composes
with** the language-specific review skills — [/code-review](../code-review/SKILL.md),
[/rust-review](../rust-review/SKILL.md),
[/write-flow-tests](../write-flow-tests/SKILL.md) — rather than replacing them: those
supply the checklists, this supplies the isolation. It does not run the build or the test
suites either; that is [/verify](../verify/SKILL.md)'s job.

This file is written for whoever is *requesting* the review. It covers what to withhold
and how to brief the reviewer — none of which the reviewer itself needs. The reviewer's
entire brief is the prompt template below, which is self-contained by design.

## Workflow

To perform an adversarial review:

- launch a fresh sub-agent session;
- prompt it with the template below, filling only the bracketed slot
- add nothing else to the prompt

The goal is to simulate a skeptical reviewer who only sees the PR, not the
authoring process behind it.

The initial adversarial review is required. An additional review round may be
skipped when changes made after the previous review are purely mechanical and
do not change behavior or meaning, such as formatting or lint fixes.

A follow-up round is a fresh session prompted identically. Do not tell it what the
previous round found, which findings were dismissed, or that a round already happened —
that turns an independent pass into a check on someone else's homework, and a finding the
first reviewer missed stays missed.

## Where the isolation boundary sits

The boundary is not "no context" — it is **nothing a real reviewer would not have**.

Fair game, because it is on the PR: the diff, the commit messages, the PR title and
description, the linked Jira ticket, CI results. The reviewer fetches these itself.

Withheld, because it comes from the authoring session: why an approach was chosen, which
alternatives were tried and abandoned, which parts you consider settled or already
checked, reassurance that something is intentional, and any framing of the change as a
fix for a specific problem beyond what the PR itself says.

The practical failure mode is a prompt like *"I refactored the iterator to fix a
use-after-free; please review"* — it hands the reviewer both the conclusion and the place
to stop looking.

## Prompt template

Send the template itself, filling the one bracketed slot. Add no background and do not
paste the diff — a pointer back to this file is not a substitute, since most of it is
addressed to you rather than to the reviewer.

`pr:<number>` is the form both review skills accept. With no PR yet, substitute a **git
commit SHA** and change the "accept `pr:<number>` directly" line to say the skills accept
the commit directly. [/rust-review](../rust-review/SKILL.md) also takes a jj revset, but
[/code-review](../code-review/SKILL.md) does not — it treats an unrecognised argument as a
commit and runs `git show` on it, which fails outright on a jj change id. Under `jj`, get
the SHA with `jj log -r <rev> --no-graph -T 'commit_id'`.

```text
Review [pr:1234] as an independent reviewer.

You have no context on this change beyond the change itself, and should not ask for any.
Assume nothing about it is correct or intentional.

Do read everything the change carries on its own: the diff, the commit messages, the PR
title and description, any linked ticket, and the CI results. Fetch those yourself. What
you will not be given is why this approach was chosen or what was already checked — work
without it rather than asking.

You are the independent reviewer for this change. Do not spawn a reviewer of your own —
you are already it.

Determine which languages the diff touches and load the matching skills — all of them
when it spans several:
- C changes → /code-review
- Rust changes → /rust-review
- tests/pytests/ changes → /write-flow-tests (its guidelines are the review criteria too)

/code-review and /rust-review accept `pr:<number>` directly and will fetch and diff the
change themselves.

Work through those skills' checklists in full. Where you must choose what to dig into,
rank by blast radius: silent data or index corruption first, then crashes and memory or
`unsafe` unsoundness, then compatibility breaks (persisted formats, reply shapes, accepted
syntax), then everything else.

Two things those checklists do not ask you for, which you should also report:
- Behavior that changed without the change appearing to intend it.
- For C changes, new or changed behavior with no test exercising it — /code-review has no
  test-coverage section, so nothing else will catch it. (/rust-review covers this for Rust
  in its own checklist; do not report it twice.)

And two places divergence hides that a file-local reading misses: whether src/coord/
cluster behavior still matches standalone, and whether both sides of any changed
interface were updated together.

You are read-only: do not run ./build.sh, make, or cargo — another agent may be building
concurrently, and verification is not your job. Reason from the code.

Because you run nothing, any runtime outcome you describe — a sanitizer report, a crash,
a wrong query result — is a prediction. State it as one ("this should produce ..."), name
the command that would confirm it, and never quote a report you did not observe as though
it ran. The requester is responsible for executing it.

Report your findings sorted by severity, each with file and line, the problem, and a
concrete failure scenario. State explicitly where you could not determine whether
something is a problem, rather than resolving it in the change's favour.
```

## Output

A report with findings, sorted by severity/urgency.

Present the report to the user. Do not address or dismiss any finding until the
user has reviewed it and provided explicit direction.
