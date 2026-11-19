---
name: writing-specs
description: Describes how to write a feature spec before implementation begins - the problem, the goals and what is out of scope, assumptions, acceptance criteria in Given/When/Then, and the docs verdict. Use when asked to "write a spec", "spec this out", "write a PRD", "draft acceptance criteria", or when shaping the issue for a new feature.
---
<!-- yours to edit. Copied once from the default template by outerlayer init 0.42.0; never overwritten. -->

# Writing Specs

A spec is the agreement about what to build, made before building it. It lives
in the issue body: one clean spec, edited in place as the shape is agreed,
never a trail of amendment comments. Its acceptance criteria sit under an
`## Acceptance criteria` heading. Each criterion has an id, and an id is any
stable label the team chooses, such as `LOGIN-01` or `AC-12-03`. Pick a scheme
once and keep to it. The body also records the docs verdict
with a one-line reason. Planning scratch is local and disposable. Nothing
durable belongs there.

## The shape of a spec

A spec has two parts. The first part is for any reader: a teammate, a manager
or a customer. Write it in product words. Put no file path, function name or
other code name in it. The second part, "Notes for the builder", is for
whoever builds the change. It holds the paths and the design.

Use these headings, with these words, in this order. Leave out a heading that
has nothing real under it. Never rename one. A reader who has seen one spec
then finds the same thing in the same place in the next.

For any reader:

1. `## Summary`
2. `## Problem`
3. `## What done looks like`
4. `## Out of scope`
5. `## Constraints`
6. `## Acceptance criteria`

For the builder, under `## Notes for the builder`:

7. `### Why this approach`
8. `### Design`
9. `### Assumptions`
10. `### Earlier solutions`
11. `### Hidden work`

## Summary

A person sorts a spec before reading it. The summary tells them in thirty
seconds what this is, how big it is and what it waits on. It is a list with
these labels:

- **What changes**: one or two sentences.
- **Size**: small, medium or big, then the number of criteria. Not an
  estimate and not a commitment.
- **Waits on**: issue references, and any step that is a person doing a thing
  rather than a merge, such as a provider setting, an approval or a
  credential. Everything else waits on those steps, and nobody sees them.
- **Docs**: the docs verdict with its reason.

Write the summary again whenever the body is amended. A summary that no
longer matches its spec is worse than none.

## Problem

Open with the pain: who hits it, when, and what it costs them. A concrete
scenario beats an abstract claim, and data beats both. "The app can't do X" is
a solution in disguise. If the problem statement already names the feature, the
problem is not stated yet.

## What done looks like

State the outcomes: results that can fail, not hopes.

## Out of scope

Name the neighbouring work that a reader will expect here, and where it lives
instead. State it once, in this section only. An exclusion left unstated
returns as extra work in review, or as a guess by whoever implements. Say
whether each one is deferred or dropped.

## Constraints

List the few rules of this change that a reviewer may ask to relax, and must
not. Write each in product words: "a hidden tab sends no request", not the
name of the function that makes it so.

## Notes for the builder

Add a heading here only when there is something real to put under it.

**Why this approach.** Give the reason for the decision most likely to be
reopened. Name the alternatives considered and why each lost. Without those
reasons, a later reader can only accept the design blindly or reopen it.

**Design.** The paths, functions and data the change touches.

**Assumptions.** Label each as an assumption, with what changes if it is
wrong: "we assume X; if wrong, Y changes." An assumption stated as fact
misleads every later reader.

**Earlier solutions.** How others solved this, and what they settled on. Name
the source. A team that cannot see them solves the same problem again later.

**Hidden work.** The specific places where this takes a week if nobody is
warned: a part that looks small and is not. Name it, and say what the cheap
way out is.

## Match the length to the uncertainty

A small, clear change needs a summary, a problem and a few criteria. A full
spec is for real uncertainty about the problem or the solution. Padding hides
the intent it surrounds.

## Writing criteria

Criteria are the definition of done, extracted from the agreed spec before
implementing. Never write them by reading the finished implementation back.

- **One behavior per criterion.** A Given/When/Then that needs a second When is
  two criteria. So is a Then joining unrelated outcomes with "and".
- **An observable, pass/fail Then.** If no test could fail it, it promises
  nothing. Ban vague adjectives such as "fast", "gracefully" and "user-friendly".
  State the number or the behavior instead.
- **Say what happens, not how.** Check: would this sentence change if the
  implementation did? "When Bob logs in", not "when Bob clicks Submit on
  /login". A criterion written this way stays true when the code changes.
- **Every promised error path gets its own criterion.** An unstated edge case
  is not left open. It is guessed at during implementation.
- **`(proof: ...)` between the id and Given** on every criterion whose outcome
  a user can see. Use `video` for a multi-step flow, `screenshot` for one state
  and `log` for CLI output. A test is the proof only where nothing visible
  changes. The proof shows the running
  page, never a component render. The outerlayer-evidence skill covers capture.

## The docs verdict

Every feature spec declares a docs verdict with a one-line reason, on the
**Docs** line of its summary. It is decided when the spec is written, and
decided again from the finished change before the pull request opens.

The verdict is `docs-required` when the change alters what a user sees or does.
Judge it against the repository's own docs: a `README.md`, a `docs/`
directory, a docs site. A new capability with no page yet is
`docs-required` too, and its criterion names the page to add. That no page
covers it is a reason to write one, not a reason to skip. Generated
reference, such as an API spec rendered as pages, is not a docs surface. A
new endpoint still needs a guide when using it takes a step the reference
cannot show, such as a credential to create or calls to make in order. When
required, the spec carries a criterion naming the page and its proof form.

A verdict reversed when the change is finished is not a fault. The gap
between the two calls is the useful signal, so state it rather than quietly
adopting the later one.

## Known limits

A criterion may carry prose that says what it does not cover. Useful labels:
**Known gap:** with the reason and how it will be closed, and **TODO:** for
work that is deferred, not dropped. When part of a promise is a performance
target or a judgment call no test can assert, say what the tests prove and
what they do not. A spec that claims more than its tests prove is worse than
one that states its limit. Write the limit down.

## Amendments

When implementation shows the spec was wrong, amend the issue in place and note
the owner's ruling in the body. Retire a criterion by deleting its line. Never
renumber ids.

## The reader test

Before calling a spec done, hand it to a fresh agent with no conversation
context and ask: what would you build, what is ambiguous, what would you have to
guess? Whatever that reader gets wrong is a bug in the spec, not in the
reader. Fix the wording, then get the owner's sign-off before any
implementation.

Re-reading your own spec does not satisfy this. Run it against the issue body
as published, not the draft in the working tree. A spec whose design lives in
links the reader cannot follow fails this test.

## Writing rules for the instructions file

The reader test holds a spec to the rules in
[references/agents-snippet.md](references/agents-snippet.md): write for a
reader who was not in the session, and keep session history out of code
comments. Paste that file into the repository's instructions file, such as
`AGENTS.md`, to hold every agent's prose to the same rules.
