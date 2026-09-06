---
name: commit-guidelines
description: Dirty repo, uncommitted changes, commit, amend, split, changeset, PR history. Inspect repository state and prepare atomic Git commits or jj changesets. Use whenever the worktree is already dirty or you are about to commit or rewrite history.
---

# Commit Guidelines

Use this workflow whenever the worktree is already dirty or you are about to
commit, split, amend, squash, or otherwise touch revision history.

Prefer local checkpoint commits or jj changesets during substantial work.

- Use them to preserve progress before risky edits, broad refactors, or context
  switches.
- Keep each checkpoint atomic: one clear intent, with matching tests, docs, or
  config when they are required for that intent.
- Prefer checkpoints that can pass [/verify](../verify/SKILL.md) on their own. Builds and
  test runs here take minutes, so if a full verification is too expensive or not yet
  possible, run the narrowest relevant check — [/lint](../lint/SKILL.md) or
  [/build](../build/SKILL.md) — and state what remains unverified.
- When the history is not yours to reshape (see *When history may be rewritten* below),
  add follow-up revisions instead.

## Workflow

1. Inspect the repository state before changing anything else.
2. Decide whether existing uncommitted changes and the new task belong in the
   same revision.
3. Decide whether the target branch or changeset is already under review.
4. If either relationship is unclear, ask the user before mixing work or
   rewriting history.
5. Isolate one logical group.
6. Verify that group.
7. Create the commit or changeset.
8. Report what was committed and what verification ran.

## Inspect the starting state

Use the repository's active VCS. This repo has a `.jj/` directory, so `jj` is normally
the active VCS — prefer it. Fall back to Git only when `.jj/` is absent (for example in a
`git worktree` created outside jj).

```bash
# jj (default)
jj status
jj diff
jj log

# Git (when .jj/ is absent)
git status --short
git diff
git diff --cached
git log --oneline -10
```

Do this whenever you inherit a dirty working copy, not only immediately before
creating a commit.

Inheriting a dirty working copy is a grouping question, not a branching one — resolve it
below. `AGENTS.md` § *Common Workflows* says when a worktree is warranted instead, and
how to create one.

If the repository already has uncommitted changes, decide whether the new work:

- extends the same in-progress intent and should stay together
- is unrelated and should go into a separate commit or changeset

If that relationship is unclear, ask the user before mixing work.

Also decide whether the target branch or changeset is still yours to reshape — see
*When history may be rewritten* below.

If the target revision or its ancestors are conflicted, resolve that first with
[/jj-fix-conflicts](../jj-fix-conflicts/SKILL.md) — reshaping a stack on top of
unresolved conflicts compounds them.

## When history may be rewritten

Rewriting history on a branch with an open pull request is fine while **no human has left
a review or comment on it**. Bot comments do not count.

Draft status is not sufficient on its own: a human comment on a draft PR ends the window,
because force-pushing detaches review threads from the lines they were anchored to.

Once a human has reviewed or commented, preserve that history — address the feedback with
follow-up commits and regular pushes, and do not amend, rebase, squash, or force-push
unless the user explicitly asks for history rewriting.

Before a PR exists, history cleanup is fine whenever it is useful and does not discard the
user's work.

Get the inputs for the decision with:

```bash
gh pr view <number> --json comments,reviews,author
```

`comments[].author` gives only a login, with no bot marker, so you have to recognise the
automated ones. On this repo they are:

| Login | Posts |
|---|---|
| `sonarqubecloud` | static analysis results |
| `codecov` | coverage reports |
| `chatgpt-codex-connector` | automated review output |
| `fcostaoliveira` | **benchmark reports only** |

`fcostaoliveira` is the exception that matters: it is a real person's account that also
posts automated benchmark reports. The benchmark reports do not count as human feedback —
anything else from that account does. Read the comment before deciding.

The list is not exhaustive and new bots appear. If you cannot tell whether a commenter is
human, assume human and do not rewrite.

## Grouping rules

Group by intent, not by file type.

- Keep tests, docs, and config in the same revision when they are required to
  make the intent correct, verifiable, or understandable.
- Do not mix unrelated cleanup, formatting, refactors, and behavior changes.
- If a file contains mixed logical changes and the split is ambiguous, ask the
  user before assigning those hunks.
- Keep a Rust change and the `src/redisearch_rs/headers/` output it regenerates
  (`make generate-rust-headers`) in the same revision. Splitting them apart produces an
  intermediate revision where the C side no longer matches the Rust side and the build
  breaks.

## Jujutsu workflow

1. Ensure the target changeset contains only one intent.
2. If it mixes multiple intents, split it before describing it.
3. Verify the resulting changeset.
4. Set a concise description describing the single intent.

```bash
jj diff -r <revset>
jj describe -r <rev> -m "<intent>"
```

To split a changeset — including hunk-level splits inside a single file — follow
[/jj-split-changeset](../jj-split-changeset/SKILL.md). Do not reach for `jj split`
directly; splitting the wrong way loses work, and that skill exists to prevent it.

Reshape the jj stack freely while the history is still yours per the decision above; once
it is not, stop rewriting and stack follow-up changesets on top.

## Git workflow

Use this only when `.jj/` is absent.

1. Stage only the intended files or hunks.
2. Inspect the staged result.
3. Run verification.
4. Commit with a concise message describing the single intent.

```bash
git add <paths...>
git diff --cached
# then /verify (or the narrowest relevant check)
git commit -m "<intent>"
```

Same rule as above: reshape local history while it is still yours, then switch to
follow-up commits.

## Output

Report:

- the revision you created or updated
- the intent it represents
- whether the repository had pre-existing changes
- whether those changes were kept together or separated
- what verification ran

Once the stack is atomic and verified, [/open-pr](../open-pr/SKILL.md) is the next step.
