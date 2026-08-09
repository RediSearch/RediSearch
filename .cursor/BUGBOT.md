# PR Review Guidelines

`AGENTS.md` ("Review guidelines") is the full spec for automated review in this repo. The rules below
are the parts that matter most for Bugbot, restated so they apply without loading that file.

## Scope and Severity

- Report correctness, crashes, memory safety, undefined behavior, data loss, security, and clear test/CI failures. Those are what a review here is for.
- State the failure for every finding: the input, state, or thread interleaving that produces the wrong result, and what the wrong result is. A finding you cannot ground that way is a preference — do not report it.
- Skip style, formatting, naming, and preference comments unless they violate an explicit project rule.
- One comment per root cause. If the same pattern repeats, comment on the clearest instance and mention the pattern.
- Review is advisory. A human maintainer's approval is the merge gate.

## Re-Reviews After a Push

Pushes to an open PR are usually the author addressing earlier feedback, so a re-review is a review
of the delta, not of the PR again:

- Review only what changed since your previous review on this PR. Do not re-report findings on code you already reviewed and chose not to flag, and do not reopen resolved threads.
- If your earlier finding was addressed and the fix draws a new finding in the same hunk, do not report a third variation of the same concern. Say once that the hunk needs a design decision, name the trade-off, and leave it to the human reviewer.
- A re-review that reports nothing is a good outcome.

## When to Skip Release Notes Comments

Do NOT comment about missing release notes if:
1. The PR is internal (e.g., refactoring, CI/CD changes, internal tooling, documentation updates, test-only changes)
2. The checkbox "This PR does not require release notes" is checked in the PR description
3. The PR only affects internal implementation without user-facing impact

## When to Suggest Release Notes

For PRs that have user-facing impact (new features, bug fixes, performance improvements, API changes, breaking changes), suggest a release note by:

1. Writing a concise, user-focused release note suggestion
2. Highlighting the suggestion in the PR description using the following format:

```markdown
### 📝 Suggested Release Note

> **[Category]**: Brief description of the change from the user's perspective.

Example categories: Feature, Bug Fix, Performance, Breaking Change, Deprecation
```

## Release Note Writing Guidelines

- Focus on **user impact**, not implementation details
- Be concise (1-2 sentences)
- Use active voice
- Start with a verb when possible (e.g., "Added", "Fixed", "Improved")
- Include relevant command/API names if applicable
- Mention breaking changes prominently
