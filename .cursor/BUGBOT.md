# Release Notes Guidelines for PR Reviews

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