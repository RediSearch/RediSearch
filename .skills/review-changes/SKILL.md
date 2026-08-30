---
name: review-changes
description: General review process for pull requests, commits, diffs, working-tree changes, tests, workflows, docs, and code. Use as the baseline review skill before domain-specific review skills such as code-review, rust-review, or review-enterprise-flow-tests.
---

# Review Changes

Use this skill as the review baseline for any diff, pull request, commit range,
or local change. It owns review mechanics, duplicate-comment handling, finding
quality, and output format. Apply domain-specific review skills after this
baseline when the changed files need specialized checks.

## Route Specialized Reviews

- Use this baseline first for every review.
- Use [`/code-review`](../code-review/SKILL.md) when the change touches C or C
  headers, including C/C++ module tests that exercise C behavior.
- Use [`/rust-review`](../rust-review/SKILL.md) when the change touches Rust,
  Rust docs, Rust tests, FFI crates, or C-to-Rust porting.
- Use
  [`/review-enterprise-flow-tests`](../review-enterprise-flow-tests/SKILL.md)
  when the change touches Redis Enterprise flow tests, `re-tests/`, Enterprise
  fixtures, lifecycle/profile coverage, or CI workflows that run Enterprise
  tests.
- For mixed changes, apply every relevant specialized skill after this baseline.

## Collect Context

- Identify the review target: PR, commit, commit range, path, or working-tree
  diff. Let specialized skills define language-specific path defaults and diff
  commands.
- For a PR, inspect existing PR comments, review threads, and prior bot comments
  before adding findings.
- Read full relevant files, not only diff hunks. Include nearby tests, shared
  fixtures, helper modules, workflow files, and configuration when they affect
  the changed behavior.
- Check the PR description when reviewing a PR. Exactly one release-notes
  checkbox from the repo template must be checked. User-facing behavior changes,
  bug fixes, performance changes, or new commands require release notes.

## Finding Standards

- Report only actionable, non-duplicate findings.
- Treat an issue as already reported if an existing comment identifies the same
  root cause, even if it points to a different line.
- If a previous comment is still accurate, do not restate it. Mention it only
  when the new diff changes the issue, invalidates the previous fix, or adds
  materially new evidence.
- If the same root cause appears in multiple places, report it once on the
  clearest example and mention that the same pattern may apply elsewhere.
- Prioritize correctness, crashes, memory safety, undefined behavior, data loss,
  security, broken CI, and behavioral regressions.
- Report style, naming, formatting, or preference comments only when explicitly
  requested, when an explicit repo rule is violated, or when the issue blocks
  maintainability. Nits must still be actionable and grouped by root cause.
- For test or coverage changes, treat removing an assertion, relaxing an oracle,
  accepting a less restrictive check, or narrowing the scenario matrix as a
  coverage reduction unless justified otherwise. Do not suggest or accept such
  changes without explicit rationale and reviewer approval.

## Review Output

For each finding, include:

- Severity: `blocking` or `suggestion`
- File and line
- Rule or expectation violated
- Why it matters
- Suggested fix

Omit checklist sections with no findings. Do not emit repeated "No issues found"
sections.

End with a short summary:

- Total blocking findings
- Total suggestions
- Whether the change is ready to merge or needs revision
