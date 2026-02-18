---
name: review-diff
description: Strict code review of HEAD vs master. No fluff.
model: gpt-5.3-codex
---

First run:

git diff master...HEAD

Then perform a strict code review of the diff.

Rules:
- Only comment on real issues.
- Do NOT summarize the PR.
- Do NOT restate what the code does.
- Do NOT provide generic praise.
- Do NOT invent theoretical issues.
- Ignore formatting unless it affects readability or correctness.
- Focus only on:
  - Correctness
  - Edge cases
  - Bugs
  - Security
  - Concurrency issues
  - Performance regressions
  - API contract violations
  - Maintainability problems

If the changes look clean and correct:
Write exactly:

Reviewed. No issues found.

Output:
- Write the review to `review-comments.md`
- Be concise.
- Each issue must include:
  - File name
  - Clear problem description
  - Suggested fix (with code snippet if applicable)
