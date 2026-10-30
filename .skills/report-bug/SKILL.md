---
name: report-bug
description: Report a RediSearch product bug to Jira as a MOD ticket an agent or engineer can act on without asking follow-up questions. Use when asked to open, file, or draft a bug ticket for incorrect behavior, a crash, or a wrong result. Reproduces on the current commit, searches for duplicates, fills the team bug template, and creates the ticket only after the user approves. For flaky CI tests use /report-flaky-test instead.
---

# Report Bug

File a product bug in Jira, or add a new variant to an existing ticket. The ticket must let a
reader reproduce the failure, see where its boundary is, and know when the work is done.

## Arguments

`$ARGUMENTS` may contain any of:
- A description of the bug, a failing command, or a local repro script
- A test id, log excerpt, stack trace, or crash report
- An existing Jira key the new bug relates to, or that may already cover it
- A PR, commit, or GitHub issue where the bug was noticed

Defaults:
- Jira cloud: `redislabs.atlassian.net`
- Jira cloud id: `06f73ca7-8f2c-4392-b40a-08288e9d0ba3`
- Project: `MOD`
- Parent epic: `MOD-7468` (`RediSearch: Miscellaneous Bugs`)
- Issue type: `Bug`
- Components: `RediSearch`
- Description format: `markdown`

Leave priority, fix versions, and assignee unset unless the user gives them; they are
triage decisions.

## Instructions

### 1. Reproduce on the current commit

A repro that no longer fails is the most common reason a ticket misleads its reader, so run it
before writing anything.

- Record the exact commit: `git rev-parse --short HEAD` plus `git log -1 --format=%cd`. A branch name
  alone is not enough; branches move.
- Build per [/build](../build/SKILL.md) if the module under `bin/` is missing or older than the
  commit.
- Start from an empty server (`FLUSHALL`, or a fresh `redis-server` on a spare port). The repro must
  fail on its own, with nothing already in the database.
- Capture the exact observed output. For a crash, keep the assertion or panic message and the
  symbolized frames from the Redis log; drop register dumps and unrelated thread stacks.
- Shrink the repro to the fewest keys, fields, and arguments that still fail.

If it does not reproduce, say so and stop: report which commit and configuration you tried, and
ask the user for what differs. Do not file a ticket for a failure you could not observe.

### 2. Map the boundary

A single failing command rarely shows the whole bug. Try the nearest variants and record which fail
and which pass, for example:
- field type (NUMERIC vs TAG vs TEXT), SORTABLE or not
- with and without LIMIT, MAX, or the triggering option
- `DIALECT` value and its position in the command, `default_dialect`, `WITHCOUNT`/`WITHOUTCOUNT`
- standalone vs coordinator, `WORKERS` > 0, RESP2 vs RESP3, hash vs JSON

Keep this to a handful of targeted probes. The table goes under *Relevant context*; its purpose is
to show where the bug starts and stops, not to test everything.

### 3. Search for an existing ticket

Search Jira before creating anything, using the command name, error text, and the symbol that fails:

```jql
project = MOD AND issuetype = Bug AND
(text ~ "<command or option>" AND text ~ "<error text or symbol>")
ORDER BY updated DESC
```

- If an open ticket covers the same root cause, prepare a comment adding the new variant instead of
  a new ticket.
- If a **closed** ticket covers it, check whether the fix is on the current commit
  (`git merge-base --is-ancestor <fix-commit> HEAD`). Still failing after the fix means a new ticket
  that links the closed one, not a reopen, unless the user says otherwise.
- Do not create a duplicate unless the user confirms the existing ticket is a different failure.

### 4. Draft the ticket

Summary: `<COMMAND or area>: <symptom> when <trigger>`, for example
`FT.AGGREGATE: wrong results when FILTER precedes a numeric SORTBY with the optimizer on`.
Keep it under about 100 characters and name the user-visible symptom, not the suspected fix.

Description, following the team template:

~~~markdown
## Problem

<The incorrect behavior, when it occurs, and its impact: crash, wrong result, or error; which
commands and configurations; who is likely to hit it.>

## Reproduction

Environment/version:
<commit SHA and date, branch, build type (debug/release), Redis version, standalone or
coordinator, non-default config>

Steps:

1. <Step>
2. <Step>
3. <Step>

Input or command:

```text
<Minimal reproducible example, starting from an empty database>
```

Observed:

```text
<Actual result or error, verbatim>
```

Expected:

```text
<Expected result>
```

## Relevant context

- Relevant logs, stack trace, files, or symbols: <trimmed excerpt; `file.c` function names, not
  line numbers>
- Variants: <which nearby variants fail and which pass, from step 2>
- Related issue, PR, or documentation: <Jira keys, PRs, docs>
- Constraints or behavior that must remain unchanged: <for example, cases that must stay
  optimized, error text, persistence format, backport targets>
- Suspected cause, if known: **Unconfirmed:** <hypothesis and the evidence behind it>

## Done when

- The reproduction behaves as expected.
- A regression test covers the failure.
- Relevant existing tests continue to pass.
- <Any ticket-specific compatibility or behavior requirement.>
~~~

Rules for filling it in:
- Use fenced code blocks, not indented ones: Jira's markdown import drops indentation.
- Write *Observed* from the captured output, never from memory or from the user's description.
- Mark any cause you have not proven with **Unconfirmed**. A suspected cause you did prove (for
  example by a debugger, a failing assertion, or a one-line experimental change) can say so and
  cite the evidence.
- Leave a context bullet as `None known` rather than deleting it, so the reader can tell it was
  considered.
- Keep bugs with different root causes in separate tickets, cross-linked. If you cannot yet tell
  whether two failures share a cause, file one ticket and list both as variants.

### 5. Confirm, then create

Show the user the exact summary, fields, and description. Do not create or comment in Jira until
the user approves the draft. Then create it with `createJiraIssue`:
- `projectKey`: `MOD`, `issueTypeName`: `Bug`, `contentFormat`: `markdown`
- `parent`: `MOD-7468`, unless the user names another epic
- `additional_fields`: `{"components": [{"name": "RediSearch"}]}`, plus anything the user specified

Link related tickets the user approved with `createIssueLink` (`Relates`, or `Duplicate` only when
the user confirms). Read the created issue back and check that the code blocks rendered.

## Report Back

End with:
- The Jira key and URL, or the draft still waiting for approval
- The commit the bug was reproduced on
- The variants table in brief
- Related tickets found, and whether they were linked
- Anything you could not verify, such as coordinator mode or a release branch
