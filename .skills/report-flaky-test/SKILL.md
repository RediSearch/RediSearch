---
name: report-flaky-test
description: Report a flaky RediSearch test from a CI failure, Jira issue, test id, or local log. Use this to collect GitHub Actions logs/artifacts, find or create the right MOD Jira ticket, update existing flaky-test issues, and suggest immediate triage such as a trivial test-only fix or a temporary skip_until.
---

# Report Flaky Test

Report a flaky test to Jira, or add a new failure occurrence to an existing flaky-test ticket.

## Arguments

`$ARGUMENTS` may contain any of:
- GitHub Actions run URL, job URL, PR URL, or run id
- Jira issue key
- Test id, usually `test_file:testName` or `test_file.py::test_name`
- Local log file or failure excerpt

Defaults:
- Jira cloud: `redislabs.atlassian.net`
- Jira cloud id: `06f73ca7-8f2c-4392-b40a-08288e9d0ba3`
- Project: `MOD`
- Parent epic: `MOD-9672` (`[RQE] CI Stability - MAG`)
- Issue type: `Bug`
- Components: `RediSearch`, `RedisAI`

## Instructions

### 1. Collect Failure Evidence

From the provided input, gather the best available facts:
- Test id and normalized test id (`test_file:testName`)
- Failure reason and category: assertion failure, timeout, crash, sanitizer, Redis/RLTest lifecycle,
  coordinator/cluster-only, or environment/runner issue
- GitHub Actions run, attempt, job URL, PR, branch, and HEAD SHA
- Platform, architecture, Redis ref, standalone/coordinator mode, and special config
- Relevant failure excerpt, stack trace, Redis log excerpt, Rust panic/backtrace, or C backtrace
- Likely test owner, with confidence and evidence

For GitHub Actions failures, prefer `gh`:

```bash
gh run view <run_id> --repo RediSearch/RediSearch --json url,headBranch,headSha,event,jobs
gh run view <run_id> --repo RediSearch/RediSearch --log-failed
mkdir -p /tmp/redisearch-flaky-<run_id>
gh run download <run_id> --repo RediSearch/RediSearch --dir /tmp/redisearch-flaky-<run_id>
```

CI uploads failed test artifacts named `Test Logs ...` from `task-test.yml`. Look in the downloaded
artifact tree for:
- `tests/**/logs/*.log*`
- `bin/**/redisearch.so`
- `bin/**/redisearch.so.debug`

If artifact download is unavailable, use the CI job log and include links to the run/job.

### 2. Identify The Likely Test Owner

Try to identify a likely owner to mention in the Jira ticket or PR:
- First check `CODEOWNERS` if the repo has one.
- If no ownership file exists, inspect test history with:
  ```bash
  git log --follow --format='%h %an <%ae> %ad %s' -- <test_file>
  git blame -L <start>,<end> -- <test_file>
  ```
- Prefer the person who most recently changed the failing test or the helper/assertion involved in
  the failure, not merely the oldest author of the file.
- If several candidates are plausible, list them as candidates instead of pretending certainty.
- Mention an owner only when you can map them to a GitHub or Jira identity confidently. Otherwise
  include the owner evidence in plain text and say that the owner should be confirmed manually.

### 3. Find Existing Jira Ticket

Search Jira before creating anything. Match by normalized test id first:

```jql
project = MOD AND issuetype = Bug AND
(summary ~ "<test_file>" OR text ~ "<test_file>") AND
(summary ~ "<test_name>" OR text ~ "<test_name>")
ORDER BY updated DESC
```

Treat same test with a different platform, config, or failure signature as the same candidate first.
Mention the difference in the comment. Do not create a duplicate unless the existing issue is clearly
about an unrelated failure mode and the user confirms.

### 4. Prepare The Jira Update

If an existing issue is found, prepare a comment:

~~~markdown
### New flaky occurrence

* Test: `<test_file>:<test_name>`
* CI: <job-or-run-url>
* Branch / HEAD: `<branch>` / `<sha>`
* Platform: `<platform>`, `<arch>`
* Mode/config: `<standalone-or-coordinator>`, `<config>`, Redis `<redis-ref>`
* Failure category: `<category>`
* Failure reason: `<short reason>`
* Likely owner: `<@owner or candidate names with evidence>`

```text
<short relevant excerpt>
```

Artifacts/logs:
* <artifact link or local downloaded path>

Notes:
* <anything new compared with the existing ticket, or "Same signature as existing report.">
~~~

If no existing issue is found, prepare a new Jira `Bug` under `MOD-9672`:
- Summary: `Flaky test: <test_file>:<test_name> <short symptom/context>`
- Components: `RediSearch`, `RedisAI`
- Description:

~~~markdown
### Test Failure Report

**Name of the test:**
`<test_file>:<test_name>`

**Failure reason:**
<short reason>

**Category:**
<category>

**Failure log:**

```text
<short relevant excerpt>
```

**Link to failed CI:**
<job-or-run-url>

**Branch & HEAD:**
Branch: `<branch>`
HEAD: `<sha>`

**Platform (OS & arch):**
`<platform>`, `<arch>`

**Redis version/ref:**
`<redis-ref>`

**Special test configuration:**
`<standalone-or-coordinator>`, `<config>`

**Likely owner:**
`<@owner or candidate names with evidence>`

**Artifacts/logs:**
<artifact link or local downloaded path>

**Initial triage:**
<quick read-only assessment, including immediate fix or skip recommendation when applicable>
~~~

Always show the exact new issue payload or comment body and ask the user before creating/commenting
in Jira. Do not mutate Jira without confirmation.

### 5. Attach Or Link Logs

If a Jira attachment tool is available, attach the relevant downloaded log archive or smallest useful
log files. If no attachment capability is available, state that directly and include:
- GitHub Actions run/job URL
- Artifact names
- Local downloaded artifact path under `/tmp/redisearch-flaky-<run_id>/`

### 6. Quick Triage Recommendation

Do a short read-only scan of the failing test and nearby helpers before finalizing the report.

Suggest an immediate test-only fix only when it is obvious and scoped, for example:
- Add a missing wait for indexing, async work, GC, expiration, cursor reap, or cluster state
- Make ordering deterministic before asserting
- Restore or isolate global config changes
- Use hash tags for cluster keys that must land on one shard
- Adjust a clear timeout budget mismatch

Suggest a temporary `skip_until("<date>", "<MOD-key/reason>")` from this reporting skill when:
- The failure is repeatedly disrupting CI or merge queue
- No trivial safe test-only fix is visible
- The skip can be scoped to the smallest affected generated test, test function, mode, platform, or
  configuration

Any `skip_until` recommendation must include:
- Jira key, or say it should be filled after the new ticket is created
- Expiry date, normally about one month out unless the user gives another policy
- Affected mode/platform/config
- Exact smallest test scope to skip

Do not implement the fix from this skill unless the user explicitly asks to switch from reporting to
implementation.

## Report Back

End with:
- Existing Jira issue selected, or new Jira payload prepared
- Whether Jira was updated or is waiting for confirmation
- Likely owner and confidence, or why no confident owner was identified
- Artifact/log status
- Immediate triage recommendation, if any
