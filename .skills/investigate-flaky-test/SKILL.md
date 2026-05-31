---
name: investigate-flaky-test
description: Investigate a RediSearch flaky-test report from a Jira key, CI failure, test id, or local logs. Use this to collect evidence, identify a supported root cause, and propose a real fix; if the evidence is insufficient, say so and ask for the missing data instead of suggesting workaround-only fixes or skip_until.
---

# Investigate Flaky Test

Investigate a flaky-test report and propose a proper fix only when the evidence supports it.

## Arguments

`$ARGUMENTS` may contain any of:
- Jira issue key, usually `MOD-...`
- GitHub Actions run URL, job URL, PR URL, or run id
- Test id, usually `test_file:testName` or `test_file.py::test_name`
- Local log file or failure excerpt

## Instructions

### 1. Gather Evidence

Collect enough context to reason from facts:
- Jira description and comments, if a Jira key is provided
- GitHub Actions run/job logs and `Test Logs ...` artifacts, if a CI URL/run id is provided
- Failure excerpt, stack trace, Redis server logs, Rust panic/backtrace, C backtrace, and INFO sections
- Failed test source and nearby helpers
- Relevant production code touched by the test path
- Similar Jira issues or failures for the same test or same failure signature
- Recent related changes, when they can explain a regression

For GitHub Actions failures, prefer `gh`:

```bash
gh run view <run_id> --repo RediSearch/RediSearch --json url,headBranch,headSha,event,jobs
gh run view <run_id> --repo RediSearch/RediSearch --log-failed
mkdir -p /tmp/redisearch-flaky-<run_id>
gh run download <run_id> --repo RediSearch/RediSearch --dir /tmp/redisearch-flaky-<run_id>
```

CI failed-test artifacts commonly contain `tests/**/logs/*.log*`, `bin/**/redisearch.so`, and
`bin/**/redisearch.so.debug`.

### 2. Classify The Failure

Classify the failure before proposing a fix:
- Assertion race or nondeterministic ordering
- Timeout or performance budget issue
- Redis/RLTest lifecycle issue, such as save/reload, expiration, cursor reap, or server shutdown
- Coordinator/cluster-only behavior
- Sanitizer, crash, panic, or memory safety issue
- Environment or runner issue
- Unknown or insufficient evidence

Separate proven facts from inference. Use language like "the logs show" for facts and "likely" or
"possible" for hypotheses.

### 3. Find The Root Cause

Trace from the failing assertion or exception to the code path that can produce it:
- For Python flow tests, inspect the exact test, fixtures, helper decorators, `Env()` settings, cluster
  skips, and waits.
- For C/C++ unit tests, inspect the failing assertion, setup/teardown, thread usage, and decoded stack
  traces.
- For Rust tests or panics, inspect the panic message, backtrace, unsafe boundaries, and FFI wrappers.
- For coordinator failures, compare standalone and coordinator paths and check shard placement.
- For timeouts, distinguish slow-but-correct behavior from a hang, deadlock, polling bug, or excessive
  test data size.

Avoid stopping at "probably flaky" when the logs identify a specific race, missing wait, lifecycle
conflict, or environment failure.

### 4. Propose A Real Fix Or Say Evidence Is Insufficient

Propose a root-cause fix only when the cause is supported well enough to justify a code or test
change. If the logs, source path, and failure signature do not support a specific cause, do not
claim one.

A good fix proposal includes:
- Root cause, with evidence
- Exact target files/functions
- Behavior change
- Why this is preferable to a workaround
- Verification commands
- Remaining risk

If the cause is unclear, say so plainly. Do not suggest workaround-only fixes, quarantine, or
`skip_until` from this skill. Instead, list the missing data needed, such as:
- Full `Test Logs ...` artifact
- Failed job log with timestamps
- Reproduction command and seed/config
- Server log around the assertion or crash
- A second occurrence to compare signatures

When the current evidence is not enough but the next failure could be made more informative,
recommend a diagnostic PR instead of a workaround. A valid recommendation is to add focused debug
logs, counters, state dumps, or richer test assertion messages, merge or run that instrumentation in
CI for a few days, and use the next flaky occurrence to identify the root cause. Do not present this
as "add logs and rerun once to find the root cause right away"; a focused local rerun can help, but
the main goal is better evidence when the intermittent failure happens again.

### 5. Verification Plan

Choose verification based on the proposed fix:
- Python flow test:
  ```bash
  ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="<test_file>:<test_name>"
  ```
- Coordinator-specific flow test:
  ```bash
  REDIS_STANDALONE=0 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="<test_file>:<test_name>"
  ```
- C/C++ unit test:
  ```bash
  ./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1 TEST=<unit_test_name>
  ```
- Rust test:
  ```bash
  cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate_name> <test_filter>
  ```

If the failure is timing-sensitive, recommend repeating the focused test enough times to gain
confidence, and include any required environment variables from the CI failure.

## Report Back

End with:
- Classification
- Evidence-backed root cause, or "insufficient evidence"
- Proposed real fix, if supported
- Verification plan
- Missing data, if blocked
