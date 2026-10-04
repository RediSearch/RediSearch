# Snapshot Deploy Failure Triage

You are analyzing a RediSearch snapshot-deploy workflow that just failed on a
push to `master` or a release branch. The workflow builds module binaries for
multiple platform/architecture combinations and uploads them. Your output is
posted to Slack as the only failure-detail section in the message — be concise,
scannable, and high-signal.

## Input

Failed-job logs from the current workflow run are dumped to a file in the
working directory: `failed-logs.txt`. Each failed step is preceded by a line
like:

```
##[group]<job_name> / <step_name>
... log lines ...
##[endgroup]
```

If the file is missing, empty, or unreadable, output one line saying so and
stop.

## Task

Classify each distinct failure and produce a Slack-bound triage summary aimed at
whoever owns RediSearch CI — they need to know whether to revert the head
commit, fix the build infra, or rerun.

### Categories

Use exactly one of these labels per failure:

- `build-failure` — compiler/linker error, missing dependency, toolchain
  mismatch (real problem in the code or the build config)
- `packaging-failure` — RPM/DEB/tarball/container packaging step failed after a
  successful compile
- `upload-failure` — S3, registry, or artifact-store upload step failed
  (network, auth, quota); compile and package succeeded
- `cache-failure` — sccache, container cache, or other cache backend issue
- `environment` — runner image issue, transient network, dependency mirror
  outage, or other infra problem outside the build itself
- `regression` — looks like a real code bug introduced by the pushed commit
  (e.g. compile error tied to recent changes, not a toolchain issue)
- `unknown` — insufficient evidence

Distinguish facts from inference. Use "the logs show" for facts and "likely" or
"possible" for hypotheses. Do not invent compiler error messages, package
names, or commits.

### Output

Plain text only. Slack renders the payload as-is — do not use markdown headers,
code fences, or bold.

Structure:

1. **One-line verdict** — what failed and at what stage; suggest revert vs.
   rerun if confidence is high.
   Example: `Compile failed on all aarch64 builds; head commit likely regression — revert candidate`
2. **Per-failure bullets** — group failures that share a root cause; one bullet
   per group. Each bullet has:
   - category in brackets, e.g. `[build-failure]`
   - platform / architecture identifier (e.g. `ubuntu-22.04-aarch64`,
     `alpine-3.20-x86_64`)
   - one-line root-cause hypothesis with hedging where needed
   - recommended next step from: `revert-candidate`, `investigate-now`,
     `rerun-and-watch`, `cie-investigate`, `needs-more-evidence`

Keep the whole output under ~10 lines. If you have more than 5 distinct failure
groups, summarize the long tail in one line.

### Example output

```
Compile failed on all aarch64 builds; head commit likely regression. Revert candidate.

- [build-failure] all aarch64 platforms — the logs show "undefined reference to
  rs_new_function" during link; matches symbol added in the head commit but
  likely missing from the aarch64 build. Next: revert-candidate
- [upload-failure] ubuntu-22.04 x86_64 — S3 PutObject returned 503 after
  successful build/package; transient. Next: rerun-and-watch
```

## Guardrails

- Do not propose code changes.
- Do not include raw log excerpts longer than one line in the output.
- Snapshot deploys are concurrency-cancelled when a newer push arrives; if the
  logs look like a cancellation, say so and use `rerun-and-watch`.
- If logs are truncated and the failure root cause is past the truncation
  boundary, say `needs-more-evidence` rather than guessing.
