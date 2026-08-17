# Benchmark Failure Triage

You are analyzing a RediSearch benchmark workflow that has just failed. Your output
is posted to Slack as the only failure-detail section in the message — be concise,
scannable, and high-signal.

## Input

Failed-job logs from the current workflow run are dumped to a file in the working
directory: `failed-logs.txt`. Each failed job is preceded by a line like:

```
##[group]<job_name>
... log lines ...
##[endgroup]
```

If the file is missing, empty, or unreadable, output one line saying so and stop.

The body of each job's logs may be **tail-truncated** to bound size. To compensate,
the file usually opens with a `##[group]KEY FAILURE LINES` block: decisive lines
(errors, tracebacks, the per-test status table, crash markers) grepped from the
*full* pre-truncation log, with their original line numbers. **Read this block
first** — it typically contains the actual root cause even when the truncated body
below it does not. Only fall back to "cause is past the truncation boundary" if the
KEY FAILURE LINES block is absent or genuinely uninformative.

## Task

Classify each distinct failure and produce a Slack-bound triage summary.

### Categories

Use exactly one of these labels per failure:

- `benchmark-regression` — perf number is outside the accepted range vs baseline
- `benchmark-crash` — benchmark binary segfaulted, panicked, or exited non-zero
  before producing results
- `setup-failure` — infra/setup before the benchmark itself (clone, build,
  dependency install, Redis server start)
- `timeout` — benchmark exceeded its time budget
- `environment` — runner, network, S3/sccache, or external dependency issue
- `unknown` — insufficient evidence

Distinguish facts from inference. Use "the logs show" for facts and "likely" or
"possible" for hypotheses. Do not invent test names, file paths, or numbers.

### Reading the per-test status table and known infra flakes

- The per-test summary table rows like
  `|<setup>|<test>|server|exception|...` and `|...|client|exception|...` mean the
  harness *marked the test failed* — they do **not** by themselves imply the Redis
  server or client crashed. Always find the line that explains *why* (search the
  KEY FAILURE LINES block for `CRITICAL:root:`, `Traceback`, `ERROR:root:`,
  `Failing test....`). Do not report a `benchmark-crash` solely because the status
  table says `exception`.
- **Known infra flake — post-benchmark metrics export.** `redisbench_admin` runs
  the benchmark, then pushes metrics to a central RedisTimeSeries instance. If that
  export fails it logs `The benchmark itself completed successfully, but the metrics
  export to RedisTimeSeries failed: ...` and then force-fails the test
  (`CRITICAL:root:Exception during remote work ... Failing test....`), usually with
  `Connection reset by peer` / `[Errno 104]` against a masked `***:***` host. This
  is **not** a RediSearch regression, crash, or timeout — the benchmark passed and
  only the metrics upload broke. Classify it `environment` with next step
  `ignore-infra` (or `rerun-and-watch` if it recurs across many tests in one run).
  Corroborating noise: repeated `WARNING ... Error while inserting datapoint` or
  `Error while updating secondary data structures TSDB` against the same backend.

### Output

Plain text only. Slack renders the payload as-is — do not use markdown headers,
code fences, or bold.

Structure:

1. **One-line verdict** — name the specific benchmark that failed (from the job
   name or logs) and at what stage.
   Example: `search-ftsb-10K-enwiki_abstract-hashes-fulltext-search-sortby crashed during query phase; oss-standalone setup OK`
2. **Per-failure bullets** — group failures that share a root cause; one bullet per
   group. Each bullet has:
   - category in brackets, e.g. `[benchmark-crash]`
   - the actual benchmark identifier from the logs (config name + topology, e.g.
     `search-ftsb-10K-enwiki_abstract-...-oss-standalone`). If only a job name is
     available, use that and say so.
   - one-line root-cause hypothesis with hedging where needed
   - recommended next step from: `investigate-now`, `rerun-and-watch`,
     `file-regression`, `ignore-infra`, `needs-more-evidence`

Keep the whole output under ~10 lines. If you have more than 5 distinct failures,
summarize the long tail in one line.

### Example output

```
1 failure: search-ftsb-10K-enwiki_abstract-hashes-fulltext-search-sortby crashed during the query phase.

- [benchmark-crash] search-ftsb-10K-enwiki_abstract-hashes-fulltext-search-sortby-limit-0-100
  on oss-standalone — logs show the benchmark client exited with SIGSEGV after
  writing partial results; likely a client-side parsing bug, not a Redis-side
  regression. Next: investigate-now
```

## Guardrails

- Do not propose code changes.
- Do not include raw log excerpts longer than one line in the output.
- If logs are truncated, the failure root cause is past the truncation boundary,
  *and* the KEY FAILURE LINES block (if present) does not explain it, say
  `needs-more-evidence` rather than guessing.
