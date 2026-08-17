---
name: bisect-perf-regression
description: Systematically bisect an end-to-end performance regression between two refs (tags/branches/commits) down to the exact offending commit(s). Use this when CI/customer benchmarks show one version is measurably slower than another and you need to attribute the cost to specific PRs.
---

# Bisect a performance regression

Pin a perf regression to specific commits using drift-cancelled, repeated, cold benchmarks.

You will need (or need to build) a small harness around the workload of interest.
The exact shape of the harness does not matter, as long as it satisfies the
properties described in step 1 below (interleaved labels, cold per-iteration
state, median + CV reporting).

## Arguments

`$ARGUMENTS` should identify:
- A **good** ref (older / fast) and a **bad** ref (newer / slow)
- A reproducible workload and its parameters (workload size, dataset, mode),
  expressed as whatever the harness consumes (env vars, CLI flags, config file)
- An expected gap (so you can stop early when a real step is found)

## Instructions

### 1. Reproduce the gap stably before bisecting

Before bisecting anything, prove the two endpoints are reliably distinguishable.

- Use an interleaved harness that restarts the server (or the system under
  test) per iteration so any cached state is cold each run, drives the workload
  round-robin across labels
  (so environmental drift attaches equally to every label), and reports median +
  CV per label. Sequential "all N runs of label A, then all N runs of label B"
  attributes any host drift across the wall-clock window entirely to the second
  label, and is the single largest source of false positives in perf bisection.
- Pin CPU count (e.g. `--cpus=8` for a Docker harness) and run on a quiet Linux
  host. Shared/laptop Docker setups (macOS Docker Desktop, Codespaces, noisy CI
  runners) are generally too jittery for sub-3% deltas.
- **Pick `N_RUNS` adaptively from observed CV — don't burn 5–8 runs by default.**
  - Start at `N_RUNS=3` (the minimum that yields a stdev/CV).
  - After the run, inspect the per-label CV printed by the harness:
    | observed max CV across labels | action |
    |-------------------------------|-------|
    | ≤ 1.5 %                       | 3 runs is enough, accept the medians |
    | 1.5 – 2.5 %                   | rerun with `N_RUNS=5` for the labels involved in the suspect step |
    | 2.5 – 4 %                     | rerun with `N_RUNS=8`, and only trust deltas ≥ 2 × max CV |
    | > 4 %                         | the host is too noisy — fix it before bisecting (see below) |
  - A claim is "real" when the median gap exceeds `max(3 %, 2 × max CV across the two labels)`.
- If CV stays > 4 % even at `N_RUNS=8`, increase the per-run workload size, or
  fix the host (other processes, CPU governor, thermal throttling, neighbour VMs)
  before continuing — bisection on noisy data attributes regressions to the
  wrong commit.

### 2. Bracket with version tags first, then commits

Always start with broad strokes and narrow down — never start at the commit level.

1. **Tag ladder** — list the released tags between good and bad and run them all
   in one interleaved sweep so they share drift. For example, with a project
   that has tags `v2.10.12 … v2.10.30`, run all of them as labels in a single
   interleaved sweep at `N_RUNS=3`. Look for the **step** — the adjacent pair
   with the largest jump. That's your new `[lo, hi]` interval.
2. **First-parent commit bisect** within `[lo, hi]`. Enumerate evenly-spaced
   first-parent commits that actually touched code (filter to `src/*`,
   `deps/*`, or the relevant subtrees — skip pure CI / docs / vendoring
   commits, since they cannot move performance). Build them, run the
   interleaved bench `[lo, p1, p2, …, hi]`, and inspect adjacent deltas.
   Start at `N_RUNS=3` and escalate per the CV table in step 1.
3. **Iterate** — pick the adjacent pair with the largest delta, recurse on
   that narrower interval. Stop when `[lo, hi]` is one commit, or when the
   remaining interval is below the noise floor.

### 3. Watch for compounding / multi-step regressions

A single endpoint number like "+30% slower" can hide several smaller steps that
may compound through the same code path. Always print the full per-tag /
per-commit ladder, not just endpoint deltas — a smooth slope versus a single
cliff have very different fixes.

### 3a. Isolating residuals with the "fix-on-commit" technique

When a known regression dominates the gap and a fix is already in flight,
**apply that fix on top of every historical commit you benchmark**. This
neutralises the known cost so the ladder surfaces only the *residual*
regressions that arrived alongside it. Without this technique, a large dominant
cost can mask smaller but still real per-commit jumps.

Build the fix-on-commit variants by:
1. Creating a `fix-on-<sha>` branch off `<sha>`.
2. Applying the in-flight fix on top.
3. Building and benching that branch the same way you would a stock commit.

Use a **code injector** (a small script that locates the target function by
signature regex, balances braces to find its body, and inserts the fix at a
known anchor) rather than a static `git apply` patch when:
- the target function's signature changes inside the bracketed range, or
- surrounding code shifts line numbers across tags (3-line context is too
  small to disambiguate, and `--ignore-whitespace` won't save you).

Keep the injector **idempotent** (detect its own marker before injecting) so
reruns are safe across rebuilds.

### 4. Confirm the offender

Once a single commit is suspected:

- Read the diff. The change must plausibly cause the observed cost (e.g. new
  syscalls per token, new allocations per doc, new Rust↔C boundary calls).
- Run a **confirmation pair**: `[<commit>^, <commit>]` only. Here it's worth
  spending more iterations than the bisect ladder used — start at `N_RUNS=5`
  and escalate to 8 if max CV > 2.5 %. The delta should match the step size
  found during bisection to within `2 × max CV`.
- If you can, build a **revert candidate** (`<bad>` with just that commit reverted)
  and confirm it returns to `<lo>` performance. This guards against picking up an
  innocent commit that sits next to the real offender via a noisy run.

### 5. Honest reporting

Report only what the data supports:
- Endpoint medians + CV
- The bisection ladder with adjacent deltas
- The candidate commit(s), with PR link, author, and a one-line cause hypothesis
  grounded in the diff
- Residual unexplained delta, if any, with the noise floor

Do not round, do not collapse multi-step regressions into a single cause, and do
not skip the confirmation step.

## Common pitfalls

- **Cold-cache leakage into the first iteration** — the first build in a
  session is usually slow due to docker layer pulls or compiler-cache warm-up.
  Pre-build all artifacts before starting the timed benchmark.
- **Wrong runtime/base image for the branch under test** — each build must run
  against the runtime version it was compiled against (different minor branches
  often pin different runtime/host versions). Verify the harness picks the
  correct base image / runtime per label.
- **Auto-loaded bundled component** — some base images auto-load a bundled
  version of the component under test via the entrypoint. Bypass that (e.g.
  override the entrypoint) so the component you actually want to measure is the
  only one loaded.
- **Toolchain / dependency drift across the bisect range** — historical tags may
  need patches (compiler / library API breakage on newer hosts) or a specific
  toolchain pin (e.g. a particular nightly). Wrap the per-commit build in a
  script that detects the tag and applies the required patches / toolchain pin
  on demand.

## Report Back

End with:
- The good→bad ladder (medians, CV, adjacent %)
- The implicated commit(s), short SHA + PR + title
- The confirmation-pair numbers
- A diff-grounded hypothesis for *why* the commit costs what it does
- Residual gap, if any, and what would be needed to attribute it
