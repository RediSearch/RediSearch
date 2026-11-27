---
name: compare-bench-c-vs-rust
description: Analyze a criterion benchmark tree to find where a baseline variant beats the others along a chosen pivot parameter, and group the wins into patterns. Defaults to `lang=C` (baseline) vs `lang=Rust` so the C-vs-Rust port comparison is zero-config, but works for any `*_bencher` crate that sweeps a comparison parameter — and re-pivots onto any other parameter to hunt regressions in that dimension.
---

# Compare Benchmarks along a Pivot Parameter

Find where a **baseline** variant beats the others across a criterion benchmark sweep, and
isolate the wins into parameter patterns so you know *which* code path to fix — not just
that a regression exists.

The comparison axis is a **pivot** parameter. By default the pivot is `lang` and the
baseline is `C`, so with no extra flags this reproduces the original C-vs-Rust port
comparison (e.g. `ttl_table_bencher`). Override `--pivot`/`--baseline` to compare any other
parameter — two implementations (`--pivot impl --baseline old`), two algorithms, etc. When
the pivot has **more than two values**, the baseline is compared against each other value
in its own block (N-way).

This works for any `*_bencher` crate whose benchmarks encode the pivot as a `k=v` pair in
the criterion `value_str` (a `BenchmarkGroup` matrix).

## Arguments
- (optional) a comma-separated list of criterion bench-group folders to analyze
  (as shown by `ls bin/redisearch_rs/criterion`, e.g.
  `add_sequential,verify_doc_and_field_doc_default`). When given, only those groups are
  analyzed and the rest are ignored. When omitted, every group in the tree is analyzed.
- (optional) path to the criterion output dir. Default: `bin/redisearch_rs/criterion`.
- (optional) `--pivot <param>` — the comparison axis (default `lang`).
- (optional) `--baseline <value>` — the pivot value used as the denominator (default `C`).

Arguments provided: `$ARGUMENTS`

Interpret `$ARGUMENTS`: a token that contains commas or matches a folder name under the
criterion dir is the **group list** → pass it via `--groups`. A path-like token (contains
`/` and points at a directory) is the **criterion dir** → pass it as the positional arg.
A `--pivot`/`--baseline` token is passed straight through. Any may be present; with none,
the default C-vs-Rust comparison runs.

## Prerequisites

Run the benchmark crate first (see `/run-rust-benchmarks`), e.g.:
```bash
cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p ttl_table_bencher
```
This populates `bin/redisearch_rs/criterion/<group>/<config>/new/{benchmark.json,estimates.json}`.
**Do not re-run the bench to "see more"** — analyze the saved tree. If the user says
the bench is still running, the tree is live; note that numbers may shift and offer to
re-run the analysis once it finishes.

## Instructions

1. Run the analysis script against the criterion dir. Analyze the whole tree (default
   pivot `lang`, baseline `C` — the C-vs-Rust comparison):
   ```bash
   python3 .skills/compare-bench-c-vs-rust/analyze.py bin/redisearch_rs/criterion --json /tmp/bench_pairs.json
   ```
   …or restrict to a subset of bench-group folders with `--groups`:
   ```bash
   python3 .skills/compare-bench-c-vs-rust/analyze.py bin/redisearch_rs/criterion \
     --groups add_sequential,verify_doc_and_field_doc_default --json /tmp/bench_pairs.json
   ```
   …or compare along a different pivot — any other `value_str` parameter is fair game,
   including re-pivoting onto a workload parameter to check for a regression in *that*
   dimension:
   ```bash
   python3 .skills/compare-bench-c-vs-rust/analyze.py bin/redisearch_rs/criterion \
     --pivot impl --baseline old --json /tmp/bench_pairs.json
   ```
   It prints: pair count, win/loss split, a ratio-distribution table per group, the
   top biggest baseline wins, a per-parameter correlation table, and a **worst-regions
   scan** that ranks the single dimensions and every *pair* of dimensions where the
   candidate is slowest (flagging interactions). When the pivot has more than two
   values, each non-baseline value gets its own comparison block (N-way).
   `--threshold` (default `1.10`) sets what counts as a "meaningful" baseline win;
   `--top` controls the win list. A `--groups` folder that does not exist is a hard error
   listing the available groups; an unknown `--pivot`/`--baseline` errors with the
   parameter names / pivot values actually present.

2. Read the worst-regions scan and the per-parameter correlation table together to
   locate the pattern. The metric is **ratio = candidate_ns / baseline_ns**, so **>1
   means the baseline is faster** (the candidate is worse).
   - The **worst-regions scan** does the dimension-hunting for you: it ranks every
     single dimension *and every pair of dimensions* by median ratio, so the top rows
     are literally "where the candidate is worst". You do **not** need to guess which
     parameters to cross — the scan already crossed all of them.
   - A 2-D row flagged `✦` is an **interaction**: the corner is worse than either of
     its two dimensions alone, so the slowdown is born of the *combination* (e.g.
     `slot_size=large & pop_count=small`). These are the rows a per-dimension view
     would miss, so call them out specifically.
   - Use the per-parameter correlation table to spot **noise dimensions**: a parameter
     whose every value sits near median **1.00x** does not drive the gap — say so and
     drop it from the narrative.
   - Discount sub-microsecond `candidate=...ns` rows in the scan as noise-prone, and
     cross-check the top rows against the "biggest baseline wins" list to confirm a
     corner is real and not one noisy point. The most actionable wins usually correlate
     with a single mechanism (e.g. table growth/resize when initial capacity ≪ element
     count).

3. To make a flagged 2-D region crisp, render its full heatmap with `grid.py`, passing
   the `--json` dump from step 1 and the group + the two parameters the scan flagged:
   ```bash
   python3 .skills/compare-bench-c-vs-rust/grid.py /tmp/bench_pairs.json add/sequential slot_size pop_count
   ```
   It prints a heatmap of median ratios (>1 = baseline faster) over the two parameters —
   the zoom-in on whatever the worst-regions scan surfaced. For an N-way pivot (more than
   one candidate), add `--candidate <value>` to pick which non-baseline variant to chart;
   `grid.py` lists the available candidates if you omit it.

4. Report a concise recap:
   - headline win/loss split and ratio-distribution table,
   - one section per *pattern* (not per benchmark), each naming the parameter region
     (single dimension or flagged interaction from the worst-regions scan), the
     magnitude (e.g. "3–4.5×"), and the likely mechanism,
   - flag sub-microsecond absolute timings as noise-prone (small `pop_count` rows
     often are), and flag noise dimensions,
   - end with the single most actionable hotspot and offer to dig into that code path.

## Method notes (why the script does what it does)

- **Names come from `new/benchmark.json`** (`group_id` + `value_str`), never folder
  names — criterion truncates directory names and distinct configs can collide.
- **Timings come from `mean.point_estimate`** in `new/estimates.json`.
- **Pairing**: parameters are parsed from `value_str` (`/`-separated `k=v`); variants are
  matched on *all* parameters except the pivot. The `baseline` pivot value is required for
  a pairing; every other pivot value present becomes a candidate compared against it
  (so a pivot with N values yields N-1 candidates). Groups can have different parameter
  sets (e.g. a `mask` sweep adds `mask`/`expired`/`field_filled_at`) — the script keys
  per group, so don't hard-code a parameter list.
- Leaves whose `value_str` lacks the pivot parameter (e.g. criterion group-summary dirs,
  which have none under the default `lang` pivot) are skipped and counted.

## Scripts

- `analyze.py` — pairs variants along the pivot, prints win/loss split, ratio
  distribution, biggest baseline wins, per-parameter correlation, and a worst-regions
  scan that ranks every single dimension and every pair of dimensions by median ratio
  (flagging `✦` interactions worse than either dimension alone), so the worst direction
  is surfaced automatically rather than guessed (one block per candidate for an N-way
  pivot). Flags: `--pivot` (default `lang`), `--baseline` (default `C`), `--groups`,
  `--threshold`, `--top`, `--json`.
- `grid.py` — two-parameter heatmap of median ratios from the `--json` dump. Sorts
  parameter values numerically when possible, lexically otherwise (e.g. bitmasks). The
  group argument accepts either the `group_id` (`add/sequential`) or its folder form
  (`add_sequential`); pass `--candidate <value>` when the group has more than one
  candidate (N-way pivot).
