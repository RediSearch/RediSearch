---
name: check-rust-coverage
description: Check which Rust lines are not covered by Rust tests. Use this when you developed new Rust code and want to ensure it is tested.
---

# Check Rust Coverage

Determine which Rust lines are not covered by Rust tests.

## Arguments
- `<path>`: Path to a Rust crate.
- `<path 1> <path 2>`: Multiple crate paths.

If a path doesn't include `src/`, assume it to be in the `src/redisearch_rs` directory. E.g. `numeric_range_tree` becomes `src/redisearch_rs/numeric_range_tree`.
If a path points to a directory, consider all Rust crates in that directory.

## Instructions

Run

```bash
NEXTEST_PROFILE=coverage cargo +$(cat .rust-nightly) llvm-cov nextest --manifest-path <crate_directory>/Cargo.toml --branch --json 2>/dev/null | jq -r '"Uncovered Lines:",
(.data[0].files[] |
  .filename as $f |
  [.segments[] | select(.[2] == 0 and .[4] == true) | .[0]] |
  unique |
  if length > 0 then "\($f): \(join(", "))" else empty end
),
"Partially Covered Branches (line: true/false):",
(.data[0].files[] |
  .filename as $f |
  [.branches | group_by(.[0:4])[] |
    {line: .[0][0], t: (map(.[4]) | add), f: (map(.[5]) | add)} |
    select(.t == 0 or .f == 0) | "\(.line): \(.t)/\(.f)"] |
  unique |
  if length > 0 then "\($f): \(join(", "))" else empty end
)'
```

from the repository root to get, for each file in the target crate, the lines no test
executes and the conditions no test drives both ways.

This mirrors the CI coverage job, which Codecov reports from:
- `--branch` is what catches a condition that runs but never takes one of its outcomes,
  such as one operand of an `&&` chain that is never false. Without it such a line counts
  as covered.
- `--branch` needs nightly, and only the pinned `.rust-nightly` is known to work: an
  unpinned `+nightly` can silently produce a report with every line at zero.
- `NEXTEST_PROFILE=coverage` gives tests the longer timeouts that branch instrumentation
  needs; under the default profile slow tests are killed and no report is written.
- Generic code yields one branch entry per instantiation, so the query sums them per
  condition before deciding it is partially covered.

Ignore lines that exist only to reject states the code rules out, as no test can reach
them: `unreachable!`, `unreachable_unchecked`, `debug_assert!` and its variants, and a
`panic!` or `expect` guarding an invariant. A condition inside a `debug_assert!` that never
fails shows as partially covered for the same reason.
