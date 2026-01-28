---
name: check-rust-coverage
description: Check which Rust lines are not covered by Rust tests.
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
cargo llvm-cov test --manifest-path <crate_directory>/Cargo.toml --quiet --json 2>/dev/null | jq -r '"Uncovered Lines:",
(.data[0].files[] |
  select(.summary.lines.percent < 100) |
  .filename as $f |
  [.segments[] | select(.[2] == 0 and .[4] == true) | .[0]] |
  unique |
  if length > 0 then "\($f): \(join(", "))" else empty end
)'
```

to get the list of uncovered lines for each file in the target crate.
