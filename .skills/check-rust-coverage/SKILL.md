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

Prefer the swamp model, which runs the same command and records the uncovered
lines as versioned data — as ranges per file, worst covered first — so the result
can be referenced later instead of re-measured:

```bash
swamp model method run rust-coverage run --input '{"crate":"<crate_name>"}'
swamp data get rust-coverage summary --json | jq '.content.files'
```

Omit `crate` to measure the whole workspace; it then reads the bencher-crate
exclude list out of `build.sh`, because instrumenting those crates fails at link
time. Pass `manifestPath` instead of `crate` to point at a Cargo.toml directly.

The equivalent by hand, if swamp is unavailable:

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
