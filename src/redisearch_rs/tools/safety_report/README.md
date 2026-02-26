# Safety Report

Audits unsafe code usage across the Rust codebase. Reports counts of unsafe functions,
unsafe block lines, unsafe impls, and unsafe traits per crate, along with a safety ratio.

## Usage

```bash
cd src/redisearch_rs && cargo safety-report
```
## What it excludes

- Bencher crates (`*_bencher`)
- `redis_mock`
- `tools/` crates
- `tests/` directories and `#[cfg(test)]` / `#[test]` code

## Output

Two markdown tables -- one for FFI crates, one for core crates -- each sorted by
unsafe ratio (descending) and including a totals row. The unsafe ratio is the
percentage of lines inside `unsafe` blocks relative to total lines of code.
