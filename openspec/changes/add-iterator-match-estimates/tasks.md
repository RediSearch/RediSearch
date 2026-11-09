# Tasks: advisory iterator match estimates

## 1. Common contract and interop

- [x] Add the advisory estimate method to the Rust query iterator interface.
- [x] Add the C callback and forward it in both C-to-Rust and Rust-to-C bridges.
- [x] Preserve the existing upper-bound API and all its consumers.

## 2. Iterator implementations

- [x] Cover leaves, materialized and lazy iterators, wrappers, and variants.
- [x] Compose intersection, union, negation, and optional estimates.
- [x] Add numeric range estimation with documented fallback conditions.
- [x] Cover remaining C iterators and output-limited iterator families.

## 3. Contract tests and validation

- [x] Test population bounds, zero populations, and unchanged iterator position.
- [x] Test boolean composition, correlated predicates, and numeric edge cases.
- [x] Test C callback forwarding and wrapper consistency.
- [x] Run formatting, lint, build, and appropriate Rust/C/behavioral checks.
- [x] Complete independent review and record any unresolved findings in the PR.

## 4. Benchmark evidence and specification

- [x] Add isolated accessor benchmarks for leaf, compound, nested, and FFI trees.
- [x] Add a base-compatible `numeric_filter` benchmark for construction and traversal.
- [x] Compare the unchanged repository traversal suite on base and candidate.
- [x] Compare the same `numeric_filter` harness on base and candidate.
- [x] Run accessor benchmarks and report their absolute overhead separately.
- [x] Document the advisory contract, approximation assumptions, and alternatives.
- [x] Reconcile the specification and checklist with the final implementation.
