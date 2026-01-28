---
name: review-rust-docs
description: Review the documentation of a Rust crate to ensure it meets our requirements and standards.
---

# Review Rust Docs

Read the documentation of a Rust crate or module to ensure it meets our requirements and standards.

## Arguments
- `<path>`: Path to the Rust crate or file whose documentation needs to be reviewed.
- `<path 1> <path 2>`: Multiple crates/files to review

If the path doesn't include `src/`, assume it to be in the `src/redisearch_rs` directory. E.g. `numeric_range_tree` becomes `src/redisearch_rs/numeric_range_tree`.
If the path points to a directory, review the documentation of all Rust files in that directory.

## Instructions

Read the documentation of the specific Rust files and ensure they meet the guidelines outlined in [`rust-docs-guidelines`](../rust-docs-guidelines/SKILL.md).

Emit a report for all non-conforming locations you find, with an explanation of why they are non-conforming and a suggestion for improvement.
