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

Read the documentation of the specific Rust files and ensure they meet the following requirements:

- Key concepts should be explained only once. All other documentation should use an intra-documentation link to the first explanation.
- Always use an intra-documentation link when mentioning a Rust symbol (type, function, constant, etc.).
- Avoid referring to specific lines or line ranges, as they may change over time.
  Use line comments if the documentation needs to be attached to a specific code section inside
  a function/method body.
- Focus on why, not how.
  In particular, avoid explaining trivial implementation details in line comments.
- Refer to constants using intra-documentation links. Don't hard-code their values in the documentation of other items.

Emit a report for all non-conforming locations you find, with an explanation of why they are non-conforming and a suggestion for improvement.
