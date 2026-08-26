---
name: rust-docs-guidelines
description: Guidelines for writing Rust documentation. Use this when you want to write Rust documentation.
---

# Rust Docs Guidelines

Standards to follow when writing Rust documentation.

## Guidelines

- Key concepts should be explained only once. All other documentation should use an intra-documentation link to the first explanation.
- Always use an intra-documentation link when mentioning a Rust symbol (type, function, constant, etc.).
- In a `# Safety` section, link the word `valid` to std's pointer-validity rules: write it as
  `[valid]` and put this reference definition at the end of the doc block —
  ```text
  [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
  ```
  Only where it means pointer or memory validity — `valid UTF-8`, a valid enum variant and a
  valid nul terminator keep their plain form, since std's definition says nothing about them.
  A duration clause such as "must remain valid for the lifetime of the returned iterator" does
  mean it, so link it as well; the duration it adds is orthogonal to what validity means.
  A callback parameter is the other way round: std's rules cover accesses through data
  pointers, not whether an address is callable under a given ABI, so require the signature
  to be `[ABI-compatible]` instead and define it as —
  ```text
  [ABI-compatible]: https://doc.rust-lang.org/std/primitive.fn.html#abi-compatibility
  ```
  Non-doc `// SAFETY:` comments keep the plain form too: rustdoc does not render them, so the
  brackets would stay literal text.
- Avoid referring to specific lines or line ranges, as they may change over time.
  Use line comments if the documentation needs to be attached to a specific code section inside
  a function/method body.
- Focus on why, not how.
  In particular, avoid explaining trivial implementation details in line comments.
- Refer to constants using intra-documentation links. Don't hard-code their values in the documentation of other items.
- Intra-documentation links to private items are preferable to duplication. Add `#[allow(rustdoc::private_intra_doc_links)]` where relevant.
