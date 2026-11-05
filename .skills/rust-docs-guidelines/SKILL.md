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
  Only where it means pointer or memory validity — `valid UTF-8`, a valid enum variant, a valid
  nul terminator and a by-value struct whose fields must agree keep their plain form, since
  std's definition says nothing about them.
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
  std's definition leaves two requirements out, so state them where they apply. It counts every
  pointer, null included, as valid for a zero-sized access, so a handle to an opaque type that
  Rust never dereferences (a `RedisModuleCtx`, say) must also be required to be non-null. It
  does not include alignment either, so a pointer the function dereferences, or turns into a
  reference or a slice, must also be required to be properly aligned.
- Avoid referring to specific lines or line ranges, as they may change over time.
  Use line comments if the documentation needs to be attached to a specific code section inside
  a function/method body.
- Focus on why, not how.
  In particular, avoid explaining trivial implementation details in line comments.
- A `// SAFETY:` comment justifies the operation it precedes; it does not restate the
  precondition. Say why it holds *here*:
  ```rust
  // SAFETY: caller guarantees (1).                                     // restates
  // SAFETY: 1. `filter` came from `NewGeoFilter`, which allocated it.  // justifies
  ```
  Name the number of each pre-condition it discharges, as
  [`/rust-review`](../rust-review/SKILL.md) § 3b requires. Write one `unsafe` block, and one
  comment, per unsafe operation: `clippy::multiple_unsafe_ops_per_block` rejects a block holding
  several.
- Document what an item guarantees today, not its history: leave out how the C did it, and
  narration ("This ensures…", "Now we…"). When the code deliberately differs from what a reader
  would expect, one sentence saying why is enough.
- State facts plainly. Do not hedge ("should", "generally", "it is worth noting"), and do not
  list caveats about what the docs do not imply.
- Refer to constants using intra-documentation links. Don't hard-code their values in the documentation of other items.
- Intra-documentation links to private items are preferable to duplication. Add `#[allow(rustdoc::private_intra_doc_links)]` where relevant.
