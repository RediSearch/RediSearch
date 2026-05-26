---
name: docs-guidelines
description: General documentation guidelines that apply across all languages. Use this when writing or reviewing comments, docstrings, or doc headers in any code.
---

# Documentation Guidelines

General guidelines for writing code documentation. Language-specific skills (e.g. [`rust-docs-guidelines`](../rust-docs-guidelines/SKILL.md)) layer on top of these.

## Guidelines

- **Document every function, struct, field of structs, enum.** Make sure your code additions are well commented, with concise explanations for a new reader.
- **Prefer code-enforced invariants over prose.** If an assertion, type, enum, or test can express the constraint, add it. Add a comment where a non-trivial statement that can help the reader is needed on top of the above. Comments can drift; code mostly doesn't.
- **State each fact in exactly one place.** The owner is the definition, the interface, or the implementation — whichever is canonical. Other locations reference it by name, not by restating it.
- **Focus on why, not how.** Don't restate what the code plainly does. Document non-obvious choices, invariants that are hard to infer, and constraints a maintainer would otherwise miss.
- **Do not document obvious behavior, callee implementation details, or duplicate the same detail across call sites.**

## Placement model

Each fact belongs to exactly one of these layers — pick the one that owns it and avoid restating it elsewhere:

- **Call sites** — non-trivial local intent, assumptions, sequencing constraints, or edge cases specific to this usage.
- **Definitions and interfaces** — the contract: inputs, outputs, errors, side effects, invariants.
- **Implementations** — non-obvious internal choices, tradeoffs, workarounds.
- **READMEs** — module-level context and architectural decisions.
