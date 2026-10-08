---
name: port-c-module
description: Plan and implement a RediSearch C-to-Rust migration of one module or a connected set of modules, preserving behavior and validating integration.
disable-model-invocation: true
---

# Port C code to Rust

Accept a module name or an explicit feature/dependency scope in `$ARGUMENTS`.
Keep this entrypoint for both single-module ports and broader migrations.
Create crates and FFI only where the chosen boundary requires them.

## Establish the scope

Use [migration-readiness](../migration-readiness/SKILL.md) before dependent
implementation. Read the supplied case manifest using the shared
[run template](../../docs/migration/run-template.md). The runner supplies the
[prompt](../../docs/migration/prompt.txt), pinned skills, and case inputs explicitly.
The [runner setup and directory layout](../../docs/migration/runner-setup.md)
define common operating rules; do not copy them into each case.

Read case inputs from `cases/<case>/` and write only migration artifacts to
`runs/<run-id>/author/`. Final review is separate. Private `evaluation-cases/`
inputs and `runs/<run-id>/evaluation/` outputs are not migration context and
must not be mounted for the author. Follow the supplied locations if the runner
uses different physical paths.

Read the original C source and headers at the chosen starting SHA, its callers,
callbacks, shared structs, global state, Redis API interactions, and tests.
Use existing Rust crates where their ownership and API assumptions fit. A static
dependency graph assists discovery; verify indirect calls and lifecycle edges.
Treat the starting SHA as the source knowledge boundary. Do not read future
revisions, other checkouts, external task implementations, or later reference
code. Local source and permitted ancestor history remain usable. Newer general
skills are separately pinned and must not reveal target-specific future answers.
Report accidental exposure. Do not substitute current `master` for the baseline. Use the
[dependency-graph guidance](references/dependency-graph.md) when analysis is
available to compare scopes, identify consumers, and check integration.

Choose a boundary by shared ownership and observable behavior, not file count.
Compare a single connected migration with smaller changes using temporary FFI,
review/test cost, integration risk, and likely rework. Record one short rationale
and a task dependency map. Large migrations are valid; partition review by
behavior and invariants even when delivery is one PR. Do not assume an FFI
boundary prevents optimization or cross-language LTO is enabled: inspect the
actual build only when relevant to the performance decision.

## Resolve compatibility risks

Apply the [compatibility checklist](references/compatibility.md), recording
scope-specific risks and checks. Preserve supported behavior, including accepted
inputs and error paths. Existing C test coverage is a baseline, not a limit.
Compare new edge cases against C where safe and meaningful; observed behavior
is evidence, not proof of an intended contract or absence of undefined behavior.

Use [batch-findings](../batch-findings/SKILL.md) for unresolved requirements,
behavior changes, and discovered defects. Repair clear migration mistakes
autonomously. Do not silently include unrelated fixes or recreate undefined
operations. Continue useful independent work while a decision is pending.

## Implement using repository patterns

- Follow the target revision's `AGENTS.md`, applicable subdirectory guides,
  [Rust docs](../rust-docs-guidelines/SKILL.md), and
  [Rust tests](../rust-tests-guidelines/SKILL.md). Record conflicts with a
  separately supplied skill snapshot rather than silently overriding constraints.
- Inspect approved reference ports and their decisions. `src/redisearch_rs/trie_rs/`
  and `c_entrypoint/trie_ffi/` are existing examples; use them only when available,
  relevant, and present at the starting SHA or a permitted ancestor.
- Reuse or create Rust crates under `src/redisearch_rs/` according to ownership
  and API boundaries. Keep algorithms testable independently of Redis when useful.
- Account for existing C/C++ test assertions. Keep boundary/integration tests;
  add equivalent Rust unit tests where useful rather than mechanically duplicating
  the entire suite. Use property tests for meaningful invariants and microbenchmarks
  for performance-sensitive paths.
- Where C callers remain, use the repository's `c_entrypoint/` and generated-header
  conventions. Verify calling convention, representation, ownership transfer,
  allocator pairing, error mapping, null handling, and panic boundaries.
  Document each unsafe operation's actual safety argument.
- Update callers and regenerate affected headers with the target revision's
  supported command. Remove replaced C sources, headers, and build entries only
  after accounting for their consumers. A larger port need not add intermediate FFI.

## Validate and hand off

Build a concrete plan using [validation guidance](references/validation.md).
Run focused checks while repairing, broader checks at component checkpoints,
and required CI against the final candidate before acceptance. Record exact
revision/patch identity and explain differences from the C baseline.

Deliver the patch, short design rationale, behavior-to-test mapping, evidence,
and remaining findings. Prepare for independent review; obey applicable repository
review gates. Freeze the candidate and end the attempt as ready for review or
incomplete. Reference-PR comparison is a separate, optional POC workflow after
migration concludes; it is never an automatic repair step or merge authorization.
