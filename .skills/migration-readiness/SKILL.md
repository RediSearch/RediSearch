---
name: migration-readiness
description: Assess requirements and consequential design decisions before a code migration, identify missing behavior cases, and scope blockers without requiring a design document for routine work.
---

# Migration readiness

Produce a short evidence-backed readiness record before dependent implementation.
The input can be a task brief plus existing code, tests, and approved references;
a formal requirements document is not mandatory.

## Minimize required input

Use the shared migration objective: equivalent Rust meeting repository quality,
test, safety, and performance criteria. Require only the source/start SHA and
migration target from the task; use runner defaults for execution controls.
Derive the requirement map and validation plan from permitted repository evidence.
Do not ask humans to enumerate discoverable tests, benchmarks, callers, or routine
design choices. Batch only essential missing inputs and unresolved consequential
decisions after investigation. Keep independent work moving.

## Establish the contract

1. Pin the repository/start SHA and supplied guidance version. Identify in-scope
   behavior, exclusions, external consumers, and permitted references. Reference
   code must exist at the starting SHA or permitted ancestors; do not inspect
   later revisions to resolve ambiguities. Flag accidental future-source exposure.
2. Read requirements alongside implementation, callers, and tests. Label each
   claim as documented, observed, approved decision, or assumption. Evidence from
   legacy code does not automatically establish what the product should promise.
3. Check completeness: valid/invalid inputs, errors, boundary cases, lifecycle,
   persistence, concurrency, performance constraints, and relevant platform/protocol
   variants. Mentioning RESP2 does not establish that RESP3 is out of scope.
4. Map each requirement to a concrete acceptance check and identify gaps. A vague
   goal such as "good coverage" is insufficient; name behavior cases and checks.
5. Resolve routine questions from repository guidance and comparable approved
   migrations. Flag only unresolved material decisions; record evidence and options.

## Propose the scope before implementation

Analyze the specified starting SHA with matching source, generated headers, and
build configuration. Propose the boundary, affected consumers, exclusions,
alternatives, and validation plan. Use dependency analysis when available and
record gaps; graph isolation does not prove safety.

The requested scope defines the objective; the initial boundary is a proposal.
Adjust implementation boundaries when evidence supports it, recording why and
updating affected tasks and checks. Changes to required outcomes or externally
observable behavior need a scoped batch decision. A boundary revision must not
silently drop requirements or expand product scope. Preserve proposal revisions
so the evaluator can distinguish justified changes from omissions.

## Assess design proportionally

For an established local pattern, record the chosen boundary, ownership, and a
short rationale. Review consequential choices such as shared data representation,
FFI ownership, concurrency, persistence, or changed accepted inputs before dependent
implementation. A dedicated design document is conditional; follow applicable
repository review rules for large or cross-cutting changes. This skill does not
waive those rules or introduce a universal extra human approval gate.

## Report readiness by task

Use [batch-findings](../batch-findings/SKILL.md) for external awareness or decisions.
Keep routine readiness tasks and repairable blockers in the work plan.
Missing detail blocks only the tasks that need it. Clear repairable code/test
failures can return to the agent; final acceptance still requires passing checks.
Continue work that remains useful under all plausible answers. Pause the run
when no such work remains or a global decision makes further work speculative.

Return:

- Requirement → evidence → acceptance check, including uncovered variants.
- Scope/dependency map and consequential design choices.
- Ready/blocked tasks in the work plan; link finding IDs only where external
  awareness or decisions are needed, with options and recommendations.
- Unverified assumptions and the next check that could resolve each.

Double-check the record against callers, variants, and test selection. Use an
independent review only when configured or the uncertainty/risk justifies its cost.
Do not invent a numerical readiness score or treat consensus as evidence.
