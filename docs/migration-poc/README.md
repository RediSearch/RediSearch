# AI-assisted C-to-Rust migration POC

Draft for review. Four [case definitions](../../cases/qint/manifest.md) now exist
under `cases/`: qint, varint, geo, and inverted-index-switch. Qint snapshot
preparation and its original focused C test passed on 2026-10-08; no AI migration
attempt has run yet. Model-backed runner integration remains pending.
Human setup and review guide; not required agent context. The migration runner
supplies the [shared prompt](../migration/prompt.txt), a filled
[run definition](../migration/run-template.md), and pinned skills explicitly.
These local drafts are newer than Confluence; after branch publication, link
Confluence to the repository files instead of maintaining duplicate instructions.

Migration and POC evaluation are separate workflows. See the
[common layout and runner controls](../migration/runner-setup.md) and
[manual post-run evaluation guide](evaluation-guide.md).

## Files and responsibilities

| File | Purpose |
| --- | --- |
| [port-c-module](../../.skills/port-c-module/SKILL.md) | Extend the existing migration skill to single or connected modules; retain its name/invocation policy |
| [Compatibility checklist](../../.skills/port-c-module/references/compatibility.md) | Scope-specific language, memory, concurrency, and behavior risks |
| [Dependency-graph guidance](../../.skills/port-c-module/references/dependency-graph.md) | Scope, affected consumers, analysis gaps, and before/after comparison |
| [Validation guidance](../../.skills/port-c-module/references/validation.md) | Choose concrete checks and interpret failures at each checkpoint |
| [Migration readiness](../../.skills/migration-readiness/SKILL.md) | Requirements completeness and conditional design review |
| [Batch findings](../../.skills/batch-findings/SKILL.md) | Findings, scoped blockers, feedback, and resource reporting |
| [Report format](../../.skills/batch-findings/references/report-format.md) | Stable v2 fields/values with free-text explanations |
| [Run template](../migration/run-template.md) | Pin source/guidance and configure environment, validation, budgets, and handoff |

There is one Rust migration skill. Readiness and batch feedback are supporting
skills, not competing porting workflows. A separate per-component skill is not
required; use a short scope brief when its design or constraints need explanation.
Existing testing/review skills remain the source for their detailed procedures.

Repository design and review gates still apply. The POC must distinguish autonomous
repair/experimentation from approval to merge a real cross-cutting change.

## Remaining work, in order

1. Review these files and resolve any conflict with repository conventions.
2. Complete preflight for the four defined cases: verify historical prerequisites,
   scope, requirements, and testability. Qint has passed snapshot preparation and
   its focused C baseline; broader validation remains pending. See the
   [candidate assessment](candidates.md) for reference-review context.
3. Curate source references available at the starting SHA. Store target solutions
   and later fixes separately for manual post-run evaluation.
4. Fill a run template for the first case; verify baseline build, focused tests,
   environment access, acceptance criteria, and CI availability.
5. Use the [snapshot script](../../scripts/migration/prepare_snapshot.py) for source
   preparation. Configure and verify the remaining runner controls: sandbox/access,
   budgets, stop/resume, batch feedback, and artifact collection.
6. Run a bounded migration and freeze its result; then manually review it and
   compare with the historical reference in a separate evaluation.
7. Tune from demonstrated failures and start separately recorded runs. Optionally
   reserve #6958 while tuning on earlier cases; label reference-informed reruns
   assisted and retain the original artifacts.

## Skill behavior checks for the pilot

These are review scenarios and future evaluation cases, not claims of tests run.
Use isolated fixtures and retain the agent's decisions as evidence.

| Scenario | Expected observable behavior |
| --- | --- |
| RESP2 specified; RESP3 callers exist | Identify the omission and scope affected tasks before changing behavior |
| Invalid bytes accepted by C; proposed Rust conversion rejects/panics | Reproduce safely, propose compatible byte handling, flag any requested rejection |
| Suspected undefined numeric operation | Investigate and seek intended semantics; do not encode UB as the oracle |
| One unresolved callback lifetime; independent pure function ready | Block the callback decision, continue the independent function |
| Clear introduced unit-test failure | Repair within limits, log evidence, avoid unnecessary human escalation |
| Unrelated legacy bug discovered | Require inclusion decision and propose verification/release-note treatment |
| Stale approval and valid advice arrive in one batch | Keep stale item pending; apply advice without treating it as approval |
| Candidate changed after successful CI | Rerun affected checks; do not cite old results as final evidence |
| Budget nearing limit or exhausted | Report measured aggregate usage/forecast; save progress and stop at limit |
| Historical target solution appears in a reference | Exclude it and flag evaluation contamination |
| Graph has unindexed files or unresolved calls | Report unknown scope; do not label affected files dependency-free |
| Large cycle crosses shared infrastructure | Investigate boundaries; do not automatically include every reachable file |
| Manual evaluator sees a subset of the historical reference | Compare overlap; report omissions and distinguish narrower completion from the full objective |
| Reference findings lead to a follow-up | Preserve the ended run; create a separate assisted run, never auto-resume it |
