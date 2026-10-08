# Migration run template

The task supplies repository, starting SHA, and target component/functionality.
Use the shared objective and configured runner defaults; optional exclusions or
focus areas refine the task. The runner fills identity, access, limits, and paths.
The agent fills requirements, scope, and validation during readiness; those sections
need not be completed by a human before launch. Missing essential task or execution
inputs must be resolved, but an empty derived plan does not block starting analysis.
The runner explicitly supplies this manifest and the prompt; the agent need not
discover a README. Follow the [runner setup](runner-setup.md).
The harness must supply actual isolation, counters, timeouts, and stopping behavior;
these files are instructions, not an implemented harness.

## Identity and context manifest

| Input | Run value |
| --- | --- |
| Run ID and owner | To select |
| Repository and full starting SHA | To select; original C implementation |
| Guidance commit or snapshot digest | To pin separately from the source baseline |
| Migration target | Required component/functionality; agent proposes the dependency boundary; exclusions optional |
| Model/configuration and optional subagents | To select; no subagents assumed |
| Approved process snapshot | To pin |
| Task brief and current decisions | Shared objective by default; provide only target-specific constraints/decisions |
| Repository/subdirectory instructions | Paths at the starting SHA |
| Migration readiness skill | [.skills/migration-readiness/SKILL.md](../../.skills/migration-readiness/SKILL.md) |
| Rust migration skill | [.skills/port-c-module/SKILL.md](../../.skills/port-c-module/SKILL.md) |
| Batch findings skill and format | [.skills/batch-findings/SKILL.md](../../.skills/batch-findings/SKILL.md), format v1 |
| Prompt | [prompt.txt](prompt.txt); substitute scope, repository, SHA, and manifest path |
| Permitted reference ports/decisions | Allowlist of source examples at the starting SHA or permitted ancestors |
| Output directory | `runs/<run-id>/author/`: readiness, plan, patch, report, logs, resource snapshots |

Supply the guidance snapshot separately when replaying an older source revision.
Resolve skill-relative references within that snapshot. Check every command and
code example against the historical source; do not copy later APIs or dependencies
merely because the newer guidance mentions them. Newer guidance must not include
target-specific future answers; reference code cannot bypass the starting-SHA boundary. If instructions conflict, record
which task is affected and resolve before dependent work.

## Requirements and tasks — agent-derived

The common required outcome is maintainable, idiomatic Rust preserving required
behavior, following repository constraints, passing existing and added tests,
and meeting agreed performance/memory criteria. Sound ownership, justified unsafe
code, and no unresolved blocking quality findings are required; tests alone are
insufficient. Do not weaken tests or benchmarks to pass. Required failed or
unavailable checks leave the run incomplete.


| Requirement ID | Supported behavior and variants | Evidence / assumption | Acceptance check IDs |
| --- | --- | --- | --- |
| To fill | Include edge cases, errors, and applicable protocols | Document/code/test/decision | To fill |

Produce the scope proposal before implementation using the specified starting
SHA: boundary, consumers, exclusions, alternatives, rationale, task dependencies,
and validation plan. Revise boundaries based on evidence and record the changes;
changed required outcomes or external behavior need a scoped batch decision. If a graph is available,
record its source SHA, build configuration, tool version/hash, scoped files,
external dependencies/consumers, cycle groups, FFI boundaries, and diagnostics.
Use the matching historical checkout; current graph counts are not historical
measurements. Keep the baseline snapshot for comparison with the candidate. Name which unresolved decisions
block which tasks/checkpoints. Use approved references for routine choices. Include
performance tolerances and persistence/API compatibility where applicable.

## Environment and run controls

| Setting | Run value |
| --- | --- |
| Workspace and separate build/output directories | To select |
| OS/architecture, toolchain, dependencies/submodules, build flags | To pin |
| Available Redis/services and test data | To supply; isolated test resources |
| Network, credentials, allowed commands/actions | Explicit limits; no production access assumed |
| Shared agent security guidance | Path/version; supply before execution |
| CI workflows and invocation access | To select; missing CI access must be reported |
| Input/output token, monetary, wall-time, retry limits | To set, with units and aggregate agent accounting |
| Counter source, checkpoint interval, warning threshold | To configure in harness |
| Enforced stop and resume procedure | To implement/verify in harness; counters persist |
| Source snapshot identity and original/submodule SHAs | Record snapshot script provenance.json, export digest, and fresh local Git baseline |
| Isolation verification | Record denied future-history, network, connector, and cross-workspace access |
| Decision authority | Direct human batch replies are authorized by default; name any different decision owners and applicable repository review gates |

Credentials are supplied by the execution environment, not pasted into this file.
Reference additional models/tools only if available, authorized, and within the
run budget. A second model is an optional investigation/review cost, not a default
requirement or a substitute for executable evidence.

## Validation manifest — agent-derived

Use the [validation guidance](../../.skills/port-c-module/references/validation.md)
to fill the actual commands. Existing CI scripts should be reused locally where
possible; do not invent a second incompatible test definition.

| Check ID | Requirement/risk | Exact command or CI job + working directory | Prerequisites/configuration | Checkpoint | Timeout | Expected tests/pass criteria | Evidence path |
| --- | --- | --- | --- | --- | --- | --- | --- |
| To fill | To fill | Verified for starting SHA | To fill | baseline / repair / component / final / scheduled | To set | Non-empty selection; explicit expectation | To select |

For required performance checks, fill benchmark workloads, C baseline identity,
build/environment settings, metrics, repetitions/noise handling, and approved
regression criteria before evaluating candidate results. Reuse established limits;
otherwise require no demonstrated regression against C under comparable conditions.
The agent defines measurement/repetition and noise handling in advance. Inconclusive
results need investigation and cannot establish a pass; a tolerated regression or
unresolved tradeoff requires a scoped decision. Do not ask the user to pick every
workload or invent a percentage simply because none is supplied.

Use focused compile/reproducer/unit checks for function changes and repairs.
At component checkpoints, run all unit tests for the component and affected
consumers plus relevant flow/integration and applicable safety/performance checks.
Final acceptance requires all applicable unit and flow/integration suites, added
tests, agreed benchmarks, and required CI configurations. Define this matrix before
implementation; intermediate passes do not satisfy final acceptance. Broaden checks
for changes to shared code, ownership, FFI, concurrency, persistence, or build settings.

Classify each check as required, recommended, or optional with a reason. Required
checks must pass before their named acceptance checkpoint; scheduling a check for
later does not satisfy that gate. Record availability gaps separately from failures.
Compare equivalent C/Rust configurations. Add edge-case tests beyond baseline
coverage and use review/analysis where differential execution is insufficient.

The candidate is the starting revision plus generated code and tests. Identify it
by commit or base-plus-complete-patch digest, including new files. Keep baseline
results separate. Invalidate affected results after any subsequent change.

## End the migration attempt

Save the candidate identity, scope revisions, checks, resource usage, and pending
findings under `runs/<run-id>/author/`. End as ready for review or incomplete;
readiness is not merge approval. The runner freezes the artifacts before final
review. Ordinary automated review and repairs can occur during migration under
the same source boundary; historical-reference comparison is not part of this
workflow. Final reviewers write separately and do not mutate the saved attempt.
