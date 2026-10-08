---
name: batch-findings
description: Maintain migration batch findings and apply item-specific feedback, with stable IDs, scoped blockers, decisions, verification, and resource usage.
---

# Batch findings and feedback

Use [report format v1](references/report-format.md) for runs configured with this
version. Keep it stable during a run; version future changes deliberately. Text
explanations are free-form. The defined identifiers and values support later
automation; this draft does not implement a report parser or budget controller.

## Record findings as work proceeds

Add an entry when investigation, generation, testing, or review reveals a material
uncertainty, constraint violation, behavior difference, or product defect. Include
evidence, impact, alternatives, recommendation, and exactly which tasks/checkpoints
depend on resolution. Deduplicate repeated symptoms and revise the existing entry.
Log ordinary repair attempts in the run history; summarize noteworthy repairs as
Informational findings instead of asking for a decision on every failed attempt.

Severity describes whether work can advance; category describes the issue; requested
action describes the help needed. An investigation can be Informational or Blocking.
An open TODO is not automatically a blocker. Requirements may be ambiguous,
incomplete, contradictory, or omit variants. Design/code-quality blockers can be
repaired by the agent; a blocker does not always require a human reply.

Pause only work that depends on the unresolved decision. Continue tasks that
remain useful regardless of the answer, such as inspecting callers, characterizing
existing behavior, or implementing independent functionality. Avoid implementing
an unresolved option when choosing another would require substantial rework.
Pause the whole run when no useful independent work remains, an unresolved decision
prevents choosing the overall approach, or a run limit is reached. Save progress
before stopping. Deferred blockers remain blocked at their named checkpoint.

Before implementing a potentially breaking change or a change that may exceed
agreed performance/resource limits, flag affected users, inputs, outputs, and
observable behavior. Include relevant CPU, memory, latency, or throughput impact,
evidence and uncertainty, less-breaking alternatives when available, and a
recommendation. If no viable alternative is known, say so; do not invent one or
investigate indefinitely. Block only the affected decision and dependent work.
Routine choices preserving agreed behavior and repairs already within approved
scope do not require another approval.

## Apply a batch of replies

For each reply, match the finding ID and revision and record the original response.
Treat direct human replies to this run's batch report as authorized unless the run
configuration specifies otherwise. Agent suggestions and quoted comments are not
human approval. Do not infer approval from silence or elapsed time. Stale,
conflicting, or ambiguous replies stay pending; unaffected valid replies can proceed.

- **Advice:** use the guidance to investigate or refine the approach. A suggestion
  alone does not approve a behavior change. If a human reply explicitly authorizes
  a concrete change and its stated impact, record that part as option approval;
  no special keyword is required. Otherwise, keep the affected decision pending.
- **Investigation:** perform the requested bounded inquiry and update evidence.
- **Change request:** update affected requirements/design/code and revalidate.
- **Option approval:** implement the identified option within its approved scope.
- **Dismissal:** record why the concern is resolved or its disposition authorized;
  dismissal cannot turn a required failing check into a pass.
- **Deferral:** retain the issue, owner/revisit trigger when known, and blocking scope.

All severities can receive these actions when meaningful. Option approval requires
a concrete option; investigation alone does not resolve a blocker. After a valid
resolution, rerun affected checks before closing the finding and resuming dependent
checkpoints. Existing/unrelated bugs may enter migration scope only by explicit
decision; include verification and a release-note snippet or a reason none is needed.

## Track resources

Report measured parent-plus-subagent usage against configured limits. Record unknown
measurements as unavailable, never zero. Forecast likely overruns with a basis and
uncertainty; report percentage only when both measurement and limit are known.
Prefer the cheapest check that resolves the current uncertainty; use additional
models/reviews when their expected value warrants their cost. Harness counters and
limits remain authoritative across repairs, restarts, and delegation.
