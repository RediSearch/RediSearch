# POC evaluation guide — after migration

This workflow applies only to selected historical POC cases. It starts after the
migration has ended and the runner has frozen its outputs. Initial final review
is manual; a later automated evaluator must use a fresh context separate from
the migration. Normal migrations do not require a historical reference PR.

The evaluator holds the target PR, review discussion, later fixes, and optional
newer applicable tests. The authoring agent must not inspect these through Git
history, branches, network, or reference files. Enforce isolation in the harness;
a prompt alone does not prevent answer leakage.

Evaluate behavior, safety, repository constraints, integration, test adequacy,
performance, resource use, and required human decisions against predeclared criteria.
Use evidence-backed pass/fail/incomplete findings; do not require a universal
per-stage numerical score. Record costs of additional reviews/models and whether
they found actionable defects.

### Reference-comparison checkpoint

Save the candidate identity and scope proposal before comparison. Have the manual
reviewer evaluate the saved attempt; if automated later, use an independent
evaluator in a fresh context. Another model is optional. Supply the evaluator with
the task/approved decisions, baseline, candidate, scope revisions, validation evidence,
and target PR plus applicable follow-up fixes. Keep this reference material outside
the author's context and preserve the original blind-run artifacts.

Map both implementations to requirements and behavior, accounting for renames, moved
code, prerequisites, and approved scope differences. Compare their overlapping scope;
report candidate-only and reference-only work separately. Do not penalize justified
implementation differences or require textual similarity.

A subset may satisfy an approved narrower task but cannot establish completion of
the original broader objective. Unapproved missing requirements remain gaps. For a
superset, check scope authorization and validate the extra work independently; a
reference covering less code cannot establish correctness of that extra work.

Run relevant checks against the candidate. The reference PR is evidence, not an
unquestionable correctness oracle. Use newer tests only when they express historical
requirements; report them separately. Flag later requirement changes explicitly.

The evaluation report records:

| Requirement/behavior | Candidate implementation | Reference implementation | Scope relation and decision | Check/evidence | Verdict or finding ID |
| --- | --- | --- | --- | --- | --- |
| To fill | Path/symbol or absent | Path/symbol or absent | overlap / candidate-only / reference-only; approved difference or gap | Candidate check result | pass / fail / incomplete |

Include exact revisions, limitations, and remaining decisions. Write the report to
`runs/<run-id>/evaluation/`; do not write into private reference inputs or modify
`runs/<run-id>/author/`. Evaluation never automatically resumes migration. Any
follow-up is a new run ID linked to this attempt. If it receives reference findings,
label it an assisted iteration and do not claim starting-SHA-only knowledge.

For each iteration, record the observed failure, guidance change, and rerun results.
Replaying a disclosed case is a regression check, not a fresh blind evaluation.
Retain a held-out case and repeat earlier cases after skill changes to detect
regressions. Promotion criteria and cost limits must be agreed before declaring
this capability ready for broader use.

## Inputs and access

Private reference inputs live in `evaluation-cases/<case>/` in reviewer-only
storage, not in the migration sandbox. They contain the target PR, applicable
fixes, and hidden checks. Preserve inputs as read-only. Directory names alone
are not isolation; the runner controls mounts, tools, credentials, and network.

The case brief is sanitized before migration. The author receives no target PR
identifier, reference implementation, or evaluation report. The reviewer receives
the original SHA mapping, task/decisions, frozen candidate, scope proposal/revisions,
validation results, and resource usage. Compare against applicable requirements,
not merely the choices or test deletions made by the historical PR.
