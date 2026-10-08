# Batch report v1

Required field names and allowed values below form the format contract. Store a
human-readable Markdown report for the POC; machine validation can be added after
the workflow is exercised. Use `unavailable` for missing measurements and `none`
for an intentionally empty set. Do not silently omit required fields.

## Run record

| Field | Value |
| --- | --- |
| format_version | `1` |
| run_id | Stable run identifier |
| starting_sha | Full C baseline commit |
| candidate | Commit, or base SHA plus full patch digest including new files |
| guidance_revision | Commit/digest of skills, prompt, and configuration |
| status | `running`, `waiting`, `incomplete`, `ready-for-review` |
| evidence | Paths/links to baseline, checks, repair history, and review results |
| resources | Timestamped usage table below |
| remaining_work | Tasks and estimated effort/cost with uncertainty |

Resource rows: `metric`, `used`, `limit`, `percent_used`, `measurement_source`,
`forecast`, `forecast_basis`. Metrics: input/output tokens, monetary cost with
currency, wall time, and retries. Identify included agents, model/configuration,
and whether cached-token accounting affects reported cost. Keep snapshots for
later comparisons; redact secrets and avoid logging credentials.

## Finding record

| Field | Value |
| --- | --- |
| id / revision | Stable ID such as `F-001`; increasing integer revision |
| severity | `Informational`, `Consultation`, `Blocking` |
| category | `Requirements`, `Design`, `Implementation`, `Compatibility`, `Validation`, `Existing/unrelated defect` |
| status | `open`, `in-progress`, `resolved`, `dismissed`, `deferred` |
| summary | The question or defect |
| evidence | Reproducer, source, or check result; separate observations from assumptions |
| impact | Affected users, inputs, outputs, observable behavior, relevant CPU/memory/latency/throughput impact, and uncertainty |
| affected_tasks | Tasks/checkpoints affected; explicitly name what is blocked |
| requested_action | `Advice`, `Investigation`, `Change request`, `Option approval`, `Dismissal`, `Deferral`, or `none` for awareness only |
| options / recommendation | Available options with compatibility/cost tradeoffs; state when none is known rather than inventing alternatives |
| resolution | Decision, authority, scope, reason, and verification; `pending` while unresolved |
| release_note | Proposed snippet for included product fixes/changes; otherwise reason or `pending` |

## Reply record

Use `finding_id`, `finding_revision`, `action`, `message`, `option_id` (when
approving an option), and `author`. Record application outcome and resulting
finding revision. A batch may address only some findings; omitted items retain
their state. When one decision changes shared requirements, identify and reopen
affected findings rather than applying incompatible replies mechanically.

Example: `F-003`, revision `2`, `Option approval`, option `preserve-bytes`:
"Keep accepting byte strings; UTF-8 rejection is outside this migration."
The agent records the decision and verifies invalid-byte tests before resolution.
