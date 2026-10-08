# geo migration case

Status: task defined; execution preflight pending. This is a case input, not a claim
that the historical environment or final acceptance checks have passed.

| Input | Value |
| --- | --- |
| Case | `geo` |
| Repository | RediSearch/RediSearch; trusted local checkout supplied by setup |
| Starting SHA | `353e0a45b06cbec5d927a4163eb3f919504ebdb7` |
| Objective | Shared equivalent-Rust quality standard |
| Task | [task.md](task.md) |
| Validation | [validation.md](validation.md), completed by the agent |
| Shared configuration | [run template](../../docs/migration/run-template.md) |
| Execution profile | Required from runner: model, isolation, runtime/retry/resource limits, services and artifact paths |
| Guidance | Pin the reviewed local bundle digest or commit before launch |
| Reference code | Only source at this SHA or supplied permitted ancestors |

Trusted setup prepares the exact snapshot and pins submodules. Fill run identity,
environment and enforceable controls before launching an author. Do not include
POC candidate assessments, target PRs, later fixes or evaluation reports in the
author bundle. Requirements, graph scope and exact checks are agent-derived.
