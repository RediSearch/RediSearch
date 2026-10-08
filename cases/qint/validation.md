# Validation plan — populate during readiness

Derive exact commands from the starting SHA, existing test/build/CI definitions,
and the approved validation skill. Do not fetch later tests or a target solution.

Record baseline build/test failures and missing prerequisites separately. Select
focused checks for repairs, component/consumer suites at checkpoints, and all
applicable existing/added tests, agreed benchmarks and required CI at final acceptance.
No final gate may be omitted just because this is a POC.

Before comparing performance, identify C baseline workloads and equivalent Rust
workloads, metrics, build flags, hardware, repetitions and noise handling. Follow
existing limits or the shared no-demonstrated-regression default. Inconclusive or
unavailable required evidence cannot establish a pass.

| Check ID | Behavior/risk | Command and directory | Prerequisites | Checkpoint | Timeout | Expected selection/pass criteria | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Agent-derived | | | | | | | |

## Setup evidence

On 2026-10-08, the original qint C test passed as a focused standalone build
against this SHA in a network-disabled GCC 14 container. This does not replace
canonical build/suite validation. The setup report records the exact command,
allocator/API flags, failures and outcome; the runner supplies its location.
Derive the remaining plan from the source as described above.
