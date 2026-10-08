# POC candidates — evaluator preparation

This document includes target PR references and a known compatibility change.
Keep it outside the historical replay author's context; supply a sanitized task
brief and permitted reference list instead. These candidates are not yet approved
or validated run configurations.

| Case | Purpose | Target reference |
| --- | --- | --- |
| qint | Small warm-up: encoding, edge cases, tests, benchmarks; no C caller switch | [#6033](https://github.com/RediSearch/RediSearch/pull/6033) |
| varint | Implementation, C integration, and refinement | [#6287](https://github.com/RediSearch/RediSearch/pull/6287), [#6308](https://github.com/RediSearch/RediSearch/pull/6308), [#6346](https://github.com/RediSearch/RediSearch/pull/6346) |
| Geo | Parsing, algorithms, FFI, errors, flow tests | [#9340](https://github.com/RediSearch/RediSearch/pull/9340), [#9341](https://github.com/RediSearch/RediSearch/pull/9341), [#9342](https://github.com/RediSearch/RediSearch/pull/9342) |
| Inverted-index switch | Broader integration across GC, numeric indexes, iterators, FFI, build and test infrastructure | [#6958](https://github.com/RediSearch/RediSearch/pull/6958) |

## Fourth case: inverted-index switch

PR #6958 merged as `a8e9b3c47adf68dc88adbf715821b990030ae251`.
Its first parent is `718477883ca3699cce9944092ae94a217789696f`: a proposed replay
baseline, subject to checking prerequisites and a reproducible baseline build.
The merged change touches 46 files. File count is not dependency-graph size.

Earlier PRs already implemented the Rust inverted index, reader, FFI, and GC.
For the first replay, keep those prerequisites and ask the agent to perform the
integration switch. Do not claim this exercises generating the whole subsystem
from C. A combined implementation-and-integration replay would require a separately
curated earlier baseline and a substantially larger budget.

Evaluator focus:

- Caller/API changes across GC, numeric indexes, iterators, and debug output.
- Ownership and GC-delta cleanup, iterator behavior, and memory accounting.
- Whether changed or removed tests lose contract coverage; replacing assertions
  on old internals requires equivalent behavior checks, not automatic deletion.
- Numeric-compression configuration: the PR explicitly states that changing it
  now affects new indexes; existing indexes require recreation. Check whether the
  agent detects the difference, reports impact and less-breaking options, and
  waits for the scoped decision before adopting a behavior change.
- The PR notes follow-up memory work. Historical merge is not a blanket quality
  oracle; agree performance/memory criteria and inspect later fixes independently.

## Graph evidence and remaining preparation

Exploratory snapshot at `3c021835dab3282aa97ddbdf4a61a72a859df559`:

| Current implementation plus matching FFI crate | Scoped source files | Direct external dependencies | Direct external consumers |
| --- | ---: | ---: | ---: |
| qint | 1 | 0 | 7 |
| varint | 6 | 3 | 13 |
| Geo | 8 | 1 | 6 |
| inverted_index | 30 | 11 | 50 |

Counts use first-party implementation files, excluding headers and tests, and
count distinct external files rather than symbols. C parsing was complete for
194 translation units; Rust syntax/resolution gaps remain. The inverted-index
scope intersects cycle groups of 3, 9, and 279 files; the largest includes shared
infrastructure and is not a proposed mandatory migration scope.

These measurements support a broader-connectivity hypothesis for #6958; they
are not historical measurements. Before each replay:

1. Confirm the intended scope and target PR set with an owner.
2. Verify baseline and prerequisites; do not blindly combine PRs landed at
   different times onto the first implementation PR's parent.
3. Build matching headers/database and record historical graph metrics, including
   diagnostics and transitive consumers. Manually inspect ownership/callback edges.
4. Pin the environment and concrete tests, baseline failures, budgets, and acceptance
   criteria. Separate permitted references from evaluator-only solutions/fixes.

All four are fixed-boundary historical replays. Add a future graph-selected scope
if evaluating autonomous choice of a new migration boundary is required.
