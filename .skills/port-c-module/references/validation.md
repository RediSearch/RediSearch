# Validation plan

Resolve exact commands from the target SHA's scripts and test skills. These
current repository entrypoints are starting points, not a guarantee that an
older revision supports the same flags:

| Check | Starting point | Working directory |
| --- | --- | --- |
| Build | `./build.sh DEBUG=1` | Repository root |
| Rust crate tests | `cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p <crate>` | Repository root |
| C/C++ tests | `./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1`; see [filters](../../run-c-unit-tests/SKILL.md) | Repository root |
| Flow tests | `./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST='<file>:<function>'`; see [runner](../../run-python-tests/SKILL.md) | Repository root |
| Formatting and lint | `make fmt CHECK=1`, then `make lint` | Repository root |
| FFI headers | `make generate-rust-headers` when supported and needed | Repository root |
| Performance | [Rust benchmarks](../../run-rust-benchmarks/SKILL.md) and applicable [macro benchmarks](../../run-macro-benchmarks/SKILL.md) | As specified by the selected skill |

## Reuse the existing skills

Select relevant skills from the approved guidance bundle; do not load every skill
for every run. Check commands and available test targets against the starting SHA.

| Purpose | Guidance |
| --- | --- |
| Review generated Rust | [rust-review](../../rust-review/SKILL.md) |
| Review changed C interfaces | [code-review](../../code-review/SKILL.md) |
| Write Rust tests | [write-rust-tests](../../write-rust-tests/SKILL.md), [rust-tests-guidelines](../../rust-tests-guidelines/SKILL.md) |
| Write flow tests | [write-flow-tests](../../write-flow-tests/SKILL.md) |
| Run Rust tests | [run-rust-tests](../../run-rust-tests/SKILL.md) |
| Find coverage gaps | [check-rust-coverage](../../check-rust-coverage/SKILL.md), [check-flow-coverage](../../check-flow-coverage/SKILL.md) |

C/flow test runners and micro/macro benchmarks are linked in the command table
above. Review local files or diffs only during migration; PR-fetching, future-source
lookup, and external posting modes are outside this run. All workers keep the
starting-SHA boundary. Final review remains separate from automated repair.

## Agent-owned selection

Derive the test/benchmark plan from the starting revision's code, consumers, tests,
build scripts, CI, and permitted skills. Users provide the target, not a complete
list of checks. Select focused intermediate checks and add missing edge-case tests
or benchmark workloads. All applicable existing/added tests and required CI gates
must pass at final acceptance; selection does not authorize dropping mandatory gates.
Record why a check is inapplicable; inability to run it is an evidence gap, not an
exclusion. Never relax the final matrix merely to fit the remaining budget.

## Define acceptance before evaluating results

For each selected check record its requirement/risk, exact command or CI job,
working directory, dependencies, environment, timeout, expected test selection,
pass criteria, checkpoint, and evidence path. Verify selection actually executes
tests. A build-only run is not a test pass.

- **Baseline:** run focused existing tests on C before edits; retain failures.
- **Function change or repair:** compile affected code; run the reproducer,
  affected unit tests, and relevant new tests.
- **Component checkpoint:** run all unit tests for the migrated component and
  affected consumers, relevant flow/integration tests, and applicable differential,
  safety, and performance checks.
- **Final candidate:** run all applicable unit and flow/integration suites, added
  tests, agreed benchmarks, and required CI configurations before merge. A local
  POC without required CI remains an experimental candidate, not merge-ready.
- **Scheduled:** longer fuzzing, broader configurations, or soak tests may run
  nightly/weekly if they are not required acceptance evidence for this change.
  A flagged deployment risk may require an isolated beta/canary before acceptance.

Define the final validation matrix before implementation, with justified exclusions
and unavailable checks recorded. Intermediate success does not satisfy final
acceptance. Broaden checks when shared code, ownership, FFI, concurrency,
persistence, or build settings change; final evidence must apply to the delivered
candidate. "All" means all applicable required suites and agreed benchmarks, not
every unrelated workload or platform available in the repository.

Use CI's underlying commands locally when prerequisites are available. The harness
chooses checks/checkpoints, drives bounded repair, and aggregates results. CI owns
reproducible jobs and enforcement of required checks. This skill provides neither
a scheduler nor a sandbox nor budget enforcement.

Set explicit acceptance criteria before generation: named behavior cases and
variants, unchanged supported outputs, justified coverage gaps, applicable safety
checks, and agreed latency/throughput/memory tolerances. Use coverage to find gaps,
not as a sole quality score. Where valuable, seed a representative defect in a
disposable copy to check that selected tests detect it; do not modify the candidate
or baseline evidence. Benchmark equivalent workloads under comparable conditions.
Record named benchmark workloads, C baseline, build flags, hardware/environment,
metrics, repetitions/noise handling, and agreed latency/throughput/memory thresholds.
A benchmark completing successfully does not itself establish a performance pass.
Do not weaken workloads or move thresholds after seeing results. Reuse established
limits; absent those, require no demonstrated regression against C under comparable
conditions. Define repetitions and noise handling before candidate comparison;
measurement noise is not an allowed regression budget. Investigate inconclusive
results within run limits and report incomplete if required evidence remains missing.
Request a decision only for an unresolved tradeoff, a proposed tolerated regression,
or evidence that cannot be obtained within permitted resources.

Preserve existing behavioral coverage and add tests for migration risks and
uncovered cases. Do not delete, skip, or weaken tests to make the migration pass.
If an implementation-specific test becomes obsolete, explain why and provide
equivalent behavioral coverage. Changing expected external behavior requires
an explicit decision.

On failure, inspect saved logs once and classify: introduced regression, baseline
failure, environment problem, flaky/timeout result, or unknown. Repair clear
in-scope failures within retry limits. Investigate uncertain causes; do not alter
expectations merely to pass. Keep timed-out, skipped, and unavailable checks visible.

Capture exit status and logs without masking failures through shell pipelines.
Record starting SHA, candidate commit or base-plus-patch digest (including new
files), dependencies, toolchain, flags, and test selection. Rerun affected checks
after edits. Run build/test/lint sequentially within a workspace. Parallel workers,
if configured, need separate build outputs; benchmarks need uncontended resources.
