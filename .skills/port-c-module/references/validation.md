# Validation plan

Resolve exact commands from the target SHA's scripts and test skills. These
current repository entrypoints are starting points, not a guarantee that an
older revision supports the same flags:

| Check | Starting point | Working directory |
| --- | --- | --- |
| Build | `./build.sh DEBUG=1` | Repository root |
| Rust crate tests | `cargo nextest run -p <crate>` | `src/redisearch_rs` |
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

Read the starting revision's PR workflow and the jobs it calls, including their
conditions and defaults. A target such as `make lint` may cover only part of its
CI job. Record required configurations and dependencies before implementation;
keep optional or scheduled checks separate. Validate generated-file consistency
against the candidate before regeneration, including new files.

## Define acceptance before evaluating results

For each selected check record its requirement/risk, exact command or CI job,
working directory, dependencies, environment, timeout, expected test selection,
pass criteria, checkpoint, and evidence path. Verify selection actually executes
tests. A build-only run is not a test pass.
Preflight shared binaries and tool components before dependent jobs, including
inside the test sandbox. Fetch dependencies before entering an offline namespace;
record a failed prerequisite as unavailable validation, not a failed test.

- **Baseline:** run focused existing tests on the implementation being replaced before edits; retain failures.
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
Match input layouts and allocation policies in microbenchmark harnesses, or explain
the intended caller difference. Keep the C oracle's headers and libraries separate
from the migrated implementation.
Record workloads, baseline SHA, effective build settings, environment, metrics,
repetitions/noise handling and thresholds. Compare against the implementation
being replaced. Before adopting the result in today's codebase, compare against
the exact implementation and callers it would replace there. Keep that comparison
outside the historical migration run's starting-SHA knowledge boundary.

Measure relevant operations and input sizes separately. Include real callers and
representative component or end-to-end workloads before claiming a product gain;
adapters, copying and allocation can reverse an isolated function's improvement.
Report timing variability and regressions for each workload. Claim an overall
speedup only when the weighting reflects expected use. Fewer allocations do not
prove lower memory use; measure the bytes allocated or retained, as appropriate.
When accounting changes, use independent allocation or process measurements;
reported totals alone cannot establish a real memory reduction.

A completed benchmark is not a performance pass. Do not remove hard workloads or
relax thresholds after seeing results. Add checks for newly discovered risks and
rerun both implementations. Reuse established limits; otherwise require no
demonstrated regression under comparable conditions. Noise is not a regression
allowance. Investigate inconclusive results within run limits and report missing
evidence as incomplete. Request a decision only for unresolved tradeoffs, a
proposed tolerated regression, or evidence unavailable within permitted resources.

Preserve existing behavioral coverage and add tests for migration risks and
uncovered cases. Do not delete, skip, or weaken tests to make the migration pass.
If an implementation-specific test becomes obsolete, explain why and provide
equivalent behavioral coverage. Changing expected external behavior requires
an explicit decision.

On failure, inspect saved logs before rerunning checks. Classify the cause:
introduced regression, baseline failure, environment problem, flaky/timeout result,
or unknown. Repair clear, in-scope failures within retry limits. Investigate uncertain
causes; do not alter expectations merely to pass. Keep timed-out, skipped, and
unavailable checks visible.

Use the repository's test runner with its required working directory, fixtures,
environment and process isolation; direct binary execution may omit them. Verify
selected tests exist at the starting SHA. Smoke-test the final runner configuration,
including optional modules and fixtures, before launching a broad suite.
If another service interferes, isolate
the test environment and rerun baseline and candidate. Do not stop unrelated
services or weaken assertions.

When invoking underlying test tools directly, preserve the wrapper's module
arguments, runtime defaults and dependency selection. Check these before treating
a failure on both C and Rust as a legacy defect. A shared setup mistake can make
both implementations fail.

Capture exit status and logs without masking failures through shell pipelines.
Record starting SHA, candidate commit or base-plus-patch digest (including new
files), dependencies, toolchain, flags, and test selection. Rerun affected checks
after edits. Run build/test/lint sequentially within a workspace. Parallel workers,
if configured, need separate build outputs; benchmarks need uncontended resources.

Check the profiles and flags actually used in build output/configuration, not
folder names. Ensure flag changes take effect. Run correctness tests with the
required assertions enabled; benchmark both implementations with equivalent
production settings. Record measured binary hashes and reject comparisons with
mismatched settings.

Run Cargo from the Rust workspace so its `.cargo/config` is loaded. Build any C
libraries required by Rust tests or benchmarks, and pass the matching build
directory when needed. Building the production module may omit those targets.

After changing code, tests or build flags, verify that the build refreshed the
intended test target. A successful command can leave an older executable in place
when cached configuration excludes that target. Record the tested executable's
identity and confirm that the expected tests ran.
For mixed C/Rust builds, check the linked Rust archive too: invoking CMake alone
may reuse an archive that the repository's build wrapper normally rebuilds and copies.

Before changing tests that check struct sizes, alignment or field offsets,
determine whether they protect a required C/Rust interface or persisted format.
Keep those guarantees tested. If a test checks only a private implementation
detail that no longer applies, explain why and retain independent checks of the
behavior and memory accounting it was meant to protect. Do not copy the candidate's
reported values into expected results merely to pass.

A failure reproduced on the unchanged baseline is not automatically a migration
regression, but it still needs a documented disposition before acceptance. Report
executed passes and failures separately from skipped and unavailable checks.
