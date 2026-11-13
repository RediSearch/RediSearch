# VecSim repository migration

VecSim is maintained in `src/vecsim` as part of RediSearch. Cross-component
changes use one Search PR and ship with the Search release. The C library
boundary, exported names, algorithms, and persistence behavior are unchanged.

## Provenance and landing

The import follows the VecSim commits pinned by Search:

- Repository: <https://github.com/RedisAI/VectorSimilarity>
- Initial VecSim import: `001f0b0df1cac1fff87f502548b1c205f061f242`
- Initial Search parent: `8751678feddc78eb5ca7651883234f6d10158f00`
- Updated VecSim pin: `eefc4e7a0feb608f3e2e50e7e5050d1f44bdec9e`
- Updated Search parent: `33d90342bc9c601d904fb58446f8bda4eaba5011`
- SVS commit: `919925a56218da10ee4ae0cb8a2124ee96fdf3fd`

The import merge retains the original VecSim commit graph as its second-parent
ancestry. It does not rewrite old commit IDs. The source tree is placed under
`src/vecsim`; historical commits retain their original paths. Original commit
IDs remain usable with `git show`, independently of rename detection in blame.

**Land this branch with a merge that preserves its commits. Do not squash or
rebase the import.** On 2026-11-13, master's effective rules allow only squash
merges and its merge queue uses `SQUASH`. Landing therefore requires a
maintainer-approved exception that permits a history-preserving merge; the
normal queue path cannot preserve this import's ancestry. No repository rules
are changed by this migration branch. Recheck the effective rules before
submission. After landing, the following command must succeed:

```sh
git merge-base --is-ancestor eefc4e7a0feb608f3e2e50e7e5050d1f44bdec9e origin/master
```

Importing commit ancestry does not migrate GitHub issues, reviews, releases,
or tag refs. Keep the original repository and its historical tags available.

## Dependencies and development

SVS remains a third-party submodule, registered in the root `.gitmodules` at
`deps/ScalableVectorSearch`. Its pin is unchanged. Other Search
submodules are unchanged. A fresh checkout uses the normal command:

```sh
git submodule update --init --recursive
make vecsim-tests
make vecsim-tests SAN=address
```

The VecSim unit runner accepts `CTEST_ARGS` for filtering and fails if no tests
are selected. Benchmarks are built separately with
`make vecsim-benchmarks`; ordinary unit builds do not build benchmarks.

Existing clones may retain the former `deps/VectorSimilarity` checkout when
switching branches. Preserve any local changes and commits before cleaning up
that checkout. Use a separate checkout when working on both layouts; do not
delete `.git/modules` or force-clean an existing submodule to switch layouts.

## CI and tests

VecSim units run unconditionally with Search units in PR and merge-queue jobs.
The shared PR unit pipelines run even for drafts and documentation changes;
master MQ retains Search's existing basic-test lane, including VecSim units.
This migration does not expand MQ coverage/sanitizer lanes or change its existing
QUICK and flaky-test filtering. Units also inherit Search's nightly OS and
architecture matrix. A separate
component build keeps its test-only compile definitions out of the shipped
module, while using Search's provisioning, compilers, and GoogleTest dependency.
ASan and alignment-sanitizer variants build SVS from source, avoiding an
uninstrumented prebuilt SVS dependency in component tests. Source-built SVS
does not contain the prebuilt LVQ implementations; their validation remains in
the normal prebuilt-SVS configuration, not in these sanitizer checks.

VecSim unit coverage is uploaded with Search unit coverage to Codecov and shown
in the job summary. Search flow coverage still excludes VecSim instrumentation:
the existing build explicitly strips coverage flags to avoid SVS flow-test
timeouts. Search ASan integration tests do instrument source-built VecSim.
Enabling flow coverage requires measuring runtime and adjusting timeouts; unit
coverage does not imply that product flows cover the same lines.

Formatting and C/C++ CodeQL remain mandatory PR/MQ checks. Python bindings are
retained as developer tooling, with no Python package CI. Search's installers
replace the old VecSim OS installers; the component helpers use Search's sccache
when available.

Search's existing spelling exclusion for the imported source and tests follows
them to the new directory, avoiding unrelated edits to the imported baseline.
VecSim documentation remains checked; C++ formatting and CodeQL apply to the
component. A spelling cleanup can remove the baseline exclusions separately.

The standalone VecSim Python flow suite and auxiliary Redis test module are
retired. Search's product flow tests remain in place. Python bindings remain
available for existing benchmark and development consumers, but their former
standalone flow suite is no longer maintained. This migration does not claim
assertion-by-assertion equivalence between the retired and Search suites.

## Maintenance branches and Enterprise

Existing maintained Search branches keep their current VecSim submodule layout.
Do not backport the import or its CI restructuring to those branches.
Fixes affecting them require a patch in the relevant VecSim maintenance branch,
followed by a Search submodule-pin update and branch-specific product tests.
Link both backports to the originating Search change. Changes that do not touch
or depend on the relocated component can use the ordinary backport workflow.

RSE maintains its existing VecSim directory independently. This migration does
not reorganize it. Its Search submodule update must be validated against RSE's
build and disk-vector tests, with any path adaptations made in the RSE change.

Before retiring feature development in the old repository, owners must inventory
open PRs, supported branch pins, consumers, release automation, and security work.
Retain maintenance CI while supported branches depend on it. Archive the old
repository only after those branches reach end of support; do not delete it.

## Release checks

- Confirm source equivalence for the imported production code and dependency pins.
- Validate clean-checkout Search and VecSim builds, unit tests, and Search flow tests.
- Validate the PR and merge-queue jobs on the final revision, including failure propagation.
- Validate RSE adoption, supported architectures, and SVS configurations.
- Compare controlled performance and recall baselines before release.
- Review the new VecSim coverage baseline: importing previously excluded source
  changes Search's coverage denominator and patch report. Keep ordinary coverage
  thresholds unchanged while reviewing that one-time baseline change.
- Rehearse a legacy VecSim fix plus Search pin-update backport.
- Verify preserved ancestry after landing.

Repository administration, RSE adoption, release benchmarking, and legacy
maintenance changes are separate from this source import; they must not be
inferred from a successful local build.

## Repository layout and benchmarks

- `src/vecsim/`: library, component CMake, and developer Python bindings.
- `tests/cpptests/vecsim/`: component units and shared test utilities.
- `tests/cpptests/micro-benchmarks/vecsim/`: C++ microbenchmarks.
- `tests/benchmarks/vecsim/`: dataset manifests, generators, recall tools, and runner.
- `docs/vecsim/`: benchmark documentation.
- `sbin/vecsim.mk` and `sbin/format-vecsim`: component build and formatting helpers.
- `deps/ScalableVectorSearch/`: unchanged SVS pin.

Search C microbenchmark jobs run the default VecSim suite on their existing x86
and ARM runners, export results through the same performance backend, and use
the same baseline comparison. Legacy `benchmarks-*` and `bm-*` selectors launch
selected suites through that infrastructure. Full-suite runs remain opt-in;
`benchmarks-all` takes precedence over other selectors. Unknown selectors fail.
Local commands are `make vecsim-benchmark-data BM_FILTER=<suite>` followed by
`make vecsim-benchmarks BM_FILTER=<suite>`. Python-based dataset regeneration is
retained; install the developer package from `src/vecsim` before running those
tools from the Search root.

Historical results remain under the VectorSimilarity repository identity.
New Search results need a Search baseline before comparisons are meaningful;
this branch does not rewrite backend records or recreate old GitHub labels.
Benchmark execution also depends on the existing hosted dataset URLs and Search
performance credentials. Validate both in CI before declaring migration complete.

The optional `USE_SVS=OFF` build had unconditional SVS header and thread-pool
references in the imported revision. These are now guarded; the non-SVS API
reports zero shared memory and retains the factory's unsupported-index behavior.
The default Search build continues to use SVS.

VecSim retains its clang-format 18 style. Its error-message skill is now in
`.skills/vecsim-error-messages`, with component commands integrated into Search's
build, unit-test, verification, and lint skills. Python metadata and bundled
license texts match Search's tri-license; SVS source and binary licenses remain
separate, as recorded in `THIRD_PARTY_NOTICES.md`.
