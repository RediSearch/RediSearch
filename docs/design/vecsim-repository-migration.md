# VecSim repository migration

VecSim is maintained in `src/vecsim` as part of RediSearch. Cross-component
changes use one Search PR and ship with the Search release. The C library
boundary, exported names, algorithms, and persistence behavior are unchanged.

## Provenance and landing

The import starts from the VecSim commit already pinned by Search:

- Repository: <https://github.com/RedisAI/VectorSimilarity>
- VecSim commit: `001f0b0df1cac1fff87f502548b1c205f061f242`
- Search parent: `8751678feddc78eb5ca7651883234f6d10158f00`
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
git merge-base --is-ancestor 001f0b0df1cac1fff87f502548b1c205f061f242 origin/master
```

Importing commit ancestry does not migrate GitHub issues, reviews, releases,
or tag refs. Keep the original repository and its historical tags available.

## Dependencies and development

SVS remains a third-party submodule, registered in the root `.gitmodules` at
`src/vecsim/deps/ScalableVectorSearch`. Its pin is unchanged. Other Search
submodules are unchanged. A fresh checkout uses the normal command:

```sh
git submodule update --init --recursive
make vecsim-tests
make vecsim-tests SAN=address
```

The VecSim unit runner accepts `CTEST_ARGS` for filtering and fails if no tests
are selected. Benchmarks are built separately with
`make -C src/vecsim benchmark`; ordinary unit builds do not build benchmarks.

Existing clones may retain the former `deps/VectorSimilarity` checkout when
switching branches. Preserve any local changes and commits before cleaning up
that checkout. Use a separate checkout when working on both layouts; do not
delete `.git/modules` or force-clean an existing submodule to switch layouts.

## CI and tests

PR and merge-queue validation always require the VecSim workflow, without path
filters. It runs release unit tests, ASan unit tests, unit coverage, formatting,
and C/C++ CodeQL analysis. The same workflow runs nightly and participates in
nightly failure reporting. Reports are uploaded as workflow artifacts. Runtime
and failure measurements should determine any later selective execution policy.

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
- Rehearse a legacy VecSim fix plus Search pin-update backport.
- Verify preserved ancestry after landing.

Repository administration, RSE adoption, release benchmarking, and legacy
maintenance changes are separate from this source import; they must not be
inferred from a successful local build.
