# VecSim migration rollout

Evidence recorded on 2026-11-15 for Search commit
`7468e314d55a3bf85a6c3ab55cce6beb33ece334`,
[PR #12329](https://github.com/RediSearch/RediSearch/pull/12329).
Recheck remote heads and required checks immediately before landing.

## Before merge

- [x] Production source history retained, including original VecSim commit
  `eefc4e7a0feb608f3e2e50e7e5050d1f44bdec9e`; SVS pin unchanged.
- [x] PR GitHub Actions checks passed at the revision above, including units,
  Search flows, ASan, coverage upload, formatting, CodeQL, and `USE_SVS=OFF`.
  [PR workflow](https://github.com/RediSearch/RediSearch/actions/runs/43443197140).
- [x] x86 and ARM benchmark execution and backend export passed.
  [Benchmark workflow](https://github.com/RediSearch/RediSearch/actions/runs/43443196790).
- [x] Reviewed coverage denominator change without changing thresholds; see below.
- [x] Inspected merge-queue failure propagation: the VecSim unit step outcome is
  checked even with `continue-on-error`; the aggregate gate rejects failed or
  cancelled dependencies and requires the separate VecSim checks to succeed.
  The CTest runner rejects an empty test selection. Master retains its existing
  basic-test/QUICK lane; component units have no change-path filter.
- [ ] Complete supported-platform validation. The
  [full platform workflow](https://github.com/RediSearch/RediSearch/actions/runs/43720529527)
  was dispatched against the revision above. Dispatch is not a passing result.
- [ ] Obtain human review of the final revision and the one-time coverage baseline.
- [ ] Complete Sonar analysis disposition; see below. No issues were suppressed.
- [ ] Resolve CLA association for `razdoburdin`; Search reports six of seven
  signed, while the contributor's [original VecSim PR #1058](https://github.com/RedisAI/VectorSimilarity/pull/1058#issuecomment-5791831501)
  reports all signed. Both repositories reference the same
  [CLA gist](https://gist.github.com/gkorland/effb0c93889899c51aa9b6ac5c32dba3),
  but both return `sharedGist: false` from CLA Assistant's public linked-item
  metadata. Both current agreement responses have update timestamp
  `2022-09-21T17:26:50Z` and identical rendered-text SHA-256
  `bc0b5785c4e338001e111eed5383abbb5432dfc8a7cd34f85649d8c9f3ecd35b`.
  The agreed route is for the contributor to sign for Search. Keep the required
  check; no signature sharing or administrative signature transfer is planned.
- [ ] A repository maintainer must handle the history-preserving landing
  exception. Effective master
  rules require one approval, last-push approval, `pr-validation`, `license/cla`,
  squash-only PR merges, and a squash merge queue. There are no configured bypass
  actors. Repository-level merge-commit availability does not override these rules.
  Preserve review and CLA requirements; do not squash or rebase this import.
- [ ] Confirm ownership and disposition of open legacy work with component owners.
  The legacy repository has 64 open PRs: 49 target `main`, six target numbered
  branches (`8.4`, `8.2`, `0.7`, `0.6`), and nine target feature branches.
  [Open PR inventory](https://github.com/RedisAI/VectorSimilarity/pulls).
  Retain maintenance CI, tags, issues, and security handling until supported
  consumers no longer require the legacy repository. Public inventory alone
  does not establish that all private consumers have been identified.

The queue routing above was inspected from source. A successful PR workflow is
not evidence that a merge-group event ran. A one-time landing exception must
also establish that its tested merge tree matches the tree being landed.

## Coverage baseline

The public Codecov commit reports were compared using per-file totals, separating
`src/vecsim/` from all other files:

| Scope | Files | Reported lines | Fully covered | Coverage |
| --- | ---: | ---: | ---: | ---: |
| Search base `33d90342bc9c601d904fb58446f8bda4eaba5011` | 695 | 92,532 | 78,414 | 84.74% |
| Migration excluding VecSim | 695 | 92,530 | 78,361 | 84.69% |
| Imported VecSim | 157 | 10,017 | 7,204 | 71.92% |
| Combined migration | 852 | 102,547 | 85,565 | 83.43% |

There are no added or removed non-VecSim file identities in these reports.
The existing Search coverage difference is about -0.06 percentage points; the
combined denominator now includes VecSim. This explains the major project-level
change but does not make either Codecov check pass. Patch coverage is 66.92%.
Keep the ordinary project and patch thresholds unchanged. Search flow coverage
retains the approved SVS runtime exception; component unit coverage and Search
ASan flows remain enabled.

[Codecov report](https://app.codecov.io/gh/RediSearch/RediSearch/pull/12329).

## Sonar analysis triage

The current analysis reports 84 issues: 19 bugs, two security findings, and
63 code smells. The failing gate reports reliability E, security C, and 12.8%
new-code duplication against a 3% limit.

The two security findings are deterministic `std::mt19937` generators in HNSW
serialization tests, producing test vectors with seed 47. They do not generate
credentials, tokens, or other security-sensitive values. Their analysis
classification should be reviewed in that test-only context.

All 19 bug reports use the same double-free rule. Each execution trace marks
both the release and the alleged second release at the same `delete` expression.
The affected allocator, component, HNSW, mock-job, and preprocessor source files
match imported VecSim after normalizing include prefixes. These reports therefore
predate the relocation, but that alone does not prove they are false positives.
The custom `VecsimBaseObject` deallocator and ownership assumptions require an
analysis-owner disposition; passing ASan tests are supporting evidence, not a
proof for every possible lifetime. No allocator behavior was changed or issues
marked resolved as part of this migration.

[Sonar report](https://sonarcloud.io/dashboard?id=RediSearch_RediSearch&pullRequest=12329).

## Enterprise adoption and 8.10 rebranch

Advance validation of the RSE main candidate completed locally:

- Full Debug module build with SVS enabled passed; all 13 Enterprise security
  patches applied to the migration revision.
- Disk-vector units: 572 passed, six async-dispatch cases skipped, 578 registered,
  including serialization and big-endian variants.
- Five selected product flows passed: KNN, delete/requery, hybrid batches, and
  save/RDB reload with reranking enabled and disabled.
- A separately built pre-migration module (RSE `27b4d496`, Search `33d90342`)
  produced disk-vector data for a hot-restart upgrade to the adoption module.
  The new module reused 14 old SST files and the RAM-only restart RDB, preserving
  KNN IDs/scores and range results with reranking enabled and disabled. Queued
  flat-buffer inserts, a backend deletion, and post-upgrade inserts were checked.
  Both processes used the same SpeedB library; binary and snapshot hashes were
  captured. This is a focused upgrade test, not release-scale certification.
- Product flows and the upgrade test used local Flex `7200aae1cca9`, older than
  the current CI pin `61974430b3b7`; this does not replace validation on that runtime.

1. Land Search with preserved history and verify the original VecSim commit is
   an ancestor of the resulting master commit.
2. Bump RSE main to that landed Search commit together with the include/build
   adaptations. The prepared adoption candidate uses the migration PR revision
   for advance validation; replace that pin with the landed commit before merge.
3. Validate RSE compilation, security patch application, disk-vector units,
   persisted-index behavior, and relevant product flows on the candidate.
4. Align public Search `8.10-rse` with the selected migration-containing release
   baseline. The prepared two-parent merge retains both histories and differs
   from the migration tree only in the 8.10.0 version file. Its submodule-delete
   and backported-source conflicts resolve to the validated migration baseline.
   Release rules also require squash, so this merge needs a history-preserving
   exception. RSE's scheduled bump map uses `8.10 -> public 8.10-rse`; leaving the
   latter behind would allow a subsequent bump PR to propose reverting migration.
5. Prepare the new RSE `8.10` from the validated RSE main commit, retaining the
   release version `8.10.0` in `src/version.h` and release submodule branch metadata.
   The current main already contains the cosine normalization fix and its test,
   plus version-header integration; do not blindly replay old release commits.
6. Before replacing either release ref, record its current SHA, retain a backup
   ref, enumerate open release PRs, and approve the concrete before/after refs.
   Use an exact expected-old-SHA lease for replacement; stop if either ref moved.
7. Run release CI and the scheduled bump workflow in dry-run mode against the
   resulting branches. Confirm the resolver selects the intended Search revision.

Search `8.10-rse` has five commits absent by SHA from master: its version
commit and four cherry-picks. The originals of all four cherry-picks are
ancestors of the migration revision, so preserve the 8.10.0 version without
reapplying those fixes. No open Search PR targeted `8.10-rse` at this snapshot.
RSE has an older private bump PR, `RediSearchEnterprise#1457`,
targeting `8.10`; reconcile or supersede it during cutover so it cannot restore
an obsolete Search pin.

Snapshot for that comparison:

| Ref | SHA |
| --- | --- |
| RSE main | `27b4d496ea4f33cd2b5c56e12c1b055a7ca03b13` |
| RSE 8.10 | `bb9febf675fa0cab5002f7775f341d5665dc4db6` |
| Search pinned by RSE main | `33d90342bc9c601d904fb58446f8bda4eaba5011` |
| Search pinned by RSE 8.10 | `c3ff3143889f7bcf79ce6046a4419d6daea239bf` |

## Remaining release validation

- [ ] Rehearse a fix through a legacy VecSim branch and a maintained Search
  submodule-pin update; retain branch-specific product tests. A local path/context
  rehearsal used Search 8.8's actual VecSim pin
  `45daaf32031021eebee81d6df753e911083bd45f` and the migration's two-file
  `USE_SVS=OFF` fix. The mechanically remapped patch conflicted in `vec_sim.cpp`;
  adapting the older include/function context made `git apply --check` and
  application succeed. The local component backport built with SVS disabled;
  all 406 selected CTest cases passed across the initial run and a rerun of three
  fixture cases with the legacy runner's required `ROOT` environment. A paired
  local Search pin-update commit was prepared. No remote maintenance branch was
  changed; Search 8.8 product validation of that pin remains outstanding.
- [ ] Establish the first Search benchmark baseline. Existing comparison jobs
  succeeded but reported no comparison points; that is not a performance result.
- [ ] Verify Grafana queries include repository `RediSearch/RediSearch` and
  triggering environment `redisearch-micro-benchmarks`, for both architectures.
  The dashboard API returned HTTP 401 without authentication. Successful backend
  export does not establish dashboard visibility. Preserve access to historical
  `RedisAI/VectorSimilarity` results under their original identity.
- [ ] Compare controlled performance and recall before release.
- [ ] Confirm consumer and maintenance ownership, then redirect new feature work
  to Search. Archive the old repository only after dependent branches reach EOL.

[Benchmark dashboard](https://benchmarksrediscom.grafana.net/d/8171e685-e93d-49dd-86a5-859e779d652c).
