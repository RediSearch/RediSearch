# Verification and review

## Integration refresh, 2026-09-10

Validated on `dorer-intel` in `/mnt/nvme/rs-pr11330-qohhy4af`, from RediSearch
`08142cdc7` plus the integration follow-ups and VecSim `fb2c5b04`. The VecSim pin
contains the reviewed #1034 -> #1035 -> #1029 stack, which remains unmerged.

- GCC 13.3, Rust 1.94.0, assertion-enabled Debug build with committed Rust headers
  and SVS v0.3.2: passed. The build reports the existing C compilation warning
  about the C++-only `-fsized-deallocation` flag.
- All 1,042 C/C++ unit cases passed: 17 C, 870 C++, 5 coordinator C, and 150
  coordinator C++.
- All 18 focused behavioral cases passed with Redis 8.10.1, RedisJSON, and the
  quick 20-second timeout. These include the entire SQ8 file, both legacy RDB
  compatibility tests, and disk-option rejection.
- Score and ranking assertions cover FLOAT32/FLOAT16 with all three metrics,
  zero/positive thresholds, both hybrid policies, L2/cosine range queries, and
  JSON multi-value labels. Checks run before training, after migration, and
  after reload. FLOAT16 L2 now also trains successfully.
- The RDB regression first demonstrated acceptance of dimension zero and an
  invalid metric. The corrected loader rejects both, as well as compressed disk
  configurations with zero or positive training thresholds.
- Changed-line C/C++ formatting, Python syntax, and whitespace checks passed.
- The full standalone behavioral suite is running; its result will be recorded
  before publishing this revision.

Logs in the checkout: `pr11330-build.log`, `pr11330-unit-final.log`,
`pr11330-focused-final.log`, `pr11330-rdb-validation-before.log`, and
`pr11330-full.log`. Source-update manifests record the tested file checksums.

The earlier results below describe older snapshots and are retained as historical
evidence, rather than results for this dependency revision.

## PR follow-ups, 2026-09-07

Validated locally on x86-64 after merging `master` at `e3a834ef7` and updating
VectorSimilarity to `0e71fad4`. The dependency includes the VecSim commit pinned
by current `master`; its only change from the previous SQ8 pin is the span input
update in the accumulation helpers. The SVS GC fixture now matches `master`.

- Confirmed legacy RDB resave rejection with the bundled VecSim fixtures. Before
  the fix, the first save/reload failed with `Invalid HNSW SQ8 parameters` and
  `Failed to load index field 0`. After clearing the reused parameter storage,
  all three fixtures (2.4.14, 2.6.9, and 8.0) load, save, and load again with both
  HNSW and FLAT vectors still searchable.
- Confirmed overflow directly in the pinned VecSim SQ8 preprocessor at dimension
  16,843,011. For a vector containing one value of 1 and otherwise 2, the stored
  sum is 16,843,012 and the stored sum of squares is 33,686,024, approximately half
  the expected values. RediSearch now rejects dimensions above the safe bound
  before creation or RDB size estimation; VecSim itself remains unchanged.
- The new dimension regression and both oversized RDB parameter cases failed
  before the bound and passed afterward. Coverage includes both supported types,
  all supported metric/training combinations, reload at the accepted boundary,
  and ordinary HNSW above the SQ8 bound.
- Assertion-enabled debug build: passed with GCC 13.3 and Rust 1.94.0.
- Full C/C++ unit suite: 1042 passed (17 C, 870 C++, 5 coordinator C,
  150 coordinator C++).
- Focused behavioral suite with the quick 20-second timeout: 16 passed,
  including SQ8, legacy RDB resaves, and existing resize-limit tests.
- Changed-line clang-format checks, Python syntax checks, and `git diff --check`:
  passed.
- Full standalone run with Redis 8.10.1, TLS, current RedisJSON (API V8), and the
  normal 300-second timeout: RLTest reported 2361 run, 2359 passed, and 2 failed;
  its passed counter includes skips. The per-case log contains 2154 PASS,
  218 SKIP, and 2 ERROR statuses. All SQ8 cases, legacy RDB compatibility, the
  upstream SVS GC fixtures, the earlier expiration failure, and TLS passed.
- The quick full-suite attempt ended after four SVS population timeouts exhausted
  the RLTest worker processes. It has no complete-suite result. The normal-timeout
  run above completed in 601 seconds.

The two full-suite errors were:

- `test_info_modules:test_pending_jobs_metrics_aggregate`: the pending-job wait
  timed out after 120 seconds, observing four high-priority jobs instead of three.
- `test_multibyte_char_terms:testTagToLowerConversionSimilarMatch`: Redis exited
  after a Rust misaligned-pointer panic in `triemap_ffi/src/iter_types.rs:57`
  (32-byte alignment required). The iterator and both failing tests are unchanged
  from `master`; no same-build comparison with `master` was performed.

Serial reruns with the normal timeout: the pending-job test passed; the
multibyte-term test reproduced the same alignment panic. The full suite is not green.

The independent review confirmed the legacy and dimension fixes and found one
remaining blocker: disk indexes accept SQ8 training settings that the available
disk provider does not implement. It also recommended score/ranking and additional
query-path coverage. The encoding-v28 schema propagation policy during mixed-version
slot migration remains unverified. These follow-ups require resolution before merge.
The VecSim stack remains unmerged; this run validates `0e71fad4`, not the later
`9776964a` job-registration update.

Local logs are `/tmp/mod14958-followups-{build-before,build-after,unit-before,unit-after,
flow-before,flow-after}.log`. The legacy server failure is preserved in
`/tmp/mod14958-followups-legacy-server-before.log`; the standalone preprocessor
reproduction is `/tmp/mod14958-sq8-overflow.cpp`.
Full-run logs are `/tmp/mod14958-followups-full-quick.log` and
`/tmp/mod14958-followups-full.log`; serial reruns are in
`/tmp/mod14958-followups-rerun.log`. The complete independent review is preserved
locally in `/tmp/mod14958-independent-review.md`.

## Earlier integration validation

Workerless integration: `4325b9333b0abfd2c88121d9a1dbeaf81a6f7cab`.
Resize implementation: `d1ad310ebf87dcb017c9597f744dfadede13de0e`.
Backend-normalization coverage: `81c4443c9fe8721d113bc7c4a5fea09aa9a265f9`.
VectorSimilarity pin: `acaad32b57854dae11b109de08a97d1eb9d9fbf7` (PR #1029,
the final PR in the stack, still open when checked).

The earlier ARM validation ran on `arm-r8g.xlarge`, in the isolated checkout
`/home/ubuntu/rs-mod14958-arm`. Earlier validation used `dorer-intel` at
`/mnt/nvme/rs-mod14958`. The results below predate the local PR follow-ups above.

## Stacked dependency and ARM validation

GitHub's stack order is [#1034](https://github.com/RedisAI/VectorSimilarity/pull/1034)
→ [#1035](https://github.com/RedisAI/VectorSimilarity/pull/1035)
→ [#1029](https://github.com/RedisAI/VectorSimilarity/pull/1029).
The pinned final head includes both ancestors and the synchronous workerless
migration fix. Its Git tree, `db34008805239c24b76275da75e8ccb198031d0e`, is identical
to the preceding `4f97b1c4` pin: splitting the PR changed history without changing
the source. All 2755 checked RediSearch and VecSim source files initially matched
between the local task checkout and ARM.

The ARM host is a four-core Graviton4/Neoverse-V2 machine with NEON and SVE2.
Validation used GCC 12.3, Rust 1.94.0, Python 3.12.14, Redis 8.10.1 built with TLS
(`3399357e7c17b668289386b8a15a3037bc4527b1`), and native RedisJSON
(`1c74f65addb4c2d89d97c8525ed993503a476060`). The assertion-enabled debug build
used the repository's checked-in Rust headers (`REDISEARCH_GENERATE_HEADERS=OFF`)
and two build jobs. SVS was enabled with its ARM compression fallback.

- RediSearch debug build: passed.
- Full RediSearch unit suite: 1033 passed (17 C, 861 C++, 5 coordinator C,
  150 coordinator C++).
- Focused SQ8 and existing resize-limit behavioral tests: 14 passed with the
  quick 20-second timeout, including FLOAT32/FLOAT16 workerless migration and RDB
  reloads with both worker settings zero.
- Native VecSim suites, with the default `FP64_TESTS=OFF`: 337 HNSW, 100 SQ8,
  68 FLAT, and 1550 distance-kernel cases passed. The initial direct invocation
  omitted the required `ROOT` environment variable, causing five serialization
  cases to throw before exercising serialization. All five passed when rerun
  with `ROOT` set to the VecSim checkout; the other cases passed initially.
- ARM SIMD linkage check against the library linked into RediSearch: passed
  for all eight tier objects and 28 pairs, with no shared external symbols.
- Full standalone behavioral suite with the normal 300-second timeout:
  RLTest reported 2333 run, 2330 passed, and 3 failed; its passed counter includes
  skips. The per-case log records 2125 PASS, 3 FAIL, and 218 SKIP statuses. All SQ8
  cases passed. The TLS test also passed with the native TLS-enabled Redis.
- The three failures were the existing expiration assertion and both SVS GC
  memory assertions documented below. There was no new SQ8 failure.
- The SVS GC fixture now inserts three blocks and deletes two. This forces
  reclamation beyond the spare block retained by the allocator introduced in
  VecSim #980, while preserving the strict memory-decrease assertion. The
  worker-enabled case passed the quick rerun. The zero-worker case exceeded
  20 seconds during population and passed with the normal timeout in 28 seconds.
  The full suite was not repeated after this fixture-only change.

The only unresolved behavioral failure is
`test_expire:testSortableFieldWithExpirationAndRegularField`: the expired `x`
field remains visible on `doc1` at the assertion. Upstream RediSearch commit
`000795272` already changes this timing-sensitive test. No expiration code or
test was changed in this task. The full suite is not reported as green.

Before the request to switch hosts, the same final pin also passed the Intel
build, 1033 unit tests, and 14 focused tests. The in-progress Intel full suite was
stopped when validation moved to ARM; it has no completion result for this run.
The three PR heads remained unchanged and open at the final check. Their recorded
sanitizer CI checks passed, but coverage jobs lost contact with their self-hosted
runners; coverage validation remains incomplete. The dependency pin is provisional
until the stack merges. No PR was opened or updated by this validation run.

## Workerless migration fix

These earlier Intel results used the equivalent `4f97b1c4` dependency tree.

The VecSim threshold transition now uses the current write mode. With workers
available it queues migration jobs; in write-in-place mode it executes those
jobs synchronously before the threshold-crossing insertion returns. Accumulation
still buffers vectors in both modes. This also handles disabling workers during
accumulation and RDB rebuilds without temporary workers.

- All eight VecSim regression cases failed before the fix and passed afterward,
  across FP32/FP16 and single/multi-value indexes.
- Full VecSim suites: 522 HNSW tests passed with one expected serialization skip,
  100 SQ8 tests passed, and 11 allocator tests passed.
- The two new RediSearch insertion regressions failed against the previous module:
  four vectors stayed in FLAT, HNSW was empty, and four jobs waited with no workers.
- RediSearch assertion-enabled debug build: passed. Cargo emitted a jobserver
  file-descriptor warning; it did not prevent compilation or linking.
- Full RediSearch unit suite: 1033 passed.
- Focused SQ8 and existing resize-limit behavioral tests: 14 passed. Coverage
  includes zero-worker insertion, disabling workers during accumulation, and
  RDB reloads before and after training with both `WORKERS 0` and
  `MIN_OPERATION_WORKERS 0` for FLOAT32 and FLOAT16.
- Remote dependency and Python source hashes match the committed files.
- Full standalone behavioral suite with the normal 300-second timeout: 2341 run,
  2337 passed, 4 failed. The failures match the preceding integration run:
  `test_expire:testSortableFieldWithExpirationAndRegularField`,
  `test_vecsim_svs:test_gc`, `test_vecsim_svs:test_gc_no_workers`, and
  `test:test_with_tls`. Their assertions and environment limitations are recorded
  below. All SQ8 tests passed; the full suite remains non-green.
- Independent review of `4325b9333` and the complete VecSim dependency delta
  found zero blocking findings and zero suggestions. It was a code review only;
  the runtime evidence above comes from the remote runs.
- VecSim PR #1029 has the pinned head; its basic, sanitizer, and coverage CI jobs
  were still running at handoff. No RediSearch PR or CI run was opened.

The [HLD section 3.2](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069)
describes unconditional job submission at the transition and omits zero workers.
The synchronous fallback follows SVS behavior; the local design and spec document
it and the latency of migrating the training set on the triggering write. The
Confluence HLD was not edited.

## RediSearch resize fix

The resize follow-up validates both SQ8 tiers during creation and RDB loading.
Three new regression tests failed against the previous build, demonstrating
incorrect acceptance of oversized frontend vectors and oversized block sizes
both before and after reload. The corrected build passes all three.

- Assertion-enabled debug build: passed.
- Full unit suite: 1033 passed.
- Focused SQ8 and existing resize-limit behavioral tests: 11 passed, including
  the additional backend-normalization boundary case.
- Full standalone behavioral run: 2337 run, 2321 passed, 16 failed with the initial
  20-second timeout. This run preceded the additional boundary test; that test
  passed in the final focused run against the same module binary.
- Rerunning the 16 failures with the normal 300-second timeout and four
  processes: 12 passed, 4 failed. The remaining failures are the same expiration,
  two SVS GC, and TLS cases documented below.
- Remote source hashes match the committed C and Python files.

The initial focused run after the fix exposed a missing exception import in the
new RDB rejection test. After adding the import, the complete focused run passed.

An independent review of the resize fix found no separate code defect and
requested coverage where the backend estimate dominates. The added test checks
acceptance, rejection, block size, and reload across the mean-normalization
boundary and passes remotely. No production code changed after that review.

## Initial integration results

These results were obtained at `3cc4ad3a8f2d70f082cff7b0c95a6d10037e9245`.

- Assertion-enabled debug build: passed, including the formatted source.
- Full unit suite: 1033 passed (17 C, 861 C++, 5 coordinator C, 150 coordinator C++).
- Focused SQ8, RESP3 FT.INFO, and legacy RDB tests: 7 passed.
- Full standalone behavioral run with Redis 8.10.1 and RedisJSON: 2334 run,
  2318 passed, 16 failed under the initial 20-second timeout.
- Those 16 failures rerun with the normal 300-second timeout and four processes:
  12 passed, 4 failed. All 12 timed-out cases recovered.

The four remaining behavioral failures are:

- `test_expire:testSortableFieldWithExpirationAndRegularField`: expiration timing
  expectation; upstream RediSearch commit `000795272` already changes this test.
- `test_vecsim_svs:test_gc` and `test_gc_no_workers`: memory does not shrink after
  deletion. Both pass against the older remote module at RediSearch `1995a73b3`
  with VecSim `807566ee`. VecSim `e647bc8d` changed block allocation and adjusted its
  own GC unit test for retained blocks; that change is present in both the previous
  integration pin and PR #1029's base. The RediSearch fixture still assumes the old
  shrinking behavior. An exact same-build comparison against #1029's base was not
  performed.
- `test:test_with_tls`: Redis startup reports `Missing implement of connection
  type tls` and `Failed finding TLS support.` The remote Redis binary lacks TLS.

The full behavioral suite is not green. No clustered behavioral run or new CI run
was performed, and no RediSearch PR was opened.

## Initial independent review

The initial independent review reported two blocking issues. Both were
reproduced on the remote machine. The resize issue is now fixed in RediSearch;
the workerless migration issue is now fixed by the VecSim update above.

1. **P1, resolved: migration with no workers.** With `WORKERS 0`, four inserts into an SQ8
   index with threshold four leave four vectors in the frontend, zero in HNSW,
   and four pending jobs with no live threads. Reload drains them with the default
   temporary workers. With `MIN_OPERATION_WORKERS 0` too, `DEBUG RELOAD` exceeded
   the reproduction's socket timeout. The code path indicates a wait for jobs
   that cannot execute. Attaching GDB was denied by the remote ptrace policy, so
   no runtime stack was obtained. The temporary reproduction server was killed.
   The new transition fallback and insertion/reload tests address this finding.

2. **P2, resolved: full-precision resize limit.** With `VSS_MAX_RESIZE=2000`, FLOAT32,
   dimension 1024, SQ8, and threshold four, index creation succeeds while ordinary
   HNSW is rejected. The first insert grows frontend memory by 4296 bytes with
   block size one. The vector payload alone needs 4096 bytes. The corrected
   validation rejects this configuration and recomputes a bounded shared block
   size during loading. Tests cover FLOAT32 and FLOAT16, trained and untrained
   SQ8, creation rejection, reload rejection, and successful reload under a
   smaller limit.

The resize fix changed only RediSearch. Its shared submission callback
also handles graph-repair jobs submitted while VecSim locks are held, so simply
running all jobs inline when there are no workers risks lock reentrancy. The
SQ8 transition now honors VecSim's in-place write mode within its own lifecycle.

The reviewer could not establish compatibility with the external disk provider,
which is absent from this checkout, or mixed-version schema propagation beyond
the documented encoding-version boundary.

## Remote artifacts

On `arm-r8g.xlarge`, under `/home/ubuntu/mod14958-logs/`:

- `source.sha256` and `source-check.log`
- `build-stack-committed-headers.log`
- `unit-stack.log`
- `build-redis-tls.log` and `build-redisjson.log`
- `focused-stack.log`
- `full-stack.log`
- `svs-gc-fixture.log` (one pass and one quick-timeout failure)
- `svs-gc-workerless-normal.log` (the timed-out case passes)
- `vecsim-configure.log` and `vecsim-build.log`
- `vecsim-test_hnsw.log` and `vecsim-test_hnsw_sq8.log`
- `vecsim-hnsw-serialization-env.log` and `vecsim-sq8-serialization-env.log`
- `vecsim-test_bruteforce.log` and `vecsim-test_spaces.log`
- `arm-tier-linkage.log`
- `source-final.sha256` and `source-check-final.log`

The following paths are on `dorer-intel`:

- `/tmp/pr1029-workers-before.log`
- `/tmp/pr1029-workers-build.log`
- `/tmp/pr1029-workers-hnsw.log`
- `/tmp/pr1029-workers-sq8.log`
- `/tmp/pr1029-workers-allocator.log`
- `/mnt/nvme/mod14958-workers-before-20260907.log`
- `/mnt/nvme/mod14958-workers-build-20260907.log`
- `/mnt/nvme/mod14958-workers-unit-20260907.log`
- `/mnt/nvme/mod14958-workers-focused-20260907.log`
- `/mnt/nvme/mod14958-workers-full-20260907.log`

- `/mnt/nvme/mod14958-resize-before-20260906.log`
- `/mnt/nvme/mod14958-resize-build-20260906.log`
- `/mnt/nvme/mod14958-resize-unit-20260906.log`
- `/mnt/nvme/mod14958-resize-focused-20260906.log` (missing test import)
- `/mnt/nvme/mod14958-resize-focused-final-20260906.log`
- `/mnt/nvme/mod14958-resize-boundary-focused-20260906.log`
- `/mnt/nvme/mod14958-resize-full-20260906.log`
- `/mnt/nvme/mod14958-resize-rerun-20260906.log`
- `/mnt/nvme/mod14958-build-20260906.log`
- `/mnt/nvme/mod14958-formatted-build-20260906.log`
- `/mnt/nvme/mod14958-unit-20260906.log`
- `/mnt/nvme/mod14958-py-focused-20260906b.log`
- `/mnt/nvme/mod14958-py-full-20260906.log`
- `/mnt/nvme/mod14958-py-rerun-20260906.log`
- `/mnt/nvme/mod14958-svs-baseline-20260906.log`
- `/mnt/nvme/mod14958-workers0-repro.py` and `.log`
- `/mnt/nvme/mod14958-review-repros.py` and `.log`
- `/mnt/nvme/mod14958-reload-hang-gdb.log` (ptrace rejection)
