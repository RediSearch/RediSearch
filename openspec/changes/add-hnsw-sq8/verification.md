# Verification and review — 2026-09-06

Implementation checkpoint: `d1ad310ebf87dcb017c9597f744dfadede13de0e`.
Backend-normalization coverage: `81c4443c9fe8721d113bc7c4a5fea09aa9a265f9`.
VectorSimilarity pin: `23993ad9ce3e00491b82aa647f931118c2cc1676` (PR #1029,
still open when checked).

All completed builds and tests ran on `dorer-intel`, using the existing checkout
at `/mnt/nvme/rs-mod14958`. A local build was started before the remote-only
instruction and stopped; no local build result is counted here.

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

## Independent review: remaining migration blocker

The initial independent review reported two blocking issues. Both were
reproduced on the remote machine. The resize issue is now fixed in RediSearch;
the workerless migration issue remains in VecSim.

1. **P1: migration with no workers.** With `WORKERS 0`, four inserts into an SQ8
   index with threshold four leave four vectors in the frontend, zero in HNSW,
   and four pending jobs with no live threads. Reload drains them with the default
   temporary workers. With `MIN_OPERATION_WORKERS 0` too, `DEBUG RELOAD` exceeded
   the reproduction's socket timeout. The code path indicates a wait for jobs
   that cannot execute. Attaching GDB was denied by the remote ptrace policy, so
   no runtime stack was obtained. The temporary reproduction server was killed.
   Fix migration without background workers and cover both insertion and reload.

2. **P2, resolved: full-precision resize limit.** With `VSS_MAX_RESIZE=2000`, FLOAT32,
   dimension 1024, SQ8, and threshold four, index creation succeeds while ordinary
   HNSW is rejected. The first insert grows frontend memory by 4296 bytes with
   block size one. The vector payload alone needs 4096 bytes. The corrected
   validation rejects this configuration and recomputes a bounded shared block
   size during loading. Tests cover FLOAT32 and FLOAT16, trained and untrained
   SQ8, creation rejection, reload rejection, and successful reload under a
   smaller limit.

VecSim is unchanged by this follow-up. RediSearch's shared submission callback
also handles graph-repair jobs submitted while VecSim locks are held, so simply
running all jobs inline when there are no workers risks lock reentrancy. The
SQ8 transition should honor VecSim's in-place write mode within its own lifecycle.

The reviewer could not establish compatibility with the external disk provider,
which is absent from this checkout, or mixed-version schema propagation beyond
the documented encoding-version boundary.

## Remote artifacts

All paths below are on `dorer-intel`:

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
