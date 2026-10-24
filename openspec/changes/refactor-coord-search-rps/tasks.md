# Tasks

Sections 2 through 5 are the four planned implementation PRs. Baseline/design
artifacts accompany PR 1; completion checks apply to each relevant PR. Update the
behavior ledger in the same change as any implementation difference; do not mark
source analysis as testing.

## 1. Baseline and design

- [x] Map custom state, ownership and reduction paths at an explicit local revision.
- [x] Create compatibility ledger with unresolved classifications visible.
- [x] Define an extraction boundary without RP or timeout behavior changes.
- [ ] Record design review and repository issue/PR references when created.
- [ ] Inventory existing cluster search/KNN/timeout/profile coverage and add only
  missing characterization cases needed by the first migration.

## 2. Mechanical extraction

- [x] Move reducer/decoder/comparator implementation behind a narrow coordinator
  interface; preserve capacities, callbacks, checkpoints and serialization.
- [x] Keep parsing and dispatch in module.c; check both CMake source lists.
- [x] Run focused cluster search/profile/KNN/timeout tests and required build checks.
- [x] Record B01's exact diff and validation evidence.

## 3. Reply-array source and ordinary ordering

- [x] Settle minimal row representation and exact destruction order.
- [x] Add source RP for RESP2/RESP3 and profile envelopes, with envelope error scan
  and separate count/warning metadata.
- [x] Reuse bounded sorter machinery with equivalent coordinator comparisons.
- [x] Integrate pager and count-only behavior without narrowing counts/limits.
- [ ] Differentially verify replies and exercise cleanup under ASan.

## 4. KNN paths

- [x] Add nearest-K selection followed by final ordering for shouldSort paths.
- [x] Preserve distance-sort shortcut pending B06 evidence.
- [x] Preserve count semantics independently of pagination and heap size.
- [ ] Cover both heaps at interruption boundaries; update B05/B06/B07/B15.

## 5. Common lifecycle and callbacks

- [x] Store standard results through QueryRequest and free the RP chain normally.
- [x] Extract minimum common reply/policy machinery; preserve output representation.
- [x] Remove old reducer heaps, wrappers, post-processing callbacks and temporary
  serializer adapter once callers have migrated.
- [ ] Preserve existing timeout ownership; verify exactly-once reply/unblock/free.

## 6. Integration checklist for the user's Drain API stack

This is a cross-stack contract, not a fifth PR owned by this refactor. Choose
merge order when the concrete changes are ready; see the design's integration
boundary. Do not implement a competing Drain API or handoff protocol here.

- [ ] Integrate the public Drain API when its contract is available.
- [ ] Prove stacked-sorter drain consumes upstream buffers before final selection.
- [ ] Establish exclusive BG/main and IO/source ownership transfer.
- [ ] Classify per-row cancellation and timeout-result differences using evidence.
- [ ] Keep nonblocking strict behavior in the user's Drain API stack.

## 7. Completion

- [ ] Run required build, lint, unit and cluster behavioral validation sequentially
  with saved logs; run memory-lifetime checks for the changed ownership paths.
- [ ] Review every ledger entry against the final diff; retain no unexplained delta.
- [ ] Update the spec to shipped behavior and perform independent review before PR.
