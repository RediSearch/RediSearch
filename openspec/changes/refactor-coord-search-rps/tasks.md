# Tasks

Sections 2 through 5 are the four planned implementation PRs. Baseline/design
artifacts accompany PR 1; completion checks apply to each relevant PR. Update the
behavior ledger in the same change as any implementation difference; do not mark
source analysis as testing.

## 1. Baseline and design

- [x] Map custom state, ownership and reduction paths at an explicit local revision.
- [x] Create compatibility ledger with unresolved classifications visible.
- [x] Define an extraction boundary without RP or timeout behavior changes.
- [x] Record the approved design in the four PRs: #11985, #11986, #11987 and #11988;
  tracking ticket MOD-20150.
- [x] Inventory existing cluster search/KNN/timeout/profile coverage and add only
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
- [x] Compare successful replies across borrowed/eager/direct benchmark variants;
  run sanitizer unit, standalone and coordinator CI suites. Original-master
  equivalence is not claimed for the approved behavior corrections.

## 4. KNN paths

- [x] Add nearest-K selection followed by final ordering for shouldSort paths.
- [x] Keep distance ASC/count-only shortcuts; select nearest K before distance DESC
  final ordering under the approved B06 correction.
- [x] Preserve count semantics independently of pagination and heap size.
- [x] Cover buffered KNN interruption paths; record B05/B06/B07/B15 corrections.

## 5. Shared result and shard-reply ownership

- [x] Store standard results through QueryRequest and free the RP chain normally.
- [x] Use shared stored-result cleanup and direct selected-row serialization; preserve
  output representation. Keep direct callbacks until policy logic can be shared.
- [x] Remove old reducer heaps, wrappers, post-processing callbacks and temporary
  serializer adapter once callers have migrated.
- [x] Preserve existing timeout ownership; verify exactly-once reply/unblock/free.

- [x] Create the reply-owning source before dispatch and replace search MRCtx.
- [x] Preallocate reply storage and acquire an atomic publication snapshot for strict timeout.
- [x] Free stored rows and downstream RPs before reply backing.
- [x] Validate admission-boundary search/profile tests in both protocols.

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

- [x] Validate the direct-row implementation with build, lint, unit, standalone,
  coordinator and sanitizer suites. CI run 40699837625 passed at b863eb4c8;
  later follow-ups must report their own exact-head status.
- [x] Reconcile the final behavior ledger with the approved corrections and deferrals.
- [x] Update the spec to implemented behavior. Independent review at b863eb4c8
  reported zero findings; this is review readiness, not a merge claim.
