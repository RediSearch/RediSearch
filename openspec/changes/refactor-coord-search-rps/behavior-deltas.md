# Behavior delta ledger

This is the authoritative ledger for observable changes. Implementation evidence is recorded below. Source observations alone are not
executed differential tests.

## Classification

- **Internal:** no change to command results, protocol, errors or lifecycle
  guarantees; record structural and resource changes.
- **Soft observable:** output/timing changes within an established permitted
  range. Requires an existing scenario demonstrating variability AND evidence
  that the changed property is not guaranteed. Missing documentation alone is
  insufficient.
- **Breaking:** changes guaranteed or relied-upon results, wire representation,
  errors or lifecycle semantics. Exclude from the mechanical refactor unless
  explicitly reviewed as a separate behavior change.
- **Unclassified:** evidence incomplete. This is not permission to change it.

For each implemented delta record baseline/replacement revisions, trigger,
before/after, classification rationale, test/log evidence and disposition.
Correctness regressions such as double replies or use-after-free are rejected,
not justified as soft behavior.

## Tracked surfaces

| ID | Trigger and baseline source evidence | Proposed after / classification | Disposition and required check |
| --- | --- | --- | --- |
| B01 | Every distributed search: custom reducer in module.c | Same functions in coordinator file; internal | First extraction; build both source configurations and run focused cluster regression tests |
| B02 | Buffered rows: searchResult borrows MRCtx replies | Standard SearchResult with explicit backing lifetime; internal if outputs unchanged | Destroy rows before backing; test partial decode, eviction, error and timeout cleanup under ASan |
| B03 | Ordinary score ties: cmp_results compares document keys | Same order; breaking if changed | Binary keys, prefixes, equal scores, cross-shard ties and pagination fixtures |
| B04 | SORTBY: cmp_results handles numeric/string/missing keys and direction-dependent key ties | Same order; breaking if changed | ASC/DESC, absent/null, mixed keys, ties, embedded NUL fixtures |
| B05 | KNN shouldSort: ProcessKNNSearchReply selects K before knnPostProcess final ordering | Same membership and order; breaking if changed | Other-field SORTBY and default score order; W below/above K, offset beyond K, distance ties |
| B06 | SORTBY distance: setKNNSpecialCase disables KNN preselection even for DESC | Preserve shortcut; unclassified if replaced with general NearestK chain | Characterize ASC/DESC across shards and SHARD_K_RATIO before declaring equivalence |
| B07 | Counts: ordinary sum, KNN heap count or min(K, summed shard counts) | Same totals; breaking if changed | Zero matches, K boundaries, small page, offset, LIMIT 0 0; retain count-only path behavior |
| B08 | Output: sendSearchResults passes through remote fields/payload/explanation | Same RESP structure and types; breaking if changed | RESP2/3, options, null vs absent, large integers, nested JSON, field order and FORMAT |
| B09 | Timeout during a large shard reply: reducer normally checks after processReply | Optional per-row cancellation; unclassified, potentially soft | Record old/new accepted and reduced row sets under deterministic pauses; demonstrate variability before classification; defer from extraction |
| B10 | Multiple shard warnings: processSearchReply retains first nonempty warning array; sendSearchResults gives it precedence over local OOM/timeout | Preserve selection; unclassified if combined/reordered | Mixed warnings and local timeout/OOM; do not normalize silently |
| B11 | Error replies: reducer pre-scans; should_return_error handles timeout/OOM policy | Same fatal-error precedence and tolerated errors; breaking candidate if changed | Fatal + success, fatal + timeout, malformed rows, zero replies |
| B12 | OOM while global config changes: should_return_error reads global oomPolicy | Optional request snapshot; unclassified | Separate semantics decision and config-change scenario; preserve initially |
| B13 | Profile timeout: profileSearchReply emits available shard profiles and timings | Same shape/labels; completeness differences unclassified | RESP2/3 with missing replies and timeout before/mid/after reduction |
| B14 | Strict timeout during reduction: callback claims or waits, then replies | Preserve protocol initially; future nonblocking handoff unclassified | Exactly one response/free, no main-thread/BG heap overlap, queue/dispatch/reduce/reply races |
| B15 | Stacked KNN/final sorter interrupted with buffered candidates | Equivalent accepted-candidate reduction; breaking candidate if buffers lost | Timeout with candidates only in first heap, partly transferred, and all transferred; offset applied once |
| B16 | count/limit widths: reducer uses size_t/long long; QITR counters/limits include uint32_t | No narrowing or overflow; breaking if changed | Audit representable limits and summed totals before wiring qctx; synthetic boundary tests |
| B17 | SORTBY rejected candidate: processSearchReplyResult returns but caller continues iterating | Preserve traversal; unclassified if new early stop skips validation/metadata | Do not add sorted-input optimization during migration |

## Executed evidence

See the implementation evidence below. Tracked surfaces without executed evidence
remain compatibility obligations, not claims of validated equivalence.

## Implementation evidence

- PR 1 extracts reduction and callbacks into `src/coord/search_reducer.c`.
  Parsing, dispatch, registrations and the request destructor use its private
  interface. Source review found no behavior changes. An assert-enabled build
  and nine focused cluster tests passed at `6ccad8d1ef`.
- PR 2 uses standard SearchResult rows with request-local reply/row locators.
  Payloads and fields remain in MRCtx-owned reply trees; comparison and output
  decode borrowed views. This preserves RESP types without changing the Rust
  SearchResult layout. Comparison decode cost is unmeasured; no performance
  improvement is claimed.
- B16: pager counters use size_t; the advisory qctx limit saturates at UINT32_MAX.
  Search hit totals remain size_t in reducer metadata. Unit coverage exercises
  offset/limit above UINT32_MAX without allocating a huge result set.
- The source uses row ordinals rather than RESP2 physical offsets in its
  locators. The UINT32_MAX row guard exceeds the shard cap
  MAX_SEARCH_REQUEST_RESULTS; it does not narrow supported shard output.
  Physical offsets are computed as size_t. Classification: internal validation.
- Two new protocol/profile/ordering characterization tests passed against the
  extraction binary before RP replacement. The initial replacement passed these
  and twelve existing focused cluster tests; final-head verification follows.
- Local cluster validation requires RLTest's
  `--cluster_bus_port_protected_mode no` for this Redis binary. This is a local
  test setting, not a repository or product configuration change.

- PR 3 removes the custom KNN heap and wrappers. Optional nearest-K selection
  and final ordering both use the standard comparator sorter. The distance
  SORTBY shortcut, including DESC, still bypasses nearest-K preselection.
- B05: the legacy numeric sort-key preference in GET_NUMERIC_SCORE is preserved
  even when SORTBY names another numeric field. This refactor does not correct
  that selection behavior. KNN totals remain bounded by K and based on accepted
  rows for the preselection path.
- Two added KNN protocol/profile tests passed against the PR 2 binary before
  replacement, covering default order, distance ASC, another field in both
  directions, and pages within/beyond K.
- PR 3 replacement passed six ResultProcessor unit tests and eighteen focused
  cluster tests, including the new KNN characterization tests and eight
  deterministic coordinator reduction timeout/disconnect cases.
- PR 4 moves stored rows into QueryRequest.reply; common cycle teardown frees
  them before MRCtx releases backing. Serialization uses stack views with no
  temporary heap of custom row wrappers. Classification: internal ownership.
- Common blocked-client reply/timeout entry points now invoke per-cycle command
  adapters, including cursor cycles. EndCycle clears adapters before parking.
  This is shared dispatch, not new timeout policy or nonblocking strict handoff.
  Existing policy adapters and serialization remain command-specific.

- The first full C/C++ unit run after lifecycle integration passed 1,283 tests.
  A later rebuild removed the additional malformed-distance guard at maintainer
  direction; full behavioral and exact-source verification remain pending.

- Buffered-KNN timeout coverage uses the existing debug-only per-row reduction
  pause hook for KNN as well as ordinary rows. Production cancellation remains
  at shard boundaries. The test interrupts with one nearest-K candidate buffered,
  checks final membership/order and offset-once semantics, and observes one reply
  and one free callback in RESP2 and RESP3. Execution evidence follows validation.

## Final local validation

- Final code at `f024bb8d5f`: assert-enabled build and all 1,283 C/C++ unit
  tests passed. The focused three-shard suite collected 118 tests with zero
  failures, including both buffered-KNN strict-timeout tests.
- Full three-shard suite at `bb3691096b` (before the debug-only KNN pause hook):
  2,673 collected tests, zero failures, including skips.
- The initial standalone run lacked RedisJSON. All failed and skipped selectors
  were subsequently rerun with RedisJSON: 594 collected, zero failures. Selector
  accounting confirmed none was omitted; collected totals include skips.
- Independent source review passed each implementation layer and the buffered-KNN
  test follow-up. Remote CI and automated review remain separate gates.
- No intentional breaking or soft observable production behavior change is
  accepted by this stack. Unmeasured comparator decoding cost and the unexecuted
  edge scenarios in the tracked-surface table remain explicit limitations.
