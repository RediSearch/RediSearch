# Behavior delta ledger

This is the authoritative ledger for observable changes. Implementation evidence is recorded below. Source observations alone are not
executed differential tests.

## Final review map

The tracked-surface table records the original audit; the implementation evidence
and approved corrections below record the final disposition. In the final stack:

- Internal changes: standard RPs, source-owned reply trees, atomic publication,
  direct selected-row serialization and shared stored-result cleanup.
- Preserved: reply types/precision, binary data, tie rules, missing-key placement,
  counts, pagination, profile snapshot consistency and shard-boundary cancellation.
- Approved observable corrections: B05/B06 nearest-K membership, B04 typed-field
  comparison alignment, and immediate malformed-reply error precedence (B11).
  These are not described as soft changes.
- Deferred: reply-buffer/Drain integration, per-row cancellation and nonblocking
  strict-timeout handoff. No new public timeout completeness guarantee is claimed.

Reply backing remains alive through request cleanup, as in existing master/8.12
coordinator SEARCH. Lower retained memory in the intermediate eager-conversion
implementation was not a property of the original implementation.

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

## Initial local validation before approved alignment

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
- At those revisions no intentional breaking or soft observable production behavior
  change was accepted. The approved follow-up below supersedes that scope. Unmeasured comparator decoding cost and the unexecuted
  edge scenarios in the tracked-surface table remain explicit limitations.

## Approved alignment follow-up (supersedes initial compatibility choices)

- Sorting now uses standard typed lookup values and standard score/field RPs.
  Explicit document-key secondary fields preserve score ties, direction-dependent
  SORTBY ties, and nearest-K distance ties. Missing sort keys remain absent lookup
  values, preserving missing-last placement in both directions. No custom
  comparator API remains in the final stack. Comparison performs no reply decoding
  or numeric parsing; source decoding adds owned lookup values with borrowed strings.
  Performance evidence is recorded in the final validation section.
- B05: nearest-K selection now reads the required vector distance even when final
  SORTBY is another numeric field. Previously a numeric sort key could replace the
  distance and change global K membership. This is an approved observable
  correctness fix, not classified as soft. Regression tests sort by numeric price
  opposite to distance in both directions, with profiles, pages and WITHSCORES.
- B06: distance DESC now selects global nearest K before descending final order.
  Previously it could select the farthest candidates from the union of shard-local
  nearest-K sets. This is an approved observable membership correction. ASC keeps
  its equivalent shortcut; count-only distance queries keep their metadata totals.
  Required-field construction reuses the distance sort field rather than asking
  for it twice in the new DESC path.
- B04: encoded numeric/string values are normalized to RSValue numbers/strings;
  heterogeneous values consequently follow standard RP coercion/error semantics
  rather than comparing wire type prefixes. This is an explicit alignment, not
  a claim of byte-for-byte ordering compatibility for heterogeneous shard values.
  Numeric, string, missing-key and document-key tie cases are covered; a public
  mixed-type SORTBY scenario has not been established by these fixtures.
- B07: count-only behavior remains path-specific: preselection paths count returned
  candidates, while distance-sort count-only paths use capped shard metadata.
  No general count-only normalization is included in this change.
- Updated six RESP2/RESP3 characterization and buffered-timeout tests pass on the
  revised PR 3 code; broader final-stack validation follows.

## PR 4 simplification

The per-cycle callback pointers and reply/timeout dispatch wrappers were removed.
They did not share execution or policy logic. The existing Redis callbacks remain
registered directly; QueryRequest.reply owns stored rows and the existing common
free callback handles teardown. Serializer stack views remain. No callback-policy
or timeout-handoff behavior change is introduced.

- The expanded RESP3 WITHSORTKEYS regression reproduced a parse error before the
  follow-up decoder fix. When the vector distance is also the sort field, decoding
  now reuses the normal sort-key view (which handles both top-level sortkey and
  required_fields). It never substitutes another field's numeric value. All six
  protocol/order/KNN/buffered-timeout tests pass with search/profile WITHSORTKEYS
  coverage after the fix.

## PR 4 reply ownership and admission

- Internal: the source RP is created before dispatch and owns accepted shard
  replies immediately. Search uses a smaller request-owned transport context
  instead of MRCtx; other commands retain generic map/reduce.
- Internal: stored rows and downstream processors are freed before their source
  backing. The blocked-client free callback still runs only after terminal
  completion; timeout publication does not free pending transport state.
- Soft timing boundary: reply acceptance and timeout closure are serialized.
  Strict timeout sees a stable collection of fully accepted replies. A callback
  racing closure can be included or excluded depending on lock acquisition;
  socket arrival time never guaranteed inclusion. No new public ordering, count,
  error-precedence or timeout-policy contract is introduced.
- No streaming reduction or new BG/main handoff is introduced. Empty-dispatch
  completion now schedules the ordinary reducer, which reports the same cluster
  dispatch error, rather than relying on the reply callback to construct it.
- Deterministic RESP2/RESP3 tests pause before or after the first admission,
  force strict timeout, inspect search/profile results, and verify that freeing
  waits for the outstanding callbacks and happens exactly once.

Local validation of the ownership follow-up: assert-enabled build and all 1,282
C/C++ unit tests passed. The 118 existing focused coordinator tests passed.
Both new admission-boundary tests pass after isolating the lifetime counter from
local shard worker callbacks; each covers search/profile before and after first
admission. Full-suite results are tracked separately.

Full ownership-follow-up validation: the standalone suite collected 2,696 cases
and the three-shard cluster suite collected 2,679, both with zero failures
(collected totals include skips). The cluster run includes both zero-dispatch
regressions added at `ba0ad1fcad`, checking the cluster error, exactly-once source
cleanup, and a successful next query. Saved local logs are
`/tmp/coord-own-full-standalone.log` and `/tmp/coord-own-full-cluster.log`.

## PR 4 atomic publication simplification

The admission mutex and closed flag are replaced by fixed-capacity storage and
a single-producer atomic published count. Capacity comes from the assigned IO
runtime's topology before dispatch, not the earlier main-thread shard estimate.
The reducer reads one immutable prefix with acquire semantics; the IO callback
publishes completed slots with release semantics. A zero snapshot avoids reading
the array pointer before allocation. Cleanup frees the final published prefix,
which can exceed the timeout snapshot when a callback was already in progress.

This preserves the soft race boundary: replies published after the snapshot are
excluded from results and profiles, but remain owned until completion. There is
no new public timeout or ordering guarantee. Deterministic tests pause between
slot initialization and publication with both zero and one earlier replies, in
addition to the existing before/after acceptance cases, for both protocols and
search/profile commands.

## Borrowed rows, direct serialization and immediate parse errors

- **Internal:** replaces the intermediate eager row conversion with source-owned
  complete replies. Candidates borrow document/sort bytes and retain a shard/row
  locator. Only sorter projections are RSValues; selected rows serialize directly
  from MRReply, including payloads, fields and explanations. Discarded candidates
  incur no returned-field conversion. Stored rows and both sorters are freed before
  their source. No new user-facing behavior is intended by this ownership change.
- **Preserved:** RESP2/RESP3 types, binary strings, numeric-looking strings, nested
  JSON STRING/EXPAND output, score explanations, missing sort keys, pagination,
  profiles and the published timeout snapshot. Serialization reuses the parsed
  candidate score and does not reparse numeric sort keys.
- **Integration boundary:** the serializer matches #11365's SerializeResult callback;
  this base still invokes it from the existing reply loop. Buffered publication and
  main-thread Drain integration remain dependent on the reply-buffer/Drain stack.
- **Intentional observable error change:** an encountered malformed row or
  required envelope field returns `RS_RESULT_ERROR` and sets QueryError
  immediately. Previously the reducer continued and checked a deferred flag;
  a tolerated shard timeout/OOM or coordinator timeout could suppress that
  error. Now the parse error wins over partial results under RETURN and
  RETURN-STRICT too. Treat this as a breaking error-precedence change, explicitly
  approved for alignment with the generic buffered-reply behavior in #11365.
  Successful replies and shard timeout/OOM policy handling are unchanged.
- **Deferred:** strict cancellation remains at shard boundaries. Per-row
  cancellation belongs with Drain continuation through the reply source and
  both KNN sorters; switching the current sorter to yielding on timeout cannot
  preserve the remaining candidate set by itself.

## Final validation and performance evidence

The direct-row implementation at `b863eb4c8` passed CI run
[40699837625](https://github.com/RediSearch/RediSearch/actions/runs/40699837625),
including unit, standalone/coordinator flow, sanitizer, coverage, lint, Miri and
Rust Valgrind lanes. Independent review of that head reported zero findings.

The comparison revisions are borrowed rows `765b056fe`, eager RSValues `0d5e04c4f`,
and direct serialization `b863eb4c8`. The harness is preserved separately at
`f292683b2`, outside the implementation stack.

The completed local replay and allocation comparisons each contain 1,296 trials
(six repetitions, three variants). Direct serialization versus eager conversion
reduced replay pipeline CPU by a median 40.8% across configurations; versus the
previous borrowed-row revision, the median reduction was 3.7%. These are descriptive
medians, not production workload-weighted speedups. One small-result configuration
was about 1.3% slower than the previous borrowed version.

For 40,000 wide candidates (8 shards, RESP3), pipeline allocation calls excluding
parsing were 80,066 for eager conversion and 52 for direct serialization. Whole-request
tracked peak was about 425 MB for both. Direct retained about 400 MB after reduction,
versus 29 MB for eager conversion: avoiding conversion churn does not mean releasing
reply trees earlier. The tracker excludes Redis core reply buffers and is not RSS.

The end-to-end run was intentionally stopped to release the machine. Its partial
comparison uses only the two complete repetitions (408 matched trials); 75 trials
from the incomplete repetition are excluded. Throughput was mostly flat against
borrowed rows, with improvements over eager conversion most visible for deep offsets
and KNN. Two repetitions do not establish precise speedups or tail-latency equivalence.
No comparison against original master was run. These benchmarks do not exercise
strict-timeout handoff and do not replace the functional lifecycle tests.
