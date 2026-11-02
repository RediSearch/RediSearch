# Behavior delta ledger

This is the authoritative ledger for observable changes. No implementation delta
has landed yet. Source observations are not executed differential tests.

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

None yet. The first extraction is intended to change only B01. Other entries are
compatibility obligations or candidate follow-ups, not claims of known deltas.
