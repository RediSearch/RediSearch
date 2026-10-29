# Design

## Final review scope

The implemented stack uses source-owned shard replies, standard sorters and direct
selected-row serialization. The final ownership contract is described under
"Implemented representation and callback boundary" below. Reply trees remain alive until
request cleanup, matching the existing coordinator SEARCH lifetime; the intermediate
eager RSValue conversion is not part of the final design.

The baseline map and staged migration discussion below explain the starting point.
Approved corrections supersede its initial compatibility choices: numeric final
SORTBY cannot replace KNN distance; distance DESC selects nearest K first; typed
comparison follows standard RPs; encountered parse errors are immediate. The behavior
ledger records their classifications. Shared reply-buffer and Drain integration
remain outside this stack.

## Baseline and scope

Source baseline: local master `833b7252ced78b33e10b5dd6150dba5321af9e42`.
This is a source audit, not runtime characterization. The earlier discussion
examined `ce5b78d4bc`; the relevant reduction and lifecycle code was checked against
this baseline before drafting. Do not infer current remote-master state from it.

Start with an array-backed source, existing comparison semantics, and bounded
sorters. Preserve blocked-client lifetime and BG/main reduction arbitration. Do not introduce streaming
fanout, a k-way merge, a general-purpose remote document subsystem, or a new
publication protocol in the first implementation.

## Source and ownership map

Symbols below are in `src/module.c` unless another file is specified.

| State | Current owner and purpose | Target |
| --- | --- | --- |
| `searchRequestCtx` (`src/module.h`) | QueryRequest, held args, MR context, weak index ref, options and reduction state | Retain concrete request; add pipeline/lookup state as needed |
| `searchResult` | Heap-owned wrapper; key, fields, payload, explanation and sort key borrow MRReply data | Standard SearchResult with explicit output-data lifetime |
| `searchReplyOffsets` | RESP2 layout; getReplyOffsets also mutates special-case offsets | Source decoder layout, initialized once |
| `searchReducerCtx` | Heap, cached row, errors, counts, warning, decode callbacks | Split among source, sorter and request reply state |
| `specialCaseCtx` (`src/coord/special_case_ctx.h`) | SORTBY/KNN planning mixed with runtime state | Keep parsing data initially; move heap ownership to RP |
| `knnContext` | Owns query node; borrows distance-field name; contains selection heap | Preserve query-node lifetime; RP owns selection heap |
| `sortbyContext` | Owns sort-key name; stores direction and wire offset | Sort/decoder configuration |
| `scoredSearchResultWrapper` | Heap-owned result wrapper plus distance | Hidden distance lookup value |
| `cachedResult` | Recycles evicted/rejected wrappers | Standard RP pooled row |
| `processReply`, `postProcess` | Select ordinary/KNN processing; flush KNN heap at exit | Explicit source/selection/sort chain |
| `MRCtx` (`src/coord/rmr/rmr.c`) | Owns replies; refcount spans request, fanout and reducer | Search uses request-owned MRSearchCtx for transport; the source RP owns replies |
| `PrintCoordProfile_ctx` | Coordinator timing envelope | Retain existing profile presentation initially |
| `fieldNames`, request `reducer` | Apparently unused state in this reduction path | Verify all references before removing |

`QueryRequest_OnFree` already ends search cycles through the generic callback.
`QueryRequest_Free` selects the concrete destructor. The missing alignment is
pipeline cleanup, result storage, serialization and policy callbacks, not a new
request ownership scheme.

## Path map

Let W be offset + limit. These are conceptual pipelines, not committed new APIs.

| Existing path | RP translation | Compatibility detail |
| --- | --- | --- |
| Ordinary score ordering | ReplyArray -> ScoreTopW -> Pager | Preserve binary document-key tie break |
| SORTBY | ReplyArray -> FieldTopW -> Pager | Preserve missing keys, mixed types and ASC/DESC ties |
| KNN without distance SORTBY | ReplyArray -> NearestK -> FinalTopW -> Pager | Membership selection precedes final ordering |
| KNN with distance SORTBY | ReplyArray -> existing distance-order selection -> Pager | Preserve existing shortcut, including DESC; do not replace with assumed equivalent nearest-K behavior |
| LIMIT 0 0 | Process metadata/counts without a row heap | KNN count must follow existing path, not assumed row count |
| FT.PROFILE | Source unwraps Results; retain profile envelope | Preserve shard profiles and coordinator timing names |
| Early OOM/empty reply | Common empty reply path after compatibility check | Preserve shape and policy |
| Fatal shard error | Envelope/error scan before result publication | Preserve precedence over successful rows |

RESP2 offsets and RESP3 maps are decoder variants, not separate downstream
pipelines. NOCONTENT/RETURN 0, WITHSCORES, EXPLAINSCORE, WITHPAYLOADS,
WITHSORTKEYS, RETURN and FORMAT configure decoding/output. Required sort/distance
fields remain hidden from output. Debug dispatch uses the same reduction chain.

Preserve `prepareCommand`: LIMIT rewriting, injected WITHSCORES, required fields,
SHARD_K_RATIO, index prefixes, slot information and dispatch timing. These are
outside the reduction migration.

## Smallest implementation boundary

First extract the existing reducer/decoder/comparator functions into a coordinator
implementation file with a narrow request-facing interface. Keep parsing,
dispatch and callback registration in module.c. Move code without changing
comparators, cancellation checkpoints, serialization or heap capacities. This
provides a reviewable baseline and a home for source tests without mixing in RP
semantics. Check both root and coordinator CMake source collection.

Next introduce the source and sorter behind that interface. Reuse RPSorter heap
and ownership machinery where possible, but do not assume its by-fields
comparator matches cmp_results. A small coordinator comparator is preferable to
changing successful-query ordering or duplicating the entire sorter lifecycle.

## Row representation decision to settle before RP migration

SearchResult already carries score and lookup-row values. Hidden lookup keys can
carry document key, distance and encoded sort key. The remaining question is how
to preserve the output attributes, payload and explanation with minimal changes.

Start by evaluating request-owned MRReply backing plus hidden lookup values,
retaining the existing serializer as a temporary adapter. That can avoid a
cross-language SearchResult layout change during the first migration. Any
borrowed value must outlive all sorter/stored results, including early exits.
Free those results before releasing their backing replies.

For common serialization, prefer a small output accessor/adapter over pretending
remote rows have local document metadata. The local serializer currently requires
RSDocumentMetadata and an RSScoreExplain tree. Determine whether existing owned
RSValue types preserve every output type before choosing conversion. RPNet's
MRReply_ToValue reconstructs the shard's numeric RSValues; serialization must use
the same STRING/EXPAND flags. Do not both consume reply nodes and leave borrowed
users outside the owning row.

Acceptance of the RP migration requires a concrete row-lifetime test and a plan
to remove the temporary serializer adapter; wrapping the old monolithic reducer
in one RP is not completion.

## Metadata and errors

Counts, warning selection, format and shard profile data belong to request reply
metadata. The source should process each envelope once. Preserve ordinary sums
and the two existing KNN count paths in knnPostProcess; never derive hit counts
from the final page or final W-sized heap.

Preserve error pre-scan and should_return_error behavior initially, including its
current OOM-policy lookup. Changing that lookup to the request snapshot is a
separate ledger entry. Preserve first-nonempty warning selection initially.

## Timeout and Drain integration boundary

The user's Drain API stack owns Drain and the future strict-timeout handoff.
This refactor owns four PRs ending with common lifecycle/callback integration.
Neither stack has a prescribed merge order:

- If this refactor lands first, preserve the current timeout protocol and expose
  the source/sorter/pager composition for the Drain stack to integrate.
- If the Drain API lands first, implement its established contract for the new
  RPs as they are introduced; do not restore obsolete compatibility machinery.

The second stack to land carries the minimal integration adjustment and combined
tests. Agree its exact placement once both diffs exist. Record resulting behavior
differences in the ledger regardless of which stack carries the code.

The baseline ResultProcessor has Next and Free, not a public Drain method.
Pipeline_DrainStoredResultsAfterTimeout is a transitional Next-based helper.
Do not label existing helpers as the completed Drain API.

Initially retain the claim/signal/wait protocol and existing accepted reply set.
Decoding one row per Next need not introduce a new timeout check per row; that is
a separately classified behavior change.

Future Drain must handle ReplyArray -> NearestK -> FinalTopW composition: buffered
nearest-K rows must reach the final sorter before it finalizes output. Switching
both sorters directly to yield can lose buffered candidates. Pager state must
survive handoff without applying offset twice.

Future strict handling must distinguish cancellation, exclusive pipeline
ownership transfer, and response publication. An atomic timeout marker is not
ownership transfer. The main thread must not drain heaps concurrently with BG,
and an IO-owned mutable replies pointer/count is not a safe published snapshot.
Do not redesign either protocol as part of extraction.

## Alternatives

- Reuse RPNet unchanged: its aggregate cursor transport and wire schema differ;
  share small decoding helpers only where contracts match.
- Rewrite through distributed FT.AGGREGATE: changes shard commands and planning,
  greatly widening the compatibility surface.
- New remote row hierarchy or generic comparator framework: defer unless the
  minimal adapter cannot satisfy ownership and serialization requirements.
- Fix suspicious KNN/count/warning behavior while moving code: separate change
  with its own classification and tests.

## Implemented representation and callback boundary

The source owns complete shard replies from arrival through request cleanup. Candidates
contain borrowed document/sort strings, numeric sort projections, score and a packed
shard/row locator in SearchResult.docId. Neither decoding nor serialization detaches
reply fields. Returned fields, payloads and score explanations are never converted
into intermediate RSValues. No SearchResult FFI layout change is needed.

QueryRequest.reply owns emitted candidate descriptors. Request cleanup first clears
stored candidates, then frees downstream processors before the reply source through
QITR_FreeChain. This ordering keeps borrowed strings and locators valid through normal
serialization, timeout serialization and error cleanup. The source also retains late
publications until transport completion; only the acquired snapshot is serialized.

CoordSearch_SerializeResult has the SerializeResult callback signature introduced by
#11365 and writes selected rows directly from MRReply into RedisModule_Reply. The current
base predates Pipeline_SerializeResults, so the existing reply loop invokes that callback.
After #11365 is available, the callback can run in the common buffered-serialization loop;
that integration must route coordinator-search pipelineEnd and preserve exclusive Drain
ownership through both KNN sorters. It is not implemented by copying the unmerged common
loop into this stack. Shard profiles remain in the same source-owned replies.

Blocked-client registration continues to use the existing command callbacks.
No extra dispatch wrapper or per-cycle callback fields are needed. The shared
QueryRequest free callback already performs cycle teardown. Actual timeout-policy
unification remains part of the Drain integration once exclusive pipeline
ownership transfer is defined.

The final reducer uses private typed lookup fields for document key, sort key,
and vector distance. Standard score/field sorters compare those values without
re-decoding replies. Document-key secondary fields preserve coordinator ties;
missing values remain absent. Vector distance always determines nearest-K
membership, including when final sorting is numeric or distance DESC. See the
behavior ledger for the approved observable corrections and count-only exception.

## Reply admission and search transport

Create RPSearchReply before dispatch, independently of reducer initialization.
MRSearchCtx owns the command and IO completion counters, and borrows the request.
Each non-null shard callback transfers its reply to the source, which either
accepts it or frees it after cancellation. Before dispatch, the assigned IO
thread preallocates the reply array to its topology's shard count, an upper bound
on successful sends. That thread is the only producer: it initializes a slot
then release-stores the published count. The array is never resized.

The exclusive reducer acquire-loads the published count once and retains that
snapshot for reduction and profile serialization. A zero snapshot never reads
the array pointer, which may not yet be allocated; a nonzero acquire publishes
both the array pointer and the initialized prefix. The producer may finish
publishing a reply after timeout takes its snapshot. Cleanup uses the final
published count after all callbacks finish, including replies outside the
reduction snapshot. No admission mutex or closed flag is needed.

The blocked-client cycle owns both source and transport. Preparation failure
and zero-dispatch completion still finish the cycle. Otherwise the last shard
callback either schedules reduction or unblocks after timeout; the reducer
unblocks as its final access. No path touches the request or transport after
its terminal handoff. No additional reference count or detach protocol is needed.

Timeout publication does not end that lifetime: outstanding callbacks continue
accounting for completion and discard late replies. The existing claim/wait
protocol still excludes simultaneous BG/main reduction; the future abort/drain
handoff remains in the Drain stack. A reply is available if its admission
was included in the acquired publication count, not merely because its bytes
reached the socket.
