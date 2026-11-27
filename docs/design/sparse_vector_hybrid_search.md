# Sparse Vectors in FT.HYBRID: Design and Plan

## Summary

Sparse vectors reach `FT.HYBRID` through the `VSIM` arm, as the API design recommends: the field type picks dense or sparse, and no new keyword is added. I propose three phases. Phase 1 ships on its own, phases 2 and 3 ship together, and all stay behind `ENABLE_UNSTABLE_FEATURES`, as sparse vectors already do.

| Phase | What users get | Size | Wire format change |
| --- | --- | --- | --- |
| 1. Sparse field in the single `VSIM` arm | Text + sparse, e.g. `SEARCH "shoes" VSIM @splade $s KNN 2 K 50` | Small: one branch where the `VSIM` arm becomes a query node, a sorter for `K`, the score | None |
| 2. N arms | Text + dense + sparse in one query, and `COMBINE ... WEIGHTS n w1 … wn` | Large: parser, request setup, EXPLAINSCORE, profile, coordinator cursor mapping | Shard cursor-mapping reply grows one pair per arm. The two-arm reply stays the same |
| 3. Optional `SEARCH` | Dense + sparse with no text arm | Small: code that assumes the mask always includes `SEARCH` | None beyond phase 2 |

The fusion layer already handles N inputs (`RPHybridMerger`, `HybridRRFScore`, `HybridLinearScore` take arrays). Most of the work is in the code around it, which hard-codes exactly two subqueries. The design gives each kind of arm a fixed slot, adds SPARSE_INDEX = 2, and passes a bitmap of the active slots, so that code loops over the active arms instead of naming two.

Decisions needed before phase 2: the open questions at the end, chiefly how `LINEAR` treats an unbounded `IP` sparse score.

## How FT.HYBRID works today

FT.HYBRID runs each arm as its own `AREQ` pipeline and fuses their outputs in `RPHybridMerger`. The request holds an array of arms, but exactly two are ever created, and code across the module indexes them as `SEARCH_INDEX` (0) and `VECTOR_INDEX` (1). As of master `87574d6aec`:

- **Parsing** (`hybrid/parse_hybrid.c`). `parseSubqueriesCount` rejects any count other than `HYBRID_REQUEST_NUM_SUBQUERIES` (2). It then parses one `SEARCH` and one `VSIM`. `MakeDefaultHybridRequest` creates both `AREQ`s before parsing starts.
- **The VSIM arm** (`applyVectorQuery`, `aggregate/aggregate_request.c`). It needs a `VECTOR` field ("Expected a VECTOR field"). It builds a `QN_VECTOR` root, and `SetFilterNode` attaches the `FILTER` to it.
- **Vector score.** `PLN_VectorNormalizerStep` adds `RPVectorNormalizer`. It reads the distance from the row and maps it into [0, 1] by metric: L2 `1/(1+d)`, IP `(1+x)/2`, COSINE `(2−d)/2`. The result is the arm's score.
- **Fusion** (`hybrid/hybrid_scoring.c`). Already N-ary: the merger takes `upstreams[]`, and both scoring functions take arrays of scores. `LINEAR` stores `linearWeights[numWeights]`, but its syntax is only `ALPHA`/`BETA`. `RRF` has no weights.
- **Sparse today.** A `@f:[SPARSE $q]` node is rejected in the `SEARCH` arm and in the `VSIM` `FILTER`. A `SPARSEVECTOR` field in `VSIM` fails the `VECTOR` field check.

Where the module assumes exactly two arms:

| Area | File | What is fixed at two |
| --- | --- | --- |
| Parser | `hybrid/parse_hybrid.c` | Count check, one `parseVectorSubquery`, params dict cloned once (one clone, one original) |
| EXPLAINSCORE | `hybrid/hybrid_search_result.c` | A `text` value and a `vector` value, `values[2]` |
| Shard replies | `hybrid/hybrid_exec.c` | Cursor reply `SEARCH <cid> VSIM <cid>`, error suffixes `(SEARCH)` / `(VSIM)` |
| Shard command | `coord/hybrid/dist_hybrid.c` | `MRCommand_appendVsim` copies from the first `VSIM` token. One `kArgIndex` for `SHARD_K_RATIO` |
| Coordinator sort | `coord/hybrid/dist_hybrid.c` | `setupCoordinatorArrangeSteps(search, vector)` reads `K` from a `QN_VECTOR` root |
| Cursor mapping | `coord/hybrid/hybrid_cursor_mappings.c` | Reply of exactly 6 elements; two read iterators `searchIt` / `vsimIt` |
| Profile | `hybrid_exec.c`, `dist_hybrid.c` | Per-shard keys `SEARCH` and `VSIM` |
| Search-only state | `module.c`, `hybrid_exec.c`, `dist_hybrid_plan.cpp` | Timeout cap flag, scorer explain and `LOAD` handling read `requests[SEARCH_INDEX]` |

## Arm slots and the active-arm mask

Each kind of arm gets a fixed slot, and the request carries a bitmap of the slots in use. `SPARSE_INDEX` = 2 joins `SEARCH_INDEX` (0) and `VECTOR_INDEX` (1). Bit 0 is the text arm, bit 1 the dense `VSIM` arm and bit 2 the sparse `VSIM` arm, so today's query has mask `0b011` and text + sparse has `0b101`.

All three bits set (`0b111`) is text + dense + sparse, the full hybrid, and the fusion, EXPLAINSCORE and mapping reply then carry three arms in slot order: `SEARCH`, dense, sparse. The mask code is written for any subset of the three from the start, so a three-arm query needs no further change to it. What phase 2 adds is parsing a repeated `VSIM` and the per-arm coordinator pieces.

**Passing it around.** Functions that take a (search, vector) pair take the request and the mask instead, and loop over the set bits in slot order. Slot order is also the arm order in the fusion, in EXPLAINSCORE and in the cursor mapping reply, so two-arm output does not change. The mask is internal: each node derives it from the arms it parses, and nothing new goes on the wire.

**What it replaces.** Kind comes from the slot, so no per-arm record is needed, and "at most one dense and one sparse arm" holds by construction: a second arm of the same kind finds its slot taken, and the parser rejects it. The cost is that a second dense arm (two embeddings) would need a fourth slot. That is one constant and one bit, and not part of this design.

**Which slot.** A `VSIM` arm lands in slot 1 or 2 depending on its field type, which is known only once `applyVectorQuery` resolves the field. The parser keeps each VSIM arm's request until then (two of them for a three-arm query), and `applyVectorQuery` stores it in the dense or sparse slot and sets the bit. Before field resolution nothing reads the VSIM arm by its slot: until then it is reached by its position after `SEARCH`, and the mask starts as `SEARCH` plus a dense `VSIM`, the only arms read by slot that early.

## Phase 1: a sparse field in the VSIM arm

A `VSIM` arm on a `SPARSEVECTOR` field becomes a sparse query node, and its `K` becomes a sorter limit. The command syntax, the shard command and every reply format stay as they are.

```
FT.HYBRID idx 2
  SEARCH "running shoes"
  VSIM @splade $s KNN 2 K 50 FILTER "@category:{footwear}" YIELD_SCORE_AS sparse_score
  COMBINE RRF 2 CONSTANT 60
  PARAMS 2 s <sparse_blob>
```

**Slot.** A `VSIM` arm on a `SPARSEVECTOR` field is stored in `SPARSE_INDEX` and sets bit 2, so text + sparse runs with mask `0b101`. The dense slot stays empty.

**Building the arm.** `applyVectorQuery` already resolves the field. On a `SPARSEVECTOR` field it builds a `QN_SPARSE_VECTOR` node instead of `QN_VECTOR`, binding `$s` the same way (`QT_PARAM_VEC`). The blob is then resolved and validated exactly as for `FT.SEARCH` (`prepare`: increasing ids, ids below `DIM`, finite weights, zero norm under `COSINE`).

**FILTER.** Dense KNN takes the filter as a child, so VecSim filters while it searches. A sparse node is joined to the filter with a plain intersection, as `RANGE` already is. This gives the exact top `K` among matching documents, the same result as `FT.SEARCH "@category:{footwear} @splade:[SPARSE $s]"`.

**K.** VecSim stops after `K` results, but a sparse union returns every candidate. The arm gets an arrange step sorted by score with limit `K`. `applyKNNTopKWindowConstraint` already keeps `K ≤ WINDOW`. `K` is stored in `ParsedVectorData`, so the coordinator reads it from there rather than from a `QN_VECTOR` root (this also fixes the TODO in `setupCoordinatorArrangeSteps`).

**Score.** Same route as dense. The sparse node yields its score into the hidden default field (`__splade_score`), or into the `YIELD_SCORE_AS` name. `PLN_VectorNormalizerStep` accepts a sparse field and picks a sparse mapping by metric (see the score scale section). No scorer is added: the VSIM arm has none today.

**Options.** The parser runs before the field type is known. It records which dense-only options it saw, and `applyVectorQuery` rejects them on a sparse field, naming the option:

| Option | Why it is rejected on a sparse field |
| --- | --- |
| `EF_RUNTIME` | HNSW parameter; sparse retrieval is exact |
| `SHARD_K_RATIO` | Trades recall for speed in the cluster; sparse stays exact |
| `FILTER ... POLICY` | Chooses how VecSim filters; a sparse filter is an intersection |
| `RANGE`, `RADIUS`, `EPSILON` | Sparse range queries are not in v1 |
| `RERANK` | Re-scores approximate candidates; sparse retrieval is already exact |
| `FILTER ... BATCH_SIZE` | Tunes VecSim's batched filtering; a sparse filter is an intersection |

**What stays rejected.** A `@f:[SPARSE $q]` node in the `SEARCH` arm or inside `FILTER`, so a query has one way to say "sparse arm". The error text changes to point at `VSIM @field`.

**Cluster.** Each shard returns its own top `K` by exact dot product. Sparse scores need no corpus statistics, so they compare across shards, and the coordinator's top `K` of the shards' top `K` is exact. The shard command copies the `VSIM` clause unchanged. A shard without this change fails the field check, so the query errors instead of returning wrong results. The mapping reply is the same six elements, read in mask order.

**EXPLAINSCORE and FT.PROFILE.** The vector line of EXPLAINSCORE names the sparse score and its mapping. The `VSIM` profile shows the `SPARSE_VECTOR` iterator, whose counters already exist.

## Phase 2: N arms

The leading count becomes the number of arms, and `VSIM` may repeat. In v1 a query has at most one dense and at most one sparse arm, as the API design states. The two-arm form is unchanged on the wire and in every reply.

```
FT.HYBRID idx 3
  SEARCH "running shoes"
  VSIM @embedding $v KNN 2 K 50
  VSIM @splade    $s KNN 2 K 50 FILTER "@category:{footwear}"
  COMBINE RRF 7 CONSTANT 60 WEIGHTS 3 1.0 1.0 0.5
  PARAMS 4 v <dense_blob> s <sparse_blob>
```

Phase 1 adds the sparse arm. Phase 2 lets the merger take all three arms at once, and in a cluster each arm is read from the shards by its own cursor.

**COMBINE WEIGHTS.** `WEIGHTS n w1 … wn` gives one weight per arm, in command order, and `n` must equal the arm count. `LINEAR` already stores a weight array, so `ALPHA`/`BETA` become another spelling of two weights, and the two cannot be mixed. `RRF` gains weights: score = Σ wᵢ / (CONSTANT + rankᵢ), with every weight 1 by default, so today's results do not change. `WEIGHTS` is useful with two arms too, so it can ship first.

**Request setup.** The handler creates the hybrid request with the three slots empty and an empty mask. Each parsed arm fills its slot and sets its bit, and the leading count is checked against the number of bits set when parsing ends. The old form with no count still means two arms. Every arm gets its own clone of the `PARAMS` dict.

**Arm identity.** The slot is the kind, so the `SEARCH_INDEX` / `VECTOR_INDEX` pair gives way to the three slots and the mask (see the arm slots section), not to a per-arm record. The one-dense-one-sparse rule is checked as each arm takes its slot.

**Labels.** With one `VSIM` arm, labels stay `SEARCH` and `VSIM`, so profile output, warnings and error suffixes do not change. With two, each `VSIM` label names its field: `VSIM @embedding`, `VSIM @splade`.

**Merger and EXPLAINSCORE.** The merger and scoring functions already loop over `numUpstreams`. EXPLAINSCORE (`hybrid_search_result.c`) replaces its fixed text and vector values with one line per arm.

**Coordinator.**

- *Shard command.* The parser records each arm's argument range, and the command builder copies each `VSIM` clause from it. Today it finds only the first `VSIM` token. `SHARD_K_RATIO` and its single `kArgIndex` stay on the dense arm, since there is at most one.
- *Cursor mapping reply.* Today it is `["SEARCH", cid, "VSIM", cid, "warnings", [...]]`, exactly 6 elements. It becomes one label/cursor pair per active arm, in slot order, then `warnings`, and the reader takes pairs until `warnings`. Two arms produce the same 6 elements as today.
- *Read iterators.* `HybridArmingCtx` holds an array of `MRIterator`s instead of `searchIt` and `vsimIt`, with one `IORuntimeCtx_RequestStarted` each. Warnings go to an arm by its label, no longer by matching the `(VSIM)` suffix.
- *Sorting.* `setupCoordinatorArrangeSteps` loops over the arms in the mask: limit `WINDOW` for `SEARCH`, `min(K, WINDOW)` for a KNN arm.
- *Debug timeouts.* `TIMEOUT_AFTER_N_VSIM` applies to every `VSIM` arm, and an arm-indexed variant is added for tests.

## Phase 3: any arm optional

With `SEARCH` optional, dense + sparse is two `VSIM` arms. That is the most common pattern among Qdrant, Milvus and Pinecone users, and it adds no dummy `SEARCH "*"` arm to the fusion.

```
FT.HYBRID idx 2
  VSIM @embedding $v KNN 2 K 50
  VSIM @splade    $s KNN 2 K 50
  PARAMS 4 v <dense_blob> s <sparse_blob>
```

Phase 2 already handles a mask with the SEARCH bit clear, so what is left is the state that only the `SEARCH` arm carries today:

- The foreground-timeout cap flag (`QEXEC_S_MAX_TIMEOUT_CAPPED`) is set on `requests[SEARCH_INDEX]`. It moves to the hybrid request.
- The scorer name for EXPLAINSCORE and `LOAD` step placement read the search plan. They read the SEARCH slot's plan, or skip it when its bit is clear.
- The tail's language and `IndexSpec_IsCoherent` prefix checks read the search request. They move to the hybrid request too.

Every arm is optional, so any non-empty subset of the three is a valid query. A lone arm is accepted too, and its fusion is just that arm's ranking (say if it should be an error instead). This ships in the same PR as phase 2, since the mask already handles any subset.

## Score scale for LINEAR

`RRF` uses ranks only, so a sparse arm fuses correctly as it is. `LINEAR` adds raw scores, and a sparse `IP` score has no upper bound. A SPLADE dot product is not confined to [0, 1], so it can outweigh a dense score in [0, 1] unless the weights compensate for each query's scale.

For dense vectors `RPVectorNormalizer` maps the distance into [0, 1] by metric. The sparse arm reuses that step with its own mapping:

| Sparse metric | Raw score | Mapping | Result |
| --- | --- | --- | --- |
| `COSINE` | Cosine similarity in [−1, 1] ([0, 1] for SPLADE, whose weights are non-negative) | `(1 + c) / 2`, the same as dense `COSINE` | [0, 1], comparable to a dense arm |
| `IP` | Dot product, unbounded | Open: see the options below | Depends on the option |

Options for `IP`:

1. **Raw score, documented.** Simplest. `LINEAR` users must pick weights to match the scale, or use `COSINE` or `RRF`.
2. **Divide by the arm's best score in the window.** `RPMaxScoreNormalizer` already does this for `BM25STD.NORM`. The result is in [0, 1], but it depends on the query, and in a cluster on the shard unless the coordinator normalizes after the merge.
3. **Min-max over the window**, like the Elasticsearch `linear` retriever's `minmax`. Same trade-offs as option 2.
4. **Reject ****`LINEAR`**** on a sparse ****`IP`**** arm.** Safe but restrictive.

I recommend option 1 for phase 1, since `RRF` is the default and needs nothing. Option 2 can come later as a `COMBINE LINEAR` normalizer setting if users ask for it. Open question 3 asks product to confirm.

## Compatibility and rolling upgrade

No two-arm dense query changes in its syntax, its results, its shard command or any reply. Every new form fails cleanly against an older node instead of returning wrong results.

- **Gate.** A sparse field can be created only with `ENABLE_UNSTABLE_FEATURES`, and since the flag can be turned off later, a `VSIM` arm on one checks it on every query, as `[SPARSE $q]` does in `FT.SEARCH`. The N-arm count, a repeated `VSIM` and `WEIGHTS` are also new surface, so they go behind the same gate until they graduate.
- **New coordinator, old shard, phase 1.** The old shard rejects a `SPARSEVECTOR` field in `VSIM` ("Expected a VECTOR field"), and the coordinator returns that error.
- **New coordinator, old shard, phase 2.** The old shard rejects a count other than 2 ("FT.HYBRID currently supports only two subqueries"). A two-arm query goes out in the old form, with the same 6-element mapping reply.
- **COMBINE forwarding.** The coordinator always forwards a legacy counted `COMBINE` so all shards use the same window. Fusion happens on the coordinator: shards only return each arm's cursor, and nothing on the shard reads the weights. So RRF `WEIGHTS` are left out of the forwarded clause, and two LINEAR weights go out as `ALPHA` and `BETA`. Old shards then accept two-arm weighted queries.
- **Persistence.** None. Nothing in this design touches RDB or the index format.

## Plan

One row is one PR. A row starts once the rows it depends on have merged, and each PR is done when its tests are green in standalone and, where marked, in cluster.

| # | PR | Depends on | Main files | Tests |
| --- | --- | --- | --- | --- |
| 1 | Arm slots and the sparse `VSIM` arm, standalone and cluster: `SPARSE_INDEX` and the arm mask for any subset of the three slots, node construction, `FILTER` intersection, `K` sorter, yielded score through the normalizer, dense-only option rejections, EXPLAINSCORE line, `K` read from `ParsedVectorData`, no `SHARD_K_RATIO`. Adds this design document as `sparse_vector_hybrid_search` | — | `aggregate_request.c` (`applyVectorQuery`), `parse_hybrid.c`, `hybrid_request.[ch]`, `pipeline_construction.c`, `vector_normalization.h`, `hybrid_search_result.c`, `coord/hybrid/dist_hybrid.c`, `docs/design/sparse_vector_hybrid_search.md` | `test_sparse_vector.py`: RRF and LINEAR results equal a Python reference fusion over brute-force scores; `FILTER`; `K < WINDOW`; one error per rejected option; `YIELD_SCORE_AS`; `FT.PROFILE` shows `SPARSE_VECTOR`. Replace `testHybridRejectsSparseQueries`, keeping the `SEARCH` and `FILTER` rejections. Cluster flow tests: same results as standalone over the same data; profile per shard |
| 2 | `COMBINE ... WEIGHTS n`: weighted `RRF`, `LINEAR` with `n` weights, `ALPHA`/`BETA` kept | — | `hybrid/parse/hybrid_combine.c`, `hybrid_scoring.c`, `dist_hybrid.c` (forwarded `COMBINE`) | Parse errors (count mismatch, mixing with `ALPHA`/`BETA`); weighted scores against a reference; unweighted results unchanged; cluster |
| 3 | N arms, any of them optional, standalone and cluster: per-arm `AREQ`s and params, arm count checked against the mask, labels, one-dense-one-sparse check, N-line EXPLAINSCORE, profile, debug timeouts; per-arm shard clauses, pair-based mapping reply, an array of read iterators, warnings by label; the timeout cap flag, scorer name, LOAD placement and tail checks move to the hybrid request | 1, 2 | `parse_hybrid.c`, `hybrid_request.[ch]`, `hybrid_exec.c`, `hybrid_search_result.c`, `module.c`, `dist_hybrid.c`, `hybrid_cursor_mappings.c`, dist_hybrid_plan.cpp | Text + dense + sparse against a reference fusion, standalone and cluster; two dense arms rejected; dense + sparse with no text arm, and every other subset; timeout in each arm; old-shard error on a three-arm count; two-arm profile, EXPLAINSCORE output and mapping reply unchanged |

PRs 1 and 2 can proceed in parallel. Most of the risk is in PR 3, where the coordinator's replies, read iterators and timeouts change. PR 1 puts the mask in place, so PR 3 only adds arms.

The MS MARCO SPLADE + dense benchmark and a `LINEAR` sparse `IP` normalizer (open question 3) are not part of this stack. They come later.

## Open questions

1. **Three arms in v1, or any two?** (API design Q1.) Recommendation: build for N in phase 2, but ship phase 1 first. Text + sparse then works early, without waiting for the coordinator changes.
2. **Is ****every arm**** optional?** (API design Q2.) Decided: yes, `SEARCH` included, and a query needs at least one arm. It ships with phase 2, in PR 3. Dense + sparse is the common pattern elsewhere, and `SEARCH "*"` would put a meaningless arm into the fusion.
3. **How does ****`LINEAR`**** treat a sparse ****`IP`**** score?** Recommendation: raw and documented for now, a max-score normalizer later if needed (see the score scale section).
4. **Is weighted ****`RRF`**** wanted?** The API design's example uses `WEIGHTS` with `RRF`. Please confirm the formula Σ wᵢ / (CONSTANT + rankᵢ).
5. **Should ****`@f:[SPARSE $q]`**** stay rejected in the ****`SEARCH`**** arm?** Recommendation: yes. It keeps a single way to write a sparse arm, and keeps `SEARCH` meaning text.
6. **How are two ****`VSIM`**** arms named in ****`FT.PROFILE`**** and errors?** Proposed: `VSIM @<field>`, used only when there are two `VSIM` arms. With fixed slots the slot could name the arm instead (dense or sparse), but the field name is what a user can map back to their schema.
