# PRD Addendum: Decisions & Clarifications

> Companion to the [Enhanced TOLIST PRD](./enhanced-ft-aggregate-tolist.md).
> Items below are either unspecified in the original PRD or need an explicit decision before implementation.

---

## 1. Response Format: Hash vs JSON

The PRD says "Array of complete documents (JSON)" but all examples use JSON documents only.

**What must be specified:**

- **JSON documents** — `LOAD *` loads the entire document as a single `RSValue_Map` under the `"$"` key. `TOLIST *` returns this map directly. RESP3: Map. RESP2: flat key-value array. Consistent with how `FT.SEARCH` returns JSON content today.

- **Hash documents** — `LOAD *` loads each field as a separate value in the pipeline row. `TOLIST *` must assemble these into a flat key-value representation per document. RESP3: Map. RESP2: flat key-value array. No nesting (Hashes are inherently flat).

- **The response format should match `FT.SEARCH` document serialization** — not raw JSON strings. The inline JSON in the PRD examples is conceptual; wire format follows standard RESP serialization.

**Decision needed:** Confirm that RESP serialization follows existing `FT.SEARCH` conventions for both document types. Update PRD examples with a note clarifying wire format.

---

## 2. Explicit `LOAD` Requirement for `TOLIST nargs * ..`

The main PRD does not state that `LOAD *` (or explicit `LOAD @f1 @f2 ...`) is mandatory before `TOLIST *`. The review doc (T2) mentions it, but it's not in the specification.

**Decision needed:**

- `TOLIST *` **requires** a preceding `LOAD *` or explicit field loading. No implicit full-document load from within the reducer.
- If omitted, fail at parse time with a clear error message.
- Sort fields used in `SORTBY` must also be available in the pipeline (loaded or in schema). If unavailable, fail at parse time.
- This applies to single-field `TOLIST @field` with `SORTBY @other_field` as well — `@other_field` must be loaded.

**Rationale:** Implicit loading would be a hidden performance trap. Failing fast avoids silent empty results and helps users debug their queries.

---

## 3. Tie-Breaking

When multiple documents share identical sort keys within a group, a tie-breaker determines ordering. This is user-visible behavior and must be specified.

**Decision needed:**

| Context | Tie-breaker | Deterministic? |
|---------|-------------|---------------|
| Standalone / Shard | Doc ID (`t_docId`) | Yes |
| Coordinator | Arbitrary (first-seen from shard merge order) | No |

- **Shard tie-break by doc ID** — consistent with how the global pipeline `RPSorter` works. Requires making the doc ID available to the reducer (currently not passed to `Add()`).
- **Coordinator tie-break is arbitrary** — loading and forwarding doc IDs from shards to coordinator purely for tie-breaking adds cost (bandwidth, processing) with little user value. `FIRST_VALUE` already uses arbitrary tie-break on coordinator.
- **Implication:** Results may differ slightly between standalone and cluster for documents with identical sort keys. This should be documented.

---

## 4. Rethink Dedup for `TOLIST *`

Default dedup (distinct) for `TOLIST *` requires deep equality comparison of full document maps. This is expensive and architecturally problematic:

**The cost:**
- **Map equality is O(n log n) per comparison** (sort entries by key, then compare pairwise), where n = number of fields per document.
- **Map hashing must be order-independent** — current `RSValue_Hash` is order-dependent and would produce different hashes for identical documents with different entry order (especially across shards).
- **Dedup dict grows to N** (all distinct docs in group) even when heap is bounded to K. Memory is O(N) not O(K).
- For large documents (many fields, nested JSON), every insertion triggers a hash + potential deep equality check.

**The reality check:**
- True content duplicates (same fields/values, different Redis keys) in `TOLIST *` are extremely rare in practice. The RJ use case has no duplicates.
- For `TOLIST @field` (single scalar), dedup is cheap and well-understood — no change needed there.
- `TOLIST *` with dedup requires either fixing global `RSValue_Equal`/`RSValue_Hash` for maps (risky, high blast radius) or implementing TOLIST-local map comparison (code duplication).

**Options to decide between:**

| Option | Description | Trade-off |
|--------|-------------|-----------|
| A | `TOLIST *` implies `ALLOWDUPS` (no dedup for whole-document) | Simplest. Users who need dedup can filter client-side. Breaks symmetry with `TOLIST @field`. |
| B | Support dedup for `TOLIST *` but with TOLIST-local map comparison | Full feature. Medium complexity. O(n log n) per insert. |
| C | Support dedup for `TOLIST *` via global `RSValue_Equal`/`RSValue_Hash` fix | Full feature. High risk — touches shared infrastructure. |
| D | Defer `TOLIST *` dedup to a future release; `TOLIST *` requires `ALLOWDUPS` for now | Ships sooner. Explicit about the limitation. |

**Recommendation:** Option D (or A). Dedup for whole-document maps is a significant engineering investment with minimal real-world demand. Ship `TOLIST *` with `ALLOWDUPS` required (or implicit). Support dedup only for `TOLIST @field`. Revisit if customers need it.

---

## 5. Timeout Behavior

The PRD does not specify what happens when the query times out during TOLIST processing (with `ON_TIMEOUT RETURN` policy).

**Decision needed:**

| Phase | Behavior |
|-------|----------|
| Timeout during GROUPBY (before groups are formed) | Zero results (current behavior, keep it) |
| Timeout after groups formed, during TOLIST accumulation/sorting | **Zero results** |
| Timeout during global pipeline SORTBY (after GROUPBY completes) | Partial results (current behavior) |

**Rationale for zero results during TOLIST:** A partially accumulated group could return an incorrect top-K window (missing documents that would have ranked higher). Returning zero is safer and consistent with GROUPBY timeout behavior. Returning partial results is significantly harder to implement correctly and harder to test.

**Alternative:** Return partial results from fully-processed groups only (groups where all docs were accumulated). This is more useful but adds implementation complexity — the Grouper would need to track per-group completion status.

---
