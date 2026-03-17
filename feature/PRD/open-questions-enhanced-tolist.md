# Open Questions: Enhanced FT.AGGREGATE TOLIST

> Pre-architecture review of the [Enhanced TOLIST PRD](./enhanced-ft-aggregate-tolist.md).
> All items below must be resolved before handing off to architects and developers.

## Status Legend

- **OPEN** — Needs a decision
- **RECOMMENDATION** — We have a suggested answer, needs PM/stakeholder sign-off
- **BUG** — Factual error in the current PRD docs that must be fixed regardless

---

## 1. Raymond James Example Output Is Incorrect [BUG]

**Document:** `raymond-james-ft-aggregate-example.md`, "Enhanced Behaviour" section.

The query specifies `SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 5`, but the expected output for "401K SEP":
- Shows **6** documents (LIMIT says 5)
- Is **not sorted** by `@target DESC @bestByDate ASC`

**Note:** `@target` is a TAG field with string values `"True"` / `"False"`. Sorting is **lexicographic** (not boolean): `"True" > "False"` because `'T' (0x54) > 'F' (0x46)`. Similarly, `@bestByDate` is a TEXT field — ISO 8601 date strings sort correctly under lexicographic comparison.

**Correct output for "401K SEP"** (5 items, sorted by `@target DESC` then `@bestByDate ASC`):

| # | relationshipId | target | bestByDate |
|---|---------------|--------|------------|
| 1 | 295987974 | True | 2025-07-08 |
| 2 | 284935865 | False | 2023-03-21 |
| 3 | 283929920 | False | 2023-04-12 |
| 4 | 278554570 | False | 2023-08-10 |
| 5 | 241908098 | False | 2024-04-20 |

**Action:** Recompute and fix the expected output for all groups in the example. Ensure reviewers understand that all sorting is lexicographic (string-based via `RSValue_Cmp`), not typed.

---

## 2. Output Format for `TOLIST *` [OPEN]

The PRD says "Array of complete documents (JSON)" but does not define the RESP-level representation.

**Context from codebase:** `FT.SEARCH` returns JSON documents as structured RESP Maps (RESP3) or flat key-value arrays (RESP2) — **not** as raw JSON strings. `FT.AGGREGATE` uses the same serialization path.

**Questions:**
- (a) Should `TOLIST *` output follow the same RESP serialization as `FT.SEARCH` document content (structured RESP, not raw JSON strings)?
- (b) Should the format differ between RESP2 and RESP3?
- (c) Should the PRD examples be updated to show the actual RESP wire format rather than inline JSON?

**Recommendation:** Yes to (a) and (b) — consistent with existing behavior. For (c), at minimum add a note clarifying that the JSON shown is conceptual; wire format follows standard RESP serialization.

---

## 3. Document Key in `TOLIST *` Output [RECOMMENDATION — needs PM sign-off]

**Context from codebase:** `LOAD *` loads all document **fields** but does **not** load the Redis key (e.g., `relationship:271083434`). The Redis key is metadata, only available via `LOAD @__key` (currently an unstable feature).

In the RJ example, `relationshipId` appears in the output because it's a JSON field inside the document — not because it's the Redis key.

**Question:** Should `TOLIST *` implicitly include the Redis key in its output, or follow current `FT.AGGREGATE` behavior (key excluded unless explicitly loaded)?

**Recommendation:** Exclude by default (consistent with `FT.AGGREGATE`). Document this explicitly so users aren't surprised.

---

## 4. Explicit `LOAD` Requirement for `TOLIST *` [OPEN]

The review doc (T2) states: "Require explicit `LOAD *` for `TOLIST *`. Fail fast with clear error (no implicit full load)."

This is not mentioned in the main PRD.

**Questions:**
- (a) Is `LOAD *` (or `LOAD @f1 @f2 ...`) mandatory before `TOLIST *`? What error is returned if omitted?
- (b) For `TOLIST N @field SORTBY 2 @rating DESC`, does `@rating` need to be explicitly loaded/available, or can the reducer implicitly load it?
- (c) Should the PRD syntax section include a "Prerequisites" note about LOAD?

**Recommendation:** Require explicit LOAD. No implicit loading from within the reducer. Fail with a clear parse-time error. Add this to the main PRD.

---

## 5. Single-Field TOLIST with SORTBY by a Different Field [RECOMMENDATION — needs PM sign-off]

**Example:** `REDUCE TOLIST 5 @title SORTBY 2 @rating DESC LIMIT 0 3 AS top_titles`

This would collect `@title` values, sort them by `@rating` to pick the top 3, and return only the titles: `["Inception", "The Matrix", "Interstellar"]`. The sort field (`@rating`) is used for ranking only and does **not** appear in the output.

**Questions:**
- (a) Is this a supported use case?
- (b) If yes, does the sort field need to be explicitly available (via LOAD or SORTABLE index)?
- (c) For single-field TOLIST, does dedup still operate on the collected field value (not the sort key)?

**Recommendation:** Support it — it falls naturally out of the architecture (comparator uses SORTBY fields, payload uses the TOLIST field). Require the sort field to be available in the pipeline (explicit LOAD or indexed). Dedup operates on payload (the collected field), not the sort keys.

---

## 6. `narg` Counting Convention [OPEN]

The extended syntax packs `*`, `ALLOWDUPS`, `SORTBY`, inner narg, fields, `ASC`/`DESC`, `LIMIT`, offset, and count all into the outer `narg`. The `AS` alias is outside.

This is never stated explicitly. Users and devs must reverse-engineer it from examples.

**Example breakdown:**
```
REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 3 AS alias
                ── ──────────────────────────────────────────────── ────────
                narg=10 tokens counted here                        outside narg
```

**Action:** Add an explicit note to the PRD syntax section explaining how narg is counted. This is not a decision — it's a documentation gap.

---

## 7. Behavioral Specs Missing from the Main PRD [OPEN]

The following behaviors are defined in the review doc but absent from the main PRD. They are user-facing and should be part of the specification:

| Behavior | Current location | Should be in PRD? |
|----------|-----------------|-------------------|
| Tie-breaking: doc ID on shard, arbitrary on coordinator | Review doc | Yes |
| Null handling: nulls sort last | Review doc | Yes |
| Sort type coercion: follows `RSValue_Cmp` | Review doc | Yes |
| Dedup is based on payload, not sort keys | Review doc | Yes |
| Deep equality for `TOLIST *` dedup (map key order isn't stable) | Review doc | Yes |
| `TOLIST` LIMIT is per-group, not global | Review doc | Yes |

**Action:** Decide whether to fold these into the main PRD or create a consolidated "Behavioral Specification" section in a new document.

---

## 8. Maximum In-Group LIMIT / Memory Bounds [OPEN]

**Question:** Is there a server-enforced maximum for the in-group `LIMIT` count? What happens with `LIMIT 0 1000000`?

The review doc (T12) mentions "bounded memory" validation but no cap is specified.

**Options:**
- (a) No cap — user is responsible. Memory is bounded by `groups × (offset + count)`.
- (b) Configurable cap with a sensible default (e.g., 1000).
- (c) Reuse existing `MAXAGGREGATERESULTS` config or similar.

**Recommendation:** At minimum, document the memory profile: `O(groups × K)` where `K = offset + count`. Consider a configurable cap for safety.

---

## 9. Timeout / Partial Results Behavior [OPEN]

The review doc discusses partial results when timeout occurs:
- Timeout during GROUPBY → zero results (current behavior, keep it).
- Timeout after groups are calculated but during TOLIST SORTBY → zero results (easier) or partial results (harder)?

**Question:** What is the specified behavior? This affects testing scope and user expectations.

**Recommendation:** Zero results on timeout during TOLIST processing (simpler, safer, consistent with GROUPBY timeout behavior). Document this.

---

## 10. Multiple TOLIST Reducers and Coexistence with Other Reducers [OPEN]

**Question:** Can a user have multiple TOLIST reducers and/or mix TOLIST with other reducers in the same GROUPBY?

```
GROUPBY 1 @genre
  REDUCE TOLIST 5 * SORTBY 2 @rating DESC LIMIT 0 3 AS top_movies
  REDUCE COUNT 0 AS movie_count
  REDUCE MAX 1 @rating AS max_rating
```

**Context from codebase:** The Grouper supports multiple reducers, so this should work architecturally.

**Recommendation:** Confirm as supported. Add an example to the PRD showing TOLIST alongside other reducers.

---

## 11. Hash Documents vs JSON Documents [OPEN]

All PRD examples use JSON documents. `TOLIST *` with Hash documents would return a flat key-value structure (since Hash docs don't have nested structure).

**Questions:**
- (a) Is Hash support in scope for this feature?
- (b) If yes, should the PRD include a Hash example?

**Recommendation:** Hash support should work naturally (same LOAD/pipeline path). Add a brief note confirming it works for both document types.

---

## 12. `SORTBY` Without `LIMIT` / `LIMIT` Without `SORTBY` [OPEN]

The PRD syntax shows both as optional, implying these combos are valid:

| Combo | Meaning |
|-------|---------|
| No SORTBY, no LIMIT | All docs, arbitrary order, deduplicated (legacy-compatible) |
| SORTBY only | All docs, sorted, no limit |
| LIMIT only | Arbitrary N docs, no guaranteed order |
| Both | Top-N docs by sort criteria |

**Question:** Are all four combinations valid? "LIMIT without SORTBY" is unusual — is the arbitrary selection acceptable, or should LIMIT require SORTBY?

**Recommendation:** Allow all four. LIMIT without SORTBY returns an arbitrary window (consistent with global pipeline behavior). Document that the result order is non-deterministic without SORTBY.

---

## Summary: Decision Matrix

| # | Topic | Status | Owner |
|---|-------|--------|-------|
| 1 | Example output bug | BUG — fix | Author |
| 2 | RESP output format | OPEN | PM / Architect |
| 3 | Document key in output | RECOMMENDATION | PM |
| 4 | Explicit LOAD requirement | OPEN | PM / Architect |
| 5 | Single-field TOLIST + cross-field SORTBY | RECOMMENDATION | PM |
| 6 | narg counting docs | OPEN — doc gap | Author |
| 7 | Behavioral specs in main PRD | OPEN — doc structure | PM |
| 8 | Max in-group LIMIT | OPEN | PM / Architect |
| 9 | Timeout behavior | OPEN | PM / Architect |
| 10 | Multiple reducers coexistence | OPEN | PM / Architect |
| 11 | Hash document support | OPEN | PM |
| 12 | SORTBY/LIMIT combinations | OPEN | PM |
