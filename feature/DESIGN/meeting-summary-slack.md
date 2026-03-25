*Enhanced TOLIST — Meeting Summary & Delivery Plan*

Quick wrap-up from my sync with @Adriano on the Enhanced TOLIST design:

---

*Decision: Sticking with the original proposal*

After reviewing the alternatives, we're going forward with the PRD as-is. The core semantics remain:
• `TOLIST @field` — collects distinct _values_ of a single field per group (deduplicated by default)
• `TOLIST *` — collects the full _payload_ (all loaded fields) per document within each group
• In-group `SORTBY` / `LIMIT` for both modes
• `ALLOWDUPS` flag to control deduplication behavior

We prepared a toy dataset with worked examples covering all option combinations — single field, wildcard, with and without `ALLOWDUPS`, `SORTBY`, and `LIMIT` — to make the expected behavior concrete.

---

*Open concern: Entity identification in the response*

This particular customer (Raymond James) happens to have an identifier field inside their document payload, so correlating grouped results back to source entities isn't a problem for them. However, other customers who rely on the Redis key as their entity identifier would struggle to adopt this feature — since `TOLIST *` returns the payload without the Redis key, they'd have no way to identify which entity each entry in the grouped list belongs to.

We have a few ideas for addressing this in the future (e.g., an `INCLUDE_KEY` flag, or a dedicated reducer for complete documents with key inclusion), but this is *out of scope for the current feature* and doesn't block the initial delivery.

---

*Development plan:*

We'll develop that feature incrementally in the following order, prioritized by what the customer needs most urgently:

1. Standalone solution for JSON — the foundation (in-group sorting with `TOLIST *` on standalone with JSON docs)
2. Cluster solution for JSON — coordinator wire format, shard-to-coordinator merge, re-sort
3. Hash support — `TOLIST *` for Hash documents
4. Dedup by deep payload comparison

This is the order we'll land things into the repo, not separate releases — the complete feature ships together once all parts are merged. Feature is gated behind "enable unstable feature" flag throughout development.
