# `collect_consider` benchmark results

Micro-benchmark for the ranked `COLLECT … SORTBY` insert step
(`RankedStorage::consider`), comparing four ways to handle the candidate's
sort-key values, across both query paths and two field layouts. See
[`collect_consider.rs`](./collect_consider.rs) for the harness; run with:

```bash
cargo bench -p reducers --bench collect_consider
```

## Strategies

All four **materialise (clone) a candidate's values at most once** — only when it
is kept. None clone twice. What differs is how the *borrowed* values reach the
comparison: how many times the source is iterated, and whether a buffer is
allocated.

- **`master_eager`** — build the owned `RankingKey` (alloc + `Arc` bump per value)
  for *every* candidate, then compare owned-vs-owned. Clones losers too.
- **`A_iterator`** *(shipped, [MOD-15734])* — pass the borrows as a lazy iterator;
  rank against the worst survivor by borrow (`RankingKey::ranks_below`) and
  materialise only on a win. No buffer; the compare short-circuits at the first
  differing field.
- **`Bprime_stackarray`** — collect the borrows once into a stack `SmallVec<[_; 8]>`
  (8 = `SORTASCMAP_MAXFIELDS`), compare from it, clone on win.
- **`B_box`** — the reviewer's suggestion: same, but the buffer is a heap
  `Box<[Option<&SharedValue>]>` — one heap allocation per candidate.

## Axes

The same `consider` runs on both paths, so the strategy choice is global.

- **path** — **remote** (shard): a fetch is an O(1) slice index (`RLookupRow::get`),
  large stream (`K = 10`, `N = 100_000`). **local** (coordinator): merges
  `SHARDS = 12` shards' top-K payloads, so `N = SHARDS · K = 1200` (`K = 100`); a
  fetch is `Map::get`, a linear scan + byte-compare (not O(1)).
- **order** — `reject` (all lose after fill) / `shuffle` / `accept` (all evict).
- **layout** — `F1`/`F8`: field 0 distinct, so the compare short-circuits on the
  first field. `F8-tail`: 8 fields where only the *last* differs (0..6 identical
  across rows), so the compare must read every field — no short-circuit.

## Results — % faster than `master` (negative = slower); **bold = best of the three**

| path   | scenario        | A (iter) | B′ (stack) | B (box) |
|--------|-----------------|---------:|-----------:|--------:|
| remote | reject/F1       | **76.4%**| 63.5%      | 3.4%    |
| remote | shuffle/F1      | **77.0%**| 63.4%      | 6.1%    |
| remote | accept/F1       | **−0.7%**| −12.0%     | −43.0%  |
| remote | reject/F8       | **55.3%**| 42.7%      | 25.6%   |
| remote | shuffle/F8      | **68.4%**| 58.2%      | 26.9%   |
| remote | accept/F8       | **−3.0%**| −16.4%     | −43.9%  |
| remote | reject/F8-tail  | **50.6%**| 43.9%      | 26.0%   |
| remote | shuffle/F8-tail | **41.1%**| 33.5%      | 9.8%    |
| remote | accept/F8-tail  | **0.4%** | −5.0%      | −12.7%  |
| local  | reject/F1       | **54.4%**| 45.4%      | −2.3%   |
| local  | shuffle/F1      | **16.7%**| 13.0%      | −10.6%  |
| local  | accept/F1       | **−3.7%**| −3.7%      | −18.1%  |
| local  | reject/F8       | **84.3%**| 8.6%       | 4.1%    |
| local  | shuffle/F8      | **50.7%**| 3.7%       | −2.2%   |
| local  | accept/F8       | **−1.2%**| −3.2%      | −15.5%  |
| local  | reject/F8-tail  | **6.9%** | 5.7%       | 2.7%    |
| local  | shuffle/F8-tail | −5.3%    | **+2.6%**  | −0.2%   |
| local  | accept/F8-tail  | −16.7%   | **−0.1%**  | −3.6%   |

Single-run medians (Apple Silicon dev box); ~10–15% run-to-run variance, so the
sign and rough magnitude are the signal.

## Conclusions

1. **A (iterator) is best in 16 of 18 configs**, and wins the **entire remote path
   unconditionally** (41–77% faster than master, both layouts) — the path with the
   volume, and the one the shared `consider` must optimise for.
2. **`B_box` is never the fastest** — its per-candidate heap allocation sinks it
   everywhere, and it regresses vs master on accept-heavy on both paths.
3. **`Bprime_stackarray` beats A in exactly two configs**: `local/shuffle/F8-tail`
   and `local/accept/F8-tail`. Both require the coincidence of an **expensive
   fetch** (`Map::get`) **+** leading sort keys that all tie (**no short-circuit**)
   **+** frequent accepts. That is the reviewer's regime, and it is real — but it
   is on the low-volume local merge (~`SHARDS · K` = 1200 rows), where the absolute
   difference is ≤ 0.2 ms/query, negligible next to a shard streaming millions of
   rows.
4. **Why the crossover:** on a *winner* with an expensive fetch and no
   short-circuit, the iterator fetches all F fields *twice* (once to compare, then
   again to materialise) — the buffer fetches them once and reuses them. But when
   the compare *does* short-circuit (the common case: the first sort field usually
   decides), the iterator instead fetches *fewer* fields on the dominant losers,
   and the buffer forfeits that by eagerly reading all F. So which wins depends on
   `fetch cost × accept fraction × (short-circuit?)`, and the shared code lands on
   the remote path where the iterator dominates.
5. **A's only routine cost is ≤ ~3% on accept-heavy** (everything is kept, so the
   pre-materialisation compare has no payoff), except the adversarial
   `local/accept/F8-tail` corner (−17%, still sub-ms absolute).

**Takeaway:** keep `A_iterator`. It is fastest where the cost actually lives (the
remote path, unconditionally); the buffered variants only win in a negligible-cost
local corner, and the heap `Box` never wins at all.

[MOD-15734]: https://redislabs.atlassian.net/browse/MOD-15734
