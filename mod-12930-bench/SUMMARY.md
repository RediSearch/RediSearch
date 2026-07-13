# MOD-12930 — FT.HYBRID performance analysis: wrap-up

## What was wrong with the current approach

It compared FT.HYBRID to `FT.AGGREGATE "(<text>)=>[KNN k ...]"`, and these return
**different results**, not just different timings. Example — K=2, four docs matching the
text query:

| doc | vector similarity | text score |
|---|---|---|
| A | 0.7 | 0.3 |
| B | 0.5 | 0.1 |
| C | not in vector top-2 | **0.9** |
| D | not in vector top-2 | 0.8 |

The aggregate form can only ever return A and B: the text acts as a filter and is scored
just for the K vector winners — it is never ranked, so C, the best text match in the
corpus, cannot appear. FT.HYBRID fuses the text top-K with the vector top-K, so C and D
compete and C surfaces near the top.

The cost difference follows from that behavior: to rank the text side at all, FT.HYBRID
must score the full text matching set (top-WINDOW), which is O(|matches|) by definition —
work the aggregate never does. So the benchmark compared a rerank against true fusion.

## Is there a regression when scaling?

No. Comparing hybrid to its subqueries run standalone (same K/WINDOW, same reply size),
hybrid tracks its slowest subquery consistently in every cell (10K–500K docs, windows
20–500, loader on/off), with no super-linear divergence as result sets grow. Dataset size
alone contributes almost nothing — latency follows the text matching set and WINDOW.
Read raw latencies side by side; derived metrics (ratios/deltas) hide which subquery
dominates and can mislead.

👆 for the perf ticket.

## Our recommendations for how to test it

- Not against a client-side implementation — too implementation-dependent.
- Against the subquery equivalents (PRD requirement): hybrid vs max(search, vsim).
  Note: OpenSearch's `bool` baseline is a single pruned traversal — not comparable to ours.
- Engineer text queries per cell so text latency is similar to the vector's (we used
  ±8%) — otherwise the comparison says little about merge cost.
- Axes: fixed window × growing result sets.
- Rule out noise: parsing, cold index (warm after load; the first cold command measured
  3–4× slower), and serialization — equalize reply sizes via the LIMIT offset trick:
  the search mirror uses `LIMIT (WINDOW−10) 10`, so its sort heap is still WINDOW-sized
  (offset+count) while it serializes the same 10 rows the hybrid returns.
- Gate on correctness: hybrid output must equal offline fusion of the two raw queries
  (all our configs passed).

## Initial results

(dbpedia, 512-dim, heavily using AI): no regression with growing result sets — hybrid
tracks its slowest subquery consistently across all cells. The overhead (hybrid minus its
slowest subquery) is smallest at window 20 with no loader, and grows with WINDOW as the
merge workload itself grows. The exact commands, subquery equivalents, and full numbers
are in the HTML report.

## Disclaimers

- All runs were on a laptop (macOS, local redis-server, redis-py client) — **relative
  numbers are meaningful, absolutes are not**.
- Single connection, sequential requests — this measures single-query latency, not
  throughput or behavior under concurrent load.
- 5000 timed repetitions per cell (the original plan called for 30K) — p50/p90 are solid,
  tail percentiles (p99+) are indicative only.
- Timing includes client round-trip (~50–100µs), identical for all contenders.
- The analysis was heavily AI-assisted; the methodology and conclusions were
  human-reviewed, and every configuration passed the correctness gates.

## Next

On/off-CPU profiling; rerun on an isolated machine with a proper benchmarking framework
and statistical analysis (p50–p999). Artifacts: branch `itzik-mod12930-bench` (notebook,
results, HTML report).
