# Design: advisory iterator match estimates

## Interface and population

The active Rust iterator trait adds
`estimated_matches(&self, total_docs: usize) -> f64`. The C iterator interface
adds the corresponding `EstimatedMatches(self, totalDocs)` callback. The caller
supplies the population relevant to the iterator tree, normally the current
shard's live index document count. Every child receives the same population.
Document IDs and the largest assigned document ID are not population counts.

The result describes total matches, including matches before the iterator's
current position. It is a prediction, not a remaining-results counter, an
observed count, or a promise that any particular document exists. Estimates are
finite, nonnegative, and bounded by the supplied population and the existing
upper bound. A zero population yields zero.

Existing `NumEstimated` / `num_estimated` semantics stay intact. The new method
is advisory: no correctness decision, EOF transition, result pruning, or
allocation safety proof may depend on it. Reading it does not consume entries
or move an iterator. Callers retain the locking and lifetime requirements of
the iterator API.

## Estimators

| Iterator family | Estimate source |
| --- | --- |
| Empty, explicit ID lists, materialized results | Available result count, capped to the population |
| Term, tag, missing, wildcard index readers | Available posting statistics; unavailable selectivity falls back to the capped upper bound |
| Numeric range readers | Range overlap, distinct-value statistics for equality, and average values per document; use the fallback where the required statistics are unavailable |
| Intersection | Product of child selectivities multiplied by the population |
| Union | Complement of the product of child non-match probabilities, multiplied by the population |
| Negation | Population less the child's predicted matches, within the iterator's own bound |
| Optional | The iterator's matching population; the optional child changes scoring rather than membership |
| Wrappers and variants | Delegate to the wrapped iterator |
| Limited or top-k iterators | Apply the iterator's output cap to its underlying estimate |

Let `N` denote the population and `e_i` each child estimate. For ordinary
nonempty boolean combinations the formulas are:

```
AND = N × product(e_i / N)
OR  = N × (1 - product(1 - e_i / N))
```

Handle empty iterators and `N = 0` before division. Apply iterator-specific
caps after composition. An empty constructed iterator retains its established
empty-result meaning rather than becoming an abstract logical identity.

Numeric unions add child estimates because the range tree partitions value
space. This avoids assuming independent membership in disjoint numeric
intervals. Multi-valued documents can still occur in several intervals, so the
sum remains an approximation capped by the common population.

## Accuracy boundaries

Boolean composition assumes independent child predicates. Identical,
mutually exclusive, or otherwise correlated filters can produce substantial
errors in either direction. Adding a method does not reveal overlap absent
from the index statistics. Range overlap likewise assumes a distribution
within the represented interval; skewed values violate that assumption.

Distinct numeric values are not distinct documents. Multi-valued fields and
documents appearing in more than one numeric range need document-level
reasoning. Within one range, the numeric model uses average values per document
to estimate the probability that at least one value matches. That model assumes
independence between a document's values and can be inaccurate for duplicates
or correlated values. Missing multiplicity statistics use a conservative
fallback. Expiration and deferred garbage collection can also make posting
statistics differ from the currently eligible population.

These limitations are part of the API contract. Neither an estimate of zero
nor a fractional result proves the iterator empty. Tests include correlated
examples to prevent accidental promotion of the estimate to an upper bound.

## Lifecycle and overhead

The C iterator header gains one callback pointer. Each numeric iterator gains
one floating-point selectivity value (eight bytes each on the benchmarked
64-bit platform). Other concrete Rust leaf layouts are unchanged.

Compound estimates are derived when called. Numeric iterators cache a matching
fraction at construction; the accessor scales the available posting count by
that fraction. The implementation adds no unconditional per-result accounting,
sampling pass, materialization, or persisted statistics. Existing execution
decisions continue using the upper bound. A compound estimate visits its
children; requesting it on a nested tree therefore costs more than retrieving
a cached upper bound. Consumers should request estimates at planning or batch
boundaries rather than on every document without evidence that the cost is
justified.

Iteration alone does not turn a total estimate into a remaining estimate.
The C optimizer estimates its current numeric window. Replacing or releasing that
window can change its prediction; after release, it falls back to the child
prediction because the numeric statistics are unavailable. This still does not
report how many results remain.

Revalidation may change available posting statistics because the index
changed; the cached numeric fraction remains a construction-time model. The
new API does not promise a stable snapshot across such changes.

## Alternatives

- Replacing the upper bound with a probability model would break established
  contracts, so the interfaces remain separate.
- Counting successful reads reports only visited results. Skips and early
  termination prevent that counter from representing the total population.
- Online counters and representative sampling can improve some estimates,
  but introduce hot-path work or planning work. This draft keeps those costs
  separate from introducing the common API.
- Histograms, joint statistics, or sketches could improve range and overlap
  estimates; their memory, update, and persistence costs require a separate
  design.

## Verification and performance

Tests cover empty populations, caps, boolean composition, correlated filters,
numeric fallbacks, forwarding through the C bridge, and reading estimates
before and after iterator movement. Existing iterator contract tests continue
to enforce the upper-bound contract independently.

Run the unchanged repository iterator benchmarks on the base and candidate
under matching build settings to measure traversal regressions. The separate
`cardinality` benchmark compares upper-bound access and advisory-estimate
access on leaves, flat and heap unions, intersections, nested trees, and nested
C callback trees. Construction is outside the measured operation, and both
the inputs and results pass through `black_box` so the accessor work remains
observable. Report absolute query-estimation cost separately from traversal
regression, with Criterion confidence intervals and the exact revisions.

The `numeric_filter` benchmark uses only APIs available on the base revision.
Run that same harness on both revisions to measure partial-range and equality
filter construction, through both direct-reader and range-tree paths, and
their traversal cost. Its source is
`src/redisearch_rs/rqe_iterators_bencher/benches/numeric_filter.rs`.
