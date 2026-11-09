# Filter quick-check API (draft)

This internal API supplies a conservative, random-access predicate check for a future
VecSim consumer. It does not add a command option, change VecSim, install a query callback,
or maintain signatures in the live index. There is consequently no query acceleration in
this PR. The deliverable is the signature representation, bounded predicate evaluator,
callback/batch interfaces, and tests of their correctness contract.

## Motivation and model

A filtered vector search should be able to reject some candidate document IDs without
first enumerating the entire matching-ID set or advancing a posting-list iterator. The
signature-file approach stores a lossy summary of each document's indexed terms instead.
The caller builds that summary at indexing time and compiles each query once.

`FilterQuickCheck_Evaluate` returns only:

- `FilterQuickCheck_NO`: this summary proves the predicate cannot match.
- `FilterQuickCheck_MAYBE`: run the original exact predicate before admitting a result.

The contract is about filter membership, not ANN recall. A rejected document may still
be needed as a graph navigation bridge. A future VecSim consumer must preserve its
traversal behavior unless a separately validated filtered traversal is selected.

## Data and predicate construction

A `FilterQuickCheckSignature` contains 256 bits. Each normalized term sets one bit in each
of four 64-bit words. Term kind (TAG or TEXT) and stable schema field index are included
in the hash. A term's query mask must be a subset of the document signature for that
document to be a possible match. Collisions and saturation produce false positives.

The producer must supply the actual indexed bytes. TAG splitting, Unicode case folding,
escapes, TEXT stemming, synonyms, phonetics, and field scoping stay with the existing
indexer/query evaluator; this API does not reimplement their normalization. A raw AST is
not accepted. Compile a necessary-condition expression after parameter resolution and
query expansion. For example, expanded TEXT alternatives are an OR, not an AND.

The expression API supports positive term membership, AND, and OR. Negation and every
unsupported predicate must be UNKNOWN. This includes ranges, phrases' positional
requirements, and prefix/fuzzy/wildcard tests unless the caller has an equivalent,
fully resolved positive-term expression. For phrases, a caller may use the required
terms as necessary conditions, but must retain the exact positional check.

AND can reject if any child rejects. OR can reject only if every child rejects: never
omit an unsupported alternative. An empty AND returns MAYBE and an empty OR returns NO.
Compile budgets bound recursion to 32 levels and the program to 256 instructions. On
budget exhaustion the entire program becomes UNKNOWN, rather than dropping alternatives.
The compiled program owns its masks; it retains neither token nor expression pointers.

## Consumer shape

```c
FilterQuickCheckTerm electronics = {
    .kind = FilterQuickCheck_TAG,
    .fieldIndex = categoryFieldIndex,
    .value = "electronics",
    .len = sizeof("electronics") - 1,
};
FilterQuickCheckPredicate predicate = {
    .type = FilterQuickCheck_TERM,
    .term = electronics,
};
FilterQuickCheck *check = FilterQuickCheck_Compile(&predicate);
FilterQuickCheckContext context = {
    .check = check,
    .lookup = lookupCompleteResidentSignature,
    .lookupContext = pinnedDocumentSummaryStore,
};

/* Candidate admission only; exact_filter is still authoritative. */
if (FilterQuickCheck_MayMatch(&context, candidateId) && exact_filter(candidateId)) {
  accept_candidate(candidateId);
}

/* Release only after every batch/callback using context has finished. */
FilterQuickCheck_Free(check);
```

`FilterQuickCheck_CheckBatch` accepts non-monotonic IDs and preserves their order. Both
evaluation paths allocate nothing per candidate and never touch a filter iterator.
The lookup callback copies one signature and returns false if it cannot safely supply
one. Missing, incomplete, invalidated, or unloaded summaries return MAYBE. A complete
empty document signature is different: it can disprove positive term membership.

## Ownership, updates, and ROF

The future provider owns signature storage and its memory accounting. It must publish
only complete summaries, pin the schema/document generation observed by the query, and
make any concurrent lookup safe. An obsolete signature missing a newly added term can
create a false negative, so it must be invalidated before the new document is visible.
Do not clear individual bits for deletion; rebuild a complete replacement signature.

There is no stored matching-ID set, persistence change, or mandatory per-document
allocation here. For ROF, the provider may use resident summaries and return unavailable
on a cache miss. A future implementation must not make the quick callback perform
blocking document reads. A reload without rebuilt summaries must also return unavailable.

The representation consumes 32 bytes per covered document before storage overhead
(320 MB for 10 million documents). More terms increase saturation; signature size,
cache layout, selectivity, and maintenance cost need workload measurements before
choosing a production representation. No speedup or false-positive-rate claim is made.

## Follow-up integration requirements

- Add a generation-safe summary provider with ingest/update/delete/reload coverage,
  memory accounting and an explicit enablement policy.
- Lower resolved query predicates to necessary conditions using the same indexed
  normalization. Unsupported branches must remain UNKNOWN.
- Define callback lifetime and synchronization across VecSim query execution, including
  tiered/multi-value indexes, timeouts, cancellation, and suspended searches.
- Preserve exact filter verification and graph connectivity. Validate filtered recall
  separately from the no-false-negative predicate property.
- Benchmark RAM and ROF end to end, including summary construction and maintenance.

## References

- Faloutsos and Christodoulakis, [Signature Files (1984)](https://doi.org/10.1145/2275.357411).
- Hellerstein, Naughton and Pfeffer, [Generalized Search Trees (1995)](https://www.vldb.org/conf/1995/P562.PDF).
- PostgreSQL's [document signatures](https://www.postgresql.org/docs/current/textsearch-indexes.html)
  and [GiST consistent/recheck interface](https://www.postgresql.org/docs/current/gist.html).
