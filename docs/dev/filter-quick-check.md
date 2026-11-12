# Filter quick-check API (draft)

This internal API supplies a conservative, random-access predicate check for a future
VecSim consumer. The RAM POC now builds summaries for every indexed TAG, TEXT and NUMERIC
field automatically, without a per-field flag. It still does not change query execution
or install a VecSim consumer.

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

The expression API supports positive term membership, numeric ranges, AND, and OR. Negation and every
unsupported predicate must be UNKNOWN. This includes geo predicates, phrases' positional
requirements, and prefix/fuzzy/wildcard tests unless the caller has an equivalent,
fully resolved positive-term expression. For phrases, a caller may use the required
terms as necessary conditions, but must retain the exact positional check.

AND can reject if any child rejects. OR can reject only if every child rejects: never
omit an unsupported alternative. An empty AND returns MAYBE and an empty OR returns NO.
Compile budgets bound recursion to 32 levels and the program to 256 instructions. On
budget exhaustion the entire program becomes UNKNOWN, rather than dropping alternatives.
The compiled program owns its masks and numeric bounds; it retains neither token nor expression pointers.

## Numeric summaries

Numeric predicates use `FilterQuickCheckNumericRange`: a resolved schema field index,
double bounds, and flags for inclusive/exclusive endpoints. Equality is a closed range
with equal endpoints. Infinities are supported; NaN or reversed query bounds become
UNKNOWN. These predicates must come from NUMERIC fields, not GEO encodings or raw strings.

`FilterQuickCheckNumericSummary` summarizes every indexed numeric value of one field in
one document version. Initialize a rebuild with `FilterQuickCheckNumericSummary_Init`
and feed the actual indexed doubles through `FilterQuickCheckNumericSummary_Add` before
publishing it. Do not round the bounds inward. Its states distinguish:

- UNKNOWN: a zero-initialized, invalidated or unavailable summary; retain the candidate.
- EMPTY: a complete field with no indexed numeric values; a positive range cannot match.
- BOUNDS: minimum and maximum of all values, including every element of a numeric array.

A disjoint range returns NO; overlap returns MAYBE. For a scalar, equal min/max accurately
describe membership, but the API still requires exact verification. For an array such as
`[10, 100]`, a query `[40, 60]` overlaps the summary despite matching neither value. Separate
AND predicates may match different elements: do not intersect their query ranges into a
single constraint without proving that the exact predicate requires the same element.
NaN input or malformed bounds poison a builder to UNKNOWN until explicit reinitialization.

Set `FilterQuickCheckContext.numericLookup` and `numericLookupContext` to a provider that
copies a summary for `(docId, fieldIndex)`. It follows the same residency and generation
contract as the term provider. Uncovered fields and absent summaries must return false or
UNKNOWN, never EMPTY. Both providers must observe the same pinned document/schema version.
No numeric summaries are embedded in the term bitmap or allocated per candidate.

The providers are optional independently. A missing term summary must not prevent a
numeric AND branch from rejecting, and vice versa; an unknown OR branch retains the
candidate. Numeric-only plans do not fetch term signatures. Numeric summaries are fetched
once per numeric leaf, including repeated predicates on a field. The standalone
`FilterQuickCheck_Evaluate` is signature-only and treats numeric leaves as MAYBE; use
`FilterQuickCheck_MayMatch` or `FilterQuickCheck_CheckBatch` for numeric and mixed plans.

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

For example, a numeric-only consumer can replace the predicate and context above with:

```c
FilterQuickCheckPredicate predicate = {
    .type = FilterQuickCheck_NUMERIC,
    .numeric = {.fieldIndex = priceFieldIndex, .min = 10, .max = 20,
                .minInclusive = true, .maxInclusive = false},
};
FilterQuickCheck *check = FilterQuickCheck_Compile(&predicate);
FilterQuickCheckContext context = {
    .check = check,
    .numericLookup = lookupCompleteResidentNumericSummary,
    .numericLookupContext = pinnedDocumentSummaryStore,
};
```

## Ownership, updates, and ROF

The provider owns signature storage and its memory accounting. It must publish
only complete summaries, pin the schema/document generation observed by the query, and
make any concurrent lookup safe. An obsolete signature missing a newly added term can
create a false negative, so it must be invalidated before the new document is visible.
Do not clear individual bits for deletion; rebuild a complete replacement signature.
Rebuild numeric min/max on removal or replacement of values too. Numeric publication,
invalidation and reload coverage remain the provider's responsibility.

There is no stored matching-ID set or persistence change. The RAM POC allocates
per-document summaries during indexing. For ROF, the provider may use resident summaries and return unavailable
on a cache miss. A future implementation must not make the quick callback perform
blocking document reads. A reload without rebuilt summaries must also return unavailable.

The term representation consumes 32 bytes per covered document before storage overhead
(320 MB for 10 million documents). More terms increase saturation; signature size,
cache layout, selectivity, and maintenance cost need workload measurements before
choosing a production representation. No speedup or false-positive-rate claim is made.
Each covered numeric field additionally needs two doubles and a state (typically 24 bytes
for the API summary on a 64-bit platform), plus provider storage overhead. The RAM POC
covers all indexed fields; persisted formats and query execution remain unchanged.

## Follow-up integration requirements

- Add production field selection, reload rebuilding, and a disk summary provider.
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

## RAM summary provider (POC)

`filter_summary.h` connects the API to document indexing. Each new document version owns
one term signature and numeric bounds for all indexed numeric fields, built from the
forward index and preprocessed field values. Numeric bounds include both the original and
float-rounded values to cover legacy numeric compression conservatively. Replacement rebuilds them; deletion frees
them. Their allocation is included in document-table memory accounting. This POC enables
collection unconditionally; a field-selection flag and production memory layout come later.

`FilterSummary_CreateContext(spec, &predicate)` compiles the predicate and resolves numeric
fields to summary slots. The returned context owns that plan; release it with
`FilterSummary_FreeContext`. Keep the spec alive and hold its read lock during creation
and evaluation:

```c
FilterSummaryContext context = FilterSummary_CreateContext(spec, &predicate);
int maybe = FilterSummary_MayMatch(&context, docId);
FilterSummary_FreeContext(&context);
```

Evaluation borrows document metadata lazily, at most once per candidate, and retains no
document or summary pointer after returning. Missing summaries, failed indexing, and
schema-field-count mismatches remain conservative. RDB-loaded metadata starts without
summaries until reindexing; the persistence format is unchanged. Disk providers remain a
follow-up. Exact filtering remains mandatory after candidate generation.
