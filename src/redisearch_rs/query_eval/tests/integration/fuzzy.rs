/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_FUZZY → expand a token over the terms trie to every term within a given
//! Levenshtein distance of it, and union the per-term readers.
//!
//! Each test populates the terms trie with a handful of terms and evaluates a
//! fuzzy token, asserting the union yields exactly the documents of the terms
//! within `maxDist` edits. The fixtures use a deliberately tight term set — a
//! query and its neighbours at distance 1, 2 and 3 — so that a test which moves
//! `maxDist` by one changes the expected document set, and an off-by-one in the
//! distance bound cannot pass.
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use query::mock::{MockQueryNode, TokenNodeType};
use query_error::QueryErrorCode;
use query_eval::{Config, EvalResult, QueryEvalContext, QueryNodeMut, eval_node};
use rqe_core::{FieldMask, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

use crate::util::{ALL_INDEXED_FIELDS, term_records};

/// The `apiVersion` from which a fuzzy expansion also covers the empty term.
/// Below it the empty term is never expanded to, whatever the distance.
const API_VERSION_EMPTY_TERM: u32 = 2;

/// The highest `apiVersion` still below [`API_VERSION_EMPTY_TERM`], so the
/// negative controls sit right against the gate rather than far below it.
const API_VERSION_NO_EMPTY_TERM: u32 = API_VERSION_EMPTY_TERM - 1;

/// The longest pattern, in runes, the trie's fuzzy walk accepts. A longer one
/// builds no iterator at all. Read from the C constant rather than restated, so
/// the two boundary tests below follow it if it ever moves.
const MAX_FUZZY_PATTERN_RUNES: usize = ffi::TRIE_MAX_PREFIX as usize;

/// The default term set, laid out as a distance ladder around the query `word`:
/// `world`(1) and `ward`(2) are one edit away (insert `l`, substitute `a`),
/// `wo`(3) is two (delete `r` and `d`), `wakld`(4) is three, and `banana`(5) is
/// far outside any distance a test uses.
///
/// Shaped this way so that each `maxDist` a test picks selects a *different*
/// document set — 1 → {1, 2}, 2 → {1, 2, 3} — which is what makes an off-by-one
/// in the distance bound visible in both directions: too tight drops a document,
/// too loose picks up the next rung.
fn default_terms() -> Vec<(Vec<u8>, Vec<u64>)> {
    terms([
        (&b"world"[..], vec![1]),
        (&b"ward"[..], vec![2]),
        (&b"wo"[..], vec![3]),
        (&b"wakld"[..], vec![4]),
        (&b"banana"[..], vec![5]),
    ])
}

/// Collect a list of term literals and their document IDs into the owned form
/// [`FuzzyOptions::terms`] takes.
fn terms<'a>(terms: impl IntoIterator<Item = (&'a [u8], Vec<u64>)>) -> Vec<(Vec<u8>, Vec<u64>)> {
    terms
        .into_iter()
        .map(|(term, doc_ids)| (term.to_vec(), doc_ids))
        .collect()
}

/// How to build a [`FuzzyFixture`].
///
/// [`Default`] describes the case most tests want: the [`default_terms`] ladder,
/// unit weight, every field queried, and the pre-empty-term `apiVersion` —
/// leaving each test to override only the one knob it exercises.
struct FuzzyOptions {
    /// The terms to index, each with the documents indexed under it. Every term
    /// goes into the terms trie (so the fuzzy walk can discover it) and gets an
    /// inverted index holding those documents. A term with no documents is still
    /// discoverable but opens no reader.
    ///
    /// The empty term is the exception, and deliberately so: no trie takes a
    /// zero-length key, so an empty term contributes only its inverted index —
    /// which is exactly the state the `apiVersion` empty-term expansion exists
    /// to reach, since the walk itself can never find it.
    terms: Vec<(Vec<u8>, Vec<u64>)>,
    /// The node's weight. Applied once by the enclosing union, so it is *not*
    /// visible on the results of a token that expands to a single term.
    weight: f64,
    /// The node's field mask: the fields the query asks about, narrowed by the
    /// query-wide mask before reaching each reader.
    field_mask: FieldMask,
    /// The query-wide field mask, which the node's mask is narrowed by before
    /// reaching each reader. Deliberately separate from
    /// [`field_mask`](Self::field_mask): an expansion that kept only the node's
    /// half would still admit fields the query as a whole excluded.
    query_field_mask: FieldMask,
    /// The field mask each indexed posting carries, which the reader intersects
    /// with the node's own mask.
    record_field_mask: FieldMask,
    /// The cap on how many terms a fuzzy token may expand to — shared with
    /// prefix expansion, warning included. [`None`] keeps the default, which no
    /// realistic test term set can reach.
    ///
    /// Applied to *both* places the cap can be read from: the threaded
    /// [`Config`] and the search context's own `IteratorsConfig`. The two hold
    /// the same value in a real query, and setting both keeps this test blind to
    /// which of them the evaluator happens to consult.
    max_expansions: Option<u32>,
    /// The search context's `apiVersion`, which gates whether the expansion also
    /// covers the empty term.
    api_version: u32,
}

impl Default for FuzzyOptions {
    fn default() -> Self {
        Self {
            terms: default_terms(),
            weight: 1.0,
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: RS_FIELDMASK_ALL,
            record_field_mask: ALL_INDEXED_FIELDS,
            max_expansions: None,
            // Below the threshold, so the default fixture never silently gains
            // an extra expansion.
            api_version: API_VERSION_NO_EMPTY_TERM,
        }
    }
}

/// Owns everything a `QN_FUZZY` evaluation borrows, so that a test can hold a
/// single value and let the whole graph — index, context, node, and the strings
/// they point into — drop together at the end of the test.
struct FuzzyFixture {
    /// Registers the process-exit cleanup of the global spec dictionaries, which
    /// are shared by every [`TestContext`] and so cannot be freed on drop.
    /// Carried purely for that side effect.
    _guard: GlobalGuard,
    /// Owns the index: the spec, its search context, the terms trie, and the
    /// per-term inverted indexes. Must outlive [`ctx`](Self::ctx), which points
    /// into it.
    _context: TestContext,
    /// The evaluation context under test, wrapping the C `QueryEvalCtx` that
    /// [`_context`](Self::_context) created. Also carries the query status, so
    /// tests read errors and warnings back through it.
    ctx: QueryEvalContext,
    /// The `QN_FUZZY` node being evaluated, carrying the query token, its
    /// maximum edit distance, and the node options from [`FuzzyOptions`].
    node: MockQueryNode,
    /// The evaluation config threaded into [`eval_node`], carrying the
    /// expansion cap ([`Config::max_prefix_expansions`]).
    config: Config,
}

impl FuzzyFixture {
    /// The default term ladder, unit weight, all fields, empty-term expansion off.
    fn new(token: &str, max_dist: i32) -> Self {
        Self::build(token, max_dist, FuzzyOptions::default())
    }

    fn build(token: &str, max_dist: i32, opts: FuzzyOptions) -> Self {
        let _guard = GlobalGuard::default();

        let record_field_mask = opts.record_field_mask;
        let records = opts
            .terms
            .into_iter()
            .map(move |(term, doc_ids)| (term, term_records(&doc_ids, record_field_mask)));
        // Fuzzy shares the terms-trie fixture with prefix expansion; it never
        // consults the suffix trie, so that path stays off.
        let context = TestContext::prefix(records, false);

        let qctx = context.qctx();
        // The expansion narrows each term's field mask with the query-wide one;
        // the zero-init options leave it 0, which would mask out every field, so
        // it is always written — defaulting to "all fields".
        // SAFETY: `qctx` points to a valid, exclusively-owned `QueryEvalCtx`.
        let opts_ptr = unsafe { (*qctx.as_ptr()).opts.cast_mut() };
        // SAFETY: `opts_ptr` is the context's valid, exclusively-owned
        // `RSSearchOptions`.
        unsafe {
            (*opts_ptr).fieldmask = opts.query_field_mask;
        };
        // The empty-term expansion is gated on the *search* context's API
        // version, not on anything in the node or the config.
        // SAFETY: `qctx` points to a valid `QueryEvalCtx` whose `sctx` is the
        // `TestContext`'s live, exclusively-owned `RedisSearchCtx`.
        unsafe {
            (*(*qctx.as_ptr()).sctx).apiVersion = opts.api_version;
        }
        let mut config = Config::default();
        if let Some(cap) = opts.max_expansions {
            config.max_prefix_expansions = cap as usize;
            // SAFETY: `qctx` points to a valid `QueryEvalCtx` whose `config` is
            // the `TestContext`'s live, exclusively-owned `IteratorsConfig`.
            unsafe {
                (*(*qctx.as_ptr()).config).maxPrefixExpansions = cap;
            }
        }
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        let mut node = MockQueryNode::with_token(TokenNodeType::Fuzzy, token);
        node.opts_mut().weight = opts.weight;
        node.opts_mut().field_mask = opts.field_mask;
        node.set_fuzzy_max_dist(max_dist);

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            config,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when
    /// evaluation produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeMut::new(self.node.as_non_null()) };
        eval_node(&mut self.ctx, node_ref, self.config).map(|e| e.into_boxed())
    }

    /// The current query error code (`Ok` when no error was set).
    fn status_code(&mut self) -> QueryErrorCode {
        self.ctx.status().code()
    }

    /// Whether the "reached max prefix expansions" warning was recorded.
    fn reached_max_expansions(&mut self) -> bool {
        self.ctx.status().warnings().reached_max_prefix_expansions()
    }
}

/// Drain an iterator into the list of document IDs it yields, in order.
fn collect_doc_ids(it: &mut EvalResult) -> Vec<u64> {
    let mut ids = Vec::new();
    while let Some(r) = it.read().expect("read must not error") {
        ids.push(r.doc_id);
    }
    ids
}

#[test]
fn eval_fuzzy_expands_and_unions_matches_within_one_edit() {
    // `word` at distance 1 reaches `world` (insert `l`) and `ward` (substitute
    // `a`), but not the next rung of the ladder, `wo` (two edits); their
    // documents union to {1, 2}.
    let mut fixture = FuzzyFixture::new("word", 1);
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_fuzzy_max_dist_widens_the_expansion() {
    // The same token at distance 2 additionally reaches `wo`, showing the bound
    // is read from the node rather than fixed: {1, 2} becomes {1, 2, 3}. The
    // distance-3 rung `wakld` stays out, so the widening is by exactly one.
    let mut fixture = FuzzyFixture::new("word", 2);
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_fuzzy_single_match_returns_reader_directly() {
    // `bananb` at distance 1 reaches only `banana`; the union of one child
    // collapses to that child's term reader.
    let mut fixture = FuzzyFixture::new("bananb", 1);
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with one match must build an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    assert_eq!(collect_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_fuzzy_single_match_uses_unit_child_weight() {
    // A fuzzy token with a non-unit node weight that expands to exactly one
    // term: the union reduces to that child's reader, whose result weight must
    // be the per-expansion unit weight (1.0), not the node weight. The node
    // weight is applied only when a real (multi-child) union is built.
    let mut fixture = FuzzyFixture::build(
        "bananb",
        1,
        FuzzyOptions {
            weight: 5.0,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with one match must build an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.weight, 1.0, "expanded child readers carry unit weight");
}

#[test]
fn eval_fuzzy_multi_match_union_carries_the_node_weight() {
    // The counterpart of the single-match case above: once a real (multi-child)
    // union is built, it is the union that carries the node's weight, so the
    // results a `%word%` query with a `$weight` attribute produces are scored
    // with it. Both `world` and `ward` are one edit away, so the union survives
    // the collapse to a single child.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            weight: 5.0,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.weight, 5.0, "the union carries the node weight");
}

#[test]
fn eval_fuzzy_union_stops_at_the_first_child_holding_a_document() {
    // `world` and `ward` are both one edit from `word`, and document 1 is indexed
    // under both of them. The union is built in quick-exit mode, which returns as
    // soon as one child holds the document instead of aggregating every child
    // that does — so the result has exactly one child, not two.
    //
    // This is the only way the quick-exit flag is visible from here: with each
    // document reachable through a single expansion, as in every other fixture,
    // both modes read identically.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            terms: terms([(&b"world"[..], vec![1]), (&b"ward"[..], vec![1, 2])]),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.doc_id, 1, "both expansions hold document 1");
    let children = r.as_aggregate().expect("a union result is an aggregate");
    assert_eq!(
        children.len(),
        1,
        "a quick-exit union does not aggregate the second child holding the document"
    );
}

#[test]
fn eval_fuzzy_lowercases_the_pattern() {
    // Terms are indexed lowercased, so the token is lowercased before the trie
    // walk: `WORD` expands exactly like `word`, to `world` and `ward`.
    let mut fixture = FuzzyFixture::new("WORD", 1);
    let mut it = fixture
        .eval()
        .expect("an upper-case fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_fuzzy_no_match_yields_empty_union() {
    // `zzzz` is more than one edit from every term: evaluation returns a
    // (non-null) empty union, not `None`, so it yields an iterator that reads
    // nothing.
    let mut fixture = FuzzyFixture::new("zzzz", 1);
    let mut it = fixture
        .eval()
        .expect("a fuzzy token always builds an iterator, even with no matches");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
}

#[test]
fn eval_fuzzy_skips_expansion_without_inverted_index() {
    // `wold` is in the terms trie and within one edit of `word`, but has no
    // document in its inverted index, so no reader opens for it and it
    // contributes nothing; `world` still yields {1}.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            terms: terms([(&b"world"[..], vec![1]), (&b"wold"[..], vec![])]),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    // With `wold` skipped, a single reader remains and the union collapses to it;
    // had a reader been opened over its empty inverted index instead, two children
    // would survive and this would be a union. The document set alone cannot tell
    // the two apart, since the skipped term holds no document either way.
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}

#[test]
fn eval_fuzzy_field_mask_excludes_every_expansion() {
    // The postings are indexed under field bit 0 only, while the node queries
    // field bit 1: no expansion has a reader for the queried field, so the
    // (empty) union yields nothing.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            field_mask: 0b10,
            record_field_mask: 0b01,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token always builds an iterator, even with no readers");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);

    // Same postings, but now the node queries the field they are indexed under:
    // the expansions do open readers, showing the exclusion above comes from the
    // field mask and not from the narrower postings.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            field_mask: 0b01,
            record_field_mask: 0b01,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_fuzzy_narrows_the_reader_field_mask_to_the_query() {
    // Each reader is opened under the node's mask intersected with the
    // query-wide one. Here the postings are indexed under field bit 0 and the
    // node asks for every field, but the query narrows to bit 1 — so no
    // expansion matches, even though the node's own mask would have admitted
    // every one of them.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: 0b10,
            record_field_mask: 0b01,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token always builds an iterator, even with no readers");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);

    // Same postings and node, but the query-wide mask now admits the field they
    // are indexed under — showing the exclusion above came from that mask alone
    // and not from the node's or the postings'.
    let mut fixture = FuzzyFixture::build(
        "word",
        1,
        FuzzyOptions {
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: 0b01,
            record_field_mask: 0b01,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_fuzzy_caps_expansions_and_warns() {
    // Three terms are within one edit of `axx`, but `max_prefix_expansions` is
    // 2: only the first two expansions are opened and the "reached max" warning
    // is set.
    let mut fixture = FuzzyFixture::build(
        "axx",
        1,
        FuzzyOptions {
            terms: terms([
                (&b"axa"[..], vec![1]),
                (&b"axb"[..], vec![2]),
                (&b"axc"[..], vec![3]),
            ]),
            max_expansions: Some(2),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(
        collect_doc_ids(&mut it),
        vec![1, 2],
        "the cap keeps the first expansions in walk order, not an arbitrary two"
    );
    drop(it);
    assert!(
        fixture.reached_max_expansions(),
        "hitting the cap records the reached-max warning"
    );
    assert_eq!(
        fixture.status_code(),
        QueryErrorCode::Ok,
        "reaching the cap is a warning, not an error"
    );
}

#[test]
fn eval_fuzzy_expansion_filling_the_cap_exactly_does_not_warn() {
    // The negative control for the test above: two terms are within one edit of
    // `axx` and the cap is also 2, so the expansion ends because the walk ran
    // out of matches, not because it was cut short. Both documents are still
    // returned and no warning is recorded — the warning reports truncation, so a
    // query that fits in the cap exactly must not claim to have been truncated.
    let mut fixture = FuzzyFixture::build(
        "axx",
        1,
        FuzzyOptions {
            terms: terms([(&b"axa"[..], vec![1]), (&b"axb"[..], vec![2])]),
            max_expansions: Some(2),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
    drop(it);
    assert!(
        !fixture.reached_max_expansions(),
        "an expansion that fits within the cap is not truncated"
    );
}

#[test]
fn eval_fuzzy_cap_counts_opened_readers_not_walked_terms() {
    // The cap bounds the number of *readers* the expansion opens, not the number
    // of terms the walk visits: `axb` has no inverted index, so it opens no
    // reader and consumes no slot, leaving room for `axc` behind it. A cap
    // applied to the walk itself would stop after `axa` and `axb` and lose
    // document 3.
    let mut fixture = FuzzyFixture::build(
        "axx",
        1,
        FuzzyOptions {
            terms: terms([
                (&b"axa"[..], vec![1]),
                (&b"axb"[..], vec![]),
                (&b"axc"[..], vec![3]),
            ]),
            max_expansions: Some(2),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 3]);
    drop(it);
    // The walk ran out of matches with the cap exactly filled, so nothing was
    // truncated — the same condition the warning reports for opened readers.
    assert!(
        !fixture.reached_max_expansions(),
        "a term that opens no reader does not count towards the cap"
    );
}

#[test]
fn eval_fuzzy_appends_the_empty_term_even_with_the_cap_full() {
    // The empty-term expansion is appended after the capped walk, so it is not
    // itself subject to the cap: with room for a single expansion, the walk
    // spends it on `ax` and the empty term's document (9) is still added. A cap
    // applied to the walk and the empty term together would drop document 9.
    let mut fixture = FuzzyFixture::build(
        "a",
        1,
        FuzzyOptions {
            terms: terms([(&b""[..], vec![9]), (&b"ax"[..], vec![1])]),
            api_version: API_VERSION_EMPTY_TERM,
            max_expansions: Some(1),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 9]);
}

#[test]
fn eval_fuzzy_pattern_over_trie_limit_yields_no_iterator_without_error() {
    // A token longer than the trie's fuzzy-walk limit starts no walk at all, so
    // evaluation yields no iterator. Unlike an over-long prefix pattern this is
    // *not* reported as an error: the query silently matches nothing.
    let long = "a".repeat(MAX_FUZZY_PATTERN_RUNES + 1);
    let mut fixture = FuzzyFixture::new(&long, 1);
    assert!(
        fixture.eval().is_none(),
        "an over-long fuzzy token yields no iterator"
    );
    assert_eq!(
        fixture.status_code(),
        QueryErrorCode::Ok,
        "an over-long fuzzy token is not an error"
    );
}

#[test]
fn eval_fuzzy_pattern_at_trie_limit_still_expands() {
    // One rune shorter than the rejected length above, the walk does run: the
    // limit is an inclusive bound on the pattern and not an off-by-one that
    // rejects it. The term is the pattern itself, so a walk that starts at all
    // must reach it.
    let pattern = "a".repeat(MAX_FUZZY_PATTERN_RUNES);
    let mut fixture = FuzzyFixture::build(
        &pattern,
        1,
        FuzzyOptions {
            terms: terms([(pattern.as_bytes(), vec![1])]),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a token at the limit must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}

#[test]
fn eval_fuzzy_pattern_over_the_limit_in_bytes_but_not_in_runes_still_expands() {
    // The limit is on the pattern's length in *runes*, measured after decoding,
    // not on its length in bytes: a pattern of two-byte characters that is well
    // over the limit in bytes but half of it in runes is walked normally. A
    // length check on the raw bytes would reject it and silently yield no
    // iterator at all.
    let pattern = "é".repeat(MAX_FUZZY_PATTERN_RUNES / 2 + 1);
    assert!(
        pattern.len() > MAX_FUZZY_PATTERN_RUNES,
        "the pattern must be over the limit in bytes to be a test of anything"
    );
    let mut fixture = FuzzyFixture::build(
        &pattern,
        1,
        FuzzyOptions {
            terms: terms([(pattern.as_bytes(), vec![1])]),
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a token under the rune limit must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}

#[test]
fn eval_fuzzy_expands_to_the_empty_term_from_api_version_2() {
    // From API version 2, a token no longer than `maxDist` — i.e. one that could
    // be deleted entirely and still be within budget — also expands to the empty
    // term. No trie holds a zero-length key, so the walk can never find it; it is
    // appended explicitly, and its document (9) joins `ax`'s (1).
    let mut fixture = FuzzyFixture::build(
        "a",
        1,
        FuzzyOptions {
            terms: terms([(&b""[..], vec![9]), (&b"ax"[..], vec![1])]),
            api_version: API_VERSION_EMPTY_TERM,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 9]);
}

#[test]
fn eval_fuzzy_does_not_expand_to_the_empty_term_below_api_version_2() {
    // The same index and token one API version lower: the empty term is not
    // expanded to, so document 9 is absent and only `ax`'s document remains.
    let mut fixture = FuzzyFixture::build(
        "a",
        1,
        FuzzyOptions {
            terms: terms([(&b""[..], vec![9]), (&b"ax"[..], vec![1])]),
            api_version: API_VERSION_NO_EMPTY_TERM,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}

#[test]
fn eval_fuzzy_measures_the_empty_term_gate_in_bytes() {
    // The empty-term gate compares the token's length in *bytes* against
    // `maxDist`, even though the distance itself counts runes: `é` is a single
    // rune but two bytes, so at `maxDist` 1 it does not reach the empty term,
    // and document 9 stays out. A gate counting runes instead would let it
    // through and add every document indexed under the empty term.
    let mut fixture = FuzzyFixture::build(
        "é",
        1,
        FuzzyOptions {
            terms: terms([(&b""[..], vec![9]), ("éx".as_bytes(), vec![1])]),
            api_version: API_VERSION_EMPTY_TERM,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}

#[test]
fn eval_fuzzy_does_not_expand_to_the_empty_term_beyond_max_dist() {
    // API version 2, but the token is longer than `maxDist`: deleting it entirely
    // costs more than the budget, so the empty term is not expanded to even
    // though the version allows it. `abd`, one edit away, still yields {1}.
    let mut fixture = FuzzyFixture::build(
        "abc",
        1,
        FuzzyOptions {
            terms: terms([(&b""[..], vec![9]), (&b"abd"[..], vec![1])]),
            api_version: API_VERSION_EMPTY_TERM,
            ..FuzzyOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a fuzzy token with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1]);
}
