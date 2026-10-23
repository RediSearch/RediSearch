/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_WILDCARD_QUERY → expand a verbatim wildcard pattern (`*` matches any run
//! of characters, `?` exactly one) over the terms trie into a union of per-term
//! readers.
//!
//! Covers both expansion walks — the brute-force terms-trie scan and the
//! suffix-trie fast path, including the fallback from one to the other. Each test
//! populates the terms trie with a handful of terms and queries a pattern,
//! asserting the union yields exactly the documents of the matching terms.
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use index_result::{RSIndexResult, RSOffsetSlice};
use query::mock::{MockQueryNode, TokenNodeType};
use query_error::QueryErrorCode;
use query_eval::{
    Config, EvalResult, QueryEvalContext, QueryNode, QueryNodeMut, QueryNodeRef, eval_node,
};
use query_term::RSQueryTerm;
use rqe_core::{FieldMask, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

/// All (low 32) field-mask bits set, so the reader's field-mask filter never
/// excludes a document unless a test narrows the mask deliberately.
const ALL_INDEXED_FIELDS: FieldMask = u32::MAX as FieldMask;

/// Build term postings for the given document IDs, each indexed under
/// `field_mask`.
fn term_records(doc_ids: &[u64], field_mask: FieldMask) -> Vec<RSIndexResult<'static>> {
    const OFFSETS: &[u8] = &[0];
    doc_ids
        .iter()
        .map(|&doc_id| {
            let mut term = RSQueryTerm::new("t", 1, 0);
            term.set_idf(5.0);
            term.set_bm25_idf(10.0);
            RSIndexResult::build_term()
                .borrowed_record(Some(term), RSOffsetSlice::from_slice(OFFSETS))
                .doc_id(doc_id)
                .field_mask(field_mask)
                .frequency(1)
                .build()
        })
        .collect()
}

/// The default term set: `apple`(1,2), `apricot`(3), `grape`(4), `banana`(5).
fn default_terms() -> Vec<(&'static [u8], Vec<u64>)> {
    vec![
        (b"apple", vec![1, 2]),
        (b"apricot", vec![3]),
        (b"grape", vec![4]),
        (b"banana", vec![5]),
    ]
}

/// How to build a [`WildcardFixture`]. [`Default`] describes the case most tests
/// want, leaving each test to override only the knob it exercises.
struct WildcardOptions {
    /// The terms to index, each with the documents indexed under it. Byte
    /// strings, not `str`s: the terms trie decodes them without validating, so a
    /// term key may hold bytes UTF-8 forbids.
    terms: Vec<(&'static [u8], Vec<u64>)>,
    /// Whether to declare the field `WITHSUFFIXTRIE`, which makes the expansion
    /// prefer the suffix trie over the brute-force terms-trie scan.
    with_suffix_trie: bool,
    /// The node's weight, applied once by the enclosing union.
    weight: f64,
    /// The node's field mask: the fields the query asks about. Also decides
    /// whether the suffix trie may be used, since every queried field must be
    /// covered by it.
    field_mask: FieldMask,
    /// The query-wide field mask, which the node's mask is narrowed by before
    /// reaching each reader. Deliberately separate from
    /// [`field_mask`](Self::field_mask): only one of the two gates the
    /// suffix-trie decision.
    query_field_mask: FieldMask,
    /// The field mask each indexed posting carries, which the reader intersects
    /// with the effective mask.
    record_field_mask: FieldMask,
    /// Overrides [`Config::max_prefix_expansions`]. [`None`] keeps the default,
    /// which no realistic test term set can reach.
    max_expansions: Option<u32>,
    /// Overrides [`Config::min_term_prefix`]. Used to show it does *not* apply to
    /// a wildcard pattern.
    min_term_prefix: Option<u32>,
}

impl Default for WildcardOptions {
    fn default() -> Self {
        Self {
            terms: default_terms(),
            with_suffix_trie: false,
            weight: 1.0,
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: RS_FIELDMASK_ALL,
            record_field_mask: ALL_INDEXED_FIELDS,
            max_expansions: None,
            min_term_prefix: None,
        }
    }
}

/// Owns everything a `QN_WILDCARD_QUERY` evaluation borrows, so a test can hold a
/// single value and let the whole graph drop together at the end.
struct WildcardFixture {
    /// Registers the process-exit cleanup of the global spec dictionaries.
    _guard: GlobalGuard,
    /// Owns the index: the spec, its search context, the terms trie, and the
    /// per-term inverted indexes. Must outlive [`ctx`](Self::ctx).
    _context: TestContext,
    /// The evaluation context under test. Also carries the query status, so tests
    /// read errors and warnings back through it.
    ctx: QueryEvalContext,
    /// The `QN_WILDCARD_QUERY` node being evaluated.
    node: MockQueryNode,
    /// The evaluation config threaded into [`eval_node`].
    config: Config,
}

impl WildcardFixture {
    /// Default terms, brute-force scan, unit weight, all fields.
    fn new(pattern: &str) -> Self {
        Self::build(pattern.as_bytes(), WildcardOptions::default())
    }

    /// Like [`new`](Self::new) but declares the field `WITHSUFFIXTRIE`, so the
    /// expansion prefers the suffix trie.
    fn with_suffix_trie(pattern: &str) -> Self {
        Self::build(
            pattern.as_bytes(),
            WildcardOptions {
                with_suffix_trie: true,
                ..WildcardOptions::default()
            },
        )
    }

    fn build(pattern: &[u8], opts: WildcardOptions) -> Self {
        let _guard = GlobalGuard::default();

        let record_field_mask = opts.record_field_mask;
        let records = opts
            .terms
            .into_iter()
            .map(move |(term, doc_ids)| (term, term_records(&doc_ids, record_field_mask)));
        let context = TestContext::prefix(records, opts.with_suffix_trie);

        let qctx = context.qctx();
        // The expansion narrows each term's field mask with the query-wide one;
        // the zero-init options leave it 0, which would mask out every field.
        // SAFETY: `qctx` points to a valid, exclusively-owned `QueryEvalCtx`.
        let opts_ptr = unsafe { (*qctx.as_ptr()).opts.cast_mut() };
        // SAFETY: `opts_ptr` is the context's valid, exclusively-owned
        // `RSSearchOptions`.
        unsafe {
            (*opts_ptr).fieldmask = opts.query_field_mask;
        };

        let mut config = Config::default();
        if let Some(cap) = opts.max_expansions {
            config.max_prefix_expansions = cap as usize;
        }
        if let Some(min) = opts.min_term_prefix {
            config.min_term_prefix = min;
        }
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        // `with_token` gives the node a writable, NUL-terminated copy of the
        // pattern that it owns: evaluation unescapes through the token in place
        // and hands it to the union as a C string, so it needs both.
        let mut node = MockQueryNode::with_token(TokenNodeType::WildcardQuery, pattern);
        node.opts_mut().weight = opts.weight;
        node.opts_mut().field_mask = opts.field_mask;

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            config,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when evaluation
    /// produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeMut::new(self.node.as_non_null()) };
        eval_node(&mut self.ctx, node_ref, self.config).map(|e| e.into_boxed())
    }

    /// The token's current string, which evaluation may have rewritten in place.
    ///
    /// Copied out rather than borrowed: the handle the token is read through
    /// borrows from the local view of the node, not from the fixture.
    fn token_bytes(&self) -> Vec<u8> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode`, and this shared
        // borrow of the fixture rules out a concurrent exclusive view of it.
        let node = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        let QueryNode::WildcardQuery { tok } = node.as_enum() else {
            panic!("fixture builds a wildcard-query node");
        };
        tok.as_bytes()
            .expect("fixture installs a token string")
            .to_vec()
    }

    /// The current query error code (`Ok` when no error was set).
    fn status_code(&mut self) -> QueryErrorCode {
        self.ctx.status().code()
    }

    /// The current query error message.
    fn status_message(&mut self) -> String {
        self.ctx
            .status()
            .public_message()
            .map(|m| m.to_string_lossy().into_owned())
            .unwrap_or_default()
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

// ── matching ───────────────────────────────────────────────────────────────

#[test]
fn eval_wildcard_query_star_expands_and_unions_matches() {
    // `ap*` matches `apple` and `apricot`, whose documents union to {1, 2, 3}.
    let mut fixture = WildcardFixture::new("ap*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_wildcard_query_question_mark_matches_exactly_one_character() {
    // `?rape` matches `grape` but not `rape` or `ggrape`: `?` is exactly one.
    let mut fixture = WildcardFixture::build(
        b"?rape",
        WildcardOptions {
            terms: vec![
                (b"grape", vec![4]),
                (b"rape", vec![6]),
                (b"ggrape", vec![7]),
            ],
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![4]);
}

#[test]
fn eval_wildcard_query_mixed_star_and_question_mark() {
    // `a?*t` — `a`, exactly one character, any run, then a final `t`: `apricot`
    // matches, `apple` does not end in `t`, `at` has nothing for the `?`.
    let mut fixture = WildcardFixture::build(
        b"a?*t",
        WildcardOptions {
            terms: vec![(b"apricot", vec![3]), (b"apple", vec![1]), (b"at", vec![8])],
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![3]);
}

#[test]
fn eval_wildcard_query_interior_star_matches_around_it() {
    // `a*t` anchors both ends: `apricot` matches, `apple` does not.
    let mut fixture = WildcardFixture::new("a*t");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![3]);
}

#[test]
fn eval_wildcard_query_lowercases_the_pattern() {
    // Terms are indexed lowercased, so the pattern is lowercased before the trie
    // walk: `AP*` expands exactly like `ap*`.
    let mut fixture = WildcardFixture::new("AP*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_wildcard_query_single_match_collapses_to_the_reader() {
    // `gr*` matches only `grape`; a union of one child collapses to that child's
    // term reader, which carries the per-expansion unit weight rather than the
    // node weight — the node weight is applied only by a real union.
    let mut fixture = WildcardFixture::build(
        b"gr*",
        WildcardOptions {
            weight: 5.0,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.doc_id, 4);
    assert_eq!(r.weight, 1.0, "expanded child readers carry unit weight");
}

#[test]
fn eval_wildcard_query_union_carries_the_node_weight() {
    // `ap*` expands to two terms, so a real union is built and it carries the
    // node's weight. This is the counterpart to the single-match case above: there
    // the union collapses to its child and the node weight is deliberately absent,
    // so without this test nothing would notice the weight being dropped, or the
    // wrong value being passed, on the path where it does apply.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            weight: 5.0,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.weight, 5.0, "a real union carries the node weight");
}

#[test]
fn eval_wildcard_query_no_match_yields_empty_union() {
    // `zz*` matches no term: evaluation still returns an iterator, which reads
    // nothing — it is never `None`.
    let mut fixture = WildcardFixture::new("zz*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
}

#[test]
fn eval_wildcard_query_expands_a_term_whose_key_is_not_utf8() {
    // A term key is not necessarily valid UTF-8. `\xED\xA0\xBD` is the three-byte
    // encoding of the lone surrogate `U+D83D`, which UTF-8 forbids — and which is
    // what a non-BMP codepoint becomes once the trie truncates it to a 16-bit
    // rune. The expansion must hand those bytes to the inverted-index lookup
    // unvalidated, or the term's document is dropped.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            terms: vec![(b"ap\xED\xA0\xBDle", vec![7]), (b"apple", vec![1, 2])],
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 7]);
}

#[test]
fn eval_wildcard_query_skips_expansion_without_inverted_index() {
    // `apex` is in the terms trie but has no document, so it opens no reader and
    // contributes nothing; the other two `ap` expansions still union to {1, 2, 3}.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            terms: vec![
                (b"apple", vec![1, 2]),
                (b"apex", vec![]),
                (b"apricot", vec![3]),
            ],
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

// ── escapes ────────────────────────────────────────────────────────────────

#[test]
fn eval_wildcard_query_removes_escapes_from_the_token() {
    // `ap\*le` unescapes to `ap*le` before the walk, so the `*` is a wildcard and
    // the pattern matches `apple`. The token is rewritten *in place*, because it
    // is also what the union reports as its profiling query string.
    let mut fixture = WildcardFixture::new("ap\\*le");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
    drop(it);
    assert_eq!(
        fixture.token_bytes(),
        b"ap*le",
        "the escape is stripped from the node's own token"
    );
}

#[test]
fn eval_wildcard_query_removes_a_trailing_backslash() {
    // A pattern ending in a lone backslash drops it: `apple\` becomes `apple`,
    // which matches the term exactly.
    //
    // This is the one input shape where escape removal reads the byte *past* the
    // token — the escape has nothing after it, so the converter copies the
    // terminator into its place and stops there. Nothing else in this file
    // exercises that read.
    let mut fixture = WildcardFixture::new("apple\\");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
    drop(it);
    assert_eq!(
        fixture.token_bytes(),
        b"apple",
        "the trailing backslash is dropped, shortening the token"
    );
}

#[test]
fn eval_wildcard_query_escape_removal_leaves_an_unescaped_token_alone() {
    // A pattern with no backslash is returned unchanged, length included.
    let mut fixture = WildcardFixture::new("ap*");
    let _ = fixture.eval();
    assert_eq!(fixture.token_bytes(), b"ap*");
}

// ── degenerate patterns ────────────────────────────────────────────────────

/// The default terms plus the empty term, indexing document 9 — what an
/// `INDEXEMPTY` field's empty value produces. It reaches the inverted index only:
/// neither trie accepts a zero-length key.
fn terms_with_empty() -> Vec<(&'static [u8], Vec<u64>)> {
    let mut terms = default_terms();
    terms.push((b"", vec![9]));
    terms
}

#[test]
fn eval_wildcard_query_empty_pattern_matches_nothing_without_an_empty_term() {
    // An empty pattern matches exactly the empty term. Without `INDEXEMPTY` there
    // is no inverted index for it, so the union comes back empty — and that is a
    // miss, not an error.
    let mut fixture = WildcardFixture::new("");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);
    assert_eq!(
        fixture.status_code(),
        QueryErrorCode::Ok,
        "an empty pattern is not an error, just a miss"
    );
}

#[test]
fn eval_wildcard_query_empty_pattern_matches_the_empty_term() {
    // With the empty term indexed, an empty pattern matches it and nothing else:
    // the reader is opened directly, since no trie holds a zero-length key for a
    // walk to find.
    let mut fixture = WildcardFixture::build(
        b"",
        WildcardOptions {
            terms: terms_with_empty(),
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![9]);
    drop(it);
    assert_eq!(fixture.status_code(), QueryErrorCode::Ok);
}

#[test]
fn eval_wildcard_query_empty_pattern_matches_the_empty_term_with_a_suffix_trie() {
    // Same answer on a spec with a suffix trie: the empty pattern is recognised
    // before either walk is chosen, so neither runs — the suffix walk's own
    // no-anchor path is not what produces this answer.
    let mut fixture = WildcardFixture::build(
        b"",
        WildcardOptions {
            terms: terms_with_empty(),
            with_suffix_trie: true,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![9]);
}

#[test]
fn eval_wildcard_query_empty_pattern_respects_the_field_mask() {
    // The empty term's reader is opened with the same effective field mask as any
    // expanded term's, so a document indexed under another field is not matched.
    let mut fixture = WildcardFixture::build(
        b"",
        WildcardOptions {
            terms: terms_with_empty(),
            field_mask: 0b01,
            record_field_mask: 0b10,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
}

#[test]
fn eval_wildcard_query_bare_star_matches_every_term() {
    // `*` is the shortest pattern that matches everything. It is also the case
    // that reads the element past the pattern — a pattern ending in `*` makes the
    // matcher dereference its cursor without re-checking it against the end — so
    // the terminator the pattern carries has to be there and has to be zero.
    let mut fixture = WildcardFixture::new("*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4, 5]);
}

#[test]
fn eval_wildcard_query_ignores_min_term_prefix() {
    // A wildcard pattern has no minimum length: unlike a prefix node, it is not
    // rejected for being shorter than `min_term_prefix`. Copying that guard over
    // would silently turn `w'*'` into a miss under the default MINPREFIX of 2.
    let mut fixture = WildcardFixture::build(
        b"*",
        WildcardOptions {
            min_term_prefix: Some(10),
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4, 5]);
}

#[test]
fn eval_wildcard_query_too_long_reports_limit_error() {
    // A pattern longer than `MAX_RUNE_STR_LEN` (1024) cannot be lowered to runes.
    // The message is user-visible, and names this node type rather than the
    // prefix/suffix/infix wording a prefix node uses.
    let long = "a".repeat(2000);
    let mut fixture = WildcardFixture::new(&long);
    assert!(
        fixture.eval().is_none(),
        "an over-long pattern yields no iterator"
    );
    assert_eq!(fixture.status_code(), QueryErrorCode::Limit);
    assert_eq!(
        fixture.status_message(),
        "Wildcard query string is too long. Maximum allowed length is 1024"
    );
}

// ── field masks ────────────────────────────────────────────────────────────

#[test]
fn eval_wildcard_query_narrows_the_reader_field_mask_to_the_query() {
    // The readers are opened under the node's mask intersected with the
    // query-wide one. Here the postings are indexed under field bit 0 and the
    // node asks for every field, but the query narrows to bit 1 — so nothing
    // matches, even though the node's own mask would have admitted it.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: 0b10,
            record_field_mask: 0b01,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);
    assert_eq!(
        fixture.status_code(),
        QueryErrorCode::Ok,
        "narrowing by the query mask is not an error"
    );

    // Same postings and node, but the query-wide mask now admits the field they
    // are indexed under — showing the exclusion above came from the mask and not
    // from the terms.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            field_mask: RS_FIELDMASK_ALL,
            query_field_mask: 0b01,
            record_field_mask: 0b01,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_wildcard_query_field_mask_excludes_every_expansion() {
    // The node queries a field the postings are not indexed under, so no
    // expansion opens a reader and the (empty) union yields nothing.
    let mut fixture = WildcardFixture::build(
        b"ap*",
        WildcardOptions {
            field_mask: 0b10,
            record_field_mask: 0b01,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
}

// ── suffix trie ────────────────────────────────────────────────────────────

#[test]
fn eval_wildcard_query_via_suffix_trie_matches_the_same_terms() {
    // The suffix-trie path yields the same result as the brute-force scan.
    let mut fixture = WildcardFixture::with_suffix_trie("*ricot");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![3]);
}

#[test]
fn eval_wildcard_query_via_suffix_trie_matches_an_interior_run() {
    // `*ppl*` anchors on `ppl`, which the suffix trie can look up.
    let mut fixture = WildcardFixture::with_suffix_trie("*ppl*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_wildcard_query_falls_back_to_brute_force_without_an_anchor() {
    // The suffix trie is chosen a token at a time, splitting the pattern on `*`,
    // so a pattern that is nothing but `*` leaves no token to anchor on. The
    // suffix walk declines it and the brute-force scan over the terms trie
    // answers instead, matching every term.
    //
    // Note that `?` does *not* make a pattern unanchorable — the split is on `*`
    // alone, so `?*` anchors on the `?` token and goes through the suffix trie.
    let mut fixture = WildcardFixture::with_suffix_trie("*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4, 5]);
    drop(it);
    assert_eq!(
        fixture.status_code(),
        QueryErrorCode::Ok,
        "falling back to brute force is not an error"
    );
}

#[test]
fn eval_wildcard_query_via_suffix_trie_anchors_on_a_question_mark_token() {
    // `?*` splits into the single token `?`, which the suffix trie can anchor on,
    // so this goes through the suffix walk rather than the fallback above. Every
    // term of at least one character matches either way; the point is that the
    // two paths agree.
    let mut fixture = WildcardFixture::with_suffix_trie("?*");
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4, 5]);
}

#[test]
fn eval_wildcard_query_unsupported_fields_errors_without_falling_back() {
    // The spec has a suffix trie but the node queries a field outside it. That is
    // an error, and — unlike the no-anchor case above — it does *not* fall back to
    // the brute-force scan: the union comes back empty.
    let mut fixture = WildcardFixture::build(
        b"*ppl*",
        WildcardOptions {
            with_suffix_trie: true,
            // A field bit the suffix trie does not cover, and not "all fields",
            // which would be accepted unconditionally.
            field_mask: 0b10,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);
    assert_eq!(fixture.status_code(), QueryErrorCode::Generic);
    assert_eq!(
        fixture.status_message(),
        "Contains query on fields without WITHSUFFIXTRIE support"
    );
}

#[test]
fn eval_wildcard_query_unsupported_fields_error_applies_to_an_empty_pattern() {
    // The field-support check does not depend on the pattern: an empty pattern on
    // a field the suffix trie does not cover is still the unsupported-fields
    // error. It outranks the empty-pattern match below it — the empty term is
    // indexed here, so answering the pattern instead would both drop a
    // user-visible error and return a document the error says nothing about.
    let mut fixture = WildcardFixture::build(
        b"",
        WildcardOptions {
            terms: terms_with_empty(),
            with_suffix_trie: true,
            field_mask: 0b10,
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);
    assert_eq!(fixture.status_code(), QueryErrorCode::Generic);
    assert_eq!(
        fixture.status_message(),
        "Contains query on fields without WITHSUFFIXTRIE support"
    );
}

// ── expansion cap ──────────────────────────────────────────────────────────

#[test]
fn eval_wildcard_query_caps_expansions_and_warns() {
    // Three terms match `ax*` but `max_prefix_expansions` is 2: only the first two
    // expansions are opened, and the "reached max" warning is set.
    let mut fixture = WildcardFixture::build(
        b"ax*",
        WildcardOptions {
            terms: vec![(b"axa", vec![1]), (b"axb", vec![2]), (b"axc", vec![3])],
            max_expansions: Some(2),
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(
        collect_doc_ids(&mut it).len(),
        2,
        "expansion is capped at max_prefix_expansions"
    );
    drop(it);
    assert!(
        fixture.reached_max_expansions(),
        "hitting the cap records the reached-max warning"
    );
}

/// The three terms `abab`, `abc` and `xab` all contain `ab`, with one document
/// each, so `*ab*` matches every one of them however it is expanded.
fn ab_terms() -> Vec<(&'static [u8], Vec<u64>)> {
    vec![(b"abab", vec![1]), (b"abc", vec![2]), (b"xab", vec![3])]
}

#[test]
fn eval_wildcard_query_caps_expansions_on_the_suffix_trie_walk() {
    // The cap applies to the suffix-trie walk too, and it counts readers *opened*,
    // not distinct terms: `abab` is reachable under more than one matching suffix
    // key, so it is expanded more than once and each expansion counts.
    //
    // A cap of 3 is what makes that observable, and is also what lets this test
    // tell the two walks apart. Three distinct terms match, so the brute-force
    // walk fits inside a cap of 3 exactly — all three documents, no warning. The
    // suffix walk spends one of its three on the repeat of `abab`, so it loses a
    // document *and* trips the warning. Were the suffix walk to quietly decline
    // and fall back, this test would see the brute-force answer and fail; that is
    // the one place the two paths differ by result rather than only by cost.
    let mut fixture = WildcardFixture::build(
        b"*ab*",
        WildcardOptions {
            with_suffix_trie: true,
            terms: ab_terms(),
            max_expansions: Some(3),
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(
        collect_doc_ids(&mut it),
        vec![1, 3],
        "a repeated expansion consumes cap budget, costing document 2"
    );
    drop(it);
    assert!(
        fixture.reached_max_expansions(),
        "hitting the cap records the reached-max warning"
    );
}

#[test]
fn eval_wildcard_query_brute_force_walk_does_not_repeat_a_term() {
    // The counterpart to the test above, on the same terms and the same cap but
    // with no suffix trie: the brute-force walk visits each matching term once, so
    // all three fit within a cap of 3 and no warning is recorded. Together the two
    // tests pin which walk ran, not merely that some walk did.
    let mut fixture = WildcardFixture::build(
        b"*ab*",
        WildcardOptions {
            with_suffix_trie: false,
            terms: ab_terms(),
            max_expansions: Some(3),
            ..WildcardOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a wildcard always builds an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
    drop(it);
    assert!(
        !fixture.reached_max_expansions(),
        "three distinct terms fit within a cap of three"
    );
}
