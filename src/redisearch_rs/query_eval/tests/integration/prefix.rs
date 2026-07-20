/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_PREFIX → expand a prefix/suffix/contains pattern over the terms trie into
//! a union of per-term readers.
//!
//! Covers the in-memory expansion path (both the brute-force terms-trie scan and
//! the suffix-trie fast path). Each test populates the terms trie with a handful
//! of terms and queries a pattern, asserting the union yields exactly the
//! documents of the matching terms.
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use std::ffi::CString;

use index_result::{RSIndexResult, RSOffsetSlice};
use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{
    QueryEvalContext, QueryNodeRef,
    eval::{self, Config, EvalResult},
};
use query_term::RSQueryTerm;
use query_types::QueryNodeType;
use rqe_core::{FieldMask, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

/// All (low 32) field-mask bits set, so the reader's field-mask filter never
/// excludes a document unless a test narrows the mask deliberately.
const ALL_INDEXED_FIELDS: FieldMask = u32::MAX as FieldMask;

/// Build term postings for the given document IDs, each indexed under
/// `field_mask`. `write_forward_index_entry` only consumes each record's doc id,
/// frequency, and field mask, so a single dummy offset is enough to form a
/// well-formed record.
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
fn default_terms() -> Vec<(&'static str, Vec<u64>)> {
    vec![
        ("apple", vec![1, 2]),
        ("apricot", vec![3]),
        ("grape", vec![4]),
        ("banana", vec![5]),
    ]
}

/// How to build a [`PrefixFixture`].
///
/// [`Default`] describes the case most tests want: the [`default_terms`] set,
/// brute-force expansion over the terms trie, unit weight, and every field
/// queried — leaving each test to override only the one knob it exercises.
struct PrefixOptions {
    /// The terms to index, each with the documents indexed under it. Every term
    /// goes into the terms trie (so the expansion walk can discover it) and gets
    /// an inverted index holding those documents. A term with no documents is
    /// still discoverable but opens no reader.
    terms: Vec<(&'static str, Vec<u64>)>,
    /// Whether to declare the field `WITHSUFFIXTRIE`, which selects the
    /// suffix-trie expansion path over the brute-force terms-trie scan for
    /// suffix and contains patterns. Prefix patterns are unaffected.
    with_suffix_trie: bool,
    /// The node's weight. Applied once by the enclosing union, so it is *not*
    /// visible on the results of a pattern that expands to a single term.
    weight: f64,
    /// The node's field mask: the fields the query asks about, narrowed by the
    /// query-wide mask before reaching each reader. Also decides whether a
    /// suffix/contains pattern is supported, since every queried field must be
    /// covered by the suffix trie.
    field_mask: FieldMask,
    /// The field mask each indexed posting carries, which the reader intersects
    /// with the node's own mask.
    record_field_mask: FieldMask,
    /// Overrides [`Config::max_prefix_expansions`], the cap on how many terms a
    /// pattern may expand to. [`None`] keeps the default, which no realistic
    /// test term set can reach.
    max_expansions: Option<u32>,
}

impl Default for PrefixOptions {
    fn default() -> Self {
        Self {
            terms: default_terms(),
            with_suffix_trie: false,
            weight: 1.0,
            field_mask: RS_FIELDMASK_ALL,
            record_field_mask: ALL_INDEXED_FIELDS,
            max_expansions: None,
        }
    }
}

/// Owns everything a `QN_PREFIX` evaluation borrows, so that a test can hold a
/// single value and let the whole graph — index, context, node, and the strings
/// they point into — drop together at the end of the test.
struct PrefixFixture {
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
    /// The `QN_PREFIX` node being evaluated, carrying the pattern token, the
    /// prefix/suffix anchoring, and the node options from [`PrefixOptions`].
    node: MockQueryNode,
    /// The evaluation config threaded into [`eval::eval_node`], carrying the
    /// prefix-expansion knobs ([`Config::max_prefix_expansions`],
    /// [`Config::min_term_prefix`], ...).
    config: Config,
    /// Backs the token's string pointer in [`node`](Self::node), which borrows
    /// it as a raw pointer; must outlive it.
    _token: CString,
}

impl PrefixFixture {
    /// Default terms, brute-force scan, unit weight, all fields.
    fn new(token: &str, prefix: bool, suffix: bool) -> Self {
        Self::build(token, prefix, suffix, PrefixOptions::default())
    }

    /// Like [`new`](Self::new) but declares the field `WITHSUFFIXTRIE`, so
    /// suffix/contains patterns expand through the suffix trie.
    fn with_suffix_trie(token: &str, prefix: bool, suffix: bool) -> Self {
        Self::build(
            token,
            prefix,
            suffix,
            PrefixOptions {
                with_suffix_trie: true,
                ..PrefixOptions::default()
            },
        )
    }

    fn build(token: &str, prefix: bool, suffix: bool, opts: PrefixOptions) -> Self {
        let _guard = GlobalGuard::default();

        let record_field_mask = opts.record_field_mask;
        let records = opts
            .terms
            .into_iter()
            .map(move |(term, doc_ids)| (term, term_records(&doc_ids, record_field_mask)));
        let context = TestContext::prefix(records, opts.with_suffix_trie);

        let qctx = context.qctx();
        // The expansion narrows each term's field mask with the query-wide one
        // (`opts.fieldmask`); the zero-init options leave it 0, which would mask
        // out every field. Set it to "all fields".
        // SAFETY: `qctx` points to a valid, exclusively-owned `QueryEvalCtx`.
        let opts_ptr = unsafe { (*qctx.as_ptr()).opts.cast_mut() };
        // SAFETY: `opts_ptr` is the context's valid, exclusively-owned
        // `RSSearchOptions`.
        unsafe {
            (*opts_ptr).fieldmask = !0;
        };
        // The prefix-expansion knobs come from the threaded `Config`, so override
        // the cap there rather than in the FFI `IteratorsConfig`.
        let mut config = Config::default();
        if let Some(cap) = opts.max_expansions {
            config.max_prefix_expansions = cap as usize;
        }
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        let token = CString::new(token).expect("token must not contain a NUL byte");
        let mut node = MockQueryNode::new(QueryNodeType::Prefix);
        node.opts_mut().weight = opts.weight;
        node.opts_mut().field_mask = opts.field_mask;
        node.set_token(token.as_ptr().cast_mut(), token.as_bytes().len());
        node.set_prefix_mode(prefix, suffix);

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            config,
            _token: token,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when
    /// evaluation produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        eval::eval_node(&mut self.ctx, &node_ref, self.config).map(|e| e.into_boxed())
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
fn eval_prefix_expands_and_unions_matches() {
    // `ap*` matches `apple` and `apricot`, whose documents union to {1, 2, 3}.
    let mut fixture = PrefixFixture::new("ap", true, false);
    let mut it = fixture
        .eval()
        .expect("a prefix with matches must build an iterator");
    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_prefix_single_match_returns_reader_directly() {
    // `gr*` matches only `grape`; the union of one child collapses to that
    // child's term reader.
    let mut fixture = PrefixFixture::new("gr", true, false);
    let mut it = fixture
        .eval()
        .expect("a prefix with one match must build an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    assert_eq!(collect_doc_ids(&mut it), vec![4]);
}

#[test]
fn eval_prefix_single_match_uses_unit_child_weight() {
    // A prefix with a non-unit node weight that expands to exactly one term: the
    // union reduces to that child's reader, whose result weight must be the
    // per-expansion unit weight (1.0), not the node weight. The node weight is
    // applied only when a real (multi-child) union is built.
    let mut fixture = PrefixFixture::build(
        "gr",
        true,
        false,
        PrefixOptions {
            weight: 5.0,
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a prefix with one match must build an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    let r = it.read().expect("read must not error").expect("one result");
    assert_eq!(r.weight, 1.0, "expanded child readers carry unit weight");
}

#[test]
fn eval_prefix_lowercases_the_pattern() {
    // Terms are indexed lowercased, so the pattern is lowercased before the trie
    // walk: `AP*` expands exactly like `ap*`, to `apple` and `apricot`.
    let mut fixture = PrefixFixture::new("AP", true, false);
    let mut it = fixture
        .eval()
        .expect("an upper-case prefix with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_prefix_skips_expansion_without_inverted_index() {
    // `apex` is in the terms trie but has no document in its inverted index, so
    // no reader opens for it and it contributes nothing; the other two `ap`
    // expansions still union to {1, 2, 3}.
    let mut fixture = PrefixFixture::build(
        "ap",
        true,
        false,
        PrefixOptions {
            terms: vec![
                ("apple", vec![1, 2]),
                ("apex", vec![]),
                ("apricot", vec![3]),
            ],
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a prefix with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_prefix_field_mask_excludes_every_expansion() {
    // The postings are indexed under field bit 0 only, while the node queries
    // field bit 1: no expansion has a reader for the queried field, so the
    // (empty) union yields nothing.
    let mut fixture = PrefixFixture::build(
        "ap",
        true,
        false,
        PrefixOptions {
            field_mask: 0b10,
            record_field_mask: 0b01,
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a prefix always builds an iterator, even with no readers");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);

    // Same postings, but now the node queries the field they are indexed under:
    // the expansions do open readers, showing the exclusion above comes from the
    // field mask and not from the narrower postings.
    let mut fixture = PrefixFixture::build(
        "ap",
        true,
        false,
        PrefixOptions {
            field_mask: 0b01,
            record_field_mask: 0b01,
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a prefix with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_prefix_no_match_yields_empty_union() {
    // `zz*` matches no term: evaluation returns a (non-null) empty union, not
    // `None`, so it yields an iterator that reads nothing.
    let mut fixture = PrefixFixture::new("zz", true, false);
    let mut it = fixture
        .eval()
        .expect("a prefix always builds an iterator, even with no matches");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
}

#[test]
fn eval_prefix_below_min_length_yields_no_iterator() {
    // A single-character prefix is shorter than the default `min_term_prefix`
    // (2), so evaluation short-circuits to `None`.
    let mut fixture = PrefixFixture::new("a", true, false);
    assert!(
        fixture.eval().is_none(),
        "a prefix shorter than the minimum must not build an iterator"
    );
}

#[test]
fn eval_prefix_too_long_reports_limit_error() {
    // A pattern longer than `MAX_RUNE_STR_LEN` (1024) cannot be lowered to runes;
    // evaluation reports a `Limit` error and builds no iterator.
    let long = "a".repeat(2000);
    let mut fixture = PrefixFixture::new(&long, true, false);
    assert!(
        fixture.eval().is_none(),
        "an over-long pattern yields no iterator"
    );
    assert_eq!(fixture.status_code(), QueryErrorCode::Limit);
}

#[test]
fn eval_prefix_caps_expansions_and_warns() {
    // Three terms share the `ax` prefix, but `max_prefix_expansions` is 2: only
    // the first two expansions are opened and the "reached max" warning is set.
    let mut fixture = PrefixFixture::build(
        "ax",
        true,
        false,
        PrefixOptions {
            terms: vec![("axa", vec![1]), ("axb", vec![2]), ("axc", vec![3])],
            max_expansions: Some(2),
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("a prefix with matches must build an iterator");
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

#[test]
fn eval_suffix_matches_terms_ending_with_pattern() {
    // `*ple` matches terms ending in `ple`: only `apple` (docs 1, 2).
    let mut fixture = PrefixFixture::new("ple", false, true);
    let mut it = fixture
        .eval()
        .expect("a suffix with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_contains_matches_terms_containing_pattern() {
    // `*ap*` matches terms containing `ap`: `apple`(1,2), `apricot`(3), and
    // `grape`(4); `banana` has no `ap`.
    let mut fixture = PrefixFixture::new("ap", true, true);
    let mut it = fixture
        .eval()
        .expect("a contains pattern with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4]);
}

#[test]
fn eval_suffix_via_suffix_trie_matches_terms_ending_with_pattern() {
    // The suffix-trie path yields the same result as the brute-force scan:
    // `*ple` matches only `apple` (docs 1, 2).
    let mut fixture = PrefixFixture::with_suffix_trie("ple", false, true);
    let mut it = fixture
        .eval()
        .expect("a suffix with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_contains_via_suffix_trie_matches_terms_containing_pattern() {
    // The suffix-trie contains path matches the brute-force result: `*ap*`
    // matches `apple`(1,2), `apricot`(3), and `grape`(4).
    let mut fixture = PrefixFixture::with_suffix_trie("ap", true, true);
    let mut it = fixture
        .eval()
        .expect("a contains pattern with matches must build an iterator");
    assert_eq!(collect_doc_ids(&mut it), vec![1, 2, 3, 4]);
}

#[test]
fn eval_contains_on_field_without_suffix_support_errors() {
    // The spec has a suffix trie, but the query targets a field not covered by
    // it (a field-mask bit outside `suffixMask`). The contains query is
    // unsupported: an error is set and the (empty) union yields nothing.
    let mut fixture = PrefixFixture::build(
        "ple",
        false,
        true,
        PrefixOptions {
            with_suffix_trie: true,
            // A field-mask bit for a field other than the single suffix-trie
            // field (index 0, bit 0b01).
            field_mask: 0b10,
            ..PrefixOptions::default()
        },
    );
    let mut it = fixture
        .eval()
        .expect("evaluation still returns the empty union");
    assert_eq!(collect_doc_ids(&mut it), Vec::<u64>::new());
    drop(it);
    assert_eq!(fixture.status_code(), QueryErrorCode::Generic);
}
