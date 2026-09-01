/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_LEXRANGE → expand every TEXT term between two lexicographic bounds into a
//! union of per-term readers.
//!
//! Each test populates the terms trie with a handful of terms and evaluates a
//! range, asserting the union yields exactly the documents of the terms inside
//! it. Tag-field ranges are evaluated in C (`Query_EvalTagLexRangeNode`) and so
//! are covered by the Python flow tests instead.
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{Config, EvalResult, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_core::RS_FIELDMASK_ALL;
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::ContractChecker;
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

use crate::util::{ALL_INDEXED_FIELDS, term_records};

/// The default term set: `apple`(1), `banana`(2), `cherry`(3), `date`(4).
fn default_terms() -> Vec<(&'static [u8], Vec<u64>)> {
    vec![
        (b"apple", vec![1]),
        (b"banana", vec![2]),
        (b"cherry", vec![3]),
        (b"date", vec![4]),
    ]
}

/// One side of the range under test: the bound's bytes and whether it is
/// inclusive. `None` leaves the side unbounded.
///
/// The bound is copied as the fixture is built, so it only has to outlive the
/// call that passes it.
type BoundSpec<'a> = Option<(&'a str, bool)>;

/// Owns everything a `QN_LEXRANGE` evaluation borrows (index, context, node, and
/// the bound strings the node points into) so it all drops together.
struct LexRangeFixture {
    /// Registers the process-exit cleanup of the global spec dictionaries, which
    /// are shared by every [`TestContext`] and so cannot be freed on drop.
    /// Carried purely for that side effect.
    _guard: GlobalGuard,
    /// Owns the index: the spec, its search context, the terms trie, and the
    /// per-term inverted indexes. Must outlive [`ctx`](Self::ctx).
    _context: TestContext,
    /// The bound bytes the node's `begin`/`end` pointers address. The node
    /// borrows rather than owns them, so the fixture holds them to outlive it.
    _bounds: Vec<Vec<u8>>,
    /// The evaluation context under test. Also carries the query status, so
    /// tests read errors back through it.
    ctx: QueryEvalContext,
    /// The `QN_LEXRANGE` node being evaluated.
    node: MockQueryNode,
    /// The evaluation config threaded into [`eval_node`], carrying the
    /// expansion cap a range shares with the other expanding node types.
    config: Config,
}

impl LexRangeFixture {
    /// A range over [`default_terms`] with the default expansion cap.
    fn new(begin: BoundSpec<'_>, end: BoundSpec<'_>) -> Self {
        Self::build(begin, end, default_terms(), None)
    }

    fn build(
        begin: BoundSpec<'_>,
        end: BoundSpec<'_>,
        terms: Vec<(&'static [u8], Vec<u64>)>,
        max_expansions: Option<usize>,
    ) -> Self {
        let _guard = GlobalGuard::default();

        let records = terms
            .into_iter()
            .map(|(term, doc_ids)| (term, term_records(&doc_ids, ALL_INDEXED_FIELDS)));
        let context = TestContext::prefix(records, false);

        let qctx = context.qctx();
        // The expansion narrows each term's field mask with the query-wide one;
        // the zero-init options leave it 0, which would mask out every field.
        // SAFETY: `qctx` points to a valid, exclusively-owned `QueryEvalCtx`.
        let opts_ptr = unsafe { (*qctx.as_ptr()).opts.cast_mut() };
        // SAFETY: `opts_ptr` is the context's valid, exclusively-owned
        // `RSSearchOptions`.
        unsafe {
            (*opts_ptr).fieldmask = !0;
        };
        let mut config = Config::default();
        if let Some(cap) = max_expansions {
            config.max_prefix_expansions = cap;
        }
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        let mut bounds: Vec<Vec<u8>> = Vec::new();
        let mut to_ptr = |bound: BoundSpec<'_>| match bound {
            None => (std::ptr::null_mut(), 0, false),
            Some((s, inclusive)) => {
                let mut owned = s.as_bytes().to_vec();
                let ptr = owned.as_mut_ptr().cast();
                let len = owned.len();
                bounds.push(owned);
                (ptr, len, inclusive)
            }
        };
        let (begin_ptr, begin_len, include_begin) = to_ptr(begin);
        let (end_ptr, end_len, include_end) = to_ptr(end);

        let mut node = MockQueryNode::new(QueryNodeType::LexRange);
        node.opts_mut().weight = 1.0;
        node.opts_mut().field_mask = RS_FIELDMASK_ALL;
        node.set_lex_range(
            begin_ptr,
            begin_len,
            include_begin,
            end_ptr,
            end_len,
            include_end,
        );

        Self {
            _guard,
            _context: context,
            _bounds: bounds,
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
fn collect_doc_ids<'index>(it: &mut impl RQEIterator<'index>) -> Vec<u64> {
    let mut ids = Vec::new();
    while let Some(r) = it.read().expect("read must not error") {
        ids.push(r.doc_id);
    }
    ids
}

/// Evaluate a range over the default terms and collect the documents it matches.
fn doc_ids_for(begin: BoundSpec<'_>, end: BoundSpec<'_>) -> Vec<u64> {
    let mut fixture = LexRangeFixture::new(begin, end);
    let mut it = ContractChecker::new(fixture.eval().expect("a range must build an iterator"));
    collect_doc_ids(&mut it)
}

#[test]
fn exclusive_lower_bound_excludes_the_bound() {
    // (banana, +inf) → cherry, date.
    assert_eq!(doc_ids_for(Some(("banana", false)), None), vec![3, 4]);
}

#[test]
fn inclusive_lower_bound_includes_the_bound() {
    // [banana, +inf) → banana, cherry, date.
    assert_eq!(doc_ids_for(Some(("banana", true)), None), vec![2, 3, 4]);
}

#[test]
fn exclusive_upper_bound_excludes_the_bound() {
    // (-inf, cherry) → apple, banana.
    assert_eq!(doc_ids_for(None, Some(("cherry", false))), vec![1, 2]);
}

#[test]
fn inclusive_upper_bound_includes_the_bound() {
    // (-inf, cherry] → apple, banana, cherry.
    assert_eq!(doc_ids_for(None, Some(("cherry", true))), vec![1, 2, 3]);
}

#[test]
fn both_bounds_close_the_range() {
    // (apple, date) → banana, cherry.
    assert_eq!(
        doc_ids_for(Some(("apple", false)), Some(("date", false))),
        vec![2, 3]
    );
}

#[test]
fn unbounded_on_both_sides_matches_every_term() {
    assert_eq!(doc_ids_for(None, None), vec![1, 2, 3, 4]);
}

#[test]
fn a_bound_need_not_be_an_indexed_term() {
    // "c" is indexed nowhere, but still orders the terms: (c, +inf) → cherry,
    // date. A range compares against its bound, it does not look it up.
    assert_eq!(doc_ids_for(Some(("c", false)), None), vec![3, 4]);
}

#[test]
fn a_bound_past_the_last_term_matches_nothing() {
    let mut fixture = LexRangeFixture::new(Some(("zzz", false)), None);
    let mut it = ContractChecker::new(fixture.eval().expect("a range must build an iterator"));
    assert!(collect_doc_ids(&mut it).is_empty());
}

#[test]
fn bounds_are_lowercased_to_match_the_terms_trie() {
    // Terms are indexed lowercased, so an upper-case bound must answer the same
    // range as its lower-case spelling rather than sort before every term.
    assert_eq!(
        doc_ids_for(Some(("BANANA", false)), None),
        doc_ids_for(Some(("banana", false)), None)
    );
}

#[test]
fn derived_terms_are_not_part_of_the_range() {
    // The indexer files a stem under `+<stem>` and a synonym under `~<id>` in the
    // same trie as the terms a document actually holds. `+` sorts below every
    // letter and `~` above, so an unfiltered range would sweep them in: here
    // (-inf, apple) would match `+zebra`'s document and (apple, +inf) would match
    // `~syn`'s, neither of which contains text in the range.
    let terms = vec![
        (b"+zebra".as_slice(), vec![10]),
        (b"apple".as_slice(), vec![1]),
        (b"~syn".as_slice(), vec![11]),
    ];

    let mut below = LexRangeFixture::build(None, Some(("apple", false)), terms.clone(), None);
    assert!(
        below
            .eval()
            .is_none_or(|mut it| collect_doc_ids(&mut it).is_empty()),
        "a stem-marked term must not answer a range below it"
    );

    let mut above = LexRangeFixture::build(Some(("apple", false)), None, terms, None);
    assert!(
        above
            .eval()
            .is_none_or(|mut it| collect_doc_ids(&mut it).is_empty()),
        "a synonym-marked term must not answer a range above it"
    );
}

#[test]
fn a_single_matching_term_collapses_to_its_reader() {
    // A union of one child reduces to that child, as the other expansions do.
    let mut fixture = LexRangeFixture::new(Some(("cherry", false)), None);
    let mut it = ContractChecker::new(fixture.eval().expect("a range must build an iterator"));
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
    assert_eq!(collect_doc_ids(&mut it), vec![4]);
}

#[test]
fn several_matching_terms_build_a_union() {
    let mut fixture = LexRangeFixture::new(Some(("apple", false)), None);
    let mut it = ContractChecker::new(fixture.eval().expect("a range must build an iterator"));
    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(collect_doc_ids(&mut it), vec![2, 3, 4]);
}

#[test]
fn the_expansion_cap_bounds_how_many_readers_open() {
    // The cap a range shares with the other expanding node types: at most two of
    // the three terms above `apple` may open a reader.
    let mut fixture =
        LexRangeFixture::build(Some(("apple", false)), None, default_terms(), Some(2));
    let doc_ids = {
        let mut it = ContractChecker::new(fixture.eval().expect("a range must build an iterator"));
        collect_doc_ids(&mut it)
    };
    assert_eq!(doc_ids, vec![2, 3]);
    assert!(
        fixture.reached_max_expansions(),
        "hitting the cap must be reported as a warning"
    );
}

/// A candidate that opens no reader still costs a visit, so it has to count
/// against the cap. Otherwise a range over a field with no matching terms walks
/// the whole shared terms trie.
#[test]
fn the_cap_bounds_visits_not_only_opened_readers() {
    // Every term is in the trie but none has documents, so no reader opens and a
    // reader-based cap would never fire.
    let terms = vec![
        (b"aa".as_slice(), vec![]),
        (b"ab".as_slice(), vec![]),
        (b"ac".as_slice(), vec![]),
        (b"ad".as_slice(), vec![]),
    ];
    let mut fixture = LexRangeFixture::build(None, None, terms, Some(2));
    let _ = fixture.eval();
    assert!(
        fixture.reached_max_expansions(),
        "the walk must report the cap even when it opened no reader"
    );
}

#[test]
fn an_over_long_bound_is_an_error() {
    // A bound longer than the trie's maximum rune-string length names no term
    // and cannot be compared against one, so the query is refused rather than
    // answered with half of it ignored.
    let long = "a".repeat(ffi::MAX_RUNE_STR_LEN as usize + 1);

    let mut fixture = LexRangeFixture::new(Some((&long, false)), None);
    assert!(
        fixture.eval().is_none(),
        "an over-long bound must not evaluate"
    );
    assert_eq!(fixture.status_code(), QueryErrorCode::Limit);
}
