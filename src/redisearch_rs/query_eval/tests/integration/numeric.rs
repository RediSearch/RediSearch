/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_NUMERIC → numeric range iterator.
//!
//! Needs a real numeric range tree, so it relies on the full-FFI `TestContext`.
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use query::mock::MockQueryNode;
use query_eval::{
    QueryEvalContext, QueryNodeRef,
    eval::{self, EvalResult},
};
use query_node_type::QueryNodeType;
use rqe_iterators::{IteratorType, IteratorsConfig, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

/// Owns everything a `QN_NUMERIC` evaluation borrows — the FFI `TestContext`, the
/// [`QueryEvalContext`], the config, the filter, and the node — so
/// [`eval`](Self::eval) can hand the evaluated iterator back to the caller.
///
/// The iterator borrows `ctx`, which in turn points into the other fields, so
/// they must all outlive it. They can: `TestContext` keeps its `QueryEvalCtx`,
/// range tree, and field spec behind heap allocations, and `config`/`filter` are
/// boxed, so every pointer stays valid after the fixture is moved out of
/// [`new`](Self::new).
struct NumericFixture {
    _guard: GlobalGuard,
    _context: TestContext,
    ctx: QueryEvalContext,
    node: MockQueryNode,
    // Boxed so their addresses stay stable and they outlive the iterator; the
    // `QueryEvalCtx`/node hold raw pointers into them.
    _config: Box<IteratorsConfig>,
    _filter: Box<NumericFilter>,
}

impl NumericFixture {
    /// Numeric range tree over documents `1..=3` (values `1.0, 2.0, 3.0`) with a
    /// `QN_NUMERIC` node for the inclusive filter `[min, max]`.
    fn new(min: f64, max: f64) -> Self {
        let _guard = GlobalGuard::default();

        // Documents 1..=3 with numeric values 1.0, 2.0, 3.0.
        let records = (1u64..=3).map(|i| RSIndexResult::build_numeric(i as f64).doc_id(i).build());
        let context = TestContext::numeric(records, false);

        // The numeric path reads `config.min_union_iter_heap`, which the zero-init
        // `QueryEvalCtx` leaves NULL; provide a real, stably-addressed config.
        let qctx = context.qctx();
        let mut config = Box::new(IteratorsConfig {
            max_prefix_expansions: 200,
            min_term_prefix: 2,
            min_stem_length: 4,
            min_union_iter_heap: 20,
        });
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `config` is
        // boxed, so it outlives `ctx` and shares the C config layout.
        unsafe { (*qctx.as_ptr()).config = (&mut *config as *mut IteratorsConfig).cast() };
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants, and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        let mut filter = Box::new(NumericFilter {
            min,
            max,
            min_inclusive: true,
            max_inclusive: true,
            field_spec: context.field_spec() as *const _,
            ..Default::default()
        });
        let mut node = MockQueryNode::new(QueryNodeType::Numeric);
        node.opts_mut().weight = 1.0;
        node.set_numeric_filter(&mut *filter as *mut NumericFilter);

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            _config: config,
            _filter: filter,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when evaluation
    /// produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        eval::eval_node(&mut self.ctx, &node_ref).map(|e| e.into_boxed())
    }
}

#[test]
fn eval_numeric_filters_range() {
    // Filter 2.0 <= x <= 3.0 → documents 2 and 3.
    let mut fixture = NumericFixture::new(2.0, 3.0);
    let mut it = fixture.eval().expect("should not be None");
    // A single sub-range covers all three docs, so `build_union` collapses to the
    // numeric leaf iterator rather than wrapping it in a union.
    assert_eq!(it.type_(), IteratorType::InvIdxNumeric);

    for expected in [2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_numeric_gap_yields_no_results() {
    // Filter 1.4 <= x <= 1.6 falls in a gap *inside* the indexed range (no doc
    // has such a value), so the sub-range iterator is still built but filters
    // every record out. The single sub-range collapses to the numeric leaf.
    let mut fixture = NumericFixture::new(1.4, 1.6);
    let mut it = fixture.eval().expect("should not be None");
    assert_eq!(it.type_(), IteratorType::InvIdxNumeric);
    assert!(
        matches!(it.read(), Ok(None)),
        "a numeric filter matching no record must yield no results"
    );
}

#[test]
fn eval_numeric_out_of_range_yields_no_results() {
    // Filter 10.0 <= x <= 20.0 is entirely outside the indexed range, so no
    // sub-range matches and evaluation short-circuits to `None` (no iterator).
    let mut fixture = NumericFixture::new(10.0, 20.0);
    assert!(
        fixture.eval().is_none(),
        "a fully out-of-range numeric filter must not build an iterator"
    );
}
