/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_GEO → geo-radius iterator.
//!
//! Needs a real geo (numeric range tree) index, so it relies on the full-FFI
//! `TestContext`.
//!
//! Disabled under Miri: `TestContext` and the geo filter validation call into
//! the C library, which Miri cannot execute.
#![cfg(not(miri))]

use geo::GEO_RANGE_COUNT;
use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{
    QueryEvalContext, QueryNodeRef,
    eval::{self, EvalResult},
};
use query_node_type::QueryNodeType;
use rqe_iterators::RQEIterator;
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

// Two well-separated points (≈ 7400 km apart), so a modest radius around one
// never reaches the other.
const PALERMO: (f64, f64) = (13.361389, 38.115556); // (lon, lat)
const NYC: (f64, f64) = (-73.9857, 40.7484);

/// Owns everything a `QN_GEO` evaluation borrows — the FFI `TestContext`, the
/// [`QueryEvalContext`], the node, and the geo filter — so
/// [`eval`](Self::eval) can hand the evaluated iterator back to the caller.
///
/// The iterator borrows `ctx`, which in turn points into the other fields
/// (including the per-range numeric filters stored inside `gf` during
/// evaluation), so they must all outlive it. They do: `TestContext` keeps its
/// state behind heap allocations, and `gf` is boxed (stable address) and freed
/// only when the fixture is dropped, after every evaluated iterator.
struct GeoFixture {
    _guard: GlobalGuard,
    _context: TestContext,
    ctx: QueryEvalContext,
    node: MockQueryNode,
    // Boxed so its address stays stable while the node holds a raw pointer to it.
    gf: Box<ffi::GeoFilter>,
}

impl GeoFixture {
    /// Geo index over `points` (`(doc_id, lon, lat)`) with a `QN_GEO` node for a
    /// radius query of `radius_km` kilometres around `(lon, lat)`.
    fn new(points: &[(u64, f64, f64)], lon: f64, lat: f64, radius_km: f64) -> Self {
        let _guard = GlobalGuard::default();

        let context = TestContext::geo(points.iter().copied());

        // SAFETY: `context.qctx()` returns a valid, exclusively-owned
        // `QueryEvalCtx` (with real `status`/`config`), upholding the
        // `QueryEvalContext::new` invariants.
        let ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let mut gf = Box::new(ffi::GeoFilter {
            fieldSpec: context.field_spec() as *const _,
            lat,
            lon,
            radius: radius_km,
            unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
            numericFilters: std::ptr::null_mut(),
        });

        let mut node = MockQueryNode::new(QueryNodeType::Geo);
        node.opts_mut().weight = 1.0;
        node.set_geo_filter(&mut *gf as *mut ffi::GeoFilter);

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            gf,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when
    /// evaluation produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        eval::eval_node(&mut self.ctx, &node_ref).map(|e| e.into_boxed())
    }
}

impl Drop for GeoFixture {
    fn drop(&mut self) {
        // A valid geo evaluation populates `gf.numericFilters` with a
        // Rust-allocated `[*mut NumericFilter; GEO_RANGE_COUNT]` array (via
        // `Box::into_raw`) whose non-null entries are C-allocated (`rm_malloc`)
        // `NumericFilter`s. This test binary uses distinct Rust and C allocators,
        // so — unlike `GeoFilter_Free`, which releases both through `rm_free` —
        // each must be reclaimed with the allocator that produced it: the array
        // with `Box`, the entries with `NumericFilter_Free`. The `gf` box itself
        // is released by the field drop below. Every evaluated iterator borrows
        // the fixture and is dropped before this runs, so nothing still
        // references the filters.
        let array = self.gf.numericFilters as *mut [*mut ffi::NumericFilter; GEO_RANGE_COUNT];
        if !array.is_null() {
            // SAFETY: `array` is exactly what `build_geo_numeric_filters` produced
            // with `Box::into_raw`; reclaiming it with `Box::from_raw` matches.
            let array = unsafe { Box::from_raw(array) };
            for &nf in array.iter() {
                if !nf.is_null() {
                    // SAFETY: each non-null entry is a live, C-allocated
                    // `NumericFilter` owned by `gf`, freed here exactly once.
                    unsafe { ffi::NumericFilter_Free(nf) };
                }
            }
        }
    }
}

/// Collect the doc ids the iterator yields, in order.
fn read_all(it: &mut EvalResult<'_>) -> Vec<u64> {
    let mut ids = Vec::new();
    while let Some(r) = it.read().expect("read must not error") {
        ids.push(r.doc_id);
    }
    ids
}

#[test]
fn eval_geo_matches_points_within_radius() {
    // A 300 km radius around Palermo covers the Palermo point but not New York.
    let mut fixture = GeoFixture::new(
        &[(1, PALERMO.0, PALERMO.1), (2, NYC.0, NYC.1)],
        PALERMO.0,
        PALERMO.1,
        300.0,
    );
    let mut it = fixture
        .eval()
        .expect("a matching geo query must build an iterator");
    assert_eq!(read_all(&mut it), vec![1]);
}

#[test]
fn eval_geo_no_match_yields_none() {
    // The only indexed point is Palermo; a small radius around New York matches
    // nothing, so evaluation short-circuits to `None` (no iterator).
    let mut fixture = GeoFixture::new(&[(1, PALERMO.0, PALERMO.1)], NYC.0, NYC.1, 50.0);
    assert!(
        fixture.eval().is_none(),
        "a geo query matching no indexed point must not build an iterator"
    );
    // No error: an empty result is not a failure.
    assert!(fixture.ctx.status().is_ok());
}

#[test]
fn eval_geo_invalid_radius_reports_error() {
    // A non-positive radius fails `GeoFilter_Validate`, which reports a syntax
    // error into the query status and yields no iterator.
    let mut fixture = GeoFixture::new(&[(1, PALERMO.0, PALERMO.1)], PALERMO.0, PALERMO.1, 0.0);
    assert!(
        fixture.eval().is_none(),
        "an invalid geo filter must not build an iterator"
    );
    assert_eq!(
        fixture.ctx.status().code(),
        QueryErrorCode::Syntax,
        "an invalid geo filter must report a syntax error into the query status"
    );
}
