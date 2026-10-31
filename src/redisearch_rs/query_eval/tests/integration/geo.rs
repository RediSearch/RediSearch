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

use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{Config, EvalResult, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_iterators::RQEIterator;
use rqe_iterators_test_utils::ContractChecker;
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
            fieldIndex: context.field_spec().index,
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
        let node_ref = unsafe { QueryNodeMut::new(self.node.as_non_null()) };
        eval_node(&mut self.ctx, node_ref, Config::default()).map(|e| e.into_boxed())
    }
}

impl Drop for GeoFixture {
    fn drop(&mut self) {
        // A valid geo evaluation populates `gf.numericFilters` with a
        // Rust-allocated array (via `build_geo_numeric_filters`) whose non-null
        // entries are C-allocated `NumericFilter`s. Release it through the same
        // Rust helper `GeoFilter_Free` uses in production, so the array is
        // reclaimed with the Rust allocator and each entry with
        // `NumericFilter_Free`. Unlike `GeoFilter_Free`, we must *not* free `gf`
        // itself — it is Rust-boxed here and released by the field drop below.
        // Every evaluated iterator borrows the fixture and is dropped before this
        // runs, so nothing still references the filters.
        //
        // SAFETY: `gf.numericFilters` is NULL or exactly what
        // `build_geo_numeric_filters` stored, and no iterator references it any
        // longer.
        unsafe { rqe_iterators::free_geo_numeric_filters(self.gf.numericFilters.cast()) };
    }
}

/// Collect the doc ids the iterator yields, in order.
fn read_all<'index>(it: &mut impl RQEIterator<'index>) -> Vec<u64> {
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
    let mut it = ContractChecker::new(
        fixture
            .eval()
            .expect("a matching geo query must build an iterator"),
    );
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

// These declarations deliberately keep the boundary tests unchanged across the
// swap: stage one resolves them to C, stage two to Rust.
unsafe extern "C" {
    fn GeoDistance_Parse(s: *const std::ffi::c_char) -> i32;
    fn GeoDistance_Parse_Buffer(s: *const std::ffi::c_char, len: usize) -> i32;
    fn GeoDistance_ToString(unit: i32) -> *const std::ffi::c_char;
    fn NewGeoFilter(
        lon: f64,
        lat: f64,
        radius: f64,
        unit: *const std::ffi::c_char,
        len: usize,
    ) -> *mut geo::GeoFilter;
    fn GeoFilter_SetField(gf: *mut geo::GeoFilter, fs: *const ffi::FieldSpec);
    fn GeoFilter_Validate(
        gf: *const geo::GeoFilter,
        status: *mut query_error::opaque::OpaqueQueryError,
    ) -> i32;
    fn isWithinRadius(gf: *const geo::GeoFilter, score: f64, distance: *mut f64) -> i32;
    fn GeoFilter_Free(gf: *mut geo::GeoFilter);
    fn GeoFilter_LegacyParse(
        gf: *mut geo::LegacyGeoFilter,
        ac: *mut ffi::ArgsCursor,
        empty: *mut bool,
        status: *mut query_error::opaque::OpaqueQueryError,
    ) -> i32;
    fn LegacyGeoFilter_Free(gf: *mut geo::LegacyGeoFilter);
}

// Track live allocations on the calling thread so a missing destructor is an
// observable contract failure without replacing any Redis function pointers.
struct Allocations;
thread_local! { static LIVE: std::cell::Cell<isize> = const { std::cell::Cell::new(0) }; }
#[global_allocator]
static ALLOCATOR: Allocations = Allocations;

// SAFETY: all allocation operations preserve System's pointer and layout contracts.
unsafe impl std::alloc::GlobalAlloc for Allocations {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid allocation layout.
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if !ptr.is_null() {
            let _ = LIVE.try_with(|n| n.set(n.get() + 1));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        let _ = LIVE.try_with(|n| n.set(n.get() - 1));
        // SAFETY: pointer and layout came from the matching System allocation.
        unsafe { std::alloc::System.dealloc(ptr, layout) };
    }
}

#[test]
fn geo_filter_boundary_contract() {
    use geo::{DistanceUnit, GeoFilter};
    use std::{ffi::CStr, ptr};
    let _guard = GlobalGuard::default();
    let context = TestContext::geo([(1, PALERMO.0, PALERMO.1)].into_iter());
    for (token, unit, canonical) in [
        (c"KM", 0, c"km"),
        (c"m", 1, c"m"),
        (c"Ft", 2, c"ft"),
        (c"mi", 3, c"mi"),
        (c"bad", -1, c"<badunit>"),
    ] {
        // SAFETY: token is a static NUL-terminated string.
        assert_eq!(unsafe { GeoDistance_Parse(token.as_ptr()) }, unit);
        // SAFETY: token is readable for its exact length.
        assert_eq!(
            unsafe { GeoDistance_Parse_Buffer(token.as_ptr(), token.count_bytes()) },
            unit
        );
        // SAFETY: this function accepts every integer.
        let name = unsafe { GeoDistance_ToString(unit) };
        // SAFETY: the ABI returns a static NUL-terminated abbreviation.
        assert_eq!(unsafe { CStr::from_ptr(name) }, canonical);
    }
    for (bytes, len, unit) in [
        (b"m\0xxxxxxxxxxxxxx", 0, -1),
        (b"m\0xxxxxxxxxxxxxx", 15, 1),
        (b"m\0xxxxxxxxxxxxxx", 16, -1),
    ] {
        // SAFETY: all lengths are within the supplied buffer.
        assert_eq!(
            unsafe { GeoDistance_Parse_Buffer(bytes.as_ptr().cast(), len) },
            unit
        );
    }
    for (token, unit) in [
        (ptr::null(), Some(DistanceUnit::Kilometers)),
        (c"mi".as_ptr(), Some(DistanceUnit::Miles)),
        (c"xx".as_ptr(), None),
    ] {
        let expected = GeoFilter::new(13.0, 38.0, 17.0, unit);
        assert_eq!(
            (expected.lon, expected.lat, expected.radius),
            (13.0, 38.0, 17.0)
        );
        assert_eq!(expected.unitType, unit.map_or(-1, |u| u as i32));
        assert_eq!(expected.fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert!(expected.numericFilters.is_null());
        assert_eq!(expected.validate().is_ok(), unit.is_some());
        let before = LIVE.get();
        // SAFETY: allocator is initialized; non-null tokens have two readable bytes.
        let raw = unsafe { NewGeoFilter(13.0, 38.0, 17.0, token, 2) };
        // SAFETY: the constructor returned a live, uniquely owned filter.
        let gf = unsafe { &mut *raw };
        assert_eq!(
            (gf.lon, gf.lat, gf.radius, gf.unitType),
            (13.0, 38.0, 17.0, expected.unitType)
        );
        assert_eq!(gf.fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert!(gf.numericFilters.is_null());
        // SAFETY: context owns the live field, disjoint from the filter.
        unsafe { GeoFilter_SetField(gf, context.field_spec()) };
        assert_eq!(gf.fieldIndex, context.field_spec().index);
        // SAFETY: gf is exclusively writable; null clears the association.
        unsafe { GeoFilter_SetField(gf, ptr::null()) };
        assert_eq!(gf.fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        let mut status = query_error::QueryError::default();
        // SAFETY: both arguments are initialized and disjoint.
        assert_eq!(
            unsafe { GeoFilter_Validate(gf, status.as_opaque_mut_ptr()) },
            i32::from(unit.is_some())
        );
        if unit.is_none() {
            assert_eq!(status.code(), QueryErrorCode::Syntax);
        }
        drop(status);
        // SAFETY: ownership returns to the allocator, with no live borrows.
        unsafe { GeoFilter_Free(raw) };
        assert_eq!(LIVE.get(), before);
    }
    let gf = GeoFilter::new(PALERMO.0, PALERMO.1, 300.0, Some(DistanceUnit::Kilometers));
    for (point, inside) in [(PALERMO, 1), (NYC, 0)] {
        let score = redisearch_rs::geo::encodeGeo(point.0, point.1);
        let mut distance = -1.0;
        // SAFETY: gf is validated; distance is disjoint and exclusively writable.
        assert_eq!(unsafe { isWithinRadius(&gf, score, &mut distance) }, inside);
        assert!(if inside == 1 {
            distance < 1.0
        } else {
            (distance - 7_174_240.61).abs() < 1.0
        });
        // SAFETY: output is optional.
        assert_eq!(
            unsafe { isWithinRadius(&gf, score, ptr::null_mut()) },
            inside
        );
    }
}

#[test]
fn geo_legacy_boundary_contract() {
    redis_mock::init_redis_module_mock();
    let _guard = GlobalGuard::default();
    for (tokens, offset, ok, empty_expected) in [
        (vec!["place", "13", "38", "17", "MI"], 5, true, false),
        (vec!["place", "", "", "", "m"], 5, true, true),
        (vec!["place", "13", "38", "17"], 0, false, false),
        (vec!["place", "x", "38", "17", "m"], 1, false, false),
        (vec!["place", "13", "x", "17", "m"], 2, false, false),
        (vec!["place", "13", "38", "x", "m"], 3, false, false),
        (vec!["place", "13", "38", "17", "x"], 5, false, false),
    ] {
        let strings: Vec<_> = tokens
            .iter()
            .map(|s| redis_mock::string::create_string(s))
            .collect();
        let mut pointers: Vec<_> = strings.iter().map(|s| s.cast()).collect();
        let mut ac = ffi::ArgsCursor {
            objs: pointers.as_mut_ptr(),
            type_: 1,
            argc: pointers.len(),
            offset: 0,
        };
        let mut error = query_error::QueryError::default();
        let mut empty = false;
        let before = LIVE.get();
        let raw = redis_mock::allocator::alloc_shim(size_of::<geo::LegacyGeoFilter>())
            .cast::<geo::LegacyGeoFilter>();
        // SAFETY: output storage is fresh, inputs stay live, and error/empty are disjoint.
        let result =
            unsafe { GeoFilter_LegacyParse(raw, &mut ac, &mut empty, error.as_opaque_mut_ptr()) };
        assert_eq!(result, i32::from(!ok));
        assert_eq!(ac.offset, offset);
        assert_eq!(empty, empty_expected);
        // SAFETY: success and failure both initialize the output for cleanup.
        let gf = unsafe { &*raw };
        assert_eq!(gf.base.fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert!(gf.base.numericFilters.is_null());
        assert_eq!(gf.field.is_null(), !ok);
        if ok {
            let expected = if empty {
                (0.0, 0.0, 0.0, 1)
            } else {
                (13.0, 38.0, 17.0, 3)
            };
            assert_eq!(
                (gf.base.lon, gf.base.lat, gf.base.radius, gf.base.unitType),
                expected
            );
            assert!(error.is_ok());
        } else {
            assert_eq!(error.code(), QueryErrorCode::ParseArgs);
        }
        drop(error);
        // SAFETY: raw owns the complete allocation, including any field wrapper.
        unsafe { LegacyGeoFilter_Free(raw) };
        assert_eq!(LIVE.get(), before);
        for string in strings {
            // SAFETY: each string was allocated above and the filter no longer borrows it.
            unsafe { redis_mock::string::free_string(string) };
        }
    }
}
