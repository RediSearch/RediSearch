/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_GEOMETRY → GEOSHAPE (R-tree) query iterator.
//!
//! Needs a real GEOSHAPE field whose R-tree index is created on demand, so it
//! relies on the full-FFI `TestContext`.
//!
//! Disabled under Miri: `TestContext` and the geometry backend call into the
//! C/C++ library, which Miri cannot execute.
#![cfg(not(miri))]

use std::ffi::CString;

use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{
    QueryEvalContext, QueryNodeRef,
    eval::{self, Config, EvalResult},
};
use query_types::QueryNodeType;
use rqe_iterators::RQEIterator;
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

/// A well-formed WKT polygon.
const VALID_POLYGON: &str = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
/// The same polygon with a misspelled keyword, which the backend rejects while
/// parsing — the query syntax around it is still valid, so evaluation reaches
/// the backend before failing.
const INVALID_POLYGON: &str = "POLIGON((0 0, 0 10, 10 10, 10 0, 0 0))"; // codespell:ignore poligon

/// Owns everything a `QN_GEOMETRY` evaluation borrows — the FFI `TestContext`,
/// the [`QueryEvalContext`], the node, the geometry query, and its backing WKT
/// string — so [`eval`](Self::eval) can hand the evaluated iterator back to the
/// caller.
///
/// The iterator borrows `ctx`, which points into the other fields, so they must
/// all outlive it. They do: `TestContext` keeps its state behind heap
/// allocations, `geomq` is boxed (stable address) and the WKT string is owned
/// here, all freed only when the fixture is dropped, after every evaluated
/// iterator.
struct GeometryFixture {
    _guard: GlobalGuard,
    _context: TestContext,
    ctx: QueryEvalContext,
    node: MockQueryNode,
    // Boxed so its address stays stable while the node holds a raw pointer to it.
    _geomq: Box<ffi::GeometryQuery>,
    // Backing storage for the query string `geomq.str_` points at.
    _wkt: CString,
}

impl GeometryFixture {
    /// Build a `QN_GEOMETRY` node for a `within` query over `wkt` against an
    /// empty GEOSHAPE index.
    fn new(wkt: &str) -> Self {
        let _guard = GlobalGuard::default();

        let context = TestContext::geometry();

        // SAFETY: `context.qctx()` returns a valid, exclusively-owned
        // `QueryEvalCtx` (with real `status`/`config`), upholding the
        // `QueryEvalContext::new` invariants.
        let ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let wkt = CString::new(wkt).expect("wkt must not contain an interior NUL");
        let mut geomq = Box::new(ffi::GeometryQuery {
            format: ffi::GEOMETRY_FORMAT_GEOMETRY_FORMAT_WKT,
            query_type: ffi::QueryType_WITHIN,
            fs: context.field_spec() as *const _,
            str_: wkt.as_ptr(),
            str_len: wkt.as_bytes().len(),
        });

        let mut node = MockQueryNode::new(QueryNodeType::Geometry);
        node.opts_mut().weight = 1.0;
        node.set_geometry(&mut *geomq as *mut ffi::GeometryQuery);

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            _geomq: geomq,
            _wkt: wkt,
        }
    }

    /// Index `wkt` under `doc_id` in the fixture's GEOSHAPE index so a matching
    /// query can return it.
    ///
    /// Must be called before [`eval`](Self::eval): `OpenGeometryIndex` lazily
    /// creates the R-tree and stores it on the field spec, so both the insert
    /// here and the query in `eval` operate on the same index.
    fn add_geometry(&self, doc_id: u64, wkt: &str) {
        let fs = self._context.field_spec() as *const ffi::FieldSpec as *mut ffi::FieldSpec;
        let wkt = CString::new(wkt).expect("wkt must not contain an interior NUL");

        // SAFETY: `fs` is a valid GEOSHAPE `FieldSpec`; create-if-missing lazily
        // attaches the R-tree index.
        let index = unsafe { ffi::OpenGeometryIndex(fs, true) };
        // SAFETY: `index` is a valid `GeometryIndex` from `OpenGeometryIndex`.
        let api = unsafe { ffi::GeometryApi_Get(index) };
        // SAFETY: `GeometryApi_Get` always populates the `addGeomStr` callback.
        let add = unsafe { (*api).addGeomStr }.expect("geometry api `addGeomStr` must be set");

        let mut err: *mut ffi::RedisModuleString = std::ptr::null_mut();
        // SAFETY: `index` is valid, `wkt` points to `wkt.as_bytes().len()` bytes,
        // and `err` is a valid out-pointer.
        let rc = unsafe {
            add(
                index,
                ffi::GEOMETRY_FORMAT_GEOMETRY_FORMAT_WKT,
                wkt.as_ptr(),
                wkt.as_bytes().len(),
                doc_id,
                &mut err,
            )
        };
        // `addGeomStr` follows the C convention: non-zero on success, 0 (with
        // `err` set) on failure.
        assert_ne!(rc, 0, "indexing a well-formed geometry must succeed");
        assert!(err.is_null(), "a successful insert must not set an error");
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when
    /// evaluation produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        eval::eval_node(&mut self.ctx, &node_ref, Config::default()).map(|e| e.into_boxed())
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
fn eval_geometry_valid_query_on_empty_index_yields_empty_iterator() {
    // A well-formed query against the (empty) index builds a valid iterator that
    // yields no documents; this is not an error.
    let mut fixture = GeometryFixture::new(VALID_POLYGON);
    {
        let mut it = fixture
            .eval()
            .expect("a well-formed geometry query must build an iterator");
        assert!(
            it.read().expect("read must not error").is_none(),
            "an empty geoshape index must yield no documents"
        );
    }
    assert!(fixture.ctx.status().is_ok());
}

#[test]
fn eval_geometry_matches_contained_geometry() {
    // A `within` query over `VALID_POLYGON` (the 0..10 square) must return only
    // the indexed geometry that lies inside it, not the one outside.
    let mut fixture = GeometryFixture::new(VALID_POLYGON);
    fixture.add_geometry(1, "POLYGON((2 2, 2 4, 4 4, 4 2, 2 2))");
    fixture.add_geometry(2, "POLYGON((20 20, 20 24, 24 24, 24 20, 20 20))");

    {
        let mut it = fixture
            .eval()
            .expect("a well-formed geometry query must build an iterator");
        assert_eq!(
            read_all(&mut it),
            vec![1],
            "only the geometry within the query polygon must match"
        );
    }
    assert!(fixture.ctx.status().is_ok());
}

#[test]
fn eval_geometry_invalid_wkt_reports_error() {
    // A malformed geometry string makes the backend reject the query, which must
    // surface as a `BadVal` error and yield no iterator.
    let mut fixture = GeometryFixture::new(INVALID_POLYGON);
    assert!(
        fixture.eval().is_none(),
        "a rejected geometry query must not build an iterator"
    );

    let status = fixture.ctx.status();
    assert_eq!(
        status.code(),
        QueryErrorCode::BadVal,
        "a rejected geometry query must report a bad-value error"
    );

    // The public message must stay free of the (user-controlled) backend detail,
    // which belongs only in the private message — mirroring the C
    // `QueryError_SetWithUserDataFmt` split.
    let public = status
        .public_message()
        .expect("a set error must have a public message")
        .to_str()
        .unwrap();
    assert_eq!(public, "Error querying geoshape index");

    let private = status
        .private_message()
        .expect("a set error must have a private message")
        .to_str()
        .unwrap();
    assert!(
        private.contains("Error querying geoshape index:"),
        "the private message must carry the prefixed detail, got {private:?}"
    );
}
