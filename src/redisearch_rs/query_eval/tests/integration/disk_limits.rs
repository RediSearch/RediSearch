/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Request limits forwarded by the `QN_NUMERIC` and `QN_GEO` disk paths.
//!
//! The real disk iterators are enterprise-only, so a recording stand-in is registered
//! as [`SEARCH_ENTERPRISE_ITERATORS`]. The disk spec is a real, owned slot the stand-in
//! never reads through, since the build takes a reference to it. The snapshot is a dangling
//! placeholder, only ever carried as a [`NonNull`].
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri cannot execute.
#![cfg(not(miri))]

use std::cell::Cell;
use std::ptr::NonNull;

use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use query::mock::MockQueryNode;
use query_eval::{Config, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_core::{DocId, FieldIndex};
use rqe_iterators::{
    Empty, RQEIteratorPrintable, SEARCH_ENTERPRISE_ITERATORS, SearchEnterpriseIterators,
};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

type Limits = Option<NonNull<ffi::QueryRequestLimits>>;
type BuiltIterator<'index> =
    Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

thread_local! {
    /// What the last numeric or geo build on this thread received, `None` before any build.
    static RECEIVED: Cell<Option<Limits>> = const { Cell::new(None) };
}

/// Returns an empty iterator and records the `limits` it was handed. Every other
/// constructor is unreachable from the node kinds under test.
struct RecordingEnterpriseIterators;

impl SearchEnterpriseIterators for RecordingEnterpriseIterators {
    fn new_wildcard_on_disk<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: f64,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
        _: Option<&mut rqe_iterators::QueryError>,
    ) -> BuiltIterator<'index> {
        unimplemented!()
    }

    fn new_term_on_disk_with_offsets<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: Box<query_term::RSQueryTerm>,
        _: inverted_index::FieldMask,
        _: f64,
        _: usize,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> BuiltIterator<'index> {
        unimplemented!()
    }

    fn new_term_on_disk_without_offsets<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: Box<query_term::RSQueryTerm>,
        _: inverted_index::FieldMask,
        _: f64,
        _: usize,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> BuiltIterator<'index> {
        unimplemented!()
    }

    fn new_tag_on_disk<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: &ffi::RSToken,
        _: FieldIndex,
        _: f64,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> BuiltIterator<'index> {
        unimplemented!()
    }

    fn new_missing_on_disk<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: FieldIndex,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> BuiltIterator<'index> {
        unimplemented!()
    }

    fn new_numeric_on_disk<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: &NumericFilter,
        _: FieldIndex,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
        limits: Limits,
    ) -> BuiltIterator<'index> {
        RECEIVED.set(Some(limits));
        Ok(Box::new(Empty))
    }

    fn new_geo_on_disk<'index>(
        &self,
        _: &'index mut ffi::RedisSearchDiskIndexSpec,
        _: &'index mut ffi::GeoFilter,
        _: ffi::t_fieldIndex,
        _: NonNull<ffi::RedisSearchDiskSnapshot>,
        limits: Limits,
    ) -> BuiltIterator<'index> {
        RECEIVED.set(Some(limits));
        Ok(Box::new(Empty))
    }
}

/// Whether the request carries limits.
#[derive(Clone, Copy)]
enum Request {
    WithLimits,
    WithoutLimits,
}

/// Turns the spec behind `context` into a disk-backed one, and installs the request
/// limits when asked to. Returns the limits the request now carries.
///
/// The disk spec and snapshot are reset on drop so that tearing the context down does
/// not free a disk index and snapshot that do not exist.
struct DiskBacked<'a> {
    context: &'a mut TestContext,
    /// Backs `diskSpec`, so the `&mut` the disk build takes of it is valid. Dropped
    /// after the pointer to it is reset.
    _disk_spec: Box<ffi::RedisSearchDiskIndexSpec>,
}

impl<'a> DiskBacked<'a> {
    fn new(context: &'a mut TestContext, request: Request) -> (Self, Limits) {
        SEARCH_ENTERPRISE_ITERATORS.get_or_init(|| Box::new(RecordingEnterpriseIterators));
        RECEIVED.set(None);

        match request {
            Request::WithLimits => context.set_search_time(
                ffi::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                true,
            ),
            // A fresh context has no limits; make that explicit rather than assumed.
            Request::WithoutLimits => {
                // SAFETY: `sctx` is valid and exclusively owned.
                unsafe { context.sctx.as_mut().limits = std::ptr::null_mut() }
            }
        }
        let mut disk_spec = Box::new(std::ptr::null::<std::ffi::c_void>());
        // SAFETY: `context` exclusively owns its spec, and `disk_spec` outlives the
        // pointer to it (reset in `drop`).
        unsafe { (*context.spec).diskSpec = &mut *disk_spec };
        // SAFETY: `context` exclusively owns its `sctx`. The snapshot is never
        // dereferenced, only carried as a `NonNull` (see the module docs).
        unsafe { context.sctx.as_mut().diskSnapshot = NonNull::dangling().as_ptr() };
        // SAFETY: `sctx` is valid and exclusively owned.
        let limits = NonNull::new(unsafe { context.sctx.as_ref().limits });
        (
            Self {
                context,
                _disk_spec: disk_spec,
            },
            limits,
        )
    }
}

impl Drop for DiskBacked<'_> {
    fn drop(&mut self) {
        // SAFETY: `context` exclusively owns its spec.
        unsafe { (*self.context.spec).diskSpec = std::ptr::null_mut() };
        // SAFETY: `context` exclusively owns its `sctx`.
        unsafe { self.context.sctx.as_mut().diskSnapshot = std::ptr::null_mut() };
    }
}

/// Evaluates `node` on the disk path and returns the limits the build received.
fn limits_received_by(context: &TestContext, node: &mut MockQueryNode) -> Limits {
    // SAFETY: `context.qctx()` is a valid, exclusively owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };
    // SAFETY: `node` is a valid, live `RSQueryNode` for the call.
    let node_ref = unsafe { QueryNodeMut::new(node.as_non_null()) };
    assert!(
        eval_node(&mut ctx, node_ref, Config::default()).is_some(),
        "the disk build must produce an iterator"
    );
    RECEIVED
        .get()
        .expect("the disk build constructor must have been called")
}

fn numeric_node(field_index: FieldIndex, filter: &mut NumericFilter) -> MockQueryNode {
    filter.field_index = field_index;
    let mut node = MockQueryNode::new(QueryNodeType::Numeric);
    node.opts_mut().weight = 1.0;
    node.set_numeric_filter(filter);
    node
}

fn numeric_limits_received(request: Request) -> (Limits, Limits) {
    let _guard = GlobalGuard::default();
    let mut context = TestContext::numeric(
        (1..=3u64).map(|i| RSIndexResult::build_numeric(i as f64).doc_id(i).build()),
        false,
    );
    let field_index = context.field_spec().index;
    let (disk, expected) = DiskBacked::new(&mut context, request);
    let mut filter = NumericFilter {
        min: 1.0,
        max: 3.0,
        min_inclusive: true,
        max_inclusive: true,
        ..Default::default()
    };
    let mut node = numeric_node(field_index, &mut filter);
    (limits_received_by(disk.context, &mut node), expected)
}

fn geo_limits_received(request: Request) -> (Limits, Limits) {
    let _guard = GlobalGuard::default();
    let points: [(DocId, f64, f64); 1] = [(1, 13.361389, 38.115556)];
    let mut context = TestContext::geo(points.into_iter());
    let mut gf = Box::new(ffi::GeoFilter {
        fieldIndex: context.field_spec().index,
        lat: 38.115556,
        lon: 13.361389,
        radius: 300.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: std::ptr::null_mut(),
    });
    let (disk, expected) = DiskBacked::new(&mut context, request);
    let mut node = MockQueryNode::new(QueryNodeType::Geo);
    node.opts_mut().weight = 1.0;
    node.set_geo_filter(&mut *gf as *mut ffi::GeoFilter);
    (limits_received_by(disk.context, &mut node), expected)
}

#[test]
fn numeric_disk_build_receives_the_request_limits() {
    let (received, expected) = numeric_limits_received(Request::WithLimits);
    assert!(expected.is_some());
    assert_eq!(received, expected);
}

#[test]
fn numeric_disk_build_receives_no_limits_when_the_request_has_none() {
    let (received, expected) = numeric_limits_received(Request::WithoutLimits);
    assert!(expected.is_none());
    assert_eq!(received, None);
}

#[test]
fn geo_disk_build_receives_the_request_limits() {
    let (received, expected) = geo_limits_received(Request::WithLimits);
    assert!(expected.is_some());
    assert_eq!(received, expected);
}

#[test]
fn geo_disk_build_receives_no_limits_when_the_request_has_none() {
    let (received, expected) = geo_limits_received(Request::WithoutLimits);
    assert!(expected.is_none());
    assert_eq!(received, None);
}
