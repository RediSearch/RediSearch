/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lightweight mock for [`ffi::QueryEvalCtx`] that avoids linking any C code
//! to stay Miri-compatible.

use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
};

use query_error::QueryError;
use query_flags::QEFlags;
use rqe_iterators::IteratorsConfig;

/// Owns all the heap-allocated structs that a [`ffi::QueryEvalCtx`] points to.
///
/// Uses raw pointers for storage to avoid Stacked Borrows violations (see
/// `rqe_iterators_test_utils::MockContext` for the rationale).
pub struct MockQueryEvalCtx {
    sctx: *mut ffi::RedisSearchCtx,
    spec: *mut ffi::IndexSpec,
    opts: *mut ffi::RSSearchOptions,
    status: *mut QueryError,
    metric_requests_inner: *mut rlookup::MetricRequest<'static>,
    metric_requests_p: *mut *mut rlookup::MetricRequest<'static>,
    doc_table: *mut ffi::DocTable,
    config: *mut IteratorsConfig,
    qctx: *mut ffi::QueryEvalCtx,
    /// Backing allocation for `qctx.bcTimeoutAreq`, lazily created by
    /// [`MockQueryEvalCtx::enable_blocked_client_timeout`]; null when no
    /// Blocked Client Timeout source has been wired in.
    areq: *mut ffi::AREQ,
}

impl Drop for MockQueryEvalCtx {
    fn drop(&mut self) {
        // SAFETY: each pointer was allocated in `with_req_flags` and is
        // exclusively owned by this struct; layouts match those used at
        // allocation time.
        unsafe {
            dealloc(self.spec.cast(), Layout::new::<ffi::IndexSpec>());
            dealloc(self.sctx.cast(), Layout::new::<ffi::RedisSearchCtx>());
            dealloc(self.opts.cast(), Layout::new::<ffi::RSSearchOptions>());
            drop(Box::from_raw(self.status));
            dealloc(
                self.metric_requests_inner.cast(),
                Layout::new::<rlookup::MetricRequest<'static>>(),
            );
            dealloc(
                self.metric_requests_p.cast(),
                Layout::new::<*mut rlookup::MetricRequest<'static>>(),
            );
            dealloc(self.doc_table.cast(), Layout::new::<ffi::DocTable>());
            drop(Box::from_raw(self.config));
            dealloc(self.qctx.cast(), Layout::new::<ffi::QueryEvalCtx>());
            if !self.areq.is_null() {
                dealloc(self.areq.cast(), Layout::new::<ffi::AREQ>());
            }
        }
    }
}

impl MockQueryEvalCtx {
    pub fn new() -> Self {
        Self::with_req_flags(QEFlags::empty())
    }

    pub fn with_req_flags(flags: QEFlags) -> Self {
        // SAFETY: all allocations are zeroed and non-null-checked; pointer
        // fields are immediately initialised to valid, owned allocations.
        unsafe {
            let spec = alloc_zeroed(Layout::new::<ffi::IndexSpec>()).cast::<ffi::IndexSpec>();
            assert!(!spec.is_null());

            let sctx =
                alloc_zeroed(Layout::new::<ffi::RedisSearchCtx>()).cast::<ffi::RedisSearchCtx>();
            assert!(!sctx.is_null());

            (*sctx).spec = spec;

            let opts =
                alloc_zeroed(Layout::new::<ffi::RSSearchOptions>()).cast::<ffi::RSSearchOptions>();
            assert!(!opts.is_null());
            (*opts).slop = 42;

            let status = Box::into_raw(Box::new(QueryError::default()));

            let metric_requests_inner =
                alloc_zeroed(Layout::new::<rlookup::MetricRequest<'static>>())
                    .cast::<rlookup::MetricRequest<'static>>();
            assert!(!metric_requests_inner.is_null());

            let metric_requests_p =
                alloc_zeroed(Layout::new::<*mut rlookup::MetricRequest<'static>>())
                    .cast::<*mut rlookup::MetricRequest<'static>>();
            assert!(!metric_requests_p.is_null());
            *metric_requests_p = metric_requests_inner;

            let doc_table = alloc_zeroed(Layout::new::<ffi::DocTable>()).cast::<ffi::DocTable>();
            assert!(!doc_table.is_null());

            let config = Box::into_raw(Box::new(IteratorsConfig {
                max_prefix_expansions: 200,
                min_term_prefix: 2,
                min_stem_length: 4,
                min_union_iter_heap: 20,
            }));

            let qctx = alloc_zeroed(Layout::new::<ffi::QueryEvalCtx>()).cast::<ffi::QueryEvalCtx>();
            assert!(!qctx.is_null());

            (*qctx).sctx = sctx;
            (*qctx).opts = opts;
            (*qctx).status = status
                .cast::<query_error::opaque::OpaqueQueryError>()
                .cast::<ffi::QueryError>();
            (*qctx).metricRequestsP = metric_requests_p.cast();
            (*qctx).docTable = doc_table;
            (*qctx).reqFlags = flags.bits();
            (*qctx).config = (config as *mut IteratorsConfig).cast();
            (*qctx).tokenId = 0;
            (*qctx).inNotSubTree = false;

            Self {
                sctx,
                spec,
                opts,
                status,
                metric_requests_inner,
                metric_requests_p,
                doc_table,
                config,
                qctx,
                areq: std::ptr::null_mut(),
            }
        }
    }

    pub fn as_non_null(&mut self) -> NonNull<ffi::QueryEvalCtx> {
        NonNull::new(self.qctx).expect("qctx should not be null")
    }

    pub fn sctx_ptr(&self) -> *mut ffi::RedisSearchCtx {
        self.sctx
    }

    pub fn metric_requests_p(&self) -> *mut *mut rlookup::MetricRequest<'static> {
        self.metric_requests_p
    }

    pub fn doc_table_ptr(&self) -> *mut ffi::DocTable {
        self.doc_table
    }

    pub fn opts_ptr(&self) -> *mut ffi::RSSearchOptions {
        self.opts
    }

    pub fn set_max_doc_id(&mut self, max_doc_id: rqe_core::DocId) {
        // SAFETY: `self.doc_table` is a valid, exclusively-owned allocation.
        unsafe { (*self.doc_table).maxDocId = max_doc_id }
    }

    /// Set `spec.diskSpec` to a non-null sentinel so that
    /// `!spec.diskSpec.is_null()` holds (simulating search-on-disk mode).
    ///
    /// The pointer is dangling — only use this for code paths that check
    /// the pointer for null but never dereference it.
    pub fn enable_disk_mode(&mut self) {
        // SAFETY: `self.spec` is a valid, exclusively-owned allocation.
        unsafe { (*self.spec).diskSpec = std::ptr::NonNull::dangling().as_ptr() }
    }

    /// Wire a real, zeroed [`ffi::AREQ`] into `bcTimeoutAreq` so that the
    /// Blocked Client Timeout source is selected (simulating a
    /// background-executed request).
    ///
    /// The allocation is owned by this mock and freed on drop. It is zeroed,
    /// so its `RequestSyncState::timedOut` flag reads as "not timed out": a code
    /// path that probes the timeout (via `AREQ_CheckTimedOut`) sees a valid,
    /// non-expired request.
    pub fn enable_blocked_client_timeout(&mut self) {
        // SAFETY: `ffi::AREQ` is a `#[repr(C)]` POD struct, so an all-zero bit
        // pattern is a valid (non-expired) instance; the allocation is checked
        // non-null and stored for cleanup in `Drop`.
        unsafe {
            if self.areq.is_null() {
                let areq = alloc_zeroed(Layout::new::<ffi::AREQ>()).cast::<ffi::AREQ>();
                assert!(!areq.is_null());
                self.areq = areq;
            }
            // SAFETY: `self.qctx` is a valid, exclusively-owned allocation.
            (*self.qctx).bcTimeoutAreq = self.areq;
        }
    }
}
