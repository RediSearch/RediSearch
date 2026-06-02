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
    opts: *mut ffi::RSSearchOptions,
    status: *mut QueryError,
    metric_requests_inner: *mut rlookup::MetricRequest<'static>,
    metric_requests_p: *mut *mut rlookup::MetricRequest<'static>,
    doc_table: *mut ffi::DocTable,
    config: *mut IteratorsConfig,
    qctx: *mut ffi::QueryEvalCtx,
}

impl Drop for MockQueryEvalCtx {
    fn drop(&mut self) {
        // SAFETY: each pointer was allocated in `with_req_flags` and is
        // exclusively owned by this struct; layouts match those used at
        // allocation time.
        unsafe {
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
            let sctx =
                alloc_zeroed(Layout::new::<ffi::RedisSearchCtx>()).cast::<ffi::RedisSearchCtx>();
            assert!(!sctx.is_null());

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
            (*qctx).notSubtree = false;

            Self {
                sctx,
                opts,
                status,
                metric_requests_inner,
                metric_requests_p,
                doc_table,
                config,
                qctx,
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
}
