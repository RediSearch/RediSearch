/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust implementation of the numeric top-k (optimizer) iterator and its C
//! accessor functions.
//!
//! This module provides [`NewNumericTopKIterator`] (a drop-in replacement for
//! the C `NewOptimizerIterator`) together with the accessor shims that C callers
//! use for query-plan construction and profiling.

use std::ptr::{self, NonNull};

use ffi::{DocTable, DocTable_Exists, QueryIterator, RedisSearchCtx};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_score_source::{DocValidity, NewNumericTopK, NumericTopKIterator, new_numeric_top_k};
use rqe_core::DocId;
use rqe_iterators::{
    FieldExpirationChecker,
    c2rust::CRQEIterator,
    interop::{RQEIteratorWrapper, patch_vtable},
    utils::AnyTimeoutContext,
};

/// Probe the query deadline once every this many candidate documents.
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// The single concrete [`NumericTopKIterator`] monomorphization the FFI exposes.
/// The accessor shims cast the returned handle back to exactly this type, so it
/// is the only combination of validity/expiration/timeout the iterator carries.
type NumericTopKFfi =
    NumericTopKIterator<'static, DocTableValidity, FieldExpirationChecker, AnyTimeoutContext>;

/// [`DocValidity`] backed by the spec's document table: a doc id is valid iff it
/// still resolves to a live entry. The numeric index keeps entries for documents
/// deleted but not yet reclaimed by GC, so this drops them before they reach the
/// top-k heap.
struct DocTableValidity {
    doc_table: NonNull<DocTable>,
}

impl DocTableValidity {
    /// # Safety
    ///
    /// 1. `doc_table` is [valid] and outlives the oracle.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    const unsafe fn new(doc_table: NonNull<DocTable>) -> Self {
        Self { doc_table }
    }
}

impl DocValidity for DocTableValidity {
    fn is_valid(&self, doc_id: DocId) -> bool {
        // SAFETY: `doc_table` is valid for the oracle's lifetime per `new`.
        unsafe { DocTable_Exists(self.doc_table.as_ptr(), doc_id) }
    }
}

/// Construct a numeric top-k iterator and expose it as a C [`QueryIterator`].
///
/// This call can reduce to an `Empty` iterator (`k == 0`, or a child that can
/// never match).
///
/// Pass `child = NULL` for a plain `SORTBY numeric` range scan; pass a valid
/// owning child iterator for a filtered query, whose selectivity sizes the
/// initial value-window and drives the expand-and-retry path.
///
/// `filter` is read once to copy the numeric range parameters; it is not
/// retained after this call.
///
/// # Safety
///
/// 1. `tree` is non-null and [valid] for a [`NumericRangeTree`] that outlives the
///    returned iterator.
/// 2. `filter` is non-null and [valid] for a [`NumericFilter`] for the duration
///    of this call.
/// 3. `child`, when non-null, is a [valid], owning `QueryIterator *` with every
///    callback populated.
/// 4. `sctx` is non-null and [valid] for a [`RedisSearchCtx`] with a [valid]
///    `spec`, both outliving the returned iterator.
/// 5. `filter_ctx` is non-null and [valid] for a [`FieldFilterContext`] for the
///    duration of this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericTopKIterator(
    tree: *const NumericRangeTree,
    filter: *const NumericFilter,
    ascending: bool,
    k: usize,
    num_docs: usize,
    child: *mut QueryIterator,
    sctx: *const RedisSearchCtx,
    filter_ctx: *const FieldFilterContext,
) -> *mut QueryIterator {
    let sctx_nn = NonNull::new(sctx as *mut RedisSearchCtx).expect("sctx must be non-null");
    // SAFETY: guaranteed by 4.
    let sctx_ref = unsafe { sctx_nn.as_ref() };

    let filter_ctx_nn =
        NonNull::new(filter_ctx as *mut FieldFilterContext).expect("filter_ctx must be non-null");
    // SAFETY: guaranteed by 5. The fields are `Copy`, so we snapshot them.
    let fc = unsafe { filter_ctx_nn.as_ref() };
    let field_filter = FieldFilterContext {
        field: fc.field,
        predicate: fc.predicate,
    };

    // The sort field is a single field index (never a mask), so the wide-schema
    // flag is irrelevant; pass empty reader flags.
    // SAFETY: guaranteed by 4.
    let expiration = unsafe { FieldExpirationChecker::new(sctx_nn, field_filter, 0) };
    let timeout = AnyTimeoutContext::from_sctx(sctx_ref, TIMEOUT_CHECK_GRANULARITY);

    // Document-liveness oracle over the spec's doc table.
    // SAFETY: guaranteed by 4: `spec` is a valid pointer that outlives the iterator.
    let docs_ptr = unsafe { &raw mut (*sctx_ref.spec).docs };
    // SAFETY: the address of an inline struct field is never null.
    let doc_table = unsafe { NonNull::new_unchecked(docs_ptr) };
    // SAFETY: `doc_table` is valid for the iterator's lifetime, tied to `spec`.
    let validity = unsafe { DocTableValidity::new(doc_table) };

    // SAFETY: guaranteed by 1.
    let tree = unsafe { &*tree };
    // SAFETY: guaranteed by 2. `NumericFilter` is `Copy`.
    let filter = unsafe { *filter };

    // Adopt the filter child as an owning Rust iterator. A null child means a
    // plain range scan.
    // SAFETY: guaranteed by 3.
    let child = NonNull::new(child).map(|c| unsafe { CRQEIterator::new(c) });

    let reduced = new_numeric_top_k(
        tree, filter, ascending, k, num_docs, validity, expiration, timeout, child,
    );
    box_reduced(reduced)
}

/// Box a [`NewNumericTopK`] into the C [`QueryIterator`] handle, clearing the
/// root-only `SkipTo` vtable slot so the C highlighter falls back to sequential
/// reads.
fn box_reduced(
    reduced: NewNumericTopK<'_, DocTableValidity, FieldExpirationChecker, AnyTimeoutContext>,
) -> *mut QueryIterator {
    let clear_skip_to = |ptr| {
        // SAFETY: `ptr` comes from `boxed_new`/`boxed_new_compound` below and has no
        // other alias yet, satisfying 1 and 2.
        unsafe { patch_vtable(ptr, |h| h.SkipTo = None) }
    };
    match reduced {
        NewNumericTopK::ReducedEmpty => RQEIteratorWrapper::boxed_new(rqe_iterators::Empty),
        NewNumericTopK::Unfiltered(it) => {
            let ptr = RQEIteratorWrapper::boxed_new(it);
            clear_skip_to(ptr);
            ptr
        }
        NewNumericTopK::Filtered(it) => {
            // `boxed_new_compound` registers the `ProfileChildren` callback so
            // `FT.PROFILE` recurses into and counts the filter subtree.
            let ptr = RQEIteratorWrapper::boxed_new_compound(it);
            clear_skip_to(ptr);
            ptr
        }
    }
}

/// Cast a `*const QueryIterator` produced by [`NewNumericTopKIterator`] back to
/// its full Rust wrapper (shared).
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`NumericTopKFfi`] that was
///    created by [`NewNumericTopKIterator`].
const unsafe fn wrapper_ref(it: *const QueryIterator) -> &'static RQEIteratorWrapper<NumericTopKFfi> {
    // SAFETY: `it` was created by `RQEIteratorWrapper::boxed_new`/`boxed_new_compound`
    // with inner type `NumericTopKFfi`, so the cast is valid per `ref_from_header_ptr`'s contract.
    unsafe { RQEIteratorWrapper::<NumericTopKFfi>::ref_from_header_ptr(it) }
}

/// Return the filter child iterator, or `NULL` for a plain range scan.
///
/// The returned pointer is non-owning; its lifetime is that of `it`.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a [`NumericTopKFfi`] that was
///    created by [`NewNumericTopKIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericTopK_GetChild(it: *const QueryIterator) -> *mut QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_ref(it) };
    wrapper.inner.child().map_or(ptr::null_mut(), |c| {
        c.as_ref() as *const QueryIterator as *mut QueryIterator
    })
}
