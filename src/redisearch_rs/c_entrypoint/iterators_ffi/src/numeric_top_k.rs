/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entrypoint for the numeric top-k optimizer.

use std::ptr::NonNull;

use ffi::{DocTable_IsVisible, QueryIterator, RedisSearchCtx};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_score_source::{new_numeric_top_k, range_iterator::NumericTreeRef};
use rqe_iterators::{
    FieldExpirationChecker,
    c2rust::CRQEIterator,
    interop::{RQEIteratorWrapper, patch_vtable},
    utils::AnyTimeoutContext,
};

/// Probe the query deadline once every this many candidate documents.
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

#[track_caller]
fn debug_assert_non_null<T>(ptr: *const T, name: &str) {
    debug_assert!(!ptr.is_null(), "{name} must be non-null");
}

/// Construct a numeric top-k iterator and expose it as a C [`QueryIterator`].
///
/// This call can reduce to an `Empty` iterator (`k == 0`, or a child that can
/// never match). It returns `NULL` when `k` is too large to allocate the top-k
/// heap for, leaving `child` owned by the caller.
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
/// 1. `tree` is non-null and satisfies [`NumericTreeRef::from_raw`]'s safety
///    requirements for the returned iterator's lifetime, including owner-derived
///    provenance if the tree may be mutated.
/// 2. `filter` is non-null and [valid] for a [`NumericFilter`] for the duration
///    of this call.
/// 3. `child`, when non-null, is a [valid], owning `QueryIterator *` with every
///    callback populated.
/// 4. `sctx` is non-null and [valid] for a [`RedisSearchCtx`] with a [valid]
///    `spec`, both outliving the returned iterator at a stable address. The request
///    timeout reached through `sctx.limits` must also remain valid at a stable address
///    until the iterator is dropped. Timeout source changes and deadline writes may
///    happen only between probes; only the blocked-client flag may change concurrently.
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
    let sctx_nn = NonNull::new(sctx.cast_mut()).expect("sctx must be non-null");
    debug_assert_non_null(filter_ctx, "filter_ctx");
    // SAFETY: guaranteed by 5. `FieldFilterContext` is `Copy`.
    let field_filter = unsafe { filter_ctx.read() };

    // The sort field is a single field index (never a mask), so the wide-schema
    // flag is irrelevant; pass empty reader flags.
    // SAFETY: guaranteed by 4.
    let expiration = unsafe { FieldExpirationChecker::new(sctx_nn, field_filter, 0) };
    // SAFETY: guaranteed by 4.
    let timeout = unsafe { AnyTimeoutContext::from_sctx(sctx_nn, TIMEOUT_CHECK_GRANULARITY) };

    // SAFETY: guaranteed by 4.
    let spec = unsafe { sctx_nn.as_ref() }.spec;
    debug_assert_non_null(spec, "sctx.spec");
    // SAFETY: guaranteed by 4.
    let docs = unsafe { &raw const (*spec).docs };
    let validity = move |doc_id| {
        // SAFETY: 4 keeps the inline document table valid for the iterator's lifetime.
        unsafe { DocTable_IsVisible(docs, doc_id) }
    };

    let tree = NonNull::new(tree.cast_mut()).expect("tree must be non-null");
    // SAFETY: clause 1 supplies all three preconditions of `from_raw`.
    let tree = unsafe { NumericTreeRef::from_raw(tree) };
    debug_assert_non_null(filter, "filter");
    // SAFETY: guaranteed by 2. `NumericFilter` is `Copy`.
    let filter = unsafe { filter.read() };

    // Adopt the filter child as an owning Rust iterator. A null child means a
    // plain range scan.
    // SAFETY: guaranteed by 3.
    let child = NonNull::new(child).map(|c| unsafe { CRQEIterator::new(c) });

    let ptr = match new_numeric_top_k(
        tree, filter, ascending, k, num_docs, validity, expiration, timeout, child,
    ) {
        Ok(Some(it)) => RQEIteratorWrapper::boxed_new_compound(it),
        Ok(None) => return RQEIteratorWrapper::boxed_new(rqe_iterators::Empty),
        Err(err) => {
            let _ = err.child.map(CRQEIterator::into_raw);
            return std::ptr::null_mut();
        }
    };
    // Top-k is score-ordered: the C highlighter must use sequential reads.
    // SAFETY: `ptr` is freshly boxed above and has no other aliases.
    unsafe { patch_vtable(ptr, |h| h.SkipTo = None) };
    ptr
}
