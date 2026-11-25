/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Binding a reserved metric request to the key slot of the iterator that
//! yields the metric.

use std::ptr::NonNull;

use query::MetricRequestId;
use rqe_iterators::{IteratorType, index_load, metric};

use crate::QueryEvalContext;

/// Point the metric request `id` reserved at the freshly built iterator's
/// own-key slot, and give the iterator the back-reference it uses to invalidate
/// the handle when it is freed.
///
/// A no-op for iterator types that yield no metric — a vector range query can
/// return a plain empty iterator, for instance — which drops `id` and leaves
/// the request unbound.
///
/// # Safety
///
/// 1. `it` points to a live iterator, freshly built and not yet shared — the
///    iterator a vector or numeric node was evaluated into — that the caller
///    holds exclusively for the duration of the call.
/// 2. `it` was built by its type's constructor, so its header's type tag names
///    its concrete type.
/// 3. `id` was reserved by [`QueryEvalContext::add_metric_request`] on `ctx`.
/// 4. `ctx`'s metric-request array outlives the iterator, as the AST outlives
///    the iterator tree built from it.
pub(crate) unsafe fn bind_metric_request(
    ctx: &mut QueryEvalContext,
    id: MetricRequestId,
    it: NonNull<ffi::QueryIterator>,
) {
    // SAFETY: (1).
    let iterator_type = unsafe { it.as_ref() }.type_;

    // Neither accessor can infer the key's borrow — both reach it through a
    // type-erased header — and `bind_metric_request_key` leaves it free too, so
    // nothing constrains the lifetime inferred here. It is discharged by
    // discarding it: the key leaves this function only as a raw pointer, handed
    // straight back to the iterator that owns the slot, so no `RLookupKey`
    // reference — nor any borrow its safe accessors hand out — is ever formed at
    // that lifetime.
    match iterator_type {
        IteratorType::Hybrid => {
            // SAFETY: the discriminant says this is a vector top-k iterator (2),
            // and we hold it exclusively (1).
            let own_key = unsafe { vector_score_source::interop::own_key_ref(it) };
            // SAFETY: `id` was reserved on this context (3); `own_key` points
            // into the iterator, which clears the handle's validity flag when
            // freed, before the array that owns the handle (4).
            let handle = unsafe { ctx.bind_metric_request_key(id, own_key) };
            // SAFETY: as above, and `handle` is a valid handle that lives as
            // long as the AST.
            unsafe { vector_score_source::interop::set_key_handle(it, Some(handle)) };
        }
        IteratorType::MetricSortedById
        | IteratorType::MetricSortedByScore
        | IteratorType::MetricLazySortedById
        | IteratorType::MetricLazySortedByScore => {
            // SAFETY: the discriminant says this is one of the metric
            // iterators (2), and we hold it exclusively (1); the borrow it is
            // typed with is discarded rather than relied upon, as above.
            let own_key = unsafe { metric::own_key_ref(it) };
            // SAFETY: as above.
            let handle = unsafe { ctx.bind_metric_request_key(id, own_key.as_ptr()) };
            // SAFETY: as above.
            unsafe { metric::set_key_handle(it, Some(handle)) };
        }
        IteratorType::IndexLoad => {
            // SAFETY: the discriminant says this is an index-loading decorator
            // (2), which only `index_load::boxed_new` lowers to the C ABI, and
            // we hold it exclusively (1); the borrow it is typed with is
            // discarded, as above.
            let own_key = unsafe { index_load::own_key_ref(it) };
            // SAFETY: as above. The slot is a heap cell owned by the decorator,
            // so it stays valid across moves of the decorator until it is
            // freed, which clears the handle's validity flag.
            let handle = unsafe { ctx.bind_metric_request_key(id, own_key.as_ptr()) };
            // SAFETY: as above.
            unsafe { index_load::set_key_handle(it, Some(handle)) };
        }
        // Every other type yields no metric to bind.
        _ => {}
    }
}
