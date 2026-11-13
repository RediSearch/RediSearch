/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_SPARSE_VECTOR` query nodes.

use std::ptr::NonNull;

use query_error::QueryErrorCode;
use rqe_core::FieldIndex;
use rqe_iterators::interop::RQEIteratorWrapper;
use sparse_vector_index::{SparseMetric, SparseVectorIndex, SparseVectorIterator};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut};

/// `QN_SPARSE_VECTOR` — the union of the query dimensions' posting lists,
/// reduced to one dot-product [`SparseScore`](index_result::RSResultKind::SparseScore)
/// result per document by a [`SparseVectorIterator`].
///
/// The query vector goes through the same checks as a document's, against the
/// field's `DIM` and metric; a failing one is a query error.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: QueryNodeMut<'_>,
    field_index: FieldIndex,
    blob: &[u8],
    config: Config,
) -> Option<Evaluated<'index>> {
    let spec = ctx.spec();
    assert!(
        field_index < spec.numFields,
        "field_index must be within the spec's current field count"
    );
    // SAFETY: `field_index` is within `spec.numFields` (checked above), so this
    // stays within the bounds of the `numFields`-sized array `spec.fields` points to.
    let field_ptr = unsafe { spec.fields.add(field_index as usize) };
    // SAFETY: `field_ptr` points into the live spec's field array, which the
    // query's spec read lock keeps alive and unmodified throughout evaluation.
    // Going through the raw pointer detaches the borrow from `ctx`, which the
    // metric-request bookkeeping below needs exclusively.
    let field = unsafe { &*field_ptr };
    debug_assert!(
        field.types() & ffi::FieldType_INDEXFLD_T_SPARSE_VECTOR != 0,
        "a sparse vector node must reference a sparse vector field"
    );
    // SAFETY: the field is a sparse vector field, so `sparseVecOpts` is the
    // active union member.
    let opts = unsafe { &field.__bindgen_anon_1.sparseVecOpts };
    let metric = SparseMetric::try_from(opts.metric).expect("the schema only stores valid metrics");

    let query = match sparse_vector_index::parse_blob(blob)
        .and_then(|entries| sparse_vector_index::prepare(entries, opts.dim, metric))
    {
        Ok(query) => query,
        Err(err) => {
            ctx.status().set_error(
                QueryErrorCode::BadVal,
                &format!("Invalid sparse query vector: {err}"),
            );
            return None;
        }
    };

    // Reserved before either exit that matches nothing, so the pipeline still
    // learns the score's name and a `SORTBY` on it resolves on an empty result;
    // the request then simply stays unbound.
    let dist_field = node.opts().dist_field;
    let request_id = (!dist_field.is_null()).then(|| {
        // SAFETY: `dist_field` is non-null, per the guard above, and is the
        // NUL-terminated name the parser stored on the node's options. The
        // request borrows it; the node owns it, and the AST outlives the
        // metric-request array.
        unsafe { ctx.add_metric_request(dist_field, false) }
    });

    // A field no document has written to yet has no index, and matches nothing.
    let index = NonNull::new(opts.sparseVecIndex.cast::<SparseVectorIndex>())?;
    // SAFETY: 1. a non-null `sparseVecIndex` is the index `SparseVectorIndex_New`
    // allocated for this field, which lives as long as the field and so as the
    // query. 2. Writers, GC included, take the spec write lock, which the query
    // only releases while its iterators are parked, revalidating them on
    // re-acquire. 3. It is the field's own pointer, not one derived from a
    // shared borrow.
    let it = unsafe { SparseVectorIterator::new(index, &query, config.min_union_iter_heap) }?;

    let ptr = RQEIteratorWrapper::boxed_new(it);
    if let Some(id) = request_id {
        // SAFETY: `ptr` was just produced by `boxed_new` for a
        // `SparseVectorIterator`, and nothing else holds it yet.
        let wrapper =
            unsafe { RQEIteratorWrapper::<SparseVectorIterator<'_>>::mut_ref_from_header_ptr(ptr) };
        let own_key = wrapper.inner.key_mut_ref() as *mut _;
        // SAFETY: `id` was reserved by `add_metric_request` on this same
        // context. `own_key` points into the heap-allocated iterator, which
        // clears the handle's validity flag when it is freed.
        let handle = unsafe { ctx.bind_metric_request_key(id, own_key) };
        // SAFETY: `handle` lives as long as the AST, which outlives the
        // iterator, and only this iterator writes its validity flag.
        unsafe { wrapper.inner.set_handle(Some(handle)) };
    }
    Some(Evaluated::RustCompound(ptr))
}
