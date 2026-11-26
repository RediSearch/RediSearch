/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_NUMERIC` query nodes.

use std::ptr::NonNull;

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::NumericFilter;
use query_error::QueryErrorCode;
use rqe_core::RS_INVALID_FIELD_INDEX;
use rqe_iterators::build_numeric_filter_iterator;
use search_disk::SearchDiskHandle;

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeRef, index_load};

/// `QN_NUMERIC` — a numeric range filter on a numeric field.
///
/// When the spec is backed by an on-disk index, delegates to the enterprise
/// numeric iterator via [`SearchDiskHandle::new_numeric_iterator`]. Otherwise
/// opens the field's numeric range tree and builds a union over the matching
/// sub-ranges. Returns `None` when the field has no numeric index yet,
/// no sub-range matches, or, with the failure reported via
/// [`status`](QueryEvalContext::status), when the disk iterator cannot be
/// created or [`index_load::reserve`] rejects the node's `$as` alias (as under
/// a `NOT`).
///
/// A node with a `$as` alias is otherwise wrapped by [`index_load::decorate`].
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    nf: &NumericFilter,
    config: Config,
) -> Option<Evaluated<'index>> {
    // The numeric node always carries a field index; the filter targets that
    // single field by index.
    assert_ne!(
        nf.field_index, RS_INVALID_FIELD_INDEX,
        "numeric node must have a resolved field index"
    );
    let field_index = nf.field_index;
    if cfg!(debug_assertions) {
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
        let field = unsafe { &*field_ptr };
        // Both query parsers and the legacy `FILTER` argument build a numeric node only on a
        // NUMERIC field.
        assert!(
            field.types() & ffi::FieldType_INDEXFLD_T_NUMERIC != 0,
            "a numeric node must reference a numeric field"
        );
    }

    let load = index_load::reserve(ctx, node)?;

    // Disk-index path: when the spec is backed by an on-disk index, delegate to
    // the enterprise numeric iterator instead of opening the in-memory range
    // tree.
    //
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2). `SearchDiskHandle::new` yields `None`
    // for the null (in-memory) case.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let view = NonNull::new(ctx.sctx().diskQueryView)
            .expect("query.sctx.diskQueryView is null for a disk-backed numeric query");
        // SAFETY: the wrapped disk spec is valid for `'index` (`QueryEvalContext`
        // invariants 1/2) and single-threaded query evaluation gives us the only
        // live reference to it; the enterprise iterators are registered whenever
        // a disk index is in use; `field_index` belongs to the numeric node's
        // field spec; `view` is the disk read view taken at query start.
        return match unsafe { disk.new_numeric_iterator(nf, field_index, view) } {
            Ok(it) => Some(index_load::decorate(ctx, load, Evaluated::RustLeaf(it))),
            Err(err) => {
                // Surface the failure via `status` so the query aborts with an
                // error rather than silently returning empty results.
                ctx.status()
                    .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
                None
            }
        };
    }

    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };

    let min_union_iter_heap = config.min_union_iter_heap;
    // SAFETY: `build_numeric_filter_iterator` preconditions hold:
    // 1. `sctx`/`sctx.spec` are valid and outlive the iterator —
    //    `QueryEvalContext` invariants (1)/(2).
    // 2. `nf.field_index` is within `sctx.spec`'s current field count (asserted
    //    above).
    // 3. `field_ctx.field` is a field index, built as `Index` just above.
    let iter =
        unsafe { build_numeric_filter_iterator(ctx.sctx(), nf, min_union_iter_heap, &field_ctx) };

    iter.map(|it| index_load::decorate(ctx, load, Evaluated::RustCompound(it)))
}
