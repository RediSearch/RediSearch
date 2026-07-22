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
use rqe_iterators::build_numeric_filter_iterator;
use search_disk::SearchDiskHandle;

use crate::{Config, Evaluated, QueryEvalContext};

/// `QN_NUMERIC` — a numeric range filter on a numeric field.
///
/// When the spec is backed by an on-disk index, delegates to the enterprise
/// numeric iterator via [`SearchDiskHandle::new_numeric_iterator`]. Otherwise
/// opens the field's numeric range tree and builds a union over the matching
/// sub-ranges. Returns `None` when the field has no numeric index yet,
/// no sub-range matches, or the disk iterator not be created
/// (in which case the failure is reported via [`status`](QueryEvalContext::status)).
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    nf: &NumericFilter,
    config: Config,
) -> Option<Evaluated<'index>> {
    // The numeric node always carries a field spec; the filter targets that
    // single field by index.
    assert!(
        !nf.field_spec.is_null(),
        "numeric node must have a non-null field spec"
    );
    // SAFETY: a well-formed numeric node has a valid, non-null `field_spec`,
    // so reading its `index` is sound.
    let field_index = unsafe { (*nf.field_spec).index };

    // Disk-index path: when the spec is backed by an on-disk index, delegate to
    // the enterprise numeric iterator instead of opening the in-memory range
    // tree.
    //
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2). `SearchDiskHandle::new` yields `None`
    // for the null (in-memory) case.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed numeric query");
        // SAFETY: the wrapped disk spec is valid for `'index` (`QueryEvalContext`
        // invariants 1/2) and single-threaded query evaluation gives us the only
        // live reference to it; the enterprise iterators are registered whenever
        // a disk index is in use; `field_index` belongs to the numeric node's
        // field spec; `snapshot` is the disk snapshot taken at query start.
        return match unsafe { disk.new_numeric_iterator(nf, field_index, snapshot) } {
            Ok(it) => Some(Evaluated::RustLeaf(it)),
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
    // 2. `nf.field_spec` is a valid, non-null `FieldSpec` for a numeric field
    //    (well-formed numeric node).
    // 3. `field_ctx.field` is a field index, built as `Index` just above.
    let iter = unsafe {
        build_numeric_filter_iterator(
            ctx.sctx(),
            nf,
            min_union_iter_heap,
            &field_ctx,
            config.numeric_compress,
        )
    };

    iter.map(Evaluated::RustCompound)
}
