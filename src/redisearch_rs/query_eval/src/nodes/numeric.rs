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
use rqe_iterators::{build_keyed_numeric_filter_iterator, build_numeric_filter_iterator};
use search_disk::SearchDiskHandle;

use crate::{Config, Evaluated, QueryEvalContext};

/// `QN_NUMERIC` — a numeric range filter on a numeric field, or on one key of a map
/// numeric field when `key` is set.
///
/// A key that was never indexed matches nothing. The keyed iterator resolves the key's tree
/// again whenever it revalidates, so it aborts if GC removed the tree meanwhile.
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
    key: Option<&[u8]>,
    config: Config,
) -> Option<Evaluated<'index>> {
    // The numeric node always carries a field index; the filter targets that
    // single field by index.
    assert_ne!(
        nf.field_index, RS_INVALID_FIELD_INDEX,
        "numeric node must have a resolved field index"
    );
    let field_index = nf.field_index;

    // Map numeric fields are rejected on disk-backed indexes, so a keyed filter never reaches
    // the disk path below.
    if let Some(key) = key {
        let field_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: FieldExpirationPredicate::Default,
        };
        // SAFETY: `build_keyed_numeric_filter_iterator` preconditions hold:
        // 1. `sctx`/`sctx.spec` are valid and outlive the iterator —
        //    `QueryEvalContext` invariants (1)/(2).
        // 2. `nf.field_index` is within the field count (asserted above) and the parser
        //    only builds a keyed node for a map numeric field.
        // 3. `field_ctx.field` is a field index, built as `Index` just above.
        // 4. Evaluation runs under the spec read lock.
        let iter = unsafe {
            build_keyed_numeric_filter_iterator(
                ctx.sctx(),
                nf,
                key,
                config.min_union_iter_heap,
                &field_ctx,
            )
        };
        return iter.map(Evaluated::RustCompound);
    }

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
    // 2. `nf.field_index` is within `sctx.spec`'s current field count (asserted
    //    above).
    // 3. `field_ctx.field` is a field index, built as `Index` just above.
    let iter =
        unsafe { build_numeric_filter_iterator(ctx.sctx(), nf, min_union_iter_heap, &field_ctx) };

    iter.map(Evaluated::RustCompound)
}
