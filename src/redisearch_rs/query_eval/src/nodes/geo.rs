/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_GEO` query nodes.

use std::ptr::NonNull;

use query_error::QueryErrorCode;
use rqe_iterators::build_geo_range_iterator;
use search_disk::SearchDiskHandle;

use crate::{Config, Evaluated, QueryEvalContext};

/// `QN_GEO` — a geo-radius filter on a geo field.
///
/// Validates the geo filter (reporting any error into the query's status). When
/// the spec is backed by an on-disk index, delegates to the enterprise geo
/// iterator via [`SearchDiskHandle::new_geo_iterator`]. Otherwise builds a union
/// over the matching geohash ranges via [`build_geo_range_iterator`]. Returns
/// `None` — i.e. no iterator — when validation fails, the geo index does not
/// exist yet, no entries match, or the disk iterator could not be created (in
/// which case the failure is reported via [`status`](QueryEvalContext::status)).
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    gf: *mut ffi::GeoFilter,
    config: Config,
) -> Option<Evaluated<'index>> {
    let status = ctx.status_ptr();
    // SAFETY: `gf` is a valid, non-null `GeoFilter` (well-formed geo node) and
    // `status` is the query's valid `QueryError` accumulator
    // (`QueryEvalContext` invariant (2)).
    if unsafe { ffi::GeoFilter_Validate(gf, status) } == 0 {
        return None;
    }

    // Disk-index path: when the spec is backed by an on-disk index, delegate to
    // the enterprise geo iterator instead of opening the in-memory range tree.
    //
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2). `SearchDiskHandle::new` yields `None`
    // for the null (in-memory) case.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed geo query");
        // SAFETY: `gf` is valid and, during single-threaded evaluation,
        // exclusively owned for `'index`, so a `&'index mut` is sound.
        let gf_ref = unsafe { &mut *gf };
        // SAFETY: a well-formed geo node has a valid, non-null `fieldSpec`, so
        // reading its `index` is sound.
        let field_index = unsafe { (*gf_ref.fieldSpec).index };
        // SAFETY: the wrapped disk spec is valid for `'index` (`QueryEvalContext`
        // invariants 1/2) and single-threaded query evaluation gives us the only
        // live reference to it; the enterprise iterators are registered whenever
        // a disk index is in use; `field_index` belongs to the geo node's field
        // spec; `snapshot` is the disk snapshot taken at query start.
        return match unsafe { disk.new_geo_iterator(gf_ref, field_index, snapshot) } {
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

    let sctx = NonNull::from(ctx.sctx());
    let min_union_iter_heap = config.min_union_iter_heap;
    // SAFETY: `gf` is valid and, during evaluation, exclusively owned, so a
    // `&mut` is sound.
    let gf_ref = unsafe { &mut *gf };
    // SAFETY: `build_geo_range_iterator` preconditions hold:
    // 1. `sctx`/`sctx.spec` are valid and outlive the iterator —
    //    `QueryEvalContext` invariants (1)/(2).
    // 2. `gf.fieldSpec` is a valid, non-null `FieldSpec` for a geo field
    //    (well-formed geo node).
    // 3. `gf.numericFilters` is NULL on entry (freshly parsed geo node) and is
    //    populated/owned by `gf`, freed by `GeoFilter_Free`.
    let iter = unsafe {
        build_geo_range_iterator(sctx, gf_ref, min_union_iter_heap, config.numeric_compress)
    };

    iter.map(Evaluated::RustCompound)
}
