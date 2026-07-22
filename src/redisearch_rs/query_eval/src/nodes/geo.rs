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

use rqe_iterators::build_geo_range_iterator;

use crate::{Config, Evaluated, QueryEvalContext};

/// `QN_GEO` — a geo-radius filter on a geo field.
///
/// Validates the geo filter (reporting any error into the query's status), then
/// builds a union over the matching geohash ranges via
/// [`build_geo_range_iterator`]. Returns `None` — i.e. no iterator — when
/// validation fails, the geo index does not exist yet, or no entries match.
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
