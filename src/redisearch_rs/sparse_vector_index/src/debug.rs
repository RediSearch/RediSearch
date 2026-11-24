/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Replies for the sparse vector `FT.DEBUG` subcommands.
//!
//! A field no document has written to yet has no index; it is reported as
//! empty rather than as an error, as the numeric and tag dumps do.

use std::ptr::NonNull;

use redis_reply::{RedisModuleCtx, Replier};

use crate::SparseVectorIndex;

/// Reply to `FT.DEBUG INFO_SPARSEIDX`: the field's
/// [`stats`](SparseVectorIndex::stats), then its `top` longest dimensions as
/// [`SparseVectorIndex::longest_dimensions`] orders them.
///
/// # Safety
///
/// `ctx` is a valid Redis module context.
pub unsafe fn debug_info(
    ctx: NonNull<RedisModuleCtx>,
    index: Option<&SparseVectorIndex>,
    top: usize,
) {
    // SAFETY: guaranteed by the caller.
    let mut replier = unsafe { Replier::new(ctx) };
    let mut map = replier.map();
    let stats = index.map(SparseVectorIndex::stats).unwrap_or_default();
    map.kv_long_long(c"num_dimensions", stats.num_dimensions as i64);
    map.kv_long_long(c"num_records", stats.num_records as i64);
    map.kv_long_long(c"memory", stats.memory as i64);
    let mut dims = map.kv_array(c"longest_dimensions");
    for dim in index.map(|i| i.longest_dimensions(top)).unwrap_or_default() {
        let mut entry = dims.map();
        entry.kv_long_long(c"dim_id", i64::from(dim.dim_id));
        entry.kv_long_long(c"num_entries", dim.num_entries as i64);
        entry.kv_long_long(c"num_blocks", dim.num_blocks as i64);
        entry.kv_long_long(c"memory", dim.memory as i64);
    }
}

/// Reply to `FT.DEBUG DUMP_SPARSEIDX`: `dim_id`'s posting list as a flat
/// `[doc_id, weight, …]` array in doc-ID order, empty when no document has the
/// dimension.
///
/// # Safety
///
/// `ctx` is a valid Redis module context.
pub unsafe fn debug_dump_dimension(
    ctx: NonNull<RedisModuleCtx>,
    index: Option<&SparseVectorIndex>,
    dim_id: u32,
) {
    // SAFETY: guaranteed by the caller.
    let mut replier = unsafe { Replier::new(ctx) };
    let mut arr = replier.array();
    let entries = index.and_then(|i| i.dimension_entries(dim_id));
    for (doc_id, weight) in entries.unwrap_or_default() {
        arr.long_long(doc_id as i64);
        arr.double(f64::from(weight));
    }
}
