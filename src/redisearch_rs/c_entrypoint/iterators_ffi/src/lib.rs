/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod debug;
pub mod deferred;
pub mod empty;
pub mod geo_shape;
pub mod id_list;
pub mod intersection;
pub mod inverted_index;
pub mod metric;
pub mod not;
pub mod numeric_top_k;
pub mod profile;
mod profile_print;
pub mod union;
pub mod vector_top_k;
pub mod wildcard;

use std::ptr::NonNull;

use ffi::QueryIterator;
use rqe_iterators::interop::patch_vtable;

/// Probe the query deadline once every this many candidate documents.
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// Hand a freshly boxed top-k iterator to C with its `SkipTo` cleared.
///
/// Top-k yields in score order, so it cannot seek by doc id; without `SkipTo`
/// the C highlighter finds a document by reading instead.
///
/// # Safety
///
/// `ptr` comes straight from an
/// [`RQEIteratorWrapper`](rqe_iterators::interop::RQEIteratorWrapper) constructor
/// and has no other alias.
unsafe fn without_skip_to(ptr: NonNull<QueryIterator>) -> *mut QueryIterator {
    // SAFETY: forwarded from this function's contract.
    unsafe { patch_vtable(ptr, |h| h.SkipTo = None) };
    ptr.as_ptr()
}
