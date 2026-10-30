/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::QueryIterator;
use rqe_core::DocId;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{NewWildcardIterator, Wildcard, interop::RQEIteratorWrapper};

/// Creates a new non-optimized wildcard iterator over the `[0, max_id]` document id range.
#[unsafe(no_mangle)]
pub extern "C" fn NewWildcardIterator_NonOptimized(
    max_id: DocId,
    weight: f64,
) -> *mut QueryIterator {
    let it = NewWildcardIterator::NotOptimized(Wildcard::new(max_id, weight));
    RQEIteratorWrapper::boxed_new(it)
}

/// Returns `true` if `it` is a wildcard iterator (either optimized or non-optimized).
///
/// # Safety
///
/// `it`, when non-null, must point to a valid [`QueryIterator`].
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn IsWildcardIterator(it: *const QueryIterator) -> bool {
    // SAFETY: Caller guarantees `it`, when non-null, points to a valid `QueryIterator`.
    let Some(it) = (unsafe { it.as_ref() }) else {
        return false;
    };
    matches!(
        it.type_,
        IteratorType::Wildcard | IteratorType::InvIdxWildcard
    )
}
