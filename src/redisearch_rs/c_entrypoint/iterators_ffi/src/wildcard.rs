/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IteratorType_WILDCARD_ITERATOR, QueryIterator, t_docId};
use rqe_iterators::Wildcard;
use rqe_iterators_interop::RQEIteratorWrapper;

#[unsafe(no_mangle)]
/// Creates a new non-optimized wildcard iterator over the `[0, max_id)` document id range.
pub extern "C" fn NewWildcardIterator_NonOptimized(
    max_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    RQEIteratorWrapper::boxed_new(
        IteratorType_WILDCARD_ITERATOR,
        Wildcard::new(max_id, weight),
    )
}
