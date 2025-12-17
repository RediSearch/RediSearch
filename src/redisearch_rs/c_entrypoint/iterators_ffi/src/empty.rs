/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IteratorType_EMPTY_ITERATOR, QueryIterator};
use rqe_iterators::Empty;
use rqe_iterators_interop::RQEIteratorWrapper;

#[unsafe(no_mangle)]
/// Creates a new empty iterator.
pub extern "C" fn NewEmptyIterator() -> *mut QueryIterator {
    RQEIteratorWrapper::boxed_new(IteratorType_EMPTY_ITERATOR, Empty)
}
