/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`RQEIterator`](crate::RQEIterator) utilities

mod owned_slice;
mod timeout;
mod min_heap;

#[doc(inline)]
pub use self::owned_slice::OwnedSlice;
pub use timeout::TimeoutContext;
pub use min_heap::DocIdMinHeap;
