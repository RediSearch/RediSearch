/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Helpers shared by the vector range iterator test binaries.

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use ffi::{VecSimQueryParams, VecSimQueryReply_Order};
use iterators_ffi::deferred::NewLazyVectorRangeIteratorFromParams;
use rqe_iterators::c2rust::CRQEIterator;
use vector_score_source::test_utils::TestIndex;

/// A lazy range iterator that borrows the [`TestIndex`] it was built over, so it cannot outlive
/// the index or its timeout.
pub struct RangeIterator<'index> {
    it: CRQEIterator,
    _index: PhantomData<&'index TestIndex>,
}

impl Deref for RangeIterator<'_> {
    type Target = CRQEIterator;

    fn deref(&self) -> &CRQEIterator {
        &self.it
    }
}

impl DerefMut for RangeIterator<'_> {
    fn deref_mut(&mut self) -> &mut CRQEIterator {
        &mut self.it
    }
}

/// Builds a lazy range iterator over `index` through the C entry point. `query` must be sized
/// for the index.
pub fn range_iterator<'index>(
    index: &'index TestIndex,
    query: &[u8],
    radius: f64,
    order: VecSimQueryReply_Order,
    yields_metric: bool,
) -> RangeIterator<'index> {
    // SAFETY: all-zero is a valid `VecSimQueryParams`; the iterator sets `timeoutCtx` itself.
    let params: VecSimQueryParams = unsafe { std::mem::zeroed() };
    // SAFETY:
    // 1. The returned `RangeIterator` borrows `index`, so the index and its timeout outlive the
    //    iterator.
    // 2. `query` is a readable slice, sized for the index by the caller.
    let it = unsafe {
        NewLazyVectorRangeIteratorFromParams(
            index.as_ptr(),
            query.as_ptr().cast(),
            query.len(),
            radius,
            params,
            order,
            yields_metric,
            index.timeout_ptr(),
        )
    };
    let it = NonNull::new(it).expect("the range constructor returned null");
    RangeIterator {
        // SAFETY: `it` is an owning handle with every required callback set.
        it: unsafe { CRQEIterator::new(it) },
        _index: PhantomData,
    }
}
