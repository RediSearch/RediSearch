/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`BatchIterator`] — owned wrapper around `VecSimBatchIterator`.

use std::{marker::PhantomData, num::NonZeroUsize, ptr::NonNull};

use ffi::{
    VecSimBatchIterator, VecSimBatchIterator_Free, VecSimBatchIterator_HasNext,
    VecSimBatchIterator_Next, VecSimIndex, VecSimQueryParams,
};

use crate::{QueryError, QueryReply, ReplyOrder};

/// Owned `VecSimBatchIterator`. Freed on [`Drop`].
///
/// The lifetime parameters prevent the iterator from outliving the C state it
/// borrows:
///
/// - `'index`: the VecSim index the iterator was created from. HNSW batch
///   iterators store a raw index pointer and dereference it on every
///   [`next`](Self::next) and [`Drop`] call.
/// - `'params`: the `VecSimQueryParams` borrow passed at construction. The
///   iterator copies `queryParams->timeoutCtx` and reads that pointer on every
///   [`next`](Self::next) call; `'params` acts as a conservative lower bound
///   on the lifetime of the timeout-context object.
pub struct BatchIterator<'index, 'params> {
    /// Owned VecSim batch iterator handle.
    ///
    /// # Invariant
    ///
    /// Valid (returned by `VecSimBatchIterator_New`) and not yet freed, from
    /// construction until [`Drop`].
    inner: NonNull<VecSimBatchIterator>,
    _idx: PhantomData<&'index VecSimIndex>,
    _params: PhantomData<&'params mut VecSimQueryParams>,
}

impl<'index, 'params> BatchIterator<'index, 'params> {
    /// Wrap a non-null batch iterator obtained from
    /// `VecSimBatchIterator_New`.
    ///
    /// # Safety
    ///
    /// The caller must:
    /// 1. establish the [`inner`](Self::inner) field invariant;
    /// 2. choose `'index` so it is bounded by the index lifetime;
    /// 3. choose `'params` so it is bounded by the timeout-context lifetime.
    ///
    /// The query blob passed to `VecSimBatchIterator_New` needs no lifetime: it
    /// is copied into the iterator at construction, so the caller's buffer may
    /// be dropped before the iterator.
    pub(crate) const unsafe fn from_raw(ptr: NonNull<VecSimBatchIterator>) -> Self {
        Self {
            inner: ptr,
            _idx: PhantomData,
            _params: PhantomData,
        }
    }

    /// Returns `true` while there are more results to fetch.
    pub fn has_next(&self) -> bool {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimBatchIterator_HasNext(self.inner.as_ptr()) }
    }

    /// Fetch the next batch of at most `n` results.
    ///
    /// Returns `Ok(None)` if VecSim returned a null reply (depleted iterator
    /// or out-of-memory) and `Err(QueryError::TimedOut)` if the reply code is
    /// `VecSim_QueryReply_TimedOut`. The reply is freed by this call in both
    /// non-`Ok(Some(_))` paths.
    pub fn next(
        &mut self,
        n: NonZeroUsize,
        order: ReplyOrder,
    ) -> Result<Option<QueryReply>, QueryError> {
        // SAFETY: `self.inner` upholds its invariant.
        let raw = unsafe { VecSimBatchIterator_Next(self.inner.as_ptr(), n.get(), order.as_raw()) };
        // SAFETY: `raw` is the freshly returned reply pointer from VecSim.
        unsafe { QueryReply::from_raw_checked(raw, order) }
    }
}

impl Drop for BatchIterator<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimBatchIterator_Free(self.inner.as_ptr()) };
    }
}
