/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Construction dispatch for vector top-k, keeping this decision logic out of
//! the FFI boundary.

use std::{num::NonZeroUsize, ptr::NonNull};

use ffi::{QueryRequestTimeout, VecSearchMode_STANDARD_KNN, VecSimIndex, VecSimQueryParams};
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator};
use top_k::{ChildReduction, TopKIterator, reduce_child};

use crate::{
    VectorScoreSource, VectorTopKIterator, new_vector_top_k_filtered, new_vector_top_k_unfiltered,
};

/// Result of [`new_vector_top_k`]: the iterator to expose, already reduced to
/// the right variant.
pub enum NewVectorTopK<'index, E: ExpirationChecker> {
    /// The query yields nothing (empty child, or `k == 0`).
    ReducedEmpty,
    /// Pure KNN, no filter child.
    Unfiltered(VectorTopKIterator<'index, E>),
    /// Hybrid KNN intersected with a filter child.
    Filtered(TopKIterator<'index, VectorScoreSource<'index, E>, CRQEIterator>),
}

/// Build the vector top-k iterator for the given query, applying the child
/// reduction and choosing the unfiltered or filtered construction accordingly.
///
/// # Safety
///
/// 1. `index` must be valid for `'index`, which outlives the returned iterator.
/// 2. `query_vector` must satisfy the [`VectorScoreSource`] query-vector length
///    invariant for `index`.
/// 3. `timeout` must be non-null, remain valid for the returned iterator's lifetime, and satisfy
///    the [`VectorScoreSource`] source-transition and serialization contract.
#[expect(clippy::too_many_arguments)]
pub unsafe fn new_vector_top_k<'index, E>(
    index: NonNull<VecSimIndex>,
    query_vector: Vec<u8>,
    query_params: VecSimQueryParams,
    k: usize,
    timeout: *mut QueryRequestTimeout,
    can_trim_deep_results: bool,
    expiration: E,
    child: Option<CRQEIterator>,
) -> NewVectorTopK<'index, E>
where
    E: ExpirationChecker + 'index,
{
    let timeout = NonNull::new(timeout).expect("vector query requires request timeout state");
    let Some(k) = NonZeroUsize::new(k) else {
        return NewVectorTopK::ReducedEmpty;
    };

    match reduce_child(child) {
        ChildReduction::Empty => NewVectorTopK::ReducedEmpty,
        ChildReduction::Unfiltered => {
            // A caller-supplied hybrid policy must not reach the single-shot KNN call.
            let query_params = VecSimQueryParams {
                searchMode: VecSearchMode_STANDARD_KNN,
                ..query_params
            };
            // SAFETY: guaranteed by invariants 1 and 2 of this function's contract.
            let source = unsafe {
                VectorScoreSource::new(
                    index,
                    query_vector,
                    query_params,
                    k.get(),
                    timeout,
                    0, // no child
                    0, // dynamic batch size
                    expiration,
                )
            };
            NewVectorTopK::Unfiltered(new_vector_top_k_unfiltered(source, k))
        }
        ChildReduction::Filtered(child) => {
            let child_est = child.num_estimated();
            // SAFETY: guaranteed by invariants 1 and 2 of this function's contract.
            let source = unsafe {
                VectorScoreSource::new(
                    index,
                    query_vector,
                    query_params,
                    k.get(),
                    timeout,
                    child_est,
                    // Honor an explicit `BATCH_SIZE` (0 means dynamic).
                    query_params.batchSize,
                    expiration,
                )
            };
            NewVectorTopK::Filtered(new_vector_top_k_filtered(
                source,
                child,
                k,
                can_trim_deep_results,
            ))
        }
    }
}
