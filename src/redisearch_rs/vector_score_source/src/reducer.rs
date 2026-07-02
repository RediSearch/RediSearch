/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Child short-circuit reduction and construction dispatch for vector top-k,
//! keeping this decision logic out of the FFI boundary.

use std::{num::NonZeroUsize, ptr::NonNull};

use ffi::{IteratorType, VecSearchMode_STANDARD_KNN, VecSimIndex, VecSimQueryParams, timespec};
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator};
use top_k::TopKIterator;

use crate::{
    VectorScoreSource, VectorTopKIterator, new_vector_top_k_filtered, new_vector_top_k_unfiltered,
};

/// Outcome of reducing a filter child before constructing the iterator.
enum VectorChildReduction<C> {
    /// The child can never yield a match, so the whole query is empty.
    Empty,
    /// There is no filter (no child, or a wildcard that matches everything):
    /// run as a pure KNN query.
    Unfiltered,
    /// A real filter child to intersect the KNN results against.
    Filtered(C),
}

/// Classify a filter child into the reduction that applies to it.
///
/// A reduced-away child is dropped, freeing the underlying iterator.
fn vector_child_reducer<'index, C>(child: Option<C>) -> VectorChildReduction<C>
where
    C: RQEIterator<'index> + 'index,
{
    let Some(child) = child else {
        return VectorChildReduction::Unfiltered;
    };
    match child.type_() {
        IteratorType::Empty => VectorChildReduction::Empty,
        IteratorType::Wildcard | IteratorType::InvIdxWildcard => VectorChildReduction::Unfiltered,
        _ => VectorChildReduction::Filtered(child),
    }
}

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
/// - `index` must be valid for `'index`, which outlives the returned iterator.
/// - `query_vector` must satisfy the [`VectorScoreSource`] query-vector length
///   invariant for `index`.
#[expect(clippy::too_many_arguments)]
pub unsafe fn new_vector_top_k<'index, E>(
    index: NonNull<VecSimIndex>,
    query_vector: Vec<u8>,
    query_params: VecSimQueryParams,
    k: usize,
    timeout: timespec,
    skip_timeout_checks: bool,
    can_trim_deep_results: bool,
    expiration: Option<E>,
    child: Option<CRQEIterator>,
) -> NewVectorTopK<'index, E>
where
    E: ExpirationChecker + 'index,
{
    let Some(k) = NonZeroUsize::new(k) else {
        return NewVectorTopK::ReducedEmpty;
    };

    match vector_child_reducer(child) {
        VectorChildReduction::Empty => NewVectorTopK::ReducedEmpty,
        VectorChildReduction::Unfiltered => {
            // A caller-supplied hybrid policy must not reach the single-shot KNN call.
            let query_params = VecSimQueryParams {
                searchMode: VecSearchMode_STANDARD_KNN,
                ..query_params
            };
            // SAFETY: `index` validity and the query-vector length invariant are
            // guaranteed by this function's contract.
            let source = unsafe {
                VectorScoreSource::new(
                    index,
                    query_vector,
                    query_params,
                    k.get(),
                    timeout,
                    skip_timeout_checks,
                    0, // no child
                    0, // dynamic batch size
                    expiration,
                )
            };
            NewVectorTopK::Unfiltered(new_vector_top_k_unfiltered(source, k))
        }
        VectorChildReduction::Filtered(child) => {
            let child_est = child.num_estimated();
            // SAFETY: `index` validity and the query-vector length invariant are
            // guaranteed by this function's contract.
            let source = unsafe {
                VectorScoreSource::new(
                    index,
                    query_vector,
                    query_params,
                    k.get(),
                    timeout,
                    skip_timeout_checks,
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

#[cfg(test)]
mod tests {
    use rqe_iterators::{Empty, IdList, Wildcard};

    use super::{VectorChildReduction, vector_child_reducer};

    #[test]
    fn no_child_is_unfiltered() {
        let reduction = vector_child_reducer(None::<Empty>);
        assert!(matches!(reduction, VectorChildReduction::Unfiltered));
    }

    #[test]
    fn empty_child_reduces_to_empty() {
        let reduction = vector_child_reducer(Some(Empty));
        assert!(matches!(reduction, VectorChildReduction::Empty));
    }

    #[test]
    fn wildcard_child_is_unfiltered() {
        let reduction = vector_child_reducer(Some(Wildcard::new(10, 1.0)));
        assert!(matches!(reduction, VectorChildReduction::Unfiltered));
    }

    #[test]
    fn real_child_is_kept_as_filter() {
        let child = IdList::<true>::new(vec![1, 2, 3]);
        let reduction = vector_child_reducer(Some(child));
        assert!(matches!(reduction, VectorChildReduction::Filtered(_)));
    }
}
