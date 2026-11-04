/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Construction dispatch for numeric top-k, keeping this decision logic out of
//! the FFI boundary.

use std::{collections::TryReserveError, num::NonZeroUsize, ptr::NonNull};

use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRangeTree, RangeWindow};
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator, utils::TimeoutContext};
use top_k::{ChildReduction, TopKHeap, reduce_child};

use crate::{
    DocValidity, NumericOptimizerMode, NumericScoreSource, NumericTopKIterator, cmp_for,
    numeric_top_k_filtered_with_heap, numeric_top_k_unfiltered_with_heap,
    source::{DEFAULT_RANGE_BATCH_SIZE, estimate_limit},
};

/// Result of [`new_numeric_top_k`]: the iterator to expose, already reduced to
/// the right variant. The two iterator variants share one type and differ only
/// in holding a filter child; they are not C's optimizer modes.
pub enum NewNumericTopK<'index, V: DocValidity, E: ExpirationChecker, T: TimeoutContext> {
    /// The query yields nothing (empty child, or `k == 0`).
    ReducedEmpty,
    /// Scan the value-ordered range alone, with no filter child.
    Unfiltered(NumericTopKIterator<'index, V, E, T>),
    /// Intersect the value-ordered range with a filter child, with the
    /// expand-and-retry window path enabled.
    Filtered(NumericTopKIterator<'index, V, E, T>),
}

/// Error of [`new_numeric_top_k`]: the heap for `k` results could not be
/// reserved.
pub struct HeapReserveError {
    /// Why the reservation failed.
    pub error: TryReserveError,
    /// The filter child, handed back untouched.
    pub child: Option<CRQEIterator>,
}

impl std::fmt::Debug for HeapReserveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapReserveError")
            .field("error", &self.error)
            .field("child", &self.child.is_some())
            .finish()
    }
}

/// Build the numeric top-k iterator for the given query, applying
/// [`reduce_child`] and choosing the unfiltered or filtered construction.
///
/// `num_docs` is the total document count; with the child's selectivity
/// estimate it sizes the initial retry window. `mode` is the strategy the query
/// plan picked, reported by the profile and not otherwise acted on. `validity`,
/// `expiration` and `timeout` are the source's [`DocValidity`],
/// [`ExpirationChecker`] and [`TimeoutContext`]. A `k` of `0` reduces to an
/// empty query; callers raise a zero `LIMIT` to the default first, as C's
/// `QOptimizer_Parse` does.
///
/// # Errors
///
/// Returns [`HeapReserveError`] when the heap for `k` results cannot be
/// reserved; C's `NewOptimizerIterator` likewise rejects such a `LIMIT`.
///
/// # Safety
///
/// `tree` upholds the safety requirements of
/// [`NumericRangeIterator::from_raw`](crate::NumericRangeIterator::from_raw).
#[expect(clippy::too_many_arguments, reason = "one parameter per query input")]
pub unsafe fn new_numeric_top_k<'index, V, E, T>(
    tree: NonNull<NumericRangeTree>,
    filter: NumericFilter,
    ascending: bool,
    k: usize,
    num_docs: usize,
    mode: NumericOptimizerMode,
    validity: V,
    expiration: E,
    timeout: T,
    child: Option<CRQEIterator>,
) -> Result<NewNumericTopK<'index, V, E, T>, HeapReserveError>
where
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
    T: TimeoutContext + 'index,
{
    let Some(k) = NonZeroUsize::new(k) else {
        return Ok(NewNumericTopK::ReducedEmpty);
    };
    let heap = match TopKHeap::try_new(k, cmp_for(ascending)) {
        Ok(heap) => heap,
        Err(error) => return Err(HeapReserveError { error, child }),
    };

    match reduce_child(child) {
        ChildReduction::Empty => Ok(NewNumericTopK::ReducedEmpty),
        ChildReduction::Unfiltered => {
            // SAFETY: guaranteed by the caller.
            let source = unsafe { NumericScoreSource::unfiltered_raw(tree, filter, ascending) }
                .with_optimizer_mode(mode)
                .with_validity(validity)
                .with_expiration(expiration)
                .with_timeout(timeout);
            Ok(NewNumericTopK::Unfiltered(
                numeric_top_k_unfiltered_with_heap(source, heap),
            ))
        }
        ChildReduction::Filtered(child) => {
            let child_estimate = child.num_estimated();
            // Size the initial window from the child's selectivity, so the range
            // is wide enough to likely hold `k` matching docs; the source widens
            // it on retry when the child proves more selective than estimated.
            let window = RangeWindow {
                offset: 0,
                limit: estimate_limit(num_docs, child_estimate, k.get()),
            };
            // SAFETY: guaranteed by the caller.
            let source = unsafe {
                NumericScoreSource::filtered_raw(
                    tree,
                    filter,
                    window,
                    ascending,
                    DEFAULT_RANGE_BATCH_SIZE,
                    num_docs,
                    child_estimate,
                )
            }
            .with_optimizer_mode(mode)
            .with_validity(validity)
            .with_expiration(expiration)
            .with_timeout(timeout);
            Ok(NewNumericTopK::Filtered(numeric_top_k_filtered_with_heap(
                source, child, heap,
            )))
        }
    }
}
