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

use std::{collections::TryReserveError, num::NonZeroUsize};

use inverted_index::NumericFilter;
use numeric_range_tree::RangeWindow;
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator, utils::TimeoutContext};
use top_k::{ChildReduction, TopKHeap, TopKIterator, TopKMode, reduce_child};

use crate::{
    DocValidity, NumericScoreSource, NumericTopKIterator, cmp_for,
    range_iterator::NumericTreeRef,
    source::{DEFAULT_RANGE_BATCH_SIZE, estimate_limit},
};

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
/// estimate it sizes the initial retry window. `validity`, `expiration` and
/// `timeout` are the source's [`DocValidity`], [`ExpirationChecker`] and
/// [`TimeoutContext`]. An empty child or `k == 0` returns [`None`]; callers raise
/// a zero `LIMIT` to the default first, as C's `QOptimizer_Parse` does.
///
/// # Errors
///
/// Returns [`HeapReserveError`] when the heap for `k` results cannot be
/// reserved; C's `NewOptimizerIterator` likewise rejects such a `LIMIT`.
#[expect(clippy::too_many_arguments, reason = "one parameter per query input")]
pub fn new_numeric_top_k<'index, V, E, T>(
    tree: impl Into<NumericTreeRef<'index>>,
    filter: NumericFilter,
    ascending: bool,
    k: usize,
    num_docs: usize,
    validity: V,
    expiration: E,
    timeout: T,
    child: Option<CRQEIterator>,
) -> Result<Option<NumericTopKIterator<'index, V, E, T>>, HeapReserveError>
where
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
    T: TimeoutContext + 'index,
{
    let Some(k) = NonZeroUsize::new(k) else {
        return Ok(None);
    };
    let heap = match TopKHeap::try_new(k, cmp_for(ascending)) {
        Ok(heap) => heap,
        Err(error) => return Err(HeapReserveError { error, child }),
    };

    let (source, child) = match reduce_child(child) {
        ChildReduction::Empty => return Ok(None),
        ChildReduction::Unfiltered => {
            let mut source = NumericScoreSource::unfiltered(tree, filter, ascending);
            source.narrow_window_to_top_k(k.get());
            (source, None)
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
            let source = NumericScoreSource::filtered(
                tree,
                filter,
                window,
                ascending,
                DEFAULT_RANGE_BATCH_SIZE,
                num_docs,
                child_estimate,
            );
            (source, Some(child))
        }
    };
    Ok(Some(TopKIterator::from_heap(
        source
            .with_validity(validity)
            .with_expiration(expiration)
            .with_timeout(timeout),
        child,
        heap,
        TopKMode::Batches,
    )))
}
