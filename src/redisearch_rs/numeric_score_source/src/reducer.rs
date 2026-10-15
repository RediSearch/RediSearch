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

use std::num::NonZeroUsize;

use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRangeTree, RangeWindow};
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator, utils::TimeoutContext};
use top_k::{ChildReduction, reduce_child};

use crate::{
    DocValidity, NumericOptimizerMode, NumericScoreSource, NumericTopKIterator,
    new_numeric_top_k_filtered, new_numeric_top_k_unfiltered,
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
#[expect(clippy::too_many_arguments, reason = "one parameter per query input")]
pub fn new_numeric_top_k<'index, V, E, T>(
    tree: &'index NumericRangeTree,
    filter: NumericFilter,
    ascending: bool,
    k: usize,
    num_docs: usize,
    mode: NumericOptimizerMode,
    validity: V,
    expiration: E,
    timeout: T,
    child: Option<CRQEIterator>,
) -> NewNumericTopK<'index, V, E, T>
where
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
    T: TimeoutContext + 'index,
{
    let Some(k) = NonZeroUsize::new(k) else {
        return NewNumericTopK::ReducedEmpty;
    };

    match reduce_child(child) {
        ChildReduction::Empty => NewNumericTopK::ReducedEmpty,
        ChildReduction::Unfiltered => {
            let source = NumericScoreSource::unfiltered(tree, filter, ascending)
                .with_optimizer_mode(mode)
                .with_validity(validity)
                .with_expiration(expiration)
                .with_timeout(timeout);
            NewNumericTopK::Unfiltered(new_numeric_top_k_unfiltered(source, k))
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
            )
            .with_optimizer_mode(mode)
            .with_validity(validity)
            .with_expiration(expiration)
            .with_timeout(timeout);
            NewNumericTopK::Filtered(new_numeric_top_k_filtered(source, child, k))
        }
    }
}
