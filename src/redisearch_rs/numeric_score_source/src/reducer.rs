/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Child short-circuit reduction and construction dispatch for numeric top-k,
//! keeping this decision logic out of the FFI boundary.

use std::num::NonZeroUsize;

use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{ExpirationChecker, RQEIterator, c2rust::CRQEIterator, utils::TimeoutContext};

use crate::{
    DocValidity, NumericScoreSource, NumericTopKIterator, new_numeric_top_k_filtered,
    new_numeric_top_k_unfiltered,
    source::{DEFAULT_RANGE_BATCH_SIZE, estimate_limit},
};

/// Outcome of reducing a filter child before constructing the iterator.
enum NumericChildReduction<C> {
    /// The child can never yield a match, so the whole query is empty.
    Empty,
    /// There is no filter (no child, or a wildcard that matches everything):
    /// scan the numeric range directly with no additional filter.
    Unfiltered,
    /// A real filter child to intersect the value-ordered batches against.
    Filtered(C),
}

/// Classify a filter child into the reduction that applies to it.
///
/// A reduced-away child is dropped, freeing the underlying iterator.
fn numeric_child_reducer<'index, C>(child: Option<C>) -> NumericChildReduction<C>
where
    C: RQEIterator<'index> + 'index,
{
    let Some(child) = child else {
        return NumericChildReduction::Unfiltered;
    };
    match child.type_() {
        IteratorType::Empty => NumericChildReduction::Empty,
        IteratorType::Wildcard | IteratorType::InvIdxWildcard => NumericChildReduction::Unfiltered,
        _ => NumericChildReduction::Filtered(child),
    }
}

/// Result of [`new_numeric_top_k`]: the iterator to expose, already reduced to
/// the right variant. `Unfiltered` and `Filtered` share the same concrete
/// iterator type; the distinction drives whether the FFI registers the child
/// profile subtree.
pub enum NewNumericTopK<'index, V: DocValidity, E: ExpirationChecker, T: TimeoutContext> {
    /// The query yields nothing (empty child, or `k == 0`).
    ReducedEmpty,
    /// `PARTIAL_RANGE`: scan the value-ordered range with no filter child.
    Unfiltered(NumericTopKIterator<'index, V, E, T>),
    /// `HYBRID`: value-ordered range intersected with a filter child, with the
    /// expand-and-retry window path enabled.
    Filtered(NumericTopKIterator<'index, V, E, T>),
}

/// Build the numeric top-k iterator for the given query, applying the child
/// reduction and choosing the unfiltered or filtered construction accordingly.
///
/// `num_docs` is the total document count; the filter child's selectivity
/// estimate sizes the initial retry window. `validity`, `expiration` and
/// `timeout` are the per-document validity oracle, field-TTL checker and
/// query-deadline poll carried by the source. A `k` of `0` reduces to an empty
/// query.
#[expect(clippy::too_many_arguments)]
pub fn new_numeric_top_k<'index, V, E, T>(
    tree: &'index NumericRangeTree,
    mut filter: NumericFilter,
    ascending: bool,
    k: usize,
    num_docs: usize,
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

    match numeric_child_reducer(child) {
        NumericChildReduction::Empty => NewNumericTopK::ReducedEmpty,
        NumericChildReduction::Unfiltered => {
            let source = NumericScoreSource::unfiltered(tree, filter, ascending)
                .with_validity(validity)
                .with_expiration(expiration)
                .with_timeout(timeout);
            NewNumericTopK::Unfiltered(new_numeric_top_k_unfiltered(source, k))
        }
        NumericChildReduction::Filtered(child) => {
            let child_estimate = child.num_estimated();
            // Size the initial window from the child's selectivity, so the range
            // is wide enough to likely hold `k` matching docs; the source widens
            // it on retry when the child proves more selective than estimated.
            filter.limit = estimate_limit(num_docs, child_estimate, k.get());
            let source = NumericScoreSource::filtered(
                tree,
                filter,
                ascending,
                DEFAULT_RANGE_BATCH_SIZE,
                num_docs,
                child_estimate,
            )
            .with_validity(validity)
            .with_expiration(expiration)
            .with_timeout(timeout);
            NewNumericTopK::Filtered(new_numeric_top_k_filtered(source, child, k))
        }
    }
}

#[cfg(test)]
mod tests {
    use rqe_iterators::{Empty, IdList, Wildcard};

    use super::{NumericChildReduction, numeric_child_reducer};

    #[test]
    fn no_child_is_unfiltered() {
        let reduction = numeric_child_reducer(None::<Empty>);
        assert!(matches!(reduction, NumericChildReduction::Unfiltered));
    }

    #[test]
    fn empty_child_reduces_to_empty() {
        let reduction = numeric_child_reducer(Some(Empty));
        assert!(matches!(reduction, NumericChildReduction::Empty));
    }

    #[test]
    fn wildcard_child_is_unfiltered() {
        let reduction = numeric_child_reducer(Some(Wildcard::new(10, 1.0)));
        assert!(matches!(reduction, NumericChildReduction::Unfiltered));
    }

    #[test]
    fn real_child_is_kept_as_filter() {
        let child = IdList::<true>::new(vec![1, 2, 3]);
        let reduction = numeric_child_reducer(Some(child));
        assert!(matches!(reduction, NumericChildReduction::Filtered(_)));
    }
}
