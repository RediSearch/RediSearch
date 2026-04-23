/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reducer logic for creating the right union iterator variant,
//! with short-circuit reductions applied before construction.

use crate::{Empty, IteratorType, RQEIterator, union_flat::UnionFlat, union_heap::UnionHeap};

/// The result of [`union_iterator_reducer`].
enum UnionReduction<I> {
    /// All children were empty — return an [`Empty`] iterator.
    ReducedEmpty(Empty),
    /// A single child remains (either after filtering or wildcard short-circuit).
    ReducedSingle(I),
    /// No reduction was possible. The children are returned unchanged.
    NotReduced(Vec<I>),
}

/// Attempt to reduce a list of child iterators before constructing a union.
///
/// Applies the following reduction rules:
/// 1. Remove all empty iterators.
/// 2. If `quick_exit` is true and any child is a wildcard, return it
///    and drop the rest.
/// 3. If only one child remains, return it directly.
/// 4. If no children remain, return an [`Empty`] iterator.
fn union_iterator_reducer<'index, I>(children: Vec<I>, quick_exit: bool) -> UnionReduction<I>
where
    I: RQEIterator<'index> + 'index,
{
    // Rule 1: Remove all empty iterators.
    let mut children: Vec<I> = children
        .into_iter()
        .filter(|c| c.type_() != IteratorType::Empty)
        .collect();

    // Rule 2: In quick exit mode, if any child is a wildcard, return it.
    if quick_exit
        && let Some(pos) = children.iter().position(|c| {
            matches!(
                c.type_(),
                IteratorType::Wildcard | IteratorType::InvIdxWildcard
            )
        })
    {
        let wildcard = children.swap_remove(pos);
        drop(children);
        return UnionReduction::ReducedSingle(wildcard);
    }

    match children.len() {
        // - No children left: the union has no results, so return an empty iterator.
        0 => UnionReduction::ReducedEmpty(Empty),
        // Exactly one child: a union of a single child is just that child — unwrap it.
        1 => {
            let child = children.into_iter().next().unwrap();
            UnionReduction::ReducedSingle(child)
        }
        _ => UnionReduction::NotReduced(children),
    }
}

/// The result of [`new_union_iterator`].
pub enum NewUnionIterator<'index, I> {
    /// All children were empty — return an [`Empty`] iterator.
    ReducedEmpty(Empty),
    /// A single child remains (either after filtering or wildcard short-circuit).
    ReducedSingle(I),
    /// Flat array variant.
    /// heap threshold.
    Flat(UnionFlat<'index, I, false>),
    /// Flat array variant with quick exit.
    FlatQuick(UnionFlat<'index, I, true>),
    /// Heap variant.
    Heap(UnionHeap<'index, I, false>),
    /// Heap variant with quick exit.
    HeapQuick(UnionHeap<'index, I, true>),
}

/// Construct a union iterator, choosing between flat and heap variants based
/// on the caller-supplied `use_heap` flag.
///
/// If the children are trivially reducible (all empty, single child, or
/// wildcard in quick-exit mode), the reducer is applied first and a simplified
/// result is returned.
pub fn new_union_iterator<'index, I>(
    children: Vec<I>,
    quick_exit: bool,
    use_heap: bool,
) -> NewUnionIterator<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    let children = match union_iterator_reducer(children, quick_exit) {
        UnionReduction::ReducedEmpty(empty) => return NewUnionIterator::ReducedEmpty(empty),
        UnionReduction::ReducedSingle(child) => return NewUnionIterator::ReducedSingle(child),
        UnionReduction::NotReduced(children) => children,
    };

    match (use_heap, quick_exit) {
        (true, true) => NewUnionIterator::HeapQuick(UnionHeap::new(children)),
        (true, false) => NewUnionIterator::Heap(UnionHeap::new(children)),
        (false, true) => NewUnionIterator::FlatQuick(UnionFlat::new(children)),
        (false, false) => NewUnionIterator::Flat(UnionFlat::new(children)),
    }
}
