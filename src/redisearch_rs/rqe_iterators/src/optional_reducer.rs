/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reducer logic for creating the right optional iterator variant,
//! with shortcircuit reductions applied before construction.

use std::ptr::NonNull;

use ffi::{IteratorType, t_docId};

use crate::{
    RQEIterator,
    optional::Optional,
    optional_optimized::OptionalOptimized,
    wildcard::{WildcardIterator, new_wildcard_iterator},
};

/// The outcome of [`new_optional_iterator`].
pub enum OptionalReduction<'index, I: RQEIterator<'index> + 'index> {
    /// Shortcircuit 1: child was structurally empty ([`crate::Empty`] or `EMPTY_ITERATOR`) — a wildcard is returned instead.
    ///
    /// All results will be virtual hits.
    WildcardFallback(Box<dyn WildcardIterator<'index> + 'index>),

    /// Shortcircuit 2: child was already a wildcard — it is returned as-is,
    /// with `weight` already applied to its current result.
    ///
    /// All results will be real hits.
    WildcardPassthrough(I),

    /// Regular case, non-optimized index: wrap child in a plain [`Optional`].
    Plain(Optional<'index, I>),

    /// Regular case, optimized index (`spec.diskSpec` non-null or
    /// `spec.rule.index_all` set): wrap child in an [`OptionalOptimized`].
    Optimized(OptionalOptimized<'index, Box<dyn WildcardIterator<'index> + 'index>, I>),
}

/// Create an optional iterator over `child`, applying shortcircuit reductions
/// where possible.
///
/// The caller is responsible for intercepting null and structurally-empty children
/// (null C pointers, `EMPTY_ITERATOR`-typed C iterators, or [`crate::Empty`] Rust
/// iterators) **before** calling this function, so that `child.is_empty()`
/// reliably signals the empty case.
///
/// # Safety
///
/// 1. `query` must be a valid non-null pointer to a [`ffi::QueryEvalCtx`] such that:
///    - `query.sctx` is a non-null pointer to a valid [`ffi::RedisSearchCtx`].
///    - `query.sctx.spec` is a non-null pointer to a valid [`ffi::IndexSpec`].
///    - `query.sctx.spec.rule`, when non-null, points to a valid [`ffi::SchemaRule`].
///    - `query.docTable` is a non-null pointer to a valid [`ffi::DocTable`].
///    - All preconditions of [`new_wildcard_iterator`] are satisfied.
pub unsafe fn new_optional_iterator<'index, I>(
    mut child: I,
    weight: f64,
    query: NonNull<ffi::QueryEvalCtx>,
    max_doc_id: t_docId,
) -> OptionalReduction<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    if child.type_() == IteratorType::Empty {
        // Shortcircuit 1: child is structurally empty — drop it and return a wildcard.
        drop(child);
        // SAFETY: caller guarantees all preconditions of `new_wildcard_iterator` (1).
        let wc = unsafe { new_wildcard_iterator(query, 0.0) };
        OptionalReduction::WildcardFallback(wc)
    } else if matches!(
        child.type_(),
        IteratorType::Wildcard | IteratorType::InvIdxWildcard
    ) {
        // Shortcircuit 2: child is already a wildcard — apply weight and pass through.
        if let Some(current) = child.current() {
            current.weight = weight;
        }
        OptionalReduction::WildcardPassthrough(child)
    } else {
        // Regular case: inspect the query context to pick the right variant.
        // SAFETY: 1.
        let query_ref = unsafe { query.as_ref() };
        // SAFETY: 1.
        let sctx = unsafe { &*query_ref.sctx };
        // SAFETY: 1.
        let spec = unsafe { &*sctx.spec };
        let optimized = NonNull::new(spec.rule)
            // SAFETY: `spec.rule` is non-null (guaranteed by `NonNull::new` above) and
            // points to a valid `SchemaRule` (1).
            .map(|r| unsafe { r.as_ref() }.index_all)
            .unwrap_or(false);

        if optimized {
            // SAFETY: 1.
            let wcii = unsafe { new_wildcard_iterator(query, 0.0) };
            OptionalReduction::Optimized(OptionalOptimized::new(wcii, child, max_doc_id, weight))
        } else {
            OptionalReduction::Plain(Optional::new(max_doc_id, weight, child))
        }
    }
}
