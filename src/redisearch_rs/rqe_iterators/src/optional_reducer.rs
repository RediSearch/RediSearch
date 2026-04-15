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
    wildcard::{WildcardIterator, new_wildcard_iterator, new_wildcard_iterator_optimized},
};

/// The outcome of [`new_optional_iterator`].
pub enum NewOptionalIterator<'index, I: RQEIterator<'index> + 'index> {
    /// Shortcircuit 1: child was structurally empty ([`crate::Empty`] or `EMPTY_ITERATOR`) — a wildcard is returned instead.
    ///
    /// All results will be virtual hits.
    WildcardFallback(Box<dyn WildcardIterator<'index> + 'index>),

    /// Shortcircuit 2: child was already a wildcard — it is returned as-is,
    /// with `weight` already applied to its current result.
    ///
    /// All results will be virtual hits.
    WildcardPassthrough(I),

    /// Regular case, non-optimized index: wrap child in a plain [`Optional`].
    Optional(Optional<'index, I>),

    /// Regular case, optimized index (`spec.rule.index_all` set): wrap child in an [`OptionalOptimized`].
    OptionalOptimized(OptionalOptimized<'index, Box<dyn WildcardIterator<'index> + 'index>, I>),
}

/// Create an optional iterator over `child`, applying shortcircuit reductions
/// where possible.
///
/// # Safety
///
/// 1. `query` must be a valid non-null pointer to a [`ffi::QueryEvalCtx`].
/// 2. `query.sctx` is a non-null pointer to a valid [`ffi::RedisSearchCtx`].
/// 3. `query.sctx.spec` is a non-null pointer to a valid [`ffi::IndexSpec`].
/// 4. `query.sctx.spec.rule`, when non-null, points to a valid [`ffi::SchemaRule`].
/// 5. `query.docTable` is a non-null pointer to a valid [`ffi::DocTable`].
/// 6. All preconditions of [`new_wildcard_iterator`] are satisfied.
pub unsafe fn new_optional_iterator<'index, I>(
    mut child: I,
    weight: f64,
    query: NonNull<ffi::QueryEvalCtx>,
    max_doc_id: t_docId,
) -> NewOptionalIterator<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    match child.type_() {
        // Shortcircuit 1: child is structurally empty — drop it and return a wildcard.
        IteratorType::Empty => {
            drop(child);
            // SAFETY: 6.
            let wc = unsafe { new_wildcard_iterator(query, 0.0) };
            NewOptionalIterator::WildcardFallback(wc)
        }
        // Shortcircuit 2: child is already a wildcard — apply weight and pass through.
        IteratorType::Wildcard | IteratorType::InvIdxWildcard => {
            if let Some(current) = child.current() {
                current.weight = weight;
            }
            NewOptionalIterator::WildcardPassthrough(child)
        }
        // Regular case: inspect the query context to pick the right variant.
        _ => {
            // SAFETY: 1, 2.
            let query_ref = unsafe { query.as_ref() };
            // SAFETY: 2.
            let sctx = unsafe { &*query_ref.sctx };
            // SAFETY: 3.
            let spec = unsafe { &*sctx.spec };
            let index_all = NonNull::new(spec.rule)
                // SAFETY: 4.
                .map(|r| unsafe { r.as_ref() }.index_all)
                .unwrap_or(false);

            if index_all {
                debug_assert!(
                    spec.diskSpec.is_null(),
                    "diskSpec should be null when index_all is true"
                );
                // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
                let sctx = NonNull::new(query_ref.sctx).expect("query.sctx is null");
                // SAFETY: 6.
                let wcii = unsafe { new_wildcard_iterator_optimized(sctx, 0.0) };
                NewOptionalIterator::OptionalOptimized(OptionalOptimized::new(
                    wcii, child, max_doc_id, weight,
                ))
            } else {
                NewOptionalIterator::Optional(Optional::new(max_doc_id, weight, child))
            }
        }
    }
}
