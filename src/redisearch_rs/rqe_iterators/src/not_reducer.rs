/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reducer logic for creating the right NOT iterator variant,
//! with short-circuit reductions applied before construction.

use std::{ptr::NonNull, time::Duration};

use ffi::t_docId;

use crate::{
    Empty, IteratorType, NewWildcardIterator, RQEIterator,
    not::Not,
    not_optimized::NotOptimized,
    wildcard::{
        new_wildcard_iterator, new_wildcard_iterator_on_disk, new_wildcard_iterator_optimized,
    },
};

/// The result of [`not_iterator_reducer`].
///
/// The `ReducedWildcard` variant is intentionally large (contains an inline
/// [`NewWildcardIterator`]) to avoid an extra heap allocation on a per-query
/// construction path. This enum is short-lived and never stored.
#[expect(
    clippy::large_enum_variant,
    reason = "short-lived reducer result; boxing would add a needless allocation"
)]
enum NotReduction<'index, I> {
    /// The child is empty → NOT matches everything → wildcard.
    ReducedWildcard(NewWildcardIterator<'index>),
    /// The child is a wildcard → NOT matches nothing → empty.
    ReducedEmpty(Empty),
    /// No reduction was possible. The child is returned unchanged.
    NotReduced(I),
}

/// Attempt to reduce a NOT iterator into a simpler form.
///
/// Applies the following reduction rules:
/// 1. If the child is empty, the NOT matches everything — return a wildcard.
/// 2. If the child is a wildcard, the NOT matches nothing — return empty.
///
/// # Safety
///
/// When the child is empty (rule 1), this function calls
/// [`new_wildcard_iterator`] — all its safety preconditions on `query` must
/// hold.
unsafe fn not_iterator_reducer<'index, I>(
    child: I,
    weight: f64,
    query: NonNull<ffi::QueryEvalCtx>,
) -> NotReduction<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    match child.type_() {
        IteratorType::Empty => {
            // Rule 1: child is empty → NOT matches everything → wildcard.
            drop(child);
            // SAFETY: Caller guarantees the preconditions of `new_wildcard_iterator`.
            let mut wc = unsafe { new_wildcard_iterator(query, weight) };
            if let Some(result) = wc.current() {
                // Documents returned by this wildcard are included purely
                // by negation (NOT nothing = everything), not because any
                // term matched, so the term frequency is zero.
                result.freq = 0;
            }
            NotReduction::ReducedWildcard(wc)
        }
        IteratorType::Wildcard | IteratorType::InvIdxWildcard => {
            // Rule 2: child is wildcard → NOT matches nothing → empty.
            drop(child);
            NotReduction::ReducedEmpty(Empty)
        }
        // No reduction applicable.
        _ => NotReduction::NotReduced(child),
    }
}

/// The result of [`new_not_iterator`].
pub enum NewNotIterator<'index, I> {
    /// The child is empty → NOT matches everything → wildcard.
    ReducedWildcard(NewWildcardIterator<'index>),
    /// The child is a wildcard → NOT matches nothing → empty.
    ReducedEmpty(Empty),
    /// Non-optimized path: sequential NOT iterator.
    Not(Not<'index, I>),
    /// Optimized path (`index_all` or disk index): wildcard-backed NOT iterator.
    NotOptimized(NotOptimized<'index, NewWildcardIterator<'index>, I>),
}

/// Construct a NOT iterator, choosing between [`Not`] (sequential) and
/// [`NotOptimized`] (wildcard-backed) based on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), the reducer is
/// applied first and a simplified iterator is returned directly as
/// [`NewNotIterator::ReducedWildcard`] or [`NewNotIterator::ReducedEmpty`].
///
/// # Safety
///
/// 1. `query` must point to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx)
///    that remains valid for `'index`.
/// 2. `query.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for `'index`.
/// 3. `query.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec) that remains valid for `'index`.
/// 4. `query.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 5. All preconditions of [`new_wildcard_iterator`] must hold (it may be
///    called both on the optimized path and by the reducer when the child is
///    empty).
pub unsafe fn new_not_iterator<'index, I>(
    child: I,
    max_doc_id: t_docId,
    weight: f64,
    timeout: Duration,
    skip_timeout_checks: bool,
    query: NonNull<ffi::QueryEvalCtx>,
) -> NewNotIterator<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    // SAFETY: Caller guarantees the preconditions for `new_wildcard_iterator`
    // (used by the reducer when the child is empty).
    let child = match unsafe { not_iterator_reducer(child, weight, query) } {
        NotReduction::ReducedWildcard(wc) => {
            return NewNotIterator::ReducedWildcard(wc);
        }
        NotReduction::ReducedEmpty(empty) => {
            return NewNotIterator::ReducedEmpty(empty);
        }
        NotReduction::NotReduced(child) => child,
    };

    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query_ref = unsafe { query.as_ref() };
    let sctx = NonNull::new(query_ref.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    let index_all = NonNull::new(spec.rule)
        .map(|rule| {
            // SAFETY: Caller guarantees `spec.rule`, when non-null, points to
            // a valid `SchemaRule` (4).
            unsafe { rule.as_ref() }.index_all
        })
        .unwrap_or(false);
    let disk_index_available = !spec.diskSpec.is_null();
    let optimized = index_all || disk_index_available;

    if optimized {
        let wcii = if disk_index_available {
            // SAFETY: Caller guarantees `spec.diskSpec` is valid, non-null and
            // remains valid for `'index` (5).
            let disk_spec = unsafe { &mut *spec.diskSpec };
            // SAFETY: Caller guarantees all preconditions of `new_wildcard_iterator_on_disk` hold (5).
            unsafe { new_wildcard_iterator_on_disk(disk_spec, weight) }
        } else {
            // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2)
            // and all preconditions of `new_wildcard_iterator_optimized` hold (5).
            unsafe { new_wildcard_iterator_optimized(sctx, weight) }
        };
        NewNotIterator::NotOptimized(NotOptimized::new(
            wcii,
            child,
            max_doc_id,
            weight,
            if skip_timeout_checks {
                None
            } else {
                Some(timeout)
            },
        ))
    } else {
        NewNotIterator::Not(Not::new(
            child,
            max_doc_id,
            weight,
            timeout,
            skip_timeout_checks,
        ))
    }
}
