/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust implementation of the numeric top-k (optimizer) iterator and its C
//! accessor functions.
//!
//! This module provides [`NewNumericTopKIterator`].

use std::{cmp::Ordering, ffi::c_void, ptr::NonNull, sync::Arc};

use document_metadata::OwnedDocumentMetadata;
use ffi::{
    DocTable, DocTable_Borrow, DocTable_ForEachLiveIdUntil, DocTable_IsVisible, QueryIterator,
    RedisSearchCtx,
};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_score_source::{
    DocValidity, NumericOptimizerMode, NumericOrder, NumericRangeBound, NumericTopKIterator,
    TieBreakComparator, new_numeric_top_k_with_order, range_iterator::NumericTreeRef,
};
use rqe_core::DocId;
use rqe_iterators::{
    FieldExpirationChecker, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    utils::AnyTimeoutContext,
};
use sorting_vector::RSSortingVector;
use top_k::ScoreOrdering;
use value::Value;

use crate::{TIMEOUT_CHECK_GRANULARITY, without_skip_to};

/// A numeric interval with open or closed endpoints, passed from C's
/// `QOptimizer` for each pure numeric branch lifted out of a keyset union.
#[repr(C)]
#[cheadergen::config(export)]
#[derive(Clone, Copy)]
pub struct NumericTopKRangeBound {
    pub min: f64,
    pub max: f64,
    pub min_inclusive: bool,
    pub max_inclusive: bool,
}

/// Multi-key sorting and missing-value parameters passed from C's `QOptimizer`
/// to [`NewNumericTopKIterator`].
#[repr(C)]
#[cheadergen::config(export)]
pub struct NumericTopKSortSpec {
    /// Bitmask of sort directions (bit 0 = primary numeric key, bit `i + 1` =
    /// tie-break key `i`; set = ASC, clear = DESC), matching `PLN_ArrangeStep::sortAscMap`.
    pub sort_asc_map: u64,
    /// Sorting-vector slot indices for the `num_tie_break_keys` keys following
    /// the primary numeric key. May be NULL when `num_tie_break_keys == 0`.
    pub tie_break_sort_idx: *const i16,
    /// Number of tie-break keys in `tie_break_sort_idx`.
    pub num_tie_break_keys: usize,
    /// When `true` (unbounded whole-field walk), fill remaining heap slots with
    /// matching documents that lack a value for the primary numeric sort field.
    pub collect_missing: bool,
    /// Pure numeric range branches lifted out of a keyset/lexicographic union.
    /// May be NULL when `num_unconditional_ranges == 0`.
    pub unconditional_ranges: *const NumericTopKRangeBound,
    /// Number of lifted pure numeric ranges in `unconditional_ranges`.
    pub num_unconditional_ranges: usize,
    /// Owning iterator for the remaining conditional branches of a lifted
    /// keyset union (e.g. `@n:[c c] @m:[(k +inf]`). Adopted unconditionally
    /// (freed on drop even if heap reservation fails). May be NULL.
    pub boundary_iter: *mut QueryIterator,
}

/// Breaks ties on the primary numeric key by reading secondary `SORTABLE` keys
/// from each document's [`RSSortingVector`] in the index's [`DocTable`].
#[derive(Clone)]
struct DocTableTieBreaker {
    docs: *const DocTable,
    sort_asc_map: u64,
    tie_break_keys: Option<Arc<[i16]>>,
}

// SAFETY: `docs` points to the index's `DocTable`, which is accessed only under
// the index spec read lock during collection (`RawTopK` invariant 3 keeps the
// heap empty outside collection).
unsafe impl Send for DocTableTieBreaker {}

impl DocTableTieBreaker {
    fn sort_key_value(dmd: &OwnedDocumentMetadata, sort_idx: i16) -> Option<&Value> {
        let idx = usize::try_from(sort_idx).ok()?;
        // SAFETY: `ffi::RSSortingVector` and `sorting_vector::RSSortingVector`
        // have identical layout (the C type is generated from the Rust type).
        let sv = unsafe { &*(&raw const dmd.sortVector).cast::<RSSortingVector>() };
        let val = sv.get(idx)?;
        if matches!(&**val, Value::Null) {
            None
        } else {
            Some(&**val)
        }
    }
}

impl TieBreakComparator for DocTableTieBreaker {
    fn cmp_tie_break(&self, a: DocId, b: DocId) -> Ordering {
        let Some(ref keys) = self.tie_break_keys else {
            return Ordering::Equal;
        };
        // SAFETY: `self.docs` is valid for the lifetime of the iterator and
        // `cmp_tie_break` is only called during collection under the read lock.
        let dmd_a = NonNull::new(unsafe { DocTable_Borrow(self.docs, a) }.cast_mut())
            .map(|p| unsafe { OwnedDocumentMetadata::from_raw(p) });
        // SAFETY: as above.
        let dmd_b = NonNull::new(unsafe { DocTable_Borrow(self.docs, b) }.cast_mut())
            .map(|p| unsafe { OwnedDocumentMetadata::from_raw(p) });

        for (i, &sort_idx) in keys.iter().enumerate() {
            let val_a = dmd_a
                .as_ref()
                .and_then(|d| Self::sort_key_value(d, sort_idx));
            let val_b = dmd_b
                .as_ref()
                .and_then(|d| Self::sort_key_value(d, sort_idx));
            match (val_a, val_b) {
                (None, None) => continue,
                (Some(_), None) => return Ordering::Less,
                (None, Some(_)) => return Ordering::Greater,
                (Some(va), Some(vb)) => {
                    let ord = value::comparison::compare_with_query_error(va, vb, None);
                    if ord != Ordering::Equal {
                        let ascending = (self.sort_asc_map & (1u64 << (i + 1))) != 0;
                        return if ascending { ord } else { ord.reverse() };
                    }
                }
            }
        }
        Ordering::Equal
    }
}

/// Construct a numeric top-k iterator and expose it as a C [`QueryIterator`].
///
/// This call can reduce to an `Empty` iterator (`k == 0`, or a child that can
/// never match). It returns `NULL` when `k` is too large to allocate the top-k
/// heap for, leaving `child` owned by the caller.
///
/// Pass `child = NULL` for a plain `SORTBY numeric` range scan; pass a valid
/// owning child iterator for a filtered query, whose selectivity sizes the
/// initial value-window and drives the expand-and-retry path.
///
/// `filter` is read once to copy the numeric range parameters, including the
/// sort direction; it is not retained after this call. `mode` is the
/// [`NumericOptimizerMode`] the query plan chose.
///
/// Documents the doc table no longer shows (deleted, or hidden by a pending slot
/// deletion) are dropped before they can take a top-k slot.
///
/// # Safety
///
/// 1. `tree` is non-null and satisfies [`NumericTreeRef::from_raw`]'s safety
///    requirements for the returned iterator's lifetime, including owner-derived
///    provenance if the tree may be mutated.
/// 2. `filter` is non-null and [valid] for a [`NumericFilter`] for the duration
///    of this call.
/// 3. `child`, when non-null, is a [valid], owning `QueryIterator *` with every
///    callback populated.
/// 4. `sctx` is non-null and [valid] for a [`RedisSearchCtx`] with a [valid]
///    `spec`, both outliving the returned iterator at a stable address. The request
///    timeout reached through `sctx.limits` must also remain valid at a stable address
///    until the iterator is dropped. Timeout source changes and deadline writes may
///    happen only between probes; only the blocked-client flag may change concurrently.
/// 5. `filter_ctx` is non-null and [valid] for a [`FieldFilterContext`] for the
///    duration of this call.
///
/// 6. `sort_spec`, when non-null, must point to a [valid] [`NumericTopKSortSpec`]
///    for the duration of this call:
///    - when `num_tie_break_keys > 0`, `tie_break_sort_idx` is non-null, aligned,
///      and valid for reads of `num_tie_break_keys` initialized `i16` elements;
///    - when `num_unconditional_ranges > 0`, `unconditional_ranges` is non-null,
///      aligned, and valid for reads of `num_unconditional_ranges` initialized
///      [`NumericTopKRangeBound`] elements;
///    - `boundary_iter`, when non-null, is a [valid], uniquely-owned
///      `QueryIterator *` with every callback populated (same contract as `child`
///      in clause 3), whose ownership transfers to this constructor.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericTopKIterator(
    tree: *const NumericRangeTree,
    filter: *const NumericFilter,
    k: usize,
    num_docs: usize,
    mode: NumericOptimizerMode,
    child: *mut QueryIterator,
    sctx: *const RedisSearchCtx,
    filter_ctx: *const FieldFilterContext,
    sort_spec: *const NumericTopKSortSpec,
) -> *mut QueryIterator {
    let sctx_nn = NonNull::new(sctx.cast_mut()).expect("sctx must be non-null");
    debug_assert!(!filter_ctx.is_null(), "filter_ctx must be non-null");
    // SAFETY: guaranteed by 5. `FieldFilterContext` is `Copy`.
    let field_filter = unsafe { *filter_ctx };

    // The sort field is a single field index (never a mask), so the wide-schema
    // flag is irrelevant; pass empty reader flags.
    // SAFETY: guaranteed by 4.
    let expiration = unsafe { FieldExpirationChecker::new(sctx_nn, field_filter, 0) };
    // SAFETY: guaranteed by 4.
    let timeout = unsafe { AnyTimeoutContext::from_sctx(sctx_nn, TIMEOUT_CHECK_GRANULARITY) };

    // SAFETY: guaranteed by 4.
    let spec = unsafe { sctx_nn.as_ref() }.spec;
    debug_assert!(!spec.is_null(), "sctx.spec must be non-null");
    // SAFETY: guaranteed by 4.
    let docs = unsafe { &raw const (*spec).docs };
    // SAFETY: 4 keeps the inline doc table valid for the iterator's lifetime.
    let validity = move |doc_id| unsafe { DocTable_IsVisible(docs, doc_id) };

    let tree = NonNull::new(tree.cast_mut()).expect("tree must be non-null");
    // SAFETY: clause 1 supplies all of `from_raw`'s preconditions.
    let tree = unsafe { NumericTreeRef::from_raw(tree) };
    debug_assert!(!filter.is_null(), "filter must be non-null");
    // SAFETY: guaranteed by 2. `NumericFilter` is `Copy`.
    let filter = unsafe { *filter };

    // SAFETY: guaranteed by 6.
    let (sort_asc_map, tie_break_keys, collect_missing, union_filter) = if let Some(spec) =
        unsafe { sort_spec.as_ref() }
    {
        let keys = if spec.num_tie_break_keys > 0 {
            debug_assert!(!spec.tie_break_sort_idx.is_null());
            // SAFETY: guaranteed by 6.
            let slice = unsafe {
                std::slice::from_raw_parts(spec.tie_break_sort_idx, spec.num_tie_break_keys)
            };
            Some(Arc::<[i16]>::from(slice))
        } else {
            None
        };
        let union_filter = if spec.num_unconditional_ranges > 0 {
            debug_assert!(!spec.unconditional_ranges.is_null());
            // SAFETY: guaranteed by 6.
            let ranges = unsafe {
                std::slice::from_raw_parts(spec.unconditional_ranges, spec.num_unconditional_ranges)
            };
            let converted: Box<[NumericRangeBound]> = ranges
                .iter()
                .map(|r| NumericRangeBound {
                    min: r.min,
                    max: r.max,
                    min_inclusive: r.min_inclusive,
                    max_inclusive: r.max_inclusive,
                })
                .collect();
            // SAFETY: guaranteed by 6 (`boundary_iter`, when non-null, is a valid owning iterator).
            let boundary =
                NonNull::new(spec.boundary_iter).map(|b| unsafe { CRQEIterator::new(b) });
            Some((converted, boundary))
        } else {
            // Free any stray boundary_iter defensively if num_unconditional_ranges == 0.
            // SAFETY: guaranteed by 6 (`boundary_iter`, when non-null, is a valid owning iterator).
            let _ = NonNull::new(spec.boundary_iter).map(|b| unsafe { CRQEIterator::new(b) });
            None
        };
        (spec.sort_asc_map, keys, spec.collect_missing, union_filter)
    } else {
        (u64::from(filter.ascending), None, false, None)
    };

    let num_tie_break_keys = tie_break_keys.as_ref().map_or(0, |k| k.len());
    let last_ascending = (sort_asc_map & (1u64 << num_tie_break_keys)) != 0;
    let order = NumericOrder::with_tie_break(
        filter.ascending,
        last_ascending,
        DocTableTieBreaker {
            docs,
            sort_asc_map,
            tie_break_keys,
        },
    );
    let unscored_live_docs = collect_missing.then(|| {
        let f: numeric_score_source::UnscoredLiveDocsFn = Box::new(move |heap_count, visit| {
            // SAFETY: 4 keeps `docs` valid for the iterator's lifetime under the spec read lock.
            // `DocTable.size` is 1-based (`NewDocTable` starts it at 1), so live count is `size - 1`.
            let live_docs = unsafe { (*docs).size }.saturating_sub(1);
            if heap_count >= live_docs {
                return;
            }
            struct VisitCtx<'a> {
                visit: &'a mut dyn FnMut(DocId) -> bool,
            }
            unsafe extern "C" fn on_live_doc(ctx: *mut c_void, doc_id: DocId) -> bool {
                // SAFETY: `ctx` points to `VisitCtx` on the stack of `DocTable_ForEachLiveIdUntil`.
                let state = unsafe { &mut *ctx.cast::<VisitCtx<'_>>() };
                (state.visit)(doc_id)
            }
            let mut ctx = VisitCtx { visit };
            // SAFETY: `docs` is valid under the read lock; `on_live_doc` only touches `ctx`.
            unsafe {
                DocTable_ForEachLiveIdUntil(
                    docs,
                    Some(on_live_doc),
                    (&raw mut ctx).cast::<c_void>(),
                );
            }
        });
        f
    });

    // Adopt the filter child as an owning Rust iterator. A null child means a
    // plain range scan.
    // SAFETY: guaranteed by 3.
    let child = NonNull::new(child).map(|c| unsafe { CRQEIterator::new(c) });

    let reduced = new_numeric_top_k_with_order(
        tree,
        filter,
        k,
        num_docs,
        mode,
        order,
        unscored_live_docs,
        union_filter,
        validity,
        expiration,
        timeout,
        child,
    );
    match reduced {
        Ok(reduced) => box_reduced(reduced),
        Err(err) => {
            let _ = err.child.map(CRQEIterator::into_raw);
            std::ptr::null_mut()
        }
    }
}

/// Box a reduced numeric top-k into the C [`QueryIterator`] handle.
fn box_reduced<V: DocValidity, O: ScoreOrdering>(
    reduced: Option<
        NumericTopKIterator<'_, V, FieldExpirationChecker, AnyTimeoutContext, CRQEIterator, O>,
    >,
) -> *mut QueryIterator {
    let Some(it) = reduced else {
        return RQEIteratorWrapper::boxed_new(rqe_iterators::Empty).as_ptr();
    };
    let ptr = if it.child().is_some() {
        // `boxed_new_compound` registers the `ProfileChildren` callback so
        // `FT.PROFILE` recurses into and counts the filter subtree.
        RQEIteratorWrapper::boxed_new_compound(it)
    } else {
        RQEIteratorWrapper::boxed_new(it)
    };
    // SAFETY: `ptr` was just boxed above and has no other alias.
    unsafe { without_skip_to(ptr) }
}
