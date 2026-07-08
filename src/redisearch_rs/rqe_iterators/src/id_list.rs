/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`IdList`].

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;
use std::cmp::Ordering;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::OwnedSlice,
};

/// An iterator that yields results according to a sorted list of unique IDs, specified on construction.
pub type IdListSorted<'index> = IdList<'index, true>;
/// An iterator that yields results according to an IDs list, specified on construction,
/// which may or may not be sorted.
pub type IdListUnsorted<'index> = IdList<'index, false>;

/// An iterator that yields results according to an IDs list given on construction.
///
/// Parameterised over a [`Ref`] mode — see [`IdList`] for the [`Active`]
/// instantiation that implements [`RQEIterator`]. The struct owns its data
/// (the list of document IDs); the only `Rf`-dependent field is `result`.
#[repr(C)]
pub struct RawIdList<'query, Rf: Ref, const SORTED: bool> {
    /// The list of document IDs to iterate over.
    /// There must be no duplicates. The list must be sorted if `SORTED` is set to `true`.
    ids: OwnedSlice<DocId>,
    /// The current position of the iterator (a.k.a the next document ID to return by [`read`](RQEIterator::read)).
    /// When `offset` is equal to the length of `ids`, the iterator is at EOF.
    offset: usize,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    ///
    /// # Invariant
    ///
    /// `result`'s kind is either virtual or metric.
    result: RawIndexResult<'query, Rf>,
}

/// Alias for an [`Active`] [`RawIdList`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type IdList<'index, const SORTED: bool> = RawIdList<'index, Active<'index>, SORTED>;
/// Alias for a [`Suspended`] [`RawIdList`].
pub type SuspendedIdList<'query, const SORTED: bool> = RawIdList<'query, Suspended, SORTED>;

// Compile-time proof that the `IdList` and its suspended counterpart are layout-identical.
const _: () = {
    use std::mem::offset_of;

    const SORTED: bool = true;
    type A<'a> = IdList<'a, SORTED>;
    type S<'a> = SuspendedIdList<'a, SORTED>;

    // Every field starts at the same offset.
    assert!(offset_of!(A, ids) == offset_of!(S, ids));
    assert!(offset_of!(A, offset) == offset_of!(S, offset));
    assert!(offset_of!(A, result) == offset_of!(S, result));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, const SORTED: bool> IdList<'index, SORTED> {
    /// Creates a new ID list iterator.
    ///
    /// The list of document IDs cannot contain duplicates.
    /// If `SORTED` is set to `true`, the list must be sorted.
    #[inline(always)]
    pub fn new(ids: impl Into<OwnedSlice<DocId>>) -> Self {
        Self::with_result(ids, RSIndexResult::build_virt().build())
    }

    /// Get the current iterator offset—i.e. its position in the list of IDs.
    ///
    /// This is used by [`Metric`](crate::metric::Metric) to iterate over the corresponding list
    /// of metric data in lockstep.
    #[inline(always)]
    pub(super) const fn offset(&self) -> usize {
        self.offset
    }

    /// Same as [`IdList::new`] but with a custom [`RSIndexResult`],
    /// useful when wrapping this iterator and requiring a non-virtual result.
    pub fn with_result(ids: impl Into<OwnedSlice<DocId>>, result: RSIndexResult<'index>) -> Self {
        let kind = result.kind();
        if kind != RSResultKind::Virtual && kind != RSResultKind::Metric {
            panic!(
                "IdList iterators can only work with virtual and metric result kinds. {kind} is not supported.",
            );
        }

        let ids = ids.into();

        if SORTED {
            debug_assert!(
                ids.is_sorted_by(|a, b| a < b),
                "IDs must be sorted and unique"
            );
        }

        Self {
            ids,
            offset: 0,
            result,
        }
    }

    /// Replace the ID list, resetting the iterator to the start.
    ///
    /// Used by the lazy variants ([`IdListLazy`](crate::id_list_lazy::IdListLazy),
    /// [`MetricLazy`](crate::metric_lazy::MetricLazy)) to populate an initially-empty
    /// iterator once the deferred producer has run.
    pub(crate) fn set_ids(&mut self, ids: OwnedSlice<DocId>) {
        if SORTED {
            debug_assert!(
                ids.is_sorted_by(|a, b| a < b),
                "IDs must be sorted and unique"
            );
        }
        self.ids = ids;
        self.offset = 0;
    }
}

impl<'query, Rf: Ref, const SORTED: bool> RawIdList<'query, Rf, SORTED> {
    #[inline(always)]
    pub(super) fn _num_estimated(&self) -> usize {
        self.ids.len()
    }
}

impl<'index, const SORTED: bool> IdList<'index, SORTED> {
    #[inline(always)]
    fn get_current(&self) -> Option<DocId> {
        self.ids.get(self.offset).copied()
    }

    // this function is needed by the metric iterator to get the offset,
    // because the metric iterator borrows the iterator as mutable for read(), and the offset is changed by read().
    // This is because the IndexResult is reused.
    pub(super) fn read_and_get_offset(
        &mut self,
    ) -> Result<Option<(&mut RSIndexResult<'index>, usize)>, RQEIteratorError> {
        let Some(doc_id) = self.get_current() else {
            return Ok(None);
        };
        self.offset += 1;

        self.result.doc_id = doc_id;

        Ok(Some((&mut self.result, self.offset)))
    }

    /// Advance the iterator to the given ID, or to the first ID greater
    /// than the given ID.
    ///
    /// Returns `Some(true)` if there is a document with the given ID in the list.
    /// Returns `Some(false)` if there is no document with the given ID in the list.
    /// Returns `None` if the iterator has been advanced past the end of the ID list.
    pub(super) fn _skip_to(&mut self, target_id: DocId) -> Option<bool> {
        if !SORTED {
            panic!("Can't skip when working with unsorted document ids");
        }

        let len = self.ids.len();
        if self.at_eof() ||
            // No risk in unwrapping here since we are not at eof and
            // the list cannot be empty
            *self.ids.last().unwrap() < target_id
        {
            // The iterator has been advanced past the end of the ID list.
            self.offset = len;
            // Update result.doc_id to the last element in the list
            if len > 0 {
                self.result.doc_id = self.ids[len - 1];
            }
            return None;
        }

        debug_assert!(
            self.last_doc_id() < target_id,
            "We're trying to skip backwards!"
        );

        // Since the document ids are sorted, we can perform a binary search to find
        // the closest entry to the target document ID.
        let mut bottom = self.offset;
        // Since the document ids are also **unique**, we can restrict the
        // search space even further!
        // The difference between two consecutive document IDs is at least 1.
        // It follows that our target can't be located further than its distance
        // from the last document ID.
        let delta = target_id - self.last_doc_id();
        // We then pick the minimum between the "naive" top (the full length)
        // and the "smart" top.
        let mut top = (self.offset + delta as usize).min(len);

        // We hand-roll a binary search, rather than using
        //
        // ```rust
        // self.ids[bottom..top].binary_search(&target_id)
        // ```
        //
        // since benchmarks have shown it to be consistently faster in this context.
        // This assumption might have to be re-evaluated in the future.
        let mut i = 0usize;
        let mut undershot = false;
        let mut current_id: DocId = 0;

        while bottom < top {
            i = (bottom + top) >> 1;
            // SAFETY: We know that `i` is within bounds because `i` is always
            // within the range [bottom, top) and `bottom` is always in range
            // while `top` is always smaller or equal than the length of the list.
            current_id = unsafe { *self.ids.get_unchecked(i) };
            match current_id.cmp(&target_id) {
                Ordering::Equal => {
                    undershot = false;
                    break;
                }
                Ordering::Less => {
                    bottom = i + 1;
                    undershot = true;
                }
                Ordering::Greater => {
                    top = i;
                    undershot = false;
                }
            }
        }

        // Jump to the next entry if we haven't found an exact match
        // and we got the closest-but-smaller entry in the list.
        // We're interested in the closest-but-larger entry.
        if undershot {
            i += 1;
            // SAFETY: We know that `i` is within bounds because:
            // - `i` is always greater or equal than `bottom`, and `bottom` is always in range
            // - `i` can't be equal to `top`, otherwise the iterator would be at EOF
            //   and we covered that case with an early return at the beginning of the
            //   function
            current_id = unsafe { *self.ids.get_unchecked(i) };
        }

        self.result.doc_id = current_id;
        self.offset = i + 1;
        Some(current_id == target_id)
    }
}

impl<'index, const SORTED_BY_ID: bool> RQEIterator<'index> for IdList<'index, SORTED_BY_ID> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        Ok(self.read_and_get_offset()?.map(|t| t.0))
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        Ok(self._skip_to(doc_id).map(|found| {
            if found {
                SkipToOutcome::Found(&mut self.result)
            } else {
                SkipToOutcome::NotFound(&mut self.result)
            }
        }))
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.offset = 0;
        self.result.doc_id = 0;
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self._num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.get_current().is_none()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        if SORTED_BY_ID {
            IteratorType::IdListSorted
        } else {
            IteratorType::IdListUnsorted
        }
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<const SORTED: bool> ProfilePrint for IdList<'_, SORTED> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        if SORTED {
            ctx.print_leaf(c"ID-LIST-SORTED", map);
        } else {
            ctx.print_leaf(c"ID-LIST-UNSORTED", map);
        }
    }
}

impl<'query, Rf: Ref, const SORTED: bool> RawIdList<'query, Rf, SORTED> {
    /// Read `result.doc_id` without exposing the private `result` field to
    /// other modules. Used by [`Metric`](crate::metric::Metric)'s
    /// [`RQESuspendedIterator`] impl, which can't reach into the inner
    /// `RawIdList` directly.
    pub(crate) const fn suspended_result_doc_id(s: &Self) -> DocId {
        s.result.doc_id
    }
}

impl<'query, const SORTED: bool> SuspendedIdList<'query, SORTED> {
    /// Resume the suspended id list at `slot` in place.
    ///
    /// On success returns `Ok(ptr)`, the same slot reinterpreted as the active
    /// [`IdList`]. If the stored result kind is neither metric nor virtual it logs
    /// a warning and returns `Err(ptr)` — the same slot, **left untouched and
    /// still a valid [`SuspendedIdList`]**. Constructors enforce the kind
    /// invariant, but it can be violated at runtime because
    /// [`read`](RQEIterator::read)/[`current`](RQEIterator::current) hand out
    /// `&mut RSIndexResult`, letting a caller overwrite the result with another
    /// kind before suspension.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// 1. `slot` is non-null, aligned, and points to an initialized
    ///    `SuspendedIdList<'query, SORTED>`.
    /// 2. `slot` is unaliased for the duration of the call.
    ///
    /// The returned pointer aliases `slot`. In the `Ok` case every field is valid
    /// at the active type for `'a`; in the `Err` case the slot is byte-for-byte
    /// unchanged and remains a valid `SuspendedIdList<'query, SORTED>`.
    pub(crate) unsafe fn resume_in_place<'a>(
        slot: *mut Self,
    ) -> Result<*mut IdList<'a, SORTED>, *mut Self>
    where
        'query: 'a,
    {
        // SAFETY: `slot` is non-null, aligned, initialized, and unaliased (caller
        // contracts 1 & 2), so a shared read of the result kind is sound.
        let kind = unsafe { (*slot).result.kind() };
        if kind != RSResultKind::Metric && kind != RSResultKind::Virtual {
            tracing::warn!(
                "An internal invariant has been violated. A suspended id list is storing a {kind} \
                 result instead of the expected metric/virtual kind"
            );
            // The slot has not been touched, so it is still a valid `SuspendedIdList`.
            return Err(slot);
        }

        // SAFETY: `slot` is non-null, aligned, and points to an initialized
        // `RawIdList<'query, Suspended, SORTED>` (caller contract 1). `&raw mut` forms a
        // field pointer without creating a reference, leaving `slot`'s provenance
        // over the whole allocation intact for the cast below.
        let result_slot = unsafe { &raw mut (*slot).result };
        // SAFETY: all preconditions of `into_active_in_place` are met:
        // 1. `result_slot` points at an initialized `RawIndexResult<'query, Suspended>`.
        // 2. `result_slot` is not aliased (caller contract 2).
        // 3. `into_active`'s preconditions hold trivially: thanks to the runtime kind check above,
        //    the result is virtual or metric. Those variants own their data.
        //    So conditions (1)–(4) range over no pointers and hold for any `'a`; the
        //    `'query: 'a` bound covers any retained query-pipeline pointers.
        unsafe { RawIndexResult::<'query, Suspended>::into_active_in_place::<'a>(result_slot) };
        Ok(slot.cast::<IdList<'a, SORTED>>())
    }
}

impl<'index, const SORTED: bool> IdList<'index, SORTED> {
    /// Suspend the active id list at `slot` in place, converting its `result`
    /// field to its [`Suspended`] form without moving the allocation. Returns the
    /// same slot reinterpreted as the [`SuspendedIdList`].
    ///
    /// Splitting this out (rather than a whole-struct pointer cast) lets the
    /// wrapping iterators ([`Metric`](crate::metric::Metric), the lazy variants)
    /// recurse through the canonical [`RSIndexResult::into_suspended`] conversion
    /// for the `result` field, and keeps the allocation stable.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// 1. `slot` is non-null, aligned, and points to an initialized
    ///    `IdList<'index, SORTED>`.
    /// 2. `slot` is unaliased for the duration of the call.
    pub(crate) unsafe fn suspend_in_place(slot: *mut Self) -> *mut SuspendedIdList<'index, SORTED> {
        // SAFETY: `slot` is non-null, aligned, and initialized (caller contract 1).
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // `slot`'s provenance over the whole allocation intact for the cast below.
        let result_slot = unsafe { &raw mut (*slot).result };
        // SAFETY: `into_suspended_in_place`'s contract is met — `result_slot` points at
        // an initialized `RSIndexResult<'index>` and is not aliased (caller contracts
        // 1 and 2). Suspending is a safe widening conversion with no further precondition.
        unsafe { RawIndexResult::<Active<'index>>::into_suspended_in_place(result_slot) };

        slot.cast::<SuspendedIdList<'index, SORTED>>()
    }
}

impl<'index, const SORTED: bool> RQEIteratorBoxed<'index> for IdList<'index, SORTED> {
    type Suspended = SuspendedIdList<'index, SORTED>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let active: *mut Self = Box::into_raw(self);

        // SAFETY: `suspend_in_place`'s contract is met — `active` is non-null, aligned, and
        // initialized (it just came from a `Box`), and unaliased (this function owns `self`).
        let suspended_ptr = unsafe { IdList::<'index, SORTED>::suspend_in_place(active) };

        // SAFETY: `suspended_ptr` reuses the same allocation from `Box::into_raw` above, so the
        // address is unchanged and every field is now valid at the suspended type.
        unsafe { Box::from_raw(suspended_ptr) }
    }
}

impl<'query, const SORTED: bool> RQESuspendedIterator<'query> for SuspendedIdList<'query, SORTED> {
    type Resumed<'index>
        = IdList<'index, SORTED>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let suspended: *mut Self = Box::into_raw(self);

        // SAFETY: `resume_in_place`'s contract is met:
        // 1. `suspended` is non-null, aligned, and initialized — it just came from a `Box`.
        // 2. `suspended` is not aliased, since this function has ownership of `self`.
        match unsafe { SuspendedIdList::<'query, SORTED>::resume_in_place::<'index>(suspended) } {
            Ok(active_ptr) => {
                // SAFETY: `active_ptr` reuses the same allocation from `Box::into_raw` above, so
                // the address is unchanged and every field is now valid at the active type for
                // `'index`.
                Ok(ResumeOutcome::Ok(unsafe { Box::from_raw(active_ptr) }))
            }
            Err(suspended_ptr) => {
                // SAFETY: `suspended_ptr` is the same allocation, left untouched and still a valid
                // `SuspendedIdList`. Reclaim ownership so it is dropped.
                drop(unsafe { Box::from_raw(suspended_ptr) });
                Ok(ResumeOutcome::Aborted)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        Self::suspended_result_doc_id(self)
    }
}
