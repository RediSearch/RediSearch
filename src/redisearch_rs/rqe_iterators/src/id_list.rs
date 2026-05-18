/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`IdList`].

use index_result::{RSIndexResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref};
use rqe_core::DocId;
use std::cmp::Ordering;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    deferred::{ProducedResults, Producer},
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
pub struct RawIdList<Rf: Ref, const SORTED: bool> {
    /// The list of document IDs to iterate over.
    /// There must be no duplicates. The list must be sorted if `SORTED` is set to `true`.
    ids: OwnedSlice<DocId>,
    /// The current position of the iterator (a.k.a the next document ID to return by [`read`](RQEIterator::read)).
    /// When `offset` is equal to the length of `ids`, the iterator is at EOF.
    offset: usize,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RawIndexResult<Rf>,
}

/// Alias for an [`Active`] [`RawIdList`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type IdList<'index, const SORTED: bool> = RawIdList<Active<'index>, SORTED>;

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
    /// Used by the lazy variants ([`IdListLazy`], [`MetricLazy`](crate::metric::MetricLazy))
    /// to populate an initially-empty iterator once the deferred producer has run.
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
        self.ids.len()
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

/// A lazily-populated [`IdList`]: the IDs are not supplied up front but produced on the first
/// [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to) by a deferred [`Producer`]
/// (see [`crate::deferred`]). This keeps the eager [`IdList`] hot path free of any
/// "produced yet?" branch — that check lives only here.
///
/// Once produced it delegates entirely to the wrapped [`IdList`], so it reports the same
/// [`IteratorType`] and is interchangeable with an eagerly-built one.
pub struct IdListLazy<'index, const SORTED: bool> {
    /// The wrapped ID list, empty until [`producer`](Self::producer) runs.
    inner: IdList<'index, SORTED>,
    /// The deferred producer. Run once on the first read/skip_to (guarded by
    /// [`produced`](Self::produced)) but **retained** so any state it owns lives as long as this
    /// iterator (see [`Producer`]).
    producer: Producer<'index>,
    /// Whether [`producer`](Self::producer) has already run.
    produced: bool,
    /// Upper-bound estimate reported while the producer is still pending (the real count is
    /// unknown until it runs).
    num_estimated_hint: usize,
}

impl<'index, const SORTED: bool> IdListLazy<'index, SORTED> {
    /// Create a lazy ID list. `result` is the reusable result object the inner list yields,
    /// and `num_estimated_hint` is the estimate reported until `producer` runs.
    pub fn new(
        producer: Producer<'index>,
        num_estimated_hint: usize,
        result: RSIndexResult<'index>,
    ) -> Self {
        Self {
            inner: IdList::with_result(OwnedSlice::default(), result),
            producer,
            produced: false,
            num_estimated_hint,
        }
    }

    /// Run the producer on the first call, populating the inner list; a no-op afterwards. The
    /// producer is retained either way. On timeout the error is propagated and the inner list
    /// stays empty (so the next read reports EOF).
    #[inline]
    fn ensure_produced(&mut self) -> Result<(), RQEIteratorError> {
        if !self.produced {
            self.produced = true;
            let ProducedResults { ids, .. } = (self.producer)()?;
            self.inner.set_ids(ids);
        }
        Ok(())
    }
}

impl<'index, const SORTED: bool> RQEIterator<'index> for IdListLazy<'index, SORTED> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.ensure_produced()?;
        self.inner.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.ensure_produced()?;
        self.inner.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.inner.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        if self.produced {
            self.inner.num_estimated()
        } else {
            self.num_estimated_hint
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.inner.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        // Before production the list is not (necessarily) exhausted — report not-at-EOF so
        // callers read it and trigger production.
        self.produced && self.inner.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.inner.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.inner.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.inner
            .intersection_sort_weight(prioritize_union_children)
    }
}

impl<const SORTED: bool> ProfilePrint for IdListLazy<'_, SORTED> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.inner.print_profile(map, ctx);
    }
}
