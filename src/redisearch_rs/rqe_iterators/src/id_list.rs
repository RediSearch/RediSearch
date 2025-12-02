/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`IdList`].
use std::cmp::Ordering;

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that yields results according to a sorted list of unique IDs, specified on construction.
pub type SortedIdList<'index> = IdList<'index, true>;
/// An iterator that yields results according to an IDs list, specified on construction,
/// which may or may not be sorted.
pub type UnsortedIdList<'index> = IdList<'index, false>;

/// An iterator that yields results according to an IDs list given on construction.
pub struct IdList<'index, const SORTED: bool> {
    /// The list of document IDs to iterate over.
    /// There must be no duplicates. The list must be sorted if `SORTED` is set to `true`.
    ids: Vec<t_docId>,
    /// The current position of the iterator (a.k.a the next document ID to return by `read`).
    /// When `offset` is equal to the length of `ids`, the iterator is at EOF.
    offset: usize,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl<'index, const SORTED: bool> IdList<'index, SORTED> {
    /// Creates a new ID list iterator.
    ///
    /// The list of document IDs cannot contain duplicates.
    /// If `SORTED` is set to `true`, the list must be sorted.
    #[inline(always)]
    pub fn new(ids: Vec<t_docId>) -> Self {
        Self::with_result(ids, RSIndexResult::virt())
    }

    /// Get the current iterator offset—i.e. its position in the list of IDs.
    ///
    /// This is used by [`MetricIterator`](crate::metric::MetricIterator) to iterate over the corresponding list
    /// of metric data in lockstep.
    #[inline(always)]
    pub(super) const fn offset(&self) -> usize {
        self.offset
    }

    /// Same as [`IdList::new`] but with a custom [`RSIndexResult`],
    /// useful when wrapping this iterator and requiring a non-virtual result.
    pub fn with_result(ids: Vec<t_docId>, result: RSIndexResult<'index>) -> Self {
        if SORTED && !cfg!(feature = "disable_sort_checks_in_idlist") {
            debug_assert!(
                ids.is_sorted_by(|a, b| a < b),
                "IDs must be sorted and unique"
            );
        }

        IdList {
            ids,
            offset: 0,
            result,
        }
    }
}

impl<'index, const SORTED: bool> IdList<'index, SORTED> {
    #[inline(always)]
    fn get_current(&self) -> Option<t_docId> {
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
    pub(super) fn _skip_to(&mut self, target_id: t_docId) -> Option<bool> {
        if !SORTED && !cfg!(feature = "disable_sort_checks_in_idlist") {
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
        let mut current_id: t_docId = 0;

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
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        Ok(self._skip_to(doc_id).map(|found| {
            if found {
                SkipToOutcome::Found(&mut self.result)
            } else {
                SkipToOutcome::NotFound(&mut self.result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.offset = 0;
        self.result.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        self.ids.len()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        match self.offset {
            0 => 0,
            _ => self.ids[self.offset - 1],
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.get_current().is_none()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }
}
