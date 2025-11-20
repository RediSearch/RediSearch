/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! ID-list iterator implementation
use std::cmp::min;

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that yields results according to a sorted IDs list, specified on construction.
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
    #[inline(always)]
    /// Creates a new ID list iterator.
    ///
    /// The list of document IDs cannot contain duplicates.
    /// If `SORTED` is set to `true`, the list must be sorted.
    pub fn new(ids: Vec<t_docId>) -> Self {
        Self::with_result(ids, RSIndexResult::virt())
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

    // this function is needed by the metric iterator to get the offset,
    // because the metric iterator borrows the iterator as mutable for skip_to(), and the offset is changed by skip_to().
    // This is because the IndexResult is reused.
    pub(super) fn skip_to_and_get_offset(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<(SkipToOutcome<'_, 'index>, usize)>, RQEIteratorError> {
        if !SORTED && !cfg!(feature = "disable_sort_checks_in_idlist") {
            panic!("Can't skip when working with unsorted document ids");
        }

        // Safe to unwrap as we are not at eof + the list must not be empty
        if self.at_eof() || self.ids.last().unwrap() < &doc_id {
            self.offset = self.ids.len(); // Move to EOF
            return Ok(None);
        }

        debug_assert!(self.last_doc_id() < doc_id);
        // The top of the binary search is limited, The worst case scenario is when the List contains every DocId, and in that worst case
        // the search range is limited to a range of (docId - lastDocId) elements starting from current offset
        let top = min(
            self.offset + (doc_id - self.last_doc_id()) as usize,
            self.ids.len(),
        );
        // Pos is correct whether we found the element or not - it's the first element greater than or equal to doc_id
        let pos = self.ids[self.offset..top].binary_search(&doc_id);
        match pos {
            Ok(pos) => {
                let pos = self.offset + pos; // Convert relative to absolute index
                self.offset = pos + 1;
                self.result.doc_id = self.ids[pos];
                Ok(Some((SkipToOutcome::Found(&mut self.result), self.offset)))
            }
            Err(pos) => {
                let pos = self.offset + pos; // Convert relative to absolute index
                self.offset = pos + 1;
                self.result.doc_id = self.ids[pos];
                Ok(Some((
                    SkipToOutcome::NotFound(&mut self.result),
                    self.offset,
                )))
            }
        }
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
        Ok(self.skip_to_and_get_offset(doc_id)?.map(|t| t.0))
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
