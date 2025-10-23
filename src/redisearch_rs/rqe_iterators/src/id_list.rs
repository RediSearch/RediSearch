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

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome};

/// An iterator that yields results according to an IDs list given on construction.
pub struct IdList<'index> {
    /// The list of document IDs to iterate over. Must be sorted, unique, and non-empty.
    ids: Vec<t_docId>,
    /// The current position of the iterator (a.k.a the next document ID to return by `read`).
    /// When `offset` is equal to the length of `ids`, the iterator is at EOF.
    offset: usize,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl<'index> IdList<'index> {
    #[inline(always)]
    /// Creates a new ID list iterator. The list of document IDs must be sorted, unique, and non-empty.
    pub fn new(ids: Vec<t_docId>) -> Self {
        Self::new_with_result(ids, RSIndexResult::virt())
    }

    /// Same as [`IdList::new`] but with a custom [`RSIndexResult`],
    /// useful when wrapping this iterator and requiring a non-virtual result.
    pub fn new_with_result(ids: Vec<t_docId>, result: RSIndexResult<'index>) -> Self {
        debug_assert!(!ids.is_empty());
        debug_assert!(
            ids.is_sorted_by(|a, b| a < b),
            "IDs must be sorted and unique"
        );
        IdList {
            ids,
            offset: 0,
            result,
        }
    }
}

impl<'index> IdList<'index> {
    #[inline(always)]
    fn get_current(&self) -> Option<t_docId> {
        self.ids.get(self.offset).copied()
    }

    #[inline(always)]
    pub const fn offset(&self) -> usize {
        self.offset
    }

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

    pub(super) fn skip_to_and_get_offset(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<(SkipToOutcome<'_, 'index>, usize)>, RQEIteratorError> {
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

impl<'index> RQEIterator<'index> for IdList<'index> {
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
}
