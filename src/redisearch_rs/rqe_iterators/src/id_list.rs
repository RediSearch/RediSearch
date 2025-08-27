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
pub struct IdList {
    ids: Vec<t_docId>,
    current: usize,
}

impl IdList {
    pub fn new(ids: Vec<t_docId>) -> Self {
        debug_assert!(!ids.is_empty());
        debug_assert!(
            ids.is_sorted_by(|a, b| a < b),
            "IDs must be sorted and unique"
        );
        IdList { ids, current: 0 }
    }
}

impl RQEIterator for IdList {
    fn read(&mut self) -> Result<Option<RSIndexResult<'_, '_>>, RQEIteratorError> {
        if self.at_eof() {
            Ok(None)
        } else {
            let doc_id = self.ids[self.current];
            self.current += 1;
            Ok(Some(RSIndexResult::virt().doc_id(doc_id)))
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        // Safe to unwrap as we are not at eof + the list must not be empty
        if self.at_eof() || self.ids.last().unwrap() < &doc_id {
            self.current = self.ids.len(); // Move to EOF
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);
        // The top of the binary search is limited, The worst case scenario is when the List contains every DocId, and in that worst case
        // the search range is limited to a range of (docId - lastDocId) elements starting from current offset
        let top = min(
            self.current + (doc_id - self.last_doc_id()) as usize,
            self.ids.len(),
        );
        // Pos is correct whether we found the element or not - it's the first element greater than or equal to doc_id
        let pos = self.ids[self.current..top].binary_search(&doc_id);
        match pos {
            Ok(pos) => {
                let pos = self.current + pos; // Convert relative to absolute index
                self.current = pos + 1;
                Ok(Some(SkipToOutcome::Found(
                    RSIndexResult::virt().doc_id(self.ids[pos]),
                )))
            }
            Err(pos) => {
                let pos = self.current + pos; // Convert relative to absolute index
                self.current = pos + 1;
                Ok(Some(SkipToOutcome::NotFound(
                    RSIndexResult::virt().doc_id(self.ids[pos]),
                )))
            }
        }
    }

    fn rewind(&mut self) {
        self.current = 0;
    }

    fn num_estimated(&self) -> usize {
        self.ids.len()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        match self.current {
            0 => 0,
            _ => self.ids[self.current - 1],
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.current >= self.ids.len()
    }
}
