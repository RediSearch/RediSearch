/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Inverted index iterator implementation
use ffi::t_docId;
use inverted_index::{IndexReader, IndexReaderCore, RSIndexResult, numeric::Numeric};

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator over numeric inverted index entries.
///
/// This iterator provides full index scan to all document IDs in a numeric inverted index.
/// It is not suitable for queries.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct NumericFull<'index> {
    /// The reader used to iterate over the inverted index.
    reader: IndexReaderCore<'index, Numeric, Numeric>,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl<'index> NumericFull<'index> {
    pub fn new(reader: IndexReaderCore<'index, Numeric, Numeric>) -> Self {
        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result: RSIndexResult::numeric(0.0),
        }
    }
}

impl<'iterator, 'index> RQEIterator<'iterator, 'index> for NumericFull<'index> {
    // TODO: this a port of InvIndIterator_Read_Default, the simplest read version.
    // The more complex ones will be implemented as part of the next iterators:
    // - InvIndIterator_Read_SkipMulti_CheckExpiration
    // - InvIndIterator_Read_SkipMulti
    // - InvIndIterator_Read_CheckExpiration
    fn read(
        &'iterator mut self,
    ) -> Result<Option<&'iterator mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        if self.reader.next_record(&mut self.result)? {
            self.last_doc_id = self.result.doc_id;
            Ok(Some(&mut self.result))
        } else {
            self.at_eos = true;
            Ok(None)
        }
    }

    // TODO: this a port of InvIndIterator_SkipTo_Default, the simplest skip_to version.
    // The more complex ones will be implemented as part of the next iterators:
    // - InvIndIterator_SkipTo_withSeeker_CheckExpiration
    // - InvIndIterator_SkipTo_withSeeker
    // - InvIndIterator_SkipTo_CheckExpiration
    fn skip_to(
        &'iterator mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'iterator, 'index>>, RQEIteratorError> {
        // cannot be called with an id smaller than the last one returned by the iterator, see
        // [`RQEIterator::skip_to`].
        debug_assert!(self.last_doc_id < doc_id);

        if self.at_eos {
            return Ok(None);
        }

        if !self.reader.skip_to(doc_id) {
            self.at_eos = true;
            return Ok(None);
        }

        if !self.reader.seek_record(doc_id, &mut self.result)? {
            // reached end of iterator
            self.at_eos = true;
            return Ok(None);
        }

        self.last_doc_id = self.result.doc_id;

        if self.result.doc_id == doc_id {
            // found the record
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            // found a record with an id greater than the requested one
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    fn rewind(&mut self) {
        self.at_eos = false;
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        self.reader.reset();
    }

    fn num_estimated(&self) -> usize {
        self.reader.unique_docs()
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eos
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus, RQEIteratorError> {
        // TODO: NumericCheckAbort when implementing queries

        if !self.reader.needs_revalidation() {
            return Ok(RQEValidateStatus::Ok);
        }

        // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
        // anymore. This means that we need to seek the last docId we were at
        let last_doc_id = self.last_doc_id();
        // reset the state of the reader
        self.rewind();

        if last_doc_id == 0 {
            // Cannot skip to 0
            return Ok(RQEValidateStatus::Ok);
        }

        // try restoring the last docId
        let res = match self.skip_to(last_doc_id)? {
            Some(SkipToOutcome::Found(_)) => RQEValidateStatus::Ok,
            _ => RQEValidateStatus::Moved,
        };

        Ok(res)
    }
}
