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
use inverted_index::{IndexReader, NumericReader, RSIndexResult};

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// A generic full iterator over inverted index entries.
///
/// This iterator provides full index scan to all document IDs in an inverted index.
/// It is not suitable for queries.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
struct FullIterator<'index, R> {
    /// The reader used to iterate over the inverted index.
    reader: R,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl<'index, R> FullIterator<'index, R>
where
    R: IndexReader<'index>,
{
    const fn new(reader: R, result: RSIndexResult<'static>) -> Self {
        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
        }
    }
}

impl<'index, R> RQEIterator<'index> for FullIterator<'index, R>
where
    R: IndexReader<'index>,
{
    // TODO: this a port of InvIndIterator_Read_Default, the simplest read version.
    // The more complex ones will be implemented as part of the next iterators:
    // - InvIndIterator_Read_SkipMulti_CheckExpiration
    // - InvIndIterator_Read_SkipMulti
    // - InvIndIterator_Read_CheckExpiration
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
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

    // TODO: implement InvIndIterator_SkipTo_withSeeker_CheckExpiration with the query iterators.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // cannot be called with an id smaller than the last one returned by the iterator, see
        // [`RQEIterator::skip_to`].
        debug_assert!(self.last_doc_id < doc_id);

        if self.at_eos {
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
}

/// An iterator over numeric inverted index entries.
///
/// This iterator provides full index scan to all document IDs in a numeric inverted index.
/// It is not suitable for queries.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct NumericFull<'index, R> {
    it: FullIterator<'index, R>,
}

impl<'index, R> NumericFull<'index, R>
where
    R: NumericReader<'index>,
{
    pub fn new(reader: R) -> Self {
        let result = RSIndexResult::numeric(0.0);
        Self {
            it: FullIterator::new(reader, result),
        }
    }
}

impl<'index, R> RQEIterator<'index> for NumericFull<'index, R>
where
    R: NumericReader<'index>,
{
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    fn rewind(&mut self) {
        self.it.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.it.revalidate()
    }
}
