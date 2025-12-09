/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Numeric`] and [`Term`].

use ffi::t_docId;
use inverted_index::{IndexReader, NumericReader, RSIndexResult, TermReader};

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// A generic iterator over inverted index entries.
///
/// This iterator is used to query an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
pub struct InvIndIterator<'index, R> {
    /// The reader used to iterate over the inverted index.
    reader: R,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,

    /// The implementation of the `read` method.
    /// Using dynamic dispatch so we can pick the right version during the
    /// iterator construction saving to re-do the checks each time read() is called.
    read_impl: fn(&mut Self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>,
}

impl<'index, R> InvIndIterator<'index, R>
where
    R: IndexReader<'index>,
{
    pub fn new(reader: R, result: RSIndexResult<'static>) -> Self {
        // no need to manually skip duplicates if there is none in the II.
        let skip_multi = reader.has_duplicates();

        let read_impl = if skip_multi {
            Self::read_skip_multi
        } else {
            Self::read_default
        };

        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
            read_impl,
        }
    }

    // TODO: this a port of InvIndIterator_Read_Default, the simplest read version.
    // The more complex ones will be implemented as part of the query iterators:
    // - InvIndIterator_Read_SkipMulti_CheckExpiration
    // - InvIndIterator_Read_CheckExpiration
    /// Default read implementation, without any additional filtering.
    fn read_default(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
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

    /// Read implementation that skips multi-value entries from the same document.
    fn read_skip_multi(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        while self.reader.next_record(&mut self.result)? {
            if self.last_doc_id == self.result.doc_id {
                // Prevent returning the same doc
                continue;
            }

            self.last_doc_id = self.result.doc_id;
            return Ok(Some(&mut self.result));
        }

        // exited the while loop so we reached the end of the index
        self.at_eos = true;
        Ok(None)
    }
}

impl<'index, R> RQEIterator<'index> for InvIndIterator<'index, R>
where
    R: IndexReader<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (self.read_impl)(self)
    }

    // TODO: implement InvIndIterator_SkipTo_withSeeker_CheckExpiration with the query iterators.
    #[inline(always)]
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
        self.reader.unique_docs() as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eos
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
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
            Some(SkipToOutcome::NotFound(doc)) => RQEValidateStatus::Moved { current: Some(doc) },
            None => RQEValidateStatus::Moved { current: None },
        };

        Ok(res)
    }
}

/// An iterator over numeric inverted index entries.
///
/// This iterator can be used to query a numeric inverted index.
///
/// The [`inverted_index::IndexReader`] API can be used to fully scan an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct Numeric<'index, R> {
    it: InvIndIterator<'index, R>,
}

impl<'index, R> Numeric<'index, R>
where
    R: NumericReader<'index>,
{
    /// Create an iterator returning results from a numeric inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`NumericReader`] such as [`inverted_index::FilterNumericReader`]
    /// or [`inverted_index::FilterGeoReader`].
    pub fn new(reader: R) -> Self {
        let result = RSIndexResult::numeric(0.0);
        Self {
            it: InvIndIterator::new(reader, result),
        }
    }
}

impl<'index, R> RQEIterator<'index> for Numeric<'index, R>
where
    R: NumericReader<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.it.revalidate()
    }
}

/// An iterator over term inverted index entries.
///
/// This iterator can be used to query a term inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct Term<'index, R> {
    it: InvIndIterator<'index, R>,
}

impl<'index, R> Term<'index, R>
where
    R: TermReader<'index>,
{
    /// Create an iterator returning results from a term inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`inverted_index::FilterMaskReader`].
    pub fn new(reader: R) -> Self {
        let result = RSIndexResult::term();
        Self {
            it: InvIndIterator::new(reader, result),
        }
    }
}

impl<'index, R> RQEIterator<'index> for Term<'index, R>
where
    R: TermReader<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.it.revalidate()
    }
}
