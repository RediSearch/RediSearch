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
use inverted_index::{
    DecodedBy, Decoder, IndexReader, IndexReaderCore, RSIndexResult, TermDecoder, numeric::Numeric,
};

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome};

/// A generic full iterator over inverted index entries.
///
/// This iterator provides full index scan to all document IDs in an inverted index.
/// It is not suitable for queries.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoder type used to encode the inverted index.
/// * `D` - The decoder type used to decode the inverted index.
struct FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    /// The reader used to iterate over the inverted index.
    reader: IndexReaderCore<'index, E, D>,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl<'index, E, D> FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    fn new(reader: IndexReaderCore<'index, E, D>, result: RSIndexResult<'static>) -> Self {
        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
        }
    }
}

impl<'index, E, D> RQEIterator for FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    // TODO: this a port of InvIndIterator_Read_Default, the simplest read version.
    // The more complex ones will be implemented as part of the next iterators:
    // - InvIndIterator_Read_SkipMulti_CheckExpiration
    // - InvIndIterator_Read_SkipMulti
    // - InvIndIterator_Read_CheckExpiration
    fn read<'it>(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        if self.reader.next_record(&mut self.result)? {
            self.last_doc_id = self.result.doc_id;
            Ok(Some(&self.result))
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
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
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
            Ok(Some(SkipToOutcome::Found(&self.result)))
        } else {
            // found a record with an id greater than the requested one
            Ok(Some(SkipToOutcome::NotFound(&self.result)))
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
pub struct NumericFull<'index> {
    it: FullIterator<'index, Numeric, Numeric>,
}

impl<'index> NumericFull<'index> {
    pub fn new(reader: IndexReaderCore<'index, Numeric, Numeric>) -> Self {
        let result = RSIndexResult::numeric(0.0);
        Self {
            it: FullIterator::new(reader, result),
        }
    }
}

impl<'index> RQEIterator for NumericFull<'index> {
    fn read<'it>(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        self.it.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
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

    fn revalidate(&mut self) -> crate::RQEValidateStatus {
        self.it.revalidate()
    }
}

/// An iterator over term inverted index entries.
///
/// This iterator provides full index scan to all document IDs in a term inverted index.
/// It is not suitable for queries.
///
/// Note that 'full' is this context refers to the iterator being used for a full index scan.
/// It is not directly related to the ['inverted_inndex::full::Full'] encoder/decoder as
/// any decoder producing term results can be used with this iterator.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoder type used to encode the inverted index.
/// * `D` - The decoder type used to decode the inverted index.
pub struct TermFull<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: TermDecoder,
{
    it: FullIterator<'index, E, D>,
}

impl<'index, E, D> TermFull<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: TermDecoder,
{
    pub fn new(reader: IndexReaderCore<'index, E, D>) -> Self {
        let result = RSIndexResult::term();
        Self {
            it: FullIterator::new(reader, result),
        }
    }
}

impl<'index, E, D> RQEIterator for TermFull<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: TermDecoder,
{
    fn read<'it>(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        self.it.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
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

    fn revalidate(&mut self) -> crate::RQEValidateStatus {
        self.it.revalidate()
    }
}
