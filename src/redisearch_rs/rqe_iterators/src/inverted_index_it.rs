/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Inverted index iterator implementation
use core::panic;

use ffi::t_docId;
use inverted_index::{
    DecodedBy, Decoder, IndexReader, RSIndexResult, full::Full, numeric::Numeric,
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
/// * `E` - The encoderype used to encode the inverted index.
/// * `D` - The decoder type used to decode the inverted index.
struct FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    /// The reader used to iterate over the inverted index.
    reader: IndexReader<'index, E, D>,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'static>,
}

impl<'index, E, D> FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    fn new(reader: IndexReader<'index, E, D>, result: RSIndexResult<'static>) -> Self {
        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
        }
    }

    fn update_result(&mut self, record: RSIndexResult<'_>) {
        self.last_doc_id = record.doc_id;
        self.result.doc_id = record.doc_id;
        self.result.field_mask = record.field_mask;
        self.result.freq = record.freq;

        match (&record.data, &mut self.result.data) {
            (
                inverted_index::RSResultData::Numeric(src),
                inverted_index::RSResultData::Numeric(dst),
            ) => {
                *dst = *src;
            }
            (inverted_index::RSResultData::Term(src), inverted_index::RSResultData::Term(dst)) => {
                // Update the term data
                *dst = src.to_owned();
            }
            _ => {
                panic!("Result data type mismatch");
            }
        }
    }
}

impl<'index, E, D> RQEIterator for FullIterator<'index, E, D>
where
    E: DecodedBy<Decoder = D>,
    D: Decoder,
{
    fn read<'it>(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        let record = self.reader.next_record()?;

        if let Some(record) = record {
            self.update_result(record);
            Ok(Some(&self.result))
        } else {
            self.at_eos = true;
            Ok(None)
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        // skip_to pre-condition
        debug_assert!(self.last_doc_id < doc_id);

        if self.at_eos {
            return Ok(None);
        }

        if !self.reader.skip_to(doc_id) {
            self.at_eos = true;
            return Ok(None);
        }

        match self.reader.seek_record(doc_id)? {
            Some(record) if record.doc_id == doc_id => {
                // found the record
                self.update_result(record);
                Ok(Some(SkipToOutcome::Found(&self.result)))
            }
            Some(record) => {
                // found a record with an id greater than the requested one
                self.update_result(record);
                Ok(Some(SkipToOutcome::NotFound(&self.result)))
            }
            None => {
                // reached end of iterator
                self.at_eos = true;
                Ok(None)
            }
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
    pub fn new(reader: IndexReader<'index, Numeric, Numeric>) -> Self {
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
}

/// An iterator over term inverted index entries.
///
/// This iterator provides full index scan to all document IDs in a term inverted index.
/// It is not suitable for queries.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct TermFull<'index> {
    it: FullIterator<'index, Full, Full>,
}

impl<'index> TermFull<'index> {
    pub fn new(reader: IndexReader<'index, Full, Full>) -> Self {
        let result = RSIndexResult::term();
        Self {
            it: FullIterator::new(reader, result),
        }
    }
}

impl<'index> RQEIterator for TermFull<'index> {
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
}
