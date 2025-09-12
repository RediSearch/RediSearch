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
use inverted_index::{IndexReader, RSIndexResult, numeric::Numeric};

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome};

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
    reader: IndexReader<'index, Numeric, Numeric>,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'static>,
}

impl<'index> NumericFull<'index> {
    pub fn new(reader: IndexReader<'index, Numeric, Numeric>) -> Self {
        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result: RSIndexResult::numeric(0.0),
        }
    }

    fn update_result(&mut self, record: RSIndexResult<'_>) {
        self.last_doc_id = record.doc_id;
        self.result.doc_id = record.doc_id;
        // SAFETY: the Numeric decoder always return numeric records
        let record_num = record.as_numeric().expect("record is not numeric");
        // SAFETY: we created this record as numeric
        *self.result.as_numeric_mut().unwrap() = record_num;
    }
}

impl<'index> RQEIterator for NumericFull<'index> {
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
