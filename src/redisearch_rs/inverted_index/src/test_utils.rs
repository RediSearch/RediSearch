/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Utilities used only in tests and benchmarks.

use ffi::t_fieldMask;
use query_term::RSQueryTerm;

use crate::{RSIndexResult, RSOffsetVector, RSResultData};

/// Wrapper around `inverted_index::RSIndexResult` ensuring the offsets
/// pointer used internally stays valid for the duration of the test or bench.
#[derive(Debug)]
pub struct TestTermRecord<'index> {
    pub record: RSIndexResult<'index>,
    // offsets need to stay alive during the test
    _offsets: Vec<i8>,
}

impl TestTermRecord<'_> {
    /// Create a new `TestTermRecord` with the given parameters.
    pub fn new(doc_id: u64, field_mask: t_fieldMask, freq: u32, offsets: Vec<i8>) -> Self {
        let mut term = RSQueryTerm::new(b"test", 1, 0);
        term.idf = 5.0;
        term.bm25_idf = 10.0;

        let offsets_ptr = offsets.as_ptr() as *mut _;
        let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

        let record =
            RSIndexResult::with_term(Some(term), rs_offsets, doc_id, field_mask, freq).weight(1.0);

        Self {
            record,
            _offsets: offsets,
        }
    }
}

/// Helper to compare only the fields of a term record that are actually encoded.
/// Only used in tests.
#[derive(Debug)]
pub struct TermRecordCompare<'index>(pub &'index RSIndexResult<'index>);

impl<'a> PartialEq for TermRecordCompare<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(matches!(self.0.data, RSResultData::Term(_)));

        if !(self.0.doc_id == other.0.doc_id
            && self.0.dmd == other.0.dmd
            && self.0.field_mask == other.0.field_mask
            && self.0.freq == other.0.freq
            && self.0.data.kind() == other.0.data.kind()
            && self.0.metrics == other.0.metrics)
        {
            return false;
        }

        // do not compare `weight` as it's not encoded

        // SAFETY: we asserted the type above
        let a_term_record = self.0.as_term().unwrap();
        // SAFETY: we checked that other has the same type as self
        let b_term_record = other.0.as_term().unwrap();

        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let a_offsets = a_term_record.offsets();

        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let b_offsets = b_term_record.offsets();

        if a_offsets != b_offsets {
            return false;
        }

        // do not compare `RSTermRecord` as it's not encoded
        true
    }
}
