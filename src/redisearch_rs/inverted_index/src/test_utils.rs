/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Utilities used only in tests and benchmarks.

use std::mem::ManuallyDrop;

use ffi::t_fieldMask;

use crate::{RSIndexResult, RSOffsetVector, RSResultType, RSTermRecord};

/// Wrapper around `inverted_index::RSIndexResult` ensuring the term and offsets
/// pointers used internally stay valid for the duration of the test or bench.
#[derive(Debug)]
pub struct TestTermRecord {
    pub record: RSIndexResult,
    // both term and offsets need to stay alive during the test
    _term: Box<ffi::RSQueryTerm>,
    _offsets: Vec<i8>,
}

impl TestTermRecord {
    /// Create a new `TestTermRecord` with the given parameters.
    pub fn new(doc_id: u64, field_mask: t_fieldMask, freq: u32, offsets: Vec<i8>) -> Self {
        let mut record = RSIndexResult::term()
            .doc_id(doc_id)
            .field_mask(field_mask)
            .frequency(freq)
            .weight(1.0);
        record.offsets_sz = offsets.len() as u32;

        const TEST_STR: &str = "test";
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let mut term = Box::new(ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        });
        let offsets_ptr = offsets.as_ptr() as *mut _;

        record.data.term = ManuallyDrop::new(RSTermRecord {
            term: &mut *term,
            offsets: RSOffsetVector {
                data: offsets_ptr,
                len: offsets.len() as u32,
            },
        });

        Self {
            record,
            _term: term,
            _offsets: offsets,
        }
    }
}

/// Helper to compare only the fields of a term record that are actually encoded.
/// Only used in tests.
#[derive(Debug)]
pub struct TermRecordCompare<'a>(pub &'a RSIndexResult);

impl<'a> PartialEq for TermRecordCompare<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(matches!(self.0.result_type, RSResultType::Term));

        if !(self.0.doc_id == other.0.doc_id
            && self.0.dmd == other.0.dmd
            && self.0.field_mask == other.0.field_mask
            && self.0.freq == other.0.freq
            && self.0.offsets_sz == other.0.offsets_sz
            && self.0.result_type == other.0.result_type
            && self.0.is_copy == other.0.is_copy
            && self.0.metrics == other.0.metrics)
        {
            return false;
        }

        // do not compare `weight` as it's not encoded

        // SAFETY: we asserted the type above
        let a_term_record = unsafe { &self.0.data.term };
        // SAFETY: we checked that other has the same type as self
        let b_term_record = unsafe { &other.0.data.term };

        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let a_offsets = unsafe {
            std::slice::from_raw_parts(
                a_term_record.offsets.data as *const i8,
                a_term_record.offsets.len as usize,
            )
        };
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let b_offsets = unsafe {
            std::slice::from_raw_parts(
                b_term_record.offsets.data as *const i8,
                b_term_record.offsets.len as usize,
            )
        };

        if a_offsets != b_offsets {
            return false;
        }

        // do not compare `RSTermRecord` as it's not encoded

        true
    }
}
