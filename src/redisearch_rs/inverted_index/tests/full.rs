/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, mem::ManuallyDrop};

use inverted_index::{
    Decoder, DecoderResult, Delta, Encoder, RSOffsetVector, RSTermRecord, full::Full,
};

/// Wrapper around `inverted_index::RSIndexResult` ensuring the term and offsets
/// pointers used internally stay valid for the duration of the test.
struct TestRecord {
    record: inverted_index::RSIndexResult,
    // both term and offsets need to stay alive during the test
    _term: ffi::RSQueryTerm,
    _offsets: Vec<i8>,
}

impl TestRecord {
    fn new(doc_id: u64, field_mask: u128, freq: u32, offsets: Vec<i8>) -> Self {
        let mut record = inverted_index::RSIndexResult::token_record(
            doc_id,
            field_mask,
            freq,
            offsets.len() as u32,
        );
        record.weight = 1.0;

        const TEST_STR: &str = "test";
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let mut term = ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        };
        let offsets_ptr = offsets.as_ptr() as *mut _;

        record.data.term = ManuallyDrop::new(RSTermRecord {
            term: &mut term,
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
#[derive(Debug)]
struct TermRecordCompare<'a>(&'a inverted_index::RSIndexResult);

impl<'a> PartialEq for TermRecordCompare<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(matches!(
            self.0.result_type,
            inverted_index::RSResultType::Term
        ));

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

#[test]
fn test_encode_full() {
    // Test cases for the full encoder and decoder.
    let tests = [
        // (delta, frequency, field mask, term offsets vector, expected encoding)
        (0, 1, 1, vec![1i8, 2, 3], vec![0, 0, 1, 1, 3, 1, 2, 3]),
        (
            10,
            5,
            u32::MAX as u128,
            vec![1i8, 2, 3, 4],
            vec![48, 10, 5, 255, 255, 255, 255, 4, 1, 2, 3, 4],
        ),
        (256, 1, 1, vec![1, 2, 3], vec![1, 0, 1, 1, 1, 3, 1, 2, 3]),
        (
            65536,
            1,
            1,
            vec![1, 2, 3],
            vec![2, 0, 0, 1, 1, 1, 3, 1, 2, 3],
        ),
        (
            u16::MAX as usize,
            1,
            1,
            vec![1, 2, 3],
            vec![1, 255, 255, 1, 1, 3, 1, 2, 3],
        ),
        (
            u32::MAX as usize,
            1,
            1,
            vec![1, 2, 3],
            vec![3, 255, 255, 255, 255, 1, 1, 3, 1, 2, 3],
        ),
        (
            u32::MAX as usize,
            u32::MAX,
            u32::MAX as u128,
            vec![1; 100],
            vec![
                63, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 100, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            ],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, freq, field_mask, offsets, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());

        let record = TestRecord::new(doc_id, field_mask, freq, offsets);

        let bytes_written = Full::default()
            .encode(&mut buf, Delta::new(delta), &record.record)
            .expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let DecoderResult::Record(record_decoded) = Full::default()
            .decode(&mut buf, prev_doc_id)
            .expect("to decode freqs only record")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(
            TermRecordCompare(&record_decoded),
            TermRecordCompare(&record.record)
        );
    }
}

#[test]
#[should_panic]
fn test_encode_full_field_mask_overflow() {
    // Encoder only supports 32 bits field mask and will panic if larger
    let mut buf = [0u8; 3];
    let buf = &mut buf[0..1];
    let mut cursor = Cursor::new(buf);

    let record = TestRecord::new(10, u32::MAX as u128 + 1, 1, vec![1]);
    let _res = Full::default().encode(&mut cursor, Delta::new(0), &record.record);
}

#[test]
fn test_encode_full_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let mut buf = [0u8; 3];
    let buf = &mut buf[0..1];
    let mut cursor = Cursor::new(buf);

    let record = TestRecord::new(10, 1, 1, vec![1]);
    let res = Full::default().encode(&mut cursor, Delta::new(0), &record.record);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_full_input_too_small() {
    // Encoded data is too short.
    let buf = vec![0, 0];
    let mut cursor = Cursor::new(buf);
    let res = Full::default().decode(&mut cursor, 100);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_full_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf);
    let res = Full::default().decode(&mut cursor, 100);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
#[should_panic]
fn test_encode_full_delta_overflow() {
    // Encoder only supports 32 bits delta and will panic if larger
    let mut buf = Cursor::new(vec![0; 3]);

    let record = TestRecord::new(10, 1, 1, vec![1]);
    let _res = Full::default().encode(&mut buf, Delta::new(u32::MAX as usize + 1), &record.record);
}
