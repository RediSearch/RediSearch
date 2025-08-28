/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use ffi::RSQueryTerm;
use inverted_index::{
    Decoder, Encoder,
    offsets_only::OffsetsOnly,
    test_utils::{TermRecordCompare, TestTermRecord},
};

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any test to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut RSQueryTerm) {
    // The RSQueryTerm used in those tests is stack allocated so we don't need to free it.
}

#[test]
fn test_encode_offsets_only() {
    // Test cases for the fields offsets encoder and decoder.
    let tests = [
        // (delta, term offsets vector, expected encoding)
        (0, vec![1i8, 2, 3], vec![0, 0, 3, 1, 2, 3]),
        (10, vec![1i8, 2, 3, 4], vec![0, 10, 4, 1, 2, 3, 4]),
        (256, vec![1, 2, 3], vec![1, 0, 1, 3, 1, 2, 3]),
        (65536, vec![1, 2, 3], vec![2, 0, 0, 1, 3, 1, 2, 3]),
        (
            u16::MAX as u32,
            vec![1, 2, 3],
            vec![1, 255, 255, 3, 1, 2, 3],
        ),
        (
            u32::MAX,
            vec![1, 2, 3],
            vec![3, 255, 255, 255, 255, 3, 1, 2, 3],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, offsets, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());

        let record = TestTermRecord::new(doc_id, 0, 1, offsets);

        let bytes_written = OffsetsOnly::default()
            .encode(&mut buf, delta, &record.record)
            .expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());

        let record_decoded = OffsetsOnly::default()
            .decode(&mut buf, prev_doc_id)
            .expect("to decode freqs only record");

        assert_eq!(
            TermRecordCompare(&record_decoded),
            TermRecordCompare(&record.record)
        );
    }
}

#[test]
fn test_encode_offsets_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 1];
    let mut cursor = Cursor::new(buf);
    let record = inverted_index::RSIndexResult::term();

    let res = OffsetsOnly::default().encode(&mut cursor, 0, &record);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_offsets_only_input_too_small() {
    // Encoded data is too short.
    let buf = vec![0, 0];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = OffsetsOnly::default().decode(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_offsets_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = OffsetsOnly::default().decode(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}
