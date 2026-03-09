/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use ffi::t_fieldMask;
use inverted_index::{
    Decoder, Encoder, RSIndexResult,
    fields_only::{FieldsOnly, FieldsOnlyWide},
};

/// Helper to encode a sequence of (delta, field_mask) records using FieldsOnly.
fn encode_fields_only(records: &[(u32, u32)]) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    for &(delta, field_mask) in records {
        let record = RSIndexResult::term().field_mask(field_mask as t_fieldMask);
        FieldsOnly::encode(&mut buf, delta, &record).expect("to encode");
    }
    buf.into_inner()
}

/// Helper to encode a sequence of (delta, field_mask) records using FieldsOnlyWide.
fn encode_fields_only_wide(records: &[(u32, u128)]) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    for &(delta, field_mask) in records {
        let record = RSIndexResult::term().field_mask(field_mask);
        FieldsOnlyWide::encode(&mut buf, delta, &record).expect("to encode");
    }
    buf.into_inner()
}

#[test]
fn test_encode_fields_only() {
    // Test cases for the fields only encoder and decoder.
    let tests = [
        // (delta, field mask, expected encoding)
        (0, 1, vec![0, 0, 1]),
        (
            10,
            u32::MAX as t_fieldMask,
            vec![12, 10, 255, 255, 255, 255],
        ),
        (256, 1, vec![1, 0, 1, 1]),
        (65536, 1, vec![2, 0, 0, 1, 1]),
        (u16::MAX as u32, 1, vec![1, 255, 255, 1]),
        (u32::MAX, 1, vec![3, 255, 255, 255, 255, 1]),
        (
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![15, 255, 255, 255, 255, 255, 255, 255, 255],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, field_mask, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = inverted_index::RSIndexResult::term()
            .doc_id(doc_id)
            .field_mask(field_mask);

        let bytes_written =
            FieldsOnly::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());

        let record_decoded =
            FieldsOnly::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_fields_only_wide() {
    // Test cases for the fields only encoder and decoder.
    let tests = [
        // (delta, field mask, expected encoding)
        (0, 1, vec![0, 1]),
        (
            10,
            u32::MAX as t_fieldMask,
            vec![10, 142, 254, 254, 254, 127],
        ),
        (256, 1, vec![129, 0, 1]),
        (65536, 1, vec![130, 255, 0, 1]),
        (u16::MAX as u32, 1, vec![130, 254, 127, 1]),
        (u32::MAX, 1, vec![142, 254, 254, 254, 127, 1]),
        (
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![142, 254, 254, 254, 127, 142, 254, 254, 254, 127],
        ),
        // field mask larger than 32 bits
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![142, 254, 254, 254, 127, 142, 254, 254, 254, 127],
        ),
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX,
            u128::MAX,
            vec![
                142, 254, 254, 254, 127, 130, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 127,
            ],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, field_mask, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = inverted_index::RSIndexResult::term()
            .doc_id(doc_id)
            .field_mask(field_mask);

        let bytes_written =
            FieldsOnlyWide::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());
        let record_decoded =
            FieldsOnlyWide::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_fields_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 1];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::term();
    let res = FieldsOnly::encode(&mut cursor, 0, &record);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_fields_only_input_too_small() {
    // Encoded data is one byte too short.
    let buf = vec![0, 0];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FieldsOnly::decode_new(&mut buf, 100);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_fields_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FieldsOnly::decode_new(&mut buf, 100);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_seek_fields_only() {
    // Records: doc_ids 10, 20, 30, 35, 55, 60 (using deltas from base 10)
    let buf = encode_fields_only(&[
        (0, 1),  // doc_id = 10
        (10, 2), // doc_id = 20
        (10, 3), // doc_id = 30
        (5, 1),  // doc_id = 35
        (20, 9), // doc_id = 55
        (5, 1),  // doc_id = 60
    ]);
    let mut cursor = Cursor::new(buf.as_ref());
    let mut result = RSIndexResult::term();

    // Seek to 30 (skips first two records)
    let found = FieldsOnly::seek(&mut cursor, 10, 30, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 30);
    assert_eq!(result.field_mask, 3);

    // Seek to 40 from base 30 (should land on 55)
    let found = FieldsOnly::seek(&mut cursor, 30, 40, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 55);
    assert_eq!(result.field_mask, 9);

    // Seek past end
    let found = FieldsOnly::seek(&mut cursor, 55, 70, &mut result).expect("seek");
    assert!(!found);
}

#[test]
fn test_seek_fields_only_wide() {
    let buf = encode_fields_only_wide(&[
        (0, 1),  // doc_id = 10
        (10, 2), // doc_id = 20
        (10, 3), // doc_id = 30
        (5, 1),  // doc_id = 35
        (20, 9), // doc_id = 55
        (5, 1),  // doc_id = 60
    ]);
    let mut cursor = Cursor::new(buf.as_ref());
    let mut result = RSIndexResult::term();

    // Seek to 30
    let found = FieldsOnlyWide::seek(&mut cursor, 10, 30, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 30);
    assert_eq!(result.field_mask, 3);

    // Seek to 40 (lands on 55)
    let found = FieldsOnlyWide::seek(&mut cursor, 30, 40, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 55);
    assert_eq!(result.field_mask, 9);

    // Seek past end
    let found = FieldsOnlyWide::seek(&mut cursor, 55, 70, &mut result).expect("seek");
    assert!(!found);
}
