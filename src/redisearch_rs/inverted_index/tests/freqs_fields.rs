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
    freqs_fields::{FreqsFields, FreqsFieldsWide},
};

mod c_mocks;

#[test]
fn test_encode_freqs_fields() {
    // Test cases for the freqs fields encoder and decoder.
    let tests = [
        // (delta, frequency, field mask, expected encoding)
        (0, 1, 1, vec![0, 0, 1, 1]),
        (
            10,
            5,
            u32::MAX as t_fieldMask,
            vec![48, 10, 5, 255, 255, 255, 255],
        ),
        (256, 1, 1, vec![1, 0, 1, 1, 1]),
        (65536, 1, 1, vec![2, 0, 0, 1, 1, 1]),
        (u16::MAX as u32, 1, 1, vec![1, 255, 255, 1, 1]),
        (u32::MAX, 1, 1, vec![3, 255, 255, 255, 255, 1, 1]),
        (
            u32::MAX,
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![
                63, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, freq, field_mask, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());

        let record = RSIndexResult::term()
            .doc_id(doc_id)
            .field_mask(field_mask)
            .frequency(freq);

        let bytes_written =
            FreqsFields::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());
        let record_decoded =
            FreqsFields::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_freqs_fields_wide() {
    // Test cases for the freqs fields wide encoder and decoder.
    let tests = [
        // (delta, frequency, field mask, expected encoding)
        (0, 1, 1, vec![0, 0, 1, 1]),
        (
            10,
            5,
            u32::MAX as t_fieldMask,
            vec![0, 10, 5, 142, 254, 254, 254, 127],
        ),
        (256, 1, 1, vec![1, 0, 1, 1, 1]),
        (65536, 1, 1, vec![2, 0, 0, 1, 1, 1]),
        (u16::MAX as u32, 1, 1, vec![1, 255, 255, 1, 1]),
        (u32::MAX, 1, 1, vec![3, 255, 255, 255, 255, 1, 1]),
        // field mask larger than 32 bits, only supported on 64-bit systems
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX,
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![
                15, 255, 255, 255, 255, 255, 255, 255, 255, 142, 254, 254, 254, 127,
            ],
        ),
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX,
            u32::MAX,
            u128::MAX,
            vec![
                15, 255, 255, 255, 255, 255, 255, 255, 255, 130, 254, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 127,
            ],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, freq, field_mask, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());

        let record = RSIndexResult::term()
            .doc_id(doc_id)
            .field_mask(field_mask)
            .frequency(freq);

        let bytes_written =
            FreqsFieldsWide::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());
        let record_decoded = FreqsFieldsWide::decode_new(&mut buf, prev_doc_id)
            .expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
#[should_panic]
fn test_encode_freqs_fields_field_mask_overflow() {
    // Encoder only supports 32 bits field mask and will panic if larger
    let buf = [0u8; 100];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::term().field_mask(u32::MAX as t_fieldMask + 1);
    let _res = FreqsFields::encode(&mut cursor, 0, &record);
}

#[test]
fn test_encode_freqs_fields_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 3];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::term().field_mask(10);

    let res = FreqsFields::encode(&mut cursor, 0, &record);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);

    let res = FreqsFieldsWide::encode(&mut cursor, 0, &record);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_freqs_fields_input_too_small() {
    // Encoded data is too short.
    let buf = vec![0, 0];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FreqsFields::decode_new(&mut buf, 100);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);

    let res = FreqsFieldsWide::decode_new(&mut buf, 100);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_freqs_fields_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FreqsFields::decode_new(&mut buf, 100);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);

    let res = FreqsFieldsWide::decode_new(&mut buf, 100);
    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}
