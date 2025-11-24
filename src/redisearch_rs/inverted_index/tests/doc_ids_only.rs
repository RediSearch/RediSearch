/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use inverted_index::{Decoder, Encoder, RSIndexResult, doc_ids_only::DocIdsOnly};

mod c_mocks;

#[test]
fn test_encode_doc_ids_only() {
    // Test cases for the doc ids only encoder and decoder.
    let tests = [
        // (delta, expected encoding)
        (0, vec![0]),
        (10, vec![10]),
        (256, vec![129, 0]),
        (65536, vec![130, 255, 0]),
        (u16::MAX as u32, vec![130, 254, 127]),
        (u32::MAX, vec![142, 254, 254, 254, 127]),
    ];
    let doc_id = 4294967296;

    for (delta, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::term().doc_id(doc_id);

        let bytes_written =
            DocIdsOnly::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());
        let record_decoded =
            DocIdsOnly::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_doc_ids_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let mut buf = [0u8; 3];
    let buf = &mut buf[0..1];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::term().doc_id(10).frequency(5);
    let res = DocIdsOnly::encode(&mut cursor, 256, &record);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_doc_ids_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = DocIdsOnly::decode_new(&mut cursor, 100);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}
