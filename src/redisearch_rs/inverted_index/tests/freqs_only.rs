/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use inverted_index::{Decoder, Encoder, RSIndexResult, freqs_only::FreqsOnly};

mod c_mocks;

#[test]
fn test_encode_freqs_only() {
    // Test cases for the frequencies only encoder and decoder.
    let tests = [
        // (frequency, delta, expected encoding)
        (0, 0, vec![0, 0, 0]),
        (0, 1, vec![0, 1, 0]),
        (2, 0, vec![0, 0, 2]),
        (2, 1, vec![0, 1, 2]),
        (256, 0, vec![4, 0, 0, 1]),
        (256, 256, vec![5, 0, 1, 0, 1]),
        (2, 65536, vec![2, 0, 0, 1, 2]),
        (
            u16::MAX as u32 + 1,
            u16::MAX as u32 + 1,
            vec![10, 0, 0, 1, 0, 0, 1],
        ),
        (2, u32::MAX, vec![3, 255, 255, 255, 255, 2]),
        (
            u32::MAX,
            u32::MAX,
            vec![15, 255, 255, 255, 255, 255, 255, 255, 255],
        ),
    ];
    let doc_id = 4294967296;

    for (freq, delta, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::virt().doc_id(doc_id).frequency(freq);

        let bytes_written =
            FreqsOnly::encode(&mut buf, delta, &record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());

        let record_decoded =
            FreqsOnly::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_freqs_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 1];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::virt().doc_id(10).frequency(5);
    let res = FreqsOnly::encode(&mut cursor, 0, &record);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_freqs_only_input_too_small() {
    // Encoded data is one byte too short.
    let buf = vec![0, 0];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FreqsOnly::decode_new(&mut buf, 100);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_freqs_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut buf = Cursor::new(buf.as_ref());

    let res = FreqsOnly::decode_new(&mut buf, 100);

    assert!(res.is_err());
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}
