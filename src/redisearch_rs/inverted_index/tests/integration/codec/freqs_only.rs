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

/// Helper to encode a sequence of (delta, freq) records using FreqsOnly.
fn encode_freqs_only(records: &[(u32, u32)]) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    for &(delta, freq) in records {
        let record = RSIndexResult::virt().frequency(freq);
        FreqsOnly::encode(&mut buf, delta, &record).expect("to encode");
    }
    buf.into_inner()
}

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

#[test]
fn test_seek_freqs_only() {
    // Records: doc_ids 10, 20, 30, 35, 55, 60
    let buf = encode_freqs_only(&[
        (0, 1),  // doc_id = 10
        (10, 2), // doc_id = 20
        (10, 3), // doc_id = 30
        (5, 4),  // doc_id = 35
        (20, 5), // doc_id = 55
        (5, 6),  // doc_id = 60
    ]);
    let mut cursor = Cursor::new(buf.as_ref());
    let mut result = RSIndexResult::virt();

    // Seek to 30 (skips first two records)
    let found = FreqsOnly::seek(&mut cursor, 10, 30, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 30);
    assert_eq!(result.freq, 3);

    // Seek to 40 from base 30 (should land on 55)
    let found = FreqsOnly::seek(&mut cursor, 30, 40, &mut result).expect("seek");
    assert!(found);
    assert_eq!(result.doc_id, 55);
    assert_eq!(result.freq, 5);

    // Seek past end
    let found = FreqsOnly::seek(&mut cursor, 55, 70, &mut result).expect("seek");
    assert!(!found);
}
