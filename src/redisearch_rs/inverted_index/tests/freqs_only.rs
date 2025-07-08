/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use inverted_index::{
    Decoder, DecoderResult, Delta, Encoder, RSIndexResult, freqs_only::FreqsOnly,
};

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
            u16::MAX as usize + 1,
            vec![10, 0, 0, 1, 0, 0, 1],
        ),
        (2, u32::MAX as usize, vec![3, 255, 255, 255, 255, 2]),
        (
            u32::MAX,
            u32::MAX as usize,
            vec![15, 255, 255, 255, 255, 255, 255, 255, 255],
        ),
    ];
    let doc_id = 4294967296;

    for (freq, delta, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::freqs_only(doc_id, freq);

        let bytes_written = FreqsOnly::default()
            .encode(&mut buf, Delta::new(delta), &record)
            .expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let DecoderResult::Record(record_decoded) = FreqsOnly
            .decode(&mut buf, prev_doc_id)
            .expect("to decode freqs only record")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_freqs_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let mut buf = [0u8; 3];
    let buf = &mut buf[0..1];
    let mut cursor = Cursor::new(buf);

    let record = RSIndexResult::freqs_only(10, 5);
    let res = FreqsOnly::default().encode(&mut cursor, Delta::new(0), &record);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_freqs_only_input_too_small() {
    // Encoded data is one byte too short.
    let buf = vec![0, 0];
    let mut cursor = Cursor::new(buf);
    let res = FreqsOnly.decode(&mut cursor, 100);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_freqs_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf);
    let res = FreqsOnly.decode(&mut cursor, 100);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
#[should_panic]
fn test_encode_freqs_only_delta_overflow() {
    // Encoder only supports 32 bits delta and will panic if larger
    let mut buf = Cursor::new(vec![0; 3]);

    let record = RSIndexResult::freqs_only(10, 5);
    let _res = FreqsOnly::default().encode(&mut buf, Delta::new(u32::MAX as usize + 1), &record);
}
