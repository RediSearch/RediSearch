/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use ffi::{RSQueryTerm, t_fieldMask};
use inverted_index::{
    Decoder, Encoder, RSIndexResult,
    full::{Full, FullWide},
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
fn test_encode_full() {
    // Test cases for the full encoder and decoder.
    let tests = [
        // (delta, frequency, field mask, term offsets vector, expected encoding)
        (0, 1, 1, vec![1i8, 2, 3], vec![0, 0, 1, 1, 3, 1, 2, 3]),
        (
            10,
            5,
            u32::MAX as t_fieldMask,
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
            u16::MAX as u32,
            1,
            1,
            vec![1, 2, 3],
            vec![1, 255, 255, 1, 1, 3, 1, 2, 3],
        ),
        (
            u32::MAX,
            1,
            1,
            vec![1, 2, 3],
            vec![3, 255, 255, 255, 255, 1, 1, 3, 1, 2, 3],
        ),
        (
            u32::MAX,
            u32::MAX,
            u32::MAX as t_fieldMask,
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

        let record = TestTermRecord::new(doc_id, field_mask, freq, offsets);

        let bytes_written =
            Full::encode(&mut buf, delta, &record.record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());

        let record_decoded =
            Full::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

        assert_eq!(
            TermRecordCompare(&record_decoded),
            TermRecordCompare(&record.record)
        );
    }
}

#[test]
fn test_encode_full_wide() {
    // Test cases for the full wide encoder and decoder.
    let tests = [
        // (delta, frequency, field mask, term offsets vector, expected encoding)
        (0, 1, 1, vec![1i8, 2, 3], vec![0, 0, 1, 3, 1, 1, 2, 3]),
        (
            10,
            5,
            u32::MAX as t_fieldMask,
            vec![1i8, 2, 3, 4],
            vec![0, 10, 5, 4, 142, 254, 254, 254, 127, 1, 2, 3, 4],
        ),
        (256, 1, 1, vec![1, 2, 3], vec![1, 0, 1, 1, 3, 1, 1, 2, 3]),
        (
            65536,
            1,
            1,
            vec![1, 2, 3],
            vec![2, 0, 0, 1, 1, 3, 1, 1, 2, 3],
        ),
        (
            u16::MAX as u32,
            1,
            1,
            vec![1, 2, 3],
            vec![1, 255, 255, 1, 3, 1, 1, 2, 3],
        ),
        (
            u32::MAX as u32,
            1,
            1,
            vec![1, 2, 3],
            vec![3, 255, 255, 255, 255, 1, 3, 1, 1, 2, 3],
        ),
        // field mask larger than 32 bits
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX as u32,
            u32::MAX,
            u32::MAX as t_fieldMask,
            vec![1; 100],
            vec![
                15, 255, 255, 255, 255, 255, 255, 255, 255, 100, 142, 254, 254, 254, 127, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            ],
        ),
        #[cfg(target_pointer_width = "64")]
        (
            u32::MAX as u32,
            u32::MAX,
            u128::MAX,
            vec![1; 100],
            vec![
                15, 255, 255, 255, 255, 255, 255, 255, 255, 100, 130, 254, 254, 254, 254, 254, 254,
                254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 127, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1,
            ],
        ),
    ];
    let doc_id = 4294967296;

    for (delta, freq, field_mask, offsets, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());

        let record = TestTermRecord::new(doc_id, field_mask, freq, offsets);

        let bytes_written =
            FullWide::encode(&mut buf, delta, &record.record).expect("to encode freqs only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());
        let record_decoded =
            FullWide::decode_new(&mut buf, prev_doc_id).expect("to decode freqs only record");

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
    let buf = [0u8; 100];
    let mut cursor = Cursor::new(buf);

    let record = TestTermRecord::new(10, u32::MAX as t_fieldMask + 1, 1, vec![1]);
    let _res = Full::encode(&mut cursor, 0, &record.record);
}

#[test]
fn test_encode_full_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 3];
    let mut cursor = Cursor::new(buf);
    let record = TestTermRecord::new(10, 1, 1, vec![1]);

    let res = Full::encode(&mut cursor, 0, &record.record);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);

    let res = FullWide::encode(&mut cursor, 0, &record.record);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_full_input_too_small() {
    // Encoded data is too short.
    let buf = vec![0, 0];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = Full::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);

    let res = FullWide::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_full_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = Full::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);

    let res = FullWide::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_offsets_too_short() {
    // offsets claims to have 3 elements but actually have only 2.
    let buf = vec![0, 0, 1, 1, 3, 1, 2];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = Full::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);

    let res = FullWide::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_seek_full() {
    let buf = vec![
        0, 0, 1, 10, 3, 1, 2,
        3, // First record: 0 delta; 1 freqs; 10 field mask; 3 offsets len
        0, 10, 2, 12, 4, 1, 2, 3,
        4, // Second record: 10 delta; 2 freqs; 12 field mask; 4 offsets len
        0, 10, 3, 13, 4, 5, 6, 7,
        8, // Third record: 10 delta; 3 freqs; 13 field mask; 4 offsets len
        0, 5, 1, 10, 2, 10,
        11, // Fourth record: 5 delta; 1 freqs; 10 field mask; 2 offsets len
        0, 20, 9, 4, 2, 20,
        21, // Fifth record: 20 delta; 9 freqs; 4 field mask; 2 offsets len
        0, 5, 1, 4, 2, 20, 21, // Sixth record: 5 delta; 1 freqs; 4 field mask; 2 offsets len
    ];
    let mut buf = Cursor::new(buf.as_ref());

    let mut record_decoded = RSIndexResult::term();

    let found =
        Full::seek(&mut buf, 10, 30, &mut record_decoded).expect("to decode freqs offsets record");

    let record_expected = TestTermRecord::new(30, 13, 3, vec![5i8, 6, 7, 8]);

    assert!(found);
    assert_eq!(
        TermRecordCompare(&record_decoded),
        TermRecordCompare(&record_expected.record)
    );

    let found =
        Full::seek(&mut buf, 30, 40, &mut record_decoded).expect("to decode freqs offsets record");

    let record_expected = TestTermRecord::new(55, 4, 9, vec![20i8, 21]);

    assert!(found);
    assert_eq!(
        TermRecordCompare(&record_decoded),
        TermRecordCompare(&record_expected.record)
    );

    let found =
        Full::seek(&mut buf, 55, 70, &mut record_decoded).expect("to decode fields offsets record");

    assert!(!found);
}

#[test]
fn test_seek_full_wide() {
    let buf = vec![
        0, 0, 1, 3, 10, 1, 2,
        3, // First record: 0 delta; 1 freqs; 10 field mask; 3 offsets len
        0, 10, 2, 4, 12, 1, 2, 3,
        4, // Second record: 10 delta; 2 freqs; 12 field mask; 4 offsets len
        0, 10, 3, 4, 13, 5, 6, 7,
        8, // Third record: 10 delta; 3 freqs; 13 field mask; 4 offsets len
        0, 5, 1, 2, 10, 10,
        11, // Fourth record: 5 delta; 1 freqs; 10 field mask; 2 offsets len
        0, 20, 9, 2, 4, 20,
        21, // Fifth record: 20 delta; 9 freqs; 4 field mask; 2 offsets len
        0, 5, 1, 2, 4, 20, 21, // Sixth record: 5 delta; 1 freqs; 4 field mask; 2 offsets len
    ];
    let mut buf = Cursor::new(buf.as_ref());

    let mut record_decoded = RSIndexResult::term();

    let found =
        FullWide::seek(&mut buf, 10, 30, &mut record_decoded).expect("to decode full record");

    let record_expected = TestTermRecord::new(30, 13, 3, vec![5i8, 6, 7, 8]);

    assert!(found);
    assert_eq!(
        TermRecordCompare(&record_decoded),
        TermRecordCompare(&record_expected.record)
    );

    let found =
        FullWide::seek(&mut buf, 30, 40, &mut record_decoded).expect("to decode full record");

    let record_expected = TestTermRecord::new(55, 4, 9, vec![20i8, 21]);

    assert!(found);
    assert_eq!(
        TermRecordCompare(&record_decoded),
        TermRecordCompare(&record_expected.record)
    );

    let found = FullWide::seek(&mut buf, 55, 70, &mut record_decoded)
        .expect("to decode fields offsets record");

    assert!(!found);
}
