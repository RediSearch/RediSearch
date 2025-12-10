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
use inverted_index::{Decoder, Encoder, RSIndexResult, raw_doc_ids_only::RawDocIdsOnly};

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
fn test_encode_raw_doc_ids_only() {
    // Test cases for the raw doc ids encoder and decoder.
    let tests = [
        // (delta, expected encoding - raw 4-byte little-endian)
        (0, vec![0, 0, 0, 0]),
        (10, vec![10, 0, 0, 0]),
        (256, vec![0, 1, 0, 0]),
        (65536, vec![0, 0, 1, 0]),
        (u16::MAX as u32, vec![255, 255, 0, 0]),
        (u32::MAX, vec![255, 255, 255, 255]),
    ];
    let doc_id = 4294967296;

    for (delta, expected_encoding) in tests {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::term().doc_id(doc_id);

        let bytes_written = RawDocIdsOnly::encode(&mut buf, delta, &record)
            .expect("to encode raw doc ids only record");

        assert_eq!(bytes_written, expected_encoding.len());
        assert_eq!(buf.get_ref(), &expected_encoding);

        // decode
        buf.set_position(0);
        let prev_doc_id = doc_id - (delta as u64);
        let buf = buf.into_inner();
        let mut buf = Cursor::new(buf.as_ref());

        let record_decoded = RawDocIdsOnly::decode_new(&mut buf, prev_doc_id)
            .expect("to decode raw doc ids only record");

        assert_eq!(record_decoded, record);
    }
}

#[test]
fn test_encode_raw_doc_ids_only_output_too_small() {
    // Not enough space in the buffer to write the encoded data.
    let buf = [0u8; 1];
    let mut cursor = Cursor::new(buf);
    let record = inverted_index::RSIndexResult::virt();

    let res = RawDocIdsOnly::encode(&mut cursor, 0, &record);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::WriteZero);
}

#[test]
fn test_decode_raw_doc_ids_only_input_too_small() {
    // Encoded data is too short.
    let buf = vec![0, 0];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = RawDocIdsOnly::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_decode_raw_doc_ids_only_empty_input() {
    // Try decoding an empty buffer.
    let buf = vec![];
    let mut cursor = Cursor::new(buf.as_ref());

    let res = RawDocIdsOnly::decode_new(&mut cursor, 100);
    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn test_seek_raw_doc_ids_only() {
    let buf = vec![
        0, 0, 0, 0, // First delta
        5, 0, 0, 0, // Second delta
        6, 0, 0, 0, // Third delta
        8, 0, 0, 0, // Fourth delta
        12, 0, 0, 0, // Fifth delta
        13, 0, 0, 0, // Sixth delta
    ];
    let mut buf = Cursor::new(buf.as_ref());

    let mut record_decoded = RSIndexResult::term();

    let found = RawDocIdsOnly::seek(&mut buf, 10, 16, &mut record_decoded)
        .expect("to decode raw docs ids only record");

    assert!(found);
    assert_eq!(record_decoded, RSIndexResult::term().doc_id(16));

    let found = RawDocIdsOnly::seek(&mut buf, 10, 20, &mut record_decoded)
        .expect("to decode raw docs ids only record");

    assert!(found);
    assert_eq!(record_decoded, RSIndexResult::term().doc_id(22));

    let found = RawDocIdsOnly::seek(&mut buf, 10, 50, &mut record_decoded)
        .expect("to decode raw docs ids only record");

    assert!(!found);
}

/// Test InvertedIndex<RawDocIdsOnly> with GC operations to ensure complete coverage
/// for raw DocID encoding when removing the second test run with RAW_DOCID_ENCODING.
#[test]
fn test_inverted_index_raw_doc_ids_gc() {
    use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
    use inverted_index::{IndexBlock, IndexReader, InvertedIndex, raw_doc_ids_only::RawDocIdsOnly};

    let mut ii = InvertedIndex::<RawDocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);

    // Add 3200 documents (will span multiple blocks since RECOMMENDED_BLOCK_ENTRIES is 1000)
    for id in 0..3_200 {
        ii.add_record(&RSIndexResult::default().doc_id(id)).unwrap();
    }

    assert_eq!(ii.unique_docs(), 3_200);

    // Verify all documents can be read
    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for expected_id in 0..3_200 {
            let found = reader.next_record(&mut result).unwrap();
            assert!(found, "expected to find doc_id {}", expected_id);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Test GC: Remove the first 2000 documents
    let delta = ii
        .scan_gc(
            |doc_id| doc_id >= 2_000,
            None::<fn(&RSIndexResult, &IndexBlock)>,
        )
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 2_000);
    assert_eq!(ii.unique_docs(), 1_200);

    // Verify remaining documents can be read
    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for expected_id in 2_000..3_200 {
            let found = reader.next_record(&mut result).unwrap();
            assert!(found, "expected to find doc_id {}", expected_id);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Test GC: Remove documents in the last block
    let delta = ii
        .scan_gc(
            |doc_id| doc_id < 3_000,
            None::<fn(&RSIndexResult, &IndexBlock)>,
        )
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 200);
    assert_eq!(ii.unique_docs(), 1_000);

    // Test GC: Remove all remaining records
    let delta = ii
        .scan_gc(|_| false, None::<fn(&RSIndexResult, &IndexBlock)>)
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 1_000);
    assert_eq!(ii.unique_docs(), 0);
    assert_eq!(ii.number_of_blocks(), 0);

    // Verify empty index still works
    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();
        assert!(
            !reader.next_record(&mut result).unwrap(),
            "there is nothing to read"
        );
    }

    // Test with large deltas that cause block splits
    // RawDocIdsOnly uses 4-byte encoding, so u32::MAX is the max delta
    for i in 0..100 {
        ii.add_record(&RSIndexResult::default().doc_id(i * (u32::MAX as t_docId)))
            .unwrap();
    }

    assert_eq!(ii.unique_docs(), 100);

    // GC every second entry (causes large deltas after GC)
    let delta = ii
        .scan_gc(
            |doc_id| doc_id % (u32::MAX as t_docId * 2) == 0,
            None::<fn(&RSIndexResult, &IndexBlock)>,
        )
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 50);
    assert_eq!(ii.unique_docs(), 50);

    // Verify remaining documents can be read with seek
    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for i in 0..50 {
            let target_id = i * (u32::MAX as t_docId * 2);
            let found = reader.seek_record(target_id, &mut result).unwrap();
            assert!(found, "expected to find doc_id {}", target_id);
            assert_eq!(result.doc_id, target_id);
        }
    }
}
