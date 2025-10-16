/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::{IndexReader, InvertedIndex, RSIndexResult, doc_ids_only::DocIdsOnly};

mod c_mocks;

#[test]
fn test_inverted_index_usage() {
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, DocIdsOnly);

    for id in 0..100_000 {
        ii.add_record(&RSIndexResult::default().doc_id(id)).unwrap();
    }

    assert_eq!(ii.unique_docs(), 100_000);

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for expected_id in 0..10_000 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        for expected_id in 50_505..55_000 {
            let found = reader.seek_record(expected_id, &mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        reader.skip_to(99_000);

        for expected_id in 99_000..100_000 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Remove the first 50_000 documents
    let delta = ii
        .scan_gc(|doc_id| doc_id >= 50_000, |_, _| {})
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 50_000);
    assert_eq!(ii.unique_docs(), 50_000);

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for expected_id in 50_000..100_000 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Remove the documens in the last block
    let delta = ii
        .scan_gc(|doc_id| doc_id < 99_000, |_, _| {})
        .unwrap()
        .unwrap();

    // TODO: add new records ones the ownership wrapper for inverted index exists here
    // This will allow us to check the last block is not modified.
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 1_000);
    assert_eq!(ii.unique_docs(), 49_000);

    // Remove all the records and check that the index can still be used
    let delta = ii.scan_gc(|_| false, |_, _| {}).unwrap().unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 49_000);
    assert_eq!(ii.unique_docs(), 0);
    assert_eq!(ii.number_of_blocks(), 0);

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        assert!(
            !reader.next_record(&mut result).unwrap(),
            "there is nothing to read"
        );
    }

    // Make the new entries u32::MAX apart. This will allow us to collect every second
    // entry and cause a delta that is too big, thus causing the blocks to split.
    for i in 0..10_000 {
        ii.add_record(&RSIndexResult::default().doc_id(i * (u32::MAX as t_docId)))
            .unwrap();
    }

    assert_eq!(ii.unique_docs(), 10_000);
    assert_eq!(ii.number_of_blocks(), 10);

    let delta = ii
        .scan_gc(|doc_id| doc_id % (u32::MAX as t_docId * 2) == 0, |_, _| {})
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 5_000);
    assert_eq!(ii.unique_docs(), 5_000);
    assert_eq!(
        ii.number_of_blocks(),
        5000,
        "every record will be on its own block"
    );

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for i in 0..5_000 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, i * (u32::MAX as t_docId * 2));
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }
}
