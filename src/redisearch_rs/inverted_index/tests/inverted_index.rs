/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::{
    IndexBlock, IndexReader, InvertedIndex, RSIndexResult, doc_ids_only::DocIdsOnly,
};

mod c_mocks;

#[test]
fn test_inverted_index_usage() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);

    for id in 0..3_200 {
        ii.add_record(&RSIndexResult::default().doc_id(id)).unwrap();
    }

    assert_eq!(ii.unique_docs(), 3_200);

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        // Test reading across block boundaries
        for expected_id in 0..1_100 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        // Test skipping across block boundaries
        for expected_id in 1_805..2_200 {
            let found = reader.seek_record(expected_id, &mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        // Test skipping to a block will begin at the start of the block
        reader.skip_to(3_100);

        for expected_id in 3_000..3_200 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Remove the first 2_000 documents
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

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for expected_id in 2_000..3_200 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, expected_id);
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }

    // Remove the documents in the last block
    let delta = ii
        .scan_gc(
            |doc_id| doc_id < 3_000,
            None::<fn(&RSIndexResult, &IndexBlock)>,
        )
        .unwrap()
        .unwrap();

    // TODO: add new records ones the ownership wrapper for inverted index exists here
    // This will allow us to check the last block is not modified.
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 200);
    assert_eq!(ii.unique_docs(), 1_000);

    // Remove all the records and check that the index can still be used
    let delta = ii
        .scan_gc(|_| false, None::<fn(&RSIndexResult, &IndexBlock)>)
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 1_000);
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
    for i in 0..1_002 {
        ii.add_record(&RSIndexResult::default().doc_id(i * (u32::MAX as t_docId)))
            .unwrap();
    }

    assert_eq!(ii.unique_docs(), 1_002);
    assert_eq!(ii.number_of_blocks(), 2);

    let delta = ii
        .scan_gc(
            |doc_id| doc_id % (u32::MAX as t_docId * 2) == 0,
            None::<fn(&RSIndexResult, &IndexBlock)>,
        )
        .unwrap()
        .unwrap();
    let apply_info = ii.apply_gc(delta);

    assert_eq!(apply_info.entries_removed, 501);
    assert_eq!(ii.unique_docs(), 501);
    assert_eq!(
        ii.number_of_blocks(),
        501,
        "every record will be on its own block"
    );

    {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::default();

        for i in 0..501 {
            let found = reader.next_record(&mut result).unwrap();

            assert!(found);
            assert_eq!(result.doc_id, i * (u32::MAX as t_docId * 2));
        }

        assert!(!reader.next_record(&mut result).unwrap(), "no more records");
    }
}
