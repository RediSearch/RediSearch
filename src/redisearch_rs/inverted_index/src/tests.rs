/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use core::panic;
use std::{
    io::{Cursor, Read},
    marker::PhantomData,
    ptr,
    sync::atomic,
};

use crate::{
    BlockGcScanResult, Decoder, Encoder, EntriesTrackingIndex, FieldMaskTrackingIndex,
    FilterGeoReader, FilterMaskReader, FilterNumericReader, GcApplyInfo, GcScanDelta, IdDelta,
    IndexBlock, IndexReader, InvertedIndex, NumericFilter, NumericReader, RSAggregateResult,
    RSIndexResult, RSResultData, RSResultKind, RSTermRecord, RepairType,
    debug::{BlockSummary, Summary},
};
use ffi::{GeoDistance_GEO_DISTANCE_M, GeoFilter, t_docId};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_HasMultiValue, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreNumeric, IndexFlags_Index_StoreTermOffsets, IndexFlags_Index_WideSchema,
};
use pretty_assertions::assert_eq;
use smallvec::smallvec;
use thin_vec::medium_thin_vec;

/// Helper function to create an `IndexBlock` for testing purposes.
/// This provides default values for the scoring metadata fields.
fn test_block(
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,
    buffer: Vec<u8>,
) -> IndexBlock {
    IndexBlock {
        first_doc_id,
        last_doc_id,
        num_entries,
        max_freq: 0,
        max_doc_score: 0.0,
        min_doc_len: u32::MAX,
        buffer,
    }
}

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
pub extern "C" fn Term_Free(_t: *mut ffi::RSQueryTerm) {
    panic!("No test created a term record");
}

/// Dummy encoder which allows defaults for testing, encoding only the delta
#[derive(Clone)]
struct Dummy;

impl Encoder for Dummy {
    type Delta = u32;

    fn encode<W: std::io::Write + std::io::Seek>(
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_be_bytes())?;

        Ok(4)
    }
}

#[test]
fn memory_usage() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let empty_size = 24;

    assert_eq!(ii.memory_usage(), empty_size);

    let record = RSIndexResult::default().doc_id(10);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.memory_usage(), empty_size + mem_growth);
}

#[test]
fn adding_records() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::default().doc_id(10);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth,
        8 + IndexBlock::STACK_SIZE + 4,
        "header of the thin vec storing the blocks (8 bytes), size of the first index block (48 bytes) plus 4 bytes for the encoded delta"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1);

    let record = RSIndexResult::default().doc_id(11);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth, 5,
        "buffer needs to grow to 9 bytes to hold a total of 8 bytes"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(ii.blocks[0].num_entries, 2);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 11);
    assert_eq!(ii.n_unique_docs, 2);
}

#[test]
fn adding_same_record_twice() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::default().doc_id(10);

    ii.add_record(&record).unwrap();
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.flags(), IndexFlags_Index_DocIdsOnly);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth, 0,
        "duplicate record should not be written by default"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(
        ii.blocks[0].buffer,
        [0, 0, 0, 0],
        "buffer should remain unchanged"
    );
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1, "this second doc was not added");
    assert_eq!(ii.flags(), IndexFlags_Index_DocIdsOnly);

    /// Dummy encoder which allows duplicates for testing
    #[derive(Clone)]
    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[255])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<AllowDupsDummy>::new(IndexFlags_Index_DocIdsOnly);

    ii.add_record(&record).unwrap();
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [255]);
    assert_eq!(ii.flags(), IndexFlags_Index_DocIdsOnly);

    let _mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(
        ii.blocks[0].buffer,
        [255, 255],
        "buffer should contain two entries"
    );
    assert_eq!(ii.blocks[0].num_entries, 2);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(
        ii.n_unique_docs, 1,
        "this doc was added but should not affect the count"
    );
    assert_eq!(
        ii.flags(),
        IndexFlags_Index_DocIdsOnly | IndexFlags_Index_HasMultiValue,
        "the index now has multi values"
    );
}

#[test]
fn adding_creates_new_blocks_when_entries_is_reached() {
    /// Dummy encoder which only allows 2 entries per block for testing
    #[derive(Clone)]
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: u16 = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<SmallBlocksDummy>::new(IndexFlags_Index_DocIdsOnly);

    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    assert_eq!(
        mem_growth,
        8 + IndexBlock::STACK_SIZE + 1,
        "header of the thin vec storing the blocks (8 bytes), size of the first index block (48 bytes) plus the byte written"
    );
    assert_eq!(ii.blocks.len(), 1);
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(11)).unwrap();
    assert_eq!(mem_growth, 1, "buffer needs to grow for the new byte");
    assert_eq!(ii.blocks.len(), 1);

    // 3 entry should create a new block
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(12)).unwrap();
    assert_eq!(
        mem_growth,
        IndexBlock::STACK_SIZE + 1,
        "size of the new index block (48 bytes) plus the byte written"
    );
    assert_eq!(
        ii.blocks.len(),
        2,
        "should create a new block after reaching the limit"
    );
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(13)).unwrap();
    assert_eq!(mem_growth, 1, "buffer needs to grow for the new byte");
    assert_eq!(ii.blocks.len(), 2);

    // But duplicate entry does not go in new block even if the current block is full
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(13)).unwrap();
    assert_eq!(mem_growth, 1, "buffer needs to grow again");
    assert_eq!(
        ii.blocks.len(),
        2,
        "duplicates should stay on the same block"
    );
    assert_eq!(
        ii.blocks[1].num_entries, 3,
        "should have 3 entries in the second block because duplicate was added"
    );
}

#[test]
fn adding_big_delta_makes_new_block() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::default().doc_id(10);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth,
        4 + 8 + IndexBlock::STACK_SIZE,
        "should write 4 bytes for delta, 8 bytes of thin vec header, and STACK_SIZE bytes for the index block"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1);

    // This will create a delta that is larger than the default u32 acceptable delta size
    let doc_id = (u32::MAX as u64) + 11;
    let record = RSIndexResult::default().doc_id(doc_id);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth,
        4 + IndexBlock::STACK_SIZE,
        "should write 4 bytes for delta and STACK_SIZE bytes for the new index block"
    );
    assert_eq!(ii.blocks.len(), 2);
    assert_eq!(ii.blocks[1].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[1].num_entries, 1);
    assert_eq!(ii.blocks[1].first_doc_id, doc_id);
    assert_eq!(ii.blocks[1].last_doc_id, doc_id);
    assert_eq!(ii.n_unique_docs, 2);
}

#[test]
fn adding_tracks_max_freq() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // Add a record with freq = 5
    let record = RSIndexResult::default().doc_id(10).frequency(5);
    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks[0].max_freq(), 5, "max_freq should be 5");
    assert!(
        ii.blocks[0].has_scoring_metadata(),
        "block should have scoring metadata"
    );

    // Add a record with freq = 3 (lower than current max)
    let record = RSIndexResult::default().doc_id(11).frequency(3);
    ii.add_record(&record).unwrap();

    assert_eq!(
        ii.blocks[0].max_freq(),
        5,
        "max_freq should still be 5 (the maximum)"
    );

    // Add a record with freq = 10 (higher than current max)
    let record = RSIndexResult::default().doc_id(12).frequency(10);
    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks[0].max_freq(), 10, "max_freq should now be 10");
}

#[test]
fn has_scoring_metadata_returns_false_for_empty_block() {
    // A block with num_entries > 0 but max_freq = 0 indicates no metadata
    let block = test_block(10, 10, 1, vec![0, 0, 0, 0]);
    assert!(
        !block.has_scoring_metadata(),
        "block with max_freq=0 should not have scoring metadata"
    );
}

#[test]
fn has_scoring_metadata_returns_true_for_block_with_freq() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // Add a record with freq = 1
    let record = RSIndexResult::default().doc_id(10).frequency(1);
    ii.add_record(&record).unwrap();

    assert!(
        ii.blocks[0].has_scoring_metadata(),
        "block with max_freq > 0 should have scoring metadata"
    );
}

// An `IndexBlock` is 48 bytes so we want to carefully control the growth strategy used by it to
// limit memory usage. This test ensures the blocks field of an inverted index only grows by one
// element at a time.
#[test]
fn adding_ii_blocks_growth_strategy() {
    /// Dummy encoder which only allows 2 entries per block for testing
    #[derive(Clone)]
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: u16 = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    impl Decoder for SmallBlocksDummy {
        fn decode<'index>(
            _cursor: &mut Cursor<&'index [u8]>,
            _base: t_docId,
            _result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            unimplemented!("not used by this test")
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            unimplemented!("not used by this test")
        }
    }

    let mut ii = InvertedIndex::<SmallBlocksDummy>::new(IndexFlags_Index_DocIdsOnly);

    assert_eq!(
        ii.blocks.capacity(),
        0,
        "initially there are no blocks allocated"
    );

    // Test when a new block are added normally
    ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    ii.add_record(&RSIndexResult::default().doc_id(11)).unwrap();
    assert_eq!(
        ii.blocks.capacity(),
        1,
        "we should only have grown the capacity by one"
    );

    ii.add_record(&RSIndexResult::default().doc_id(12)).unwrap();
    assert_eq!(
        ii.blocks.capacity(),
        2,
        "only one new capacity should be added"
    );

    // Test when a new block is added due to delta overflow
    ii.add_record(&RSIndexResult::default().doc_id(u32::MAX as u64 + 13))
        .unwrap();
    assert_eq!(
        ii.blocks.capacity(),
        3,
        "only one new capacity should be added"
    );

    // Make sure GC is also smart to remove extra capacity
    ii.apply_gc(GcScanDelta {
        last_block_idx: 2,
        last_block_num_entries: 1,
        deltas: vec![
            BlockGcScanResult {
                index: 0,
                repair: RepairType::Delete {
                    n_unique_docs_removed: 1,
                },
            },
            BlockGcScanResult {
                index: 1,
                repair: RepairType::Delete {
                    n_unique_docs_removed: 1,
                },
            },
        ],
    });

    assert_eq!(
        ii.blocks.capacity(),
        1,
        "no extra capacity should be dangling"
    );
}

#[test]
fn adding_tracks_entries() {
    let mut ii = EntriesTrackingIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    let empty_size = 32;
    assert_eq!(ii.memory_usage(), empty_size);
    assert_eq!(ii.number_of_entries(), 0);

    let record = RSIndexResult::default().doc_id(10);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.memory_usage(), empty_size + mem_growth);
    assert_eq!(ii.number_of_entries(), 1);

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.number_of_entries(), 2);
}

#[test]
fn adding_track_field_mask() {
    let mut ii = FieldMaskTrackingIndex::<Dummy>::new(IndexFlags_Index_StoreFieldFlags);

    assert_eq!(ii.memory_usage(), 40);
    assert_eq!(ii.field_mask(), 0);

    let record = RSIndexResult::default().doc_id(10).field_mask(0b101);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth,
        8 + IndexBlock::STACK_SIZE + 4,
        "header of the thin vec storing the blocks (8 bytes), size of the first index block (48 bytes) plus 4 bytes for the encoded result"
    );
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::default().doc_id(11).field_mask(0b101);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 5);
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::default().doc_id(12).field_mask(0b011);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 5);
    assert_eq!(ii.field_mask(), 0b111);
}

#[test]
fn u32_delta_overflow() {
    let delta = <u32 as IdDelta>::from_u64(u32::MAX as u64 + 1);

    assert_eq!(
        delta, None,
        "Delta will overflow, so should request a new block for encoding"
    );
}

impl Decoder for Dummy {
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        prev_doc_id: u64,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let mut buffer = [0; 4];
        cursor.read_exact(&mut buffer)?;

        let delta = u32::from_be_bytes(buffer);
        let doc_id = prev_doc_id + (delta as u64);

        result.doc_id = doc_id;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::default()
    }
}

#[test]
fn reading_records() {
    // Make two blocks. The first with two records and the second with one record
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, vec![0, 0, 0, 0, 0, 0, 0, 1]),
        test_block(100, 100, 0, vec![0, 0, 0, 0]),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(10));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(11));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(100));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(!found);
}

#[test]
fn reading_over_empty_blocks() {
    // Make three blocks with the second one being empty and the other two containing one entries.
    // The second should automatically continue from the third block
    let blocks = medium_thin_vec![
        test_block(10, 10, 1, vec![0, 0, 0, 0]),
        test_block(30, 30, 1, vec![0, 0, 0, 0]),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(10));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(30));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(!found, "should not return any more records");
}

#[test]
fn read_using_the_first_block_id_as_the_base() {
    #[derive(Clone)]
    struct FirstBlockIdDummy;

    impl Encoder for FirstBlockIdDummy {
        type Delta = u32;

        fn encode<W: std::io::Write + std::io::Seek>(
            _writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            panic!("This test won't encode anything")
        }
    }

    impl Decoder for FirstBlockIdDummy {
        fn decode<'index>(
            cursor: &mut Cursor<&'index [u8]>,
            prev_doc_id: u64,
            result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            let mut buffer = [0; 4];
            cursor.read_exact(&mut buffer)?;

            let delta = u32::from_be_bytes(buffer);
            let doc_id = prev_doc_id + (delta as u64);

            result.doc_id = doc_id;

            Ok(())
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::default()
        }

        fn base_id(block: &IndexBlock, _last_doc_id: ffi::t_docId) -> ffi::t_docId {
            block.first_doc_id
        }
    }

    // Make a block with three different doc IDs
    let blocks = medium_thin_vec![test_block(
        10,
        12,
        3,
        vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2]
    )];
    let ii = InvertedIndex::<FirstBlockIdDummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(10));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(11));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(12));
}

#[test]
fn seeking_records() {
    // Make two blocks - the last one with four records
    let blocks = medium_thin_vec![
        test_block(10, 12, 3, vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1]),
        test_block(
            100,
            108,
            4,
            vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 5]
        ),
    ];

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    let found = ir
        .seek_record(101, &mut result)
        .expect("to be able to read from the buffer");

    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(101));

    let found = ir
        .seek_record(105, &mut result)
        .expect("to be able to read from the buffer");

    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::virt().doc_id(108),
        "should seek to the next highest ID"
    );

    let found = ir
        .seek_record(200, &mut result)
        .expect("to be able to read from the buffer");
    assert!(!found);
}

#[test]
fn index_reader_construction_with_no_blocks() {
    let ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    assert_eq!(ir.next_record(&mut result).unwrap(), false);
    ir.reset();
    assert_eq!(ir.seek_record(5, &mut result).unwrap(), false);
}

#[test]
fn index_reader_skip_to() {
    let blocks = medium_thin_vec![
        test_block(10, 15, 2, vec![0, 0, 0, 0, 0, 0, 0, 5]),
        test_block(16, 17, 2, vec![0, 0, 0, 0, 0, 0, 0, 1]),
        test_block(20, 24, 2, vec![0, 0, 0, 0, 0, 0, 0, 4]),
        test_block(30, 30, 1, vec![0, 0, 0, 0]),
        test_block(40, 40, 1, vec![0, 0, 0, 0]),
        test_block(50, 50, 1, vec![0, 0, 0, 0]),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();

    assert_eq!(ir.current_block_idx, 0, "should start at the first block");
    assert_eq!(ir.last_doc_id, 10);

    // Skipping to an ID in the current block should not change anything
    assert!(ir.skip_to(12));
    assert_eq!(ir.current_block_idx, 0, "we are still in the first block");
    assert_eq!(ir.last_doc_id, 10);

    // Skipping to an ID in the next block should move to the next block
    assert!(ir.skip_to(16));
    assert_eq!(ir.current_block_idx, 1, "should be in the second block");
    assert_eq!(ir.last_doc_id, 16);

    // Skipping to an ID in a later block should move to that block
    assert!(ir.skip_to(30));
    assert_eq!(ir.current_block_idx, 3, "should be in the fourth block");
    assert_eq!(ir.last_doc_id, 30);

    // Skipping to an ID between blocks should give the block with the next highest ID
    assert!(ir.skip_to(45));
    assert_eq!(ir.current_block_idx, 5, "should be in the sixth block");
    assert_eq!(ir.last_doc_id, 50);

    // Skipping to an ID beyond the last block should return false and stay at the last block
    assert!(!ir.skip_to(100), "should not find a block for this ID");
    assert_eq!(
        ir.current_block_idx, 5,
        "should still be in the sixth block"
    );
    assert_eq!(ir.last_doc_id, 50);

    // Skipping to an earlier ID should do nothing
    assert!(ir.skip_to(5));
    assert_eq!(
        ir.current_block_idx, 5,
        "should still be in the sixth block"
    );
    assert_eq!(ir.last_doc_id, 50);
}

#[test]
fn reader_reset() {
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, vec![0, 0, 0, 0, 0, 0, 0, 1]),
        test_block(100, 100, 1, vec![0, 0, 0, 0]),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::default();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(10));

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(11));

    assert_eq!(ir.gc_marker, 0);
    ii.gc_marker.fetch_add(1, atomic::Ordering::Relaxed);

    ir.reset();

    assert_eq!(ir.gc_marker, 1);

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::virt().doc_id(10));
}

#[test]
fn reader_needs_revalidation() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::virt().doc_id(10)).unwrap();

    let ir = ii.reader();

    assert!(!ir.needs_revalidation(), "index was not modified yet");

    ii.gc_marker.fetch_add(1, atomic::Ordering::Relaxed);
    assert!(ir.needs_revalidation(), "index was modified");
}

#[test]
fn reader_unique_docs() {
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, vec![0, 0, 0, 0, 0, 0, 0, 1]),
        test_block(100, 100, 1, vec![0, 0, 0, 0]),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let ir = ii.reader();

    assert_eq!(ir.unique_docs(), 3);
}

#[test]
fn reader_has_duplicates() {
    /// Dummy encoder which allows duplicates for testing
    #[derive(Clone)]
    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[255])?;

            Ok(1)
        }
    }

    impl Decoder for AllowDupsDummy {
        fn decode<'index>(
            _cursor: &mut Cursor<&'index [u8]>,
            _base: ffi::t_docId,
            _result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            panic!("This test won't decode anything")
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::default()
        }
    }

    let mut ii = InvertedIndex::<AllowDupsDummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::virt().doc_id(10)).unwrap();

    {
        let ir = ii.reader();
        assert!(!ir.has_duplicates());
    }

    ii.add_record(&RSIndexResult::virt().doc_id(10)).unwrap();
    let ir = ii.reader();
    assert!(ir.has_duplicates(), "should have duplicates");
}

#[test]
fn reader_flags() {
    let mut ii = InvertedIndex::<Dummy>::new(
        IndexFlags_Index_StoreTermOffsets | IndexFlags_Index_WideSchema,
    );
    ii.add_record(&RSIndexResult::virt().doc_id(10)).unwrap();
    let ir = ii.reader();

    assert_eq!(
        ir.flags(),
        IndexFlags_Index_StoreTermOffsets | IndexFlags_Index_WideSchema,
    );
}

#[test]
fn reader_is_index() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::virt().doc_id(10)).unwrap();
    let ir = ii.reader();

    assert!(ir.is_index(&ii));

    let ii2 = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    assert!(!ir.is_index(&ii2));
}

impl<'index, I: Iterator<Item = RSIndexResult<'index>>> IndexReader<'index> for I {
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        match self.next() {
            Some(r) => {
                *result = r;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn seek_record(
        &mut self,
        _doc_id: ffi::t_docId,
        _result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        unimplemented!("This tests won't seek anything")
    }

    fn skip_to(&mut self, _doc_id: t_docId) -> bool {
        unimplemented!("This test won't skip to anything")
    }

    fn reset(&mut self) {
        unimplemented!("This test won't reset")
    }

    fn unique_docs(&self) -> u64 {
        unimplemented!("This test won't count unique docs")
    }

    fn has_duplicates(&self) -> bool {
        false
    }

    fn flags(&self) -> ffi::IndexFlags {
        ffi::IndexFlags_Index_DocIdsOnly
    }

    fn needs_revalidation(&self) -> bool {
        false
    }

    fn refresh_buffer_pointers(&mut self) {
        unimplemented!("This test won't refresh buffer pointers")
    }
}

// Claim iterators are numeric readers so they can be used in tests.
impl<'index, I: Iterator<Item = RSIndexResult<'index>>> NumericReader<'index> for I {}

#[test]
fn reading_filter_based_on_field_mask() {
    // Make an iterator with three records having different field masks. The second record will be
    // filtered out based on the field mask.
    let iter = vec![
        RSIndexResult::default().doc_id(10).field_mask(0b0001),
        RSIndexResult::default().doc_id(11).field_mask(0b0010),
        RSIndexResult::default().doc_id(12).field_mask(0b0100),
    ];

    let mut reader = FilterMaskReader::new(0b0101 as _, iter.into_iter());
    let mut result = RSIndexResult::default();

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::default().doc_id(10).field_mask(0b0001)
    );

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::default().doc_id(12).field_mask(0b0100)
    );
}

#[test]
fn reading_filter_based_on_numeric_filter() {
    // Make an iterator with three records having different numeric values. The second record will be
    // filtered out based on the numeric filter.
    let iter = vec![
        RSIndexResult::numeric(5.0).doc_id(10),
        RSIndexResult::numeric(25.0).doc_id(11),
        RSIndexResult::numeric(15.0).doc_id(12),
    ];

    let filter = NumericFilter {
        min: 0.0,
        max: 15.0,
        min_inclusive: true,
        max_inclusive: true,
        field_spec: ptr::null(),
        geo_filter: ptr::null(),
        ascending: true,
        limit: 10,
        offset: 0,
    };

    let mut reader = FilterNumericReader::new(&filter, iter.into_iter());
    let mut result = RSIndexResult::numeric(0.0);

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(5.0).doc_id(10));

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(15.0).doc_id(12));
}

#[test]
fn reading_filter_based_on_geo_filter() {
    /// Implement this FFI call for this test
    #[unsafe(no_mangle)]
    pub extern "C" fn isWithinRadius(gf: *const GeoFilter, d: f64, distance: *mut f64) -> bool {
        if d > unsafe { (*gf).radius } {
            return false;
        }

        // Tests changing the distance value
        unsafe { *distance /= 5.0 };

        true
    }

    // Make an iterator with three records having different geo distances. The last record will be
    // filtered out based on the geo distance.
    let iter = vec![
        RSIndexResult::numeric(5.0).doc_id(10),
        RSIndexResult::numeric(15.0).doc_id(11),
        RSIndexResult::numeric(25.0).doc_id(12),
    ];

    let geo_filter = GeoFilter {
        fieldSpec: ptr::null(),
        lat: 0.0,
        lon: 0.0,
        radius: 20.0,
        unitType: GeoDistance_GEO_DISTANCE_M,
        numericFilters: ptr::null_mut(),
    };

    let filter = NumericFilter {
        min: 0.0,
        max: 0.0,
        min_inclusive: false,
        max_inclusive: false,
        field_spec: ptr::null(),
        geo_filter: &geo_filter as *const _ as *const _,
        ascending: true,
        limit: 0,
        offset: 0,
    };

    let mut reader = FilterGeoReader::new(&filter, iter.into_iter());
    let mut result = RSIndexResult::numeric(0.0);

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(1.0).doc_id(10));

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(3.0).doc_id(11));
}

#[test]
fn synced_discriminants() {
    let tests = [
        (
            RSResultData::Union(RSAggregateResult::with_capacity(0)),
            RSResultKind::Union,
        ),
        (
            RSResultData::Intersection(RSAggregateResult::with_capacity(0)),
            RSResultKind::Intersection,
        ),
        (
            RSResultData::Term(RSTermRecord::default()),
            RSResultKind::Term,
        ),
        (RSResultData::Virtual, RSResultKind::Virtual),
        (RSResultData::Numeric(0f64), RSResultKind::Numeric),
        (RSResultData::Metric(0f64), RSResultKind::Metric),
        (
            RSResultData::HybridMetric(RSAggregateResult::with_capacity(0)),
            RSResultKind::HybridMetric,
        ),
    ];

    for (data, kind) in tests {
        assert_eq!(data.kind(), kind);

        // SAFETY: since `RSResultData` is a `repr(u8)` it will have a C `union` layout with a
        // `repr(C)` struct for each variant. Each of these structs has the discriminant as the
        // first field, which we can read here without offsetting the pointer.
        //
        // For more see https://doc.rust-lang.org/std/mem/fn.discriminant.html#accessing-the-numeric-value-of-the-discriminant
        let data_discriminant = unsafe { *<*const _>::from(&data).cast::<u8>() };
        let kind_discriminant = kind as u8;

        assert_eq!(data_discriminant, kind_discriminant, "for {kind:?}");
    }
}

#[test]
fn summary() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    assert_eq!(
        ii.summary(),
        Summary {
            number_of_docs: 0,
            number_of_entries: 0,
            last_doc_id: 0,
            flags: IndexFlags_Index_DocIdsOnly as _,
            number_of_blocks: 0,
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    );

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(11);
    let _mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        ii.summary(),
        Summary {
            number_of_docs: 2,
            number_of_entries: 2,
            last_doc_id: 11,
            flags: IndexFlags_Index_DocIdsOnly as _,
            number_of_blocks: 1,
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    );
}

#[test]
fn summary_store_numeric() {
    let mut ii = EntriesTrackingIndex::<Dummy>::new(IndexFlags_Index_StoreNumeric);

    assert_eq!(
        ii.summary(),
        Summary {
            number_of_docs: 0,
            number_of_entries: 0,
            last_doc_id: 0,
            flags: IndexFlags_Index_StoreNumeric as _,
            number_of_blocks: 0,
            block_efficiency: 0.0,
            has_efficiency: true,
        }
    );

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        ii.summary(),
        Summary {
            number_of_docs: 1,
            number_of_entries: 2,
            last_doc_id: 10,
            flags: IndexFlags_Index_StoreNumeric as _,
            number_of_blocks: 1,
            block_efficiency: 2.0,
            has_efficiency: true,
        }
    );
}

#[test]
fn blocks_summary() {
    /// Dummy encoder which only allows 2 entries per block for testing
    #[derive(Clone)]
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: u16 = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<SmallBlocksDummy>::new(IndexFlags_Index_DocIdsOnly);

    assert_eq!(ii.blocks_summary().len(), 0);

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(11);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(12);
    let _mem_growth = ii.add_record(&record).unwrap();

    let summaries = ii.blocks_summary();
    assert_eq!(
        summaries,
        vec![
            BlockSummary {
                first_doc_id: 10,
                last_doc_id: 11,
                number_of_entries: 2,
            },
            BlockSummary {
                first_doc_id: 12,
                last_doc_id: 12,
                number_of_entries: 1,
            }
        ]
    );
}

#[test]
fn blocks_summary_store_numeric() {
    /// Dummy encoder which only allows 2 entries per block for testing
    #[derive(Clone)]
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: u16 = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = EntriesTrackingIndex::<SmallBlocksDummy>::new(IndexFlags_Index_StoreNumeric);

    assert_eq!(ii.blocks_summary().len(), 0);

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(11);
    let _mem_growth = ii.add_record(&record).unwrap();

    let record = RSIndexResult::default().doc_id(12);
    let _mem_growth = ii.add_record(&record).unwrap();

    let summaries = ii.blocks_summary();
    assert_eq!(
        summaries,
        vec![
            BlockSummary {
                first_doc_id: 10,
                last_doc_id: 11,
                number_of_entries: 2,
            },
            BlockSummary {
                first_doc_id: 12,
                last_doc_id: 12,
                number_of_entries: 1,
            }
        ]
    );
}

/// Helper macro to encode a series of doc IDs using the provided encoder. The first ID is encoded
/// as a delta from 0, and each subsequent ID is encoded as a delta from the previous ID.
macro_rules! encode_ids {
    ($encoder:ty, $first_id:expr $(, $doc_id:expr)* ) => {
        {
            let mut writer = Cursor::new(Vec::new());
            <$encoder>::encode(&mut writer, 0, &RSIndexResult::default().doc_id($first_id)).unwrap();

            let mut _last_id = $first_id;
            $(
                let delta = $doc_id - _last_id;
                <$encoder>::encode(&mut writer, delta, &RSIndexResult::default().doc_id($doc_id)).unwrap();
                _last_id = $doc_id;
            )*
            writer.into_inner()
        }
    };
}

#[test]
fn index_block_repair_delete() {
    // Make a block with three entries (two duplicates) which will be deleted
    let block = test_block(10, 11, 3, encode_ids!(Dummy, 10, 11, 11));

    fn cb(doc_id: t_docId) -> bool {
        ![10, 11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            cb,
            None::<fn(&RSIndexResult, &IndexBlock)>,
            PhantomData::<Dummy>::default(),
        )
        .unwrap();

    assert_eq!(
        repair_status,
        Some(RepairType::Delete {
            n_unique_docs_removed: 2
        })
    );
}

#[test]
fn index_block_repair_unchanged() {
    // Create an index block with two entries. None of which were deleted
    let block = test_block(10, 11, 2, encode_ids!(Dummy, 10, 11));

    fn cb(_doc_id: t_docId) -> bool {
        true
    }

    let repair_status = block
        .repair(
            cb,
            None::<fn(&RSIndexResult, &IndexBlock)>,
            PhantomData::<Dummy>::default(),
        )
        .unwrap();

    assert_eq!(repair_status, None);
}

#[test]
fn index_block_repair_some_deletions() {
    // Create an index block with three entries. The second one will not be deleted
    let block = test_block(10, 12, 3, encode_ids!(Dummy, 10, 11, 12));

    fn cb(doc_id: t_docId) -> bool {
        [11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            cb,
            None::<fn(&RSIndexResult, &IndexBlock)>,
            PhantomData::<Dummy>::default(),
        )
        .unwrap();

    assert_eq!(
        repair_status,
        Some(RepairType::Replace {
            blocks: smallvec![test_block(11, 11, 1, encode_ids!(Dummy, 11))],
            n_unique_docs_removed: 2
        })
    );
}

#[test]
fn index_block_repair_delta_too_big() {
    #[derive(Clone)]
    struct SmallDeltaDummy;

    struct U5Delta(u8);

    impl IdDelta for U5Delta {
        fn from_u64(delta: u64) -> Option<Self> {
            if delta < 32 {
                Some(Self(delta as u8))
            } else {
                None
            }
        }

        fn zero() -> Self {
            Self(0)
        }
    }

    impl Encoder for SmallDeltaDummy {
        type Delta = U5Delta;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&delta.0.to_be_bytes())?;

            Ok(1)
        }
    }

    impl Decoder for SmallDeltaDummy {
        fn decode<'index>(
            cursor: &mut Cursor<&'index [u8]>,
            base: t_docId,
            result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            let mut buffer = [0; 1];
            cursor.read_exact(&mut buffer)?;

            let delta = u8::from_be_bytes(buffer);
            result.doc_id = base + (delta as u64);

            Ok(())
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::default()
        }
    }

    // Create an index block with three entries - the middle entry will be deleted creating a delta that is too big
    let mut writer = Cursor::new(Vec::new());
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(0),
        &RSIndexResult::default().doc_id(10),
    )
    .unwrap();
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(31),
        &RSIndexResult::default().doc_id(41),
    )
    .unwrap();
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(1),
        &RSIndexResult::default().doc_id(42),
    )
    .unwrap();

    let block = test_block(10, 42, 3, writer.into_inner());

    fn cb(doc_id: t_docId) -> bool {
        ![41].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            cb,
            None::<fn(&RSIndexResult, &IndexBlock)>,
            PhantomData::<SmallDeltaDummy>::default(),
        )
        .unwrap();

    assert_eq!(
        repair_status,
        Some(RepairType::Replace {
            blocks: smallvec![
                test_block(10, 10, 1, {
                    let mut writer = Cursor::new(Vec::new());
                    SmallDeltaDummy::encode(
                        &mut writer,
                        U5Delta(0),
                        &RSIndexResult::default().doc_id(10),
                    )
                    .unwrap();
                    writer.into_inner()
                }),
                test_block(42, 42, 1, {
                    let mut writer = Cursor::new(Vec::new());
                    SmallDeltaDummy::encode(
                        &mut writer,
                        U5Delta(0),
                        &RSIndexResult::default().doc_id(42),
                    )
                    .unwrap();
                    writer.into_inner()
                })
            ],
            n_unique_docs_removed: 1
        })
    );
}

#[test]
fn ii_scan_gc() {
    // Create 5 blocks:
    // - One which is empty
    // - One which will be completely deleted
    // - One which will be partially deleted
    // - Two which will be unchanged
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, encode_ids!(Dummy, 10, 11)),
        test_block(20, 22, 3, encode_ids!(Dummy, 20, 21, 22)),
        test_block(30, 30, 1, encode_ids!(Dummy, 30)),
        test_block(40, 40, 1, encode_ids!(Dummy, 40)),
    ];

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    fn cb(doc_id: t_docId) -> bool {
        [21, 22, 30, 40].contains(&doc_id)
    }

    let gc_result = ii
        .scan_gc(cb, None::<fn(&RSIndexResult, &IndexBlock)>)
        .unwrap()
        .unwrap();

    assert_eq!(
        gc_result,
        GcScanDelta {
            last_block_idx: 3,
            last_block_num_entries: 1,
            deltas: vec![
                BlockGcScanResult {
                    index: 0,
                    repair: RepairType::Delete {
                        n_unique_docs_removed: 2
                    },
                },
                BlockGcScanResult {
                    index: 1,
                    repair: RepairType::Replace {
                        blocks: smallvec![test_block(21, 22, 2, encode_ids!(Dummy, 21, 22))],
                        n_unique_docs_removed: 1
                    },
                },
            ]
        }
    );
}

#[test]
fn ii_scan_gc_no_change() {
    // Create 2 blocks which will be unchanged
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, encode_ids!(Dummy, 10, 11)),
        test_block(30, 30, 1, encode_ids!(Dummy, 30)),
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    fn cb(_doc_id: t_docId) -> bool {
        true
    }

    let gc_result = ii
        .scan_gc(cb, None::<fn(&RSIndexResult, &IndexBlock)>)
        .unwrap();

    assert_eq!(gc_result, None, "there should be no changes");
}

#[test]
fn ii_apply_gc() {
    // Create 5 blocks:
    // - One which is empty
    // - One which will be completely deleted
    // - One which will be partially deleted
    // - One which will be unchanged
    // - One which will be split into multiple blocks
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, encode_ids!(Dummy, 10, 11)),
        test_block(20, 22, 3, encode_ids!(Dummy, 20, 21, 22)),
        test_block(30, 30, 1, encode_ids!(Dummy, 30)),
        test_block(40, 72, 3, encode_ids!(Dummy, 40, 71, 72)),
    ];
    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    assert_eq!(
        ii.memory_usage(),
        24// Size of an empty inverted index
        + 8 // Size of the header of the thinvec storing blocks
        + IndexBlock::STACK_SIZE * 4 // Size of the index blocks
        + 8 // Size of the buffer of the first index block
        + 16 // Size of the buffer of the second index block
        + 8 // Size of the buffer of the third index block
        + 16 // Size of the buffer of the fourth index block
    );

    let gc_result = vec![
        BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            index: 1,
            repair: RepairType::Replace {
                blocks: smallvec![test_block(21, 21, 1, encode_ids!(Dummy, 21))],
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            index: 3,
            repair: RepairType::Replace {
                blocks: smallvec![
                    test_block(40, 40, 1, encode_ids!(Dummy, 40)),
                    test_block(72, 72, 1, encode_ids!(Dummy, 72)),
                ],
                n_unique_docs_removed: 1,
            },
        },
    ];

    let delta = GcScanDelta {
        last_block_idx: 4,
        last_block_num_entries: 3,
        deltas: gc_result,
    };

    assert_eq!(ii.gc_marker(), 0);

    let apply_info = ii.apply_gc(delta);

    assert_eq!(ii.gc_marker(), 1);

    assert_eq!(
        ii.memory_usage(),
        24// Size of an empty inverted index
        + 8 // Size of the header of the thinvec storing blocks
        + IndexBlock::STACK_SIZE * 4 // Size of the index blocks
        + 8 // Size of the buffer of the first index block
        + 8 // Size of the buffer of the second index block
        + 8 // Size of the buffer of the third index block
        + 8 // Size of the buffer of the fourth index block
    );

    assert_eq!(ii.unique_docs(), 4);
    assert_eq!(
        ii.blocks,
        vec![
            test_block(21, 21, 1, encode_ids!(Dummy, 21)),
            test_block(30, 30, 1, encode_ids!(Dummy, 30)),
            test_block(40, 40, 1, encode_ids!(Dummy, 40)),
            test_block(72, 72, 1, encode_ids!(Dummy, 72)),
        ]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // The first, second and fourth block was removed totaling 208 bytes
            // (3 blocks × STACK_SIZE + buffer sizes)
            bytes_freed: 208,
            // The third and fifth block was split making 192 new bytes
            // (3 new blocks × STACK_SIZE + buffer sizes)
            bytes_allocated: 192,
            entries_removed: 5,
            blocks_ignored: 0
        }
    );
}

#[test]
fn ii_apply_gc_last_block_updated() {
    // Create 2 blocks where the last block will have new entries since the GC scan
    let blocks = medium_thin_vec![
        test_block(10, 11, 2, encode_ids!(Dummy, 10, 11)),
        test_block(20, 22, 3, encode_ids!(Dummy, 20, 21, 22)),
    ];

    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    assert_eq!(
        ii.memory_usage(),
        24// Size of an empty inverted index
        + 8 // Size of the header of the thinvec storing blocks
        + IndexBlock::STACK_SIZE * 2 // Size of the index blocks
        + 8 // Size of the buffer of the first index block
        + 16 // Size of the buffer of the second index block
    );

    let gc_result = vec![
        BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            index: 1,
            repair: RepairType::Replace {
                blocks: smallvec![test_block(21, 21, 1, encode_ids!(Dummy, 21))],
                n_unique_docs_removed: 2,
            },
        },
    ];

    let delta = GcScanDelta {
        last_block_idx: 1,
        // We want to simulate a scenario where new entries were added to the last block. Hence why
        // this is less than the actual number of entries in the last block.
        last_block_num_entries: 2,
        deltas: gc_result,
    };

    assert_eq!(ii.gc_marker(), 0);

    let apply_info = ii.apply_gc(delta);

    assert_eq!(ii.gc_marker(), 1);

    assert_eq!(
        ii.memory_usage(),
        24 // Size of an empty inverted index
        + 8 // Size of the header of the thinvec storing blocks
        + IndexBlock::STACK_SIZE * 1 // Size of the index blocks
        + 16 // Size of the buffer of the first index block
    );

    assert_eq!(ii.unique_docs(), 3);
    assert_eq!(
        ii.blocks,
        vec![test_block(20, 22, 3, encode_ids!(Dummy, 20, 21, 22))]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Freed only the first block (STACK_SIZE + 8 bytes buffer = 64)
            bytes_freed: 64,
            // Nothing new was made in the end
            bytes_allocated: 0,
            entries_removed: 2,
            // Ignored the last block
            blocks_ignored: 1
        }
    );
}

#[test]
fn ii_apply_gc_entries_tracking_index() {
    // Make a dummy encoder which allows duplicates
    #[derive(Clone)]
    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&delta.to_be_bytes())?;

            Ok(4)
        }
    }

    impl Decoder for AllowDupsDummy {
        fn decode<'index>(
            cursor: &mut Cursor<&'index [u8]>,
            prev_doc_id: u64,
            result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            let mut buffer = [0; 4];
            cursor.read_exact(&mut buffer)?;

            let delta = u32::from_be_bytes(buffer);
            result.doc_id = prev_doc_id + (delta as u64);

            Ok(())
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::default()
        }
    }

    // Create entries tracking index with two duplicate records
    let mut ii = EntriesTrackingIndex::<AllowDupsDummy>::new(IndexFlags_Index_DocIdsOnly);

    let _ = ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    let _ = ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    let _ = ii.add_record(&RSIndexResult::default().doc_id(15)).unwrap();
    let _ = ii.add_record(&RSIndexResult::default().doc_id(15)).unwrap();

    assert_eq!(ii.number_of_entries(), 4);
    assert_eq!(ii.unique_docs(), 2);

    let expected_delta = GcScanDelta {
        last_block_idx: 0,
        last_block_num_entries: 4,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Replace {
                blocks: smallvec![test_block(15, 15, 2, encode_ids!(AllowDupsDummy, 15, 15))],
                n_unique_docs_removed: 1,
            },
        }],
    };

    let doc_exist = |id| id == 15;

    let mut repaired = Vec::new();

    let repair = |result: &RSIndexResult, _ib: &IndexBlock| repaired.push(result.doc_id);

    assert_eq!(
        ii.scan_gc(doc_exist, Some(repair)).unwrap().unwrap(),
        expected_delta
    );

    assert_eq!(ii.gc_marker(), 0);

    let apply_info = ii.apply_gc(expected_delta);

    assert_eq!(ii.gc_marker(), 1);
    assert_eq!(ii.number_of_entries(), 2);
    assert_eq!(ii.unique_docs(), 1);
    assert_eq!(repaired, vec![15, 15]);
    assert_eq!(
        ii.index.blocks,
        vec![test_block(15, 15, 2, encode_ids!(AllowDupsDummy, 15, 15))]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Original block: STACK_SIZE + 17 bytes buffer (4 entries × 4 bytes + 1) = 73
            bytes_freed: 73,
            // New block: STACK_SIZE + 8 bytes buffer = 64
            bytes_allocated: 64,
            entries_removed: 2,
            blocks_ignored: 0
        }
    );
}
// the memory hack below raises error in miri
#[cfg(not(miri))]
#[test]
fn test_refresh_buffer_pointers_after_reallocation() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // Add initial records
    ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    ii.add_record(&RSIndexResult::default().doc_id(11)).unwrap();

    // SAFETY: We need to bypass Rust's borrowing rules to simulate the real-world
    // scenario where buffer reallocation happens while a reader is active.
    // This is safe because:
    // 1. We're not accessing the reader during the mutation
    // 2. The InvertedIndex structure remains valid
    // 3. We call refresh_buffer_pointers before using the reader again
    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;

    let mut reader: crate::IndexReaderCore<'_, Dummy> = ii.reader();
    let mut result = RSIndexResult::default();

    // Read first record
    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 10);

    // Force buffer reallocation by adding many records to the same block
    // This should cause the buffer to grow and potentially move
    unsafe {
        for i in 12..1000 {
            (*ii_ptr)
                .add_record(&RSIndexResult::default().doc_id(i))
                .unwrap();
        }
    }

    // Buffer was reallocated - test refresh_buffer_pointers
    reader.refresh_buffer_pointers();

    // Verify we can still read correctly from the new buffer
    let mut doc_count = 1; // Already read doc_id 10
    let mut expected_doc_id = 11;

    while reader.next_record(&mut result).unwrap() {
        assert_eq!(result.doc_id, expected_doc_id);
        doc_count += 1;
        expected_doc_id += 1;
    }

    // Should have read all 990 documents (10, 11, 12..999)
    assert_eq!(doc_count, 990);
    assert_eq!(expected_doc_id, 1000);
}

// ============================================================================
// Block Max Score Optimization Tests
// ============================================================================

use crate::block_max_score::BlockScorer;
use thin_vec::ThinVec;

/// Helper function to create an `IndexBlock` with scoring metadata for testing.
fn test_block_with_scoring(
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,
    max_freq: u32,
    min_doc_len: u32,
    max_doc_score: f32,
) -> IndexBlock {
    IndexBlock {
        first_doc_id,
        last_doc_id,
        num_entries,
        max_freq,
        max_doc_score,
        min_doc_len,
        buffer: Vec::new(),
    }
}

/// Create a test index with multiple blocks having different scoring metadata.
/// Returns an index with 3 blocks:
/// - Block 0: docs 1-10, max_freq=5, min_doc_len=100, max_doc_score=1.0
/// - Block 1: docs 11-20, max_freq=2, min_doc_len=200, max_doc_score=0.5
/// - Block 2: docs 21-30, max_freq=10, min_doc_len=50, max_doc_score=2.0
fn create_test_index_with_scoring_metadata() -> InvertedIndex<Dummy> {
    InvertedIndex::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        ThinVec::from(vec![
            test_block_with_scoring(1, 10, 10, 5, 100, 1.0),
            test_block_with_scoring(11, 20, 10, 2, 200, 0.5),
            test_block_with_scoring(21, 30, 10, 10, 50, 2.0),
        ]),
    )
}

#[test]
fn skip_to_with_threshold_skips_low_score_blocks() {
    let ii = create_test_index_with_scoring_metadata();
    let mut reader = ii.reader();
    let scorer = BlockScorer::tfidf(1.0);

    // With TF-IDF scorer (idf=1.0), block 1 has the lowest score (low freq, high doc_len)
    // Block 0: (5/100) * 1.0 * 1.0 = 0.05
    // Block 1: (2/200) * 1.0 * 1.0 = 0.01
    // Block 2: (10/50) * 1.0 * 2.0 = 0.4

    // Skip to doc 1 with threshold 0.02 - should skip block 1 when we get there
    let found = reader.skip_to_with_threshold(1, 0.02, &scorer);
    assert!(found);

    // Now skip to doc 11 (block 1) with threshold 0.02
    // Block 1's max score is 0.01, which is below threshold, so it should skip to block 2
    let found = reader.skip_to_with_threshold(11, 0.02, &scorer);
    assert!(found);

    // We should now be at block 2 (docs 21-30)
    let block_score = reader.current_block_max_score(&scorer);
    assert!((block_score - 0.4).abs() < 0.001);
}

#[test]
fn skip_to_with_threshold_zero_behaves_like_skip_to() {
    let ii = create_test_index_with_scoring_metadata();
    let mut reader = ii.reader();
    let scorer = BlockScorer::tfidf(1.0);

    // With min_score = 0.0, should behave like regular skip_to
    let found = reader.skip_to_with_threshold(15, 0.0, &scorer);
    assert!(found);

    // Should be at block 1 (docs 11-20) since we asked for doc 15
    let block_score = reader.current_block_max_score(&scorer);
    // Block 1: (2/200) * 1.0 * 1.0 = 0.01
    assert!((block_score - 0.01).abs() < 0.001);
}

#[test]
fn skip_to_with_threshold_returns_false_when_all_blocks_below_threshold() {
    let ii = create_test_index_with_scoring_metadata();
    let mut reader = ii.reader();
    let scorer = BlockScorer::tfidf(1.0);

    // Set threshold higher than any block's max score
    // Block 2 has the highest score at 0.4
    let found = reader.skip_to_with_threshold(1, 1.0, &scorer);
    assert!(!found);
}

#[test]
fn current_block_max_score_returns_correct_value() {
    let ii = create_test_index_with_scoring_metadata();
    let reader = ii.reader();
    let scorer = BlockScorer::tfidf(1.0);

    // Initially at block 0
    // Block 0: (5/100) * 1.0 * 1.0 = 0.05
    let score = reader.current_block_max_score(&scorer);
    assert!((score - 0.05).abs() < 0.001);
}

#[test]
fn skip_to_with_threshold_with_bm25_scorer() {
    let ii = create_test_index_with_scoring_metadata();
    let mut reader = ii.reader();
    let scorer = BlockScorer::bm25(2.0, 100.0, 1.2, 0.75);

    // BM25 scores:
    // Block 0: tf=5, doc_len=100, len_norm = 1.0
    //   score = 2.0 * (5 * 2.2) / (5 + 1.2 * 1.0) = 22/6.2 ≈ 3.55
    // Block 1: tf=2, doc_len=200, len_norm = 1.75
    //   score = 2.0 * (2 * 2.2) / (2 + 1.2 * 1.75) = 8.8/4.1 ≈ 2.15
    // Block 2: tf=10, doc_len=50, len_norm = 0.625
    //   score = 2.0 * (10 * 2.2) / (10 + 1.2 * 0.625) = 44/10.75 ≈ 4.09

    // Skip to doc 11 with threshold 3.0 - should skip block 1 (score 2.15) to block 2
    let found = reader.skip_to_with_threshold(11, 3.0, &scorer);
    assert!(found);

    // Should have skipped to block 2
    let score = reader.current_block_max_score(&scorer);
    // Block 2 score ≈ 4.09
    assert!(score > 4.0, "Expected score > 4.0, got {}", score);
}

#[test]
fn skip_to_with_threshold_with_docscore_scorer() {
    let ii = create_test_index_with_scoring_metadata();
    let mut reader = ii.reader();
    let scorer = BlockScorer::doc_score();

    // DocScore only considers max_doc_score
    // Block 0: 1.0
    // Block 1: 0.5
    // Block 2: 2.0

    // Skip to doc 11 with threshold 0.6 - should skip block 1 (score 0.5)
    let found = reader.skip_to_with_threshold(11, 0.6, &scorer);
    assert!(found);

    // Should be at block 2
    let score = reader.current_block_max_score(&scorer);
    assert!((score - 2.0).abs() < 0.001);
}
