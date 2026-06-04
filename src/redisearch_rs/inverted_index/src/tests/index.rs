/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::Cursor;

use crate::{
    Decoder, Encoder, EntriesTrackingIndex, FieldMaskTrackingIndex, GcScanDelta, IdDelta,
    IndexBlock, InvertedIndex,
    debug::{BlockSummary, Summary},
    gc::BlockGcScanResult,
    gc::RepairType,
};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_HasMultiValue, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreNumeric,
};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::DocId;

use super::Dummy;

#[test]
fn memory_usage() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    // 16 bytes of original fields (n_unique_docs, flags, gc_marker, unique_id) + 8 bytes
    // for `state: ArcSwap<State>`. The 8-byte `blocks: ThinVec` field was removed in
    // Epic 1, Story 1.3.
    let empty_size = 24;

    assert_eq!(ii.memory_usage(), empty_size);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(ii.memory_usage(), empty_size + mem_growth);
}

#[test]
fn adding_records() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::build_virt().doc_id(10).build();

    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth,
        IndexBlock::STACK_SIZE + 4,
        "size of the first index block (48 bytes) plus 4 bytes for the encoded delta"
    );
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(ii.block_at(0).unwrap().buffer, [0, 0, 0, 0]);
    assert_eq!(ii.block_at(0).unwrap().num_entries, 1);
    assert_eq!(ii.block_at(0).unwrap().first_doc_id, 10);
    assert_eq!(ii.block_at(0).unwrap().last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1);

    let record = RSIndexResult::build_virt().doc_id(11).build();

    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth, 5,
        "buffer needs to grow to 9 bytes to hold a total of 8 bytes"
    );
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(ii.block_at(0).unwrap().buffer, [0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(ii.block_at(0).unwrap().num_entries, 2);
    assert_eq!(ii.block_at(0).unwrap().first_doc_id, 10);
    assert_eq!(ii.block_at(0).unwrap().last_doc_id, 11);
    assert_eq!(ii.n_unique_docs, 2);
}

#[test]
fn adding_same_record_twice() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::build_virt().doc_id(10).build();

    ii.add_record(&record).unwrap();
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(ii.block_at(0).unwrap().buffer, [0, 0, 0, 0]);
    assert_eq!(ii.block_at(0).unwrap().num_entries, 1);
    assert_eq!(ii.flags(), IndexFlags_Index_DocIdsOnly);

    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth, 0,
        "duplicate record should not be written by default"
    );
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(
        ii.block_at(0).unwrap().buffer,
        [0, 0, 0, 0],
        "buffer should remain unchanged"
    );
    assert_eq!(ii.block_at(0).unwrap().num_entries, 1);
    assert_eq!(ii.block_at(0).unwrap().first_doc_id, 10);
    assert_eq!(ii.block_at(0).unwrap().last_doc_id, 10);
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
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(ii.block_at(0).unwrap().buffer, [255]);
    assert_eq!(ii.flags(), IndexFlags_Index_DocIdsOnly);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(
        ii.block_at(0).unwrap().buffer,
        [255, 255],
        "buffer should contain two entries"
    );
    assert_eq!(ii.block_at(0).unwrap().num_entries, 2);
    assert_eq!(ii.block_at(0).unwrap().first_doc_id, 10);
    assert_eq!(ii.block_at(0).unwrap().last_doc_id, 10);
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

    let mem_growth = ii
        .add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap()
        .mem_growth as usize;
    assert_eq!(
        mem_growth,
        IndexBlock::STACK_SIZE + 1,
        "size of the first index block (48 bytes) plus the byte written"
    );
    assert_eq!(ii.number_of_blocks(), 1);
    let mem_growth = ii
        .add_record(&RSIndexResult::build_virt().doc_id(11).build())
        .unwrap()
        .mem_growth as usize;
    assert_eq!(mem_growth, 1, "buffer needs to grow for the new byte");
    assert_eq!(ii.number_of_blocks(), 1);

    // 3 entry should create a new block
    let mem_growth = ii
        .add_record(&RSIndexResult::build_virt().doc_id(12).build())
        .unwrap()
        .mem_growth as usize;
    assert_eq!(
        mem_growth,
        IndexBlock::STACK_SIZE + 1,
        "size of the new index block (48 bytes) plus the byte written"
    );
    assert_eq!(
        ii.number_of_blocks(),
        2,
        "should create a new block after reaching the limit"
    );
    let mem_growth = ii
        .add_record(&RSIndexResult::build_virt().doc_id(13).build())
        .unwrap()
        .mem_growth as usize;
    assert_eq!(mem_growth, 1, "buffer needs to grow for the new byte");
    assert_eq!(ii.number_of_blocks(), 2);

    // But duplicate entry does not go in new block even if the current block is full
    let mem_growth = ii
        .add_record(&RSIndexResult::build_virt().doc_id(13).build())
        .unwrap()
        .mem_growth as usize;
    assert_eq!(mem_growth, 1, "buffer needs to grow again");
    assert_eq!(
        ii.number_of_blocks(),
        2,
        "duplicates should stay on the same block"
    );
    assert_eq!(
        ii.block_at(1).unwrap().num_entries, 3,
        "should have 3 entries in the second block because duplicate was added"
    );
}

#[test]
fn adding_big_delta_makes_new_block() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let record = RSIndexResult::build_virt().doc_id(10).build();

    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth,
        4 + 48,
        "should write 4 bytes for delta and 48 bytes for the new index block"
    );
    assert_eq!(ii.number_of_blocks(), 1);
    assert_eq!(ii.block_at(0).unwrap().buffer, [0, 0, 0, 0]);
    assert_eq!(ii.block_at(0).unwrap().num_entries, 1);
    assert_eq!(ii.block_at(0).unwrap().first_doc_id, 10);
    assert_eq!(ii.block_at(0).unwrap().last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1);

    // This will create a delta that is larger than the default u32 acceptable delta size
    let doc_id = (u32::MAX as u64) + 11;
    let record = RSIndexResult::build_virt().doc_id(doc_id).build();

    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth,
        4 + 48,
        "should write 4 bytes for delta and 48 bytes for the new index block"
    );
    assert_eq!(ii.number_of_blocks(), 2);
    assert_eq!(ii.block_at(1).unwrap().buffer, [0, 0, 0, 0]);
    assert_eq!(ii.block_at(1).unwrap().num_entries, 1);
    assert_eq!(ii.block_at(1).unwrap().first_doc_id, doc_id);
    assert_eq!(ii.block_at(1).unwrap().last_doc_id, doc_id);
    assert_eq!(ii.n_unique_docs, 2);
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
            _base: DocId,
            _result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            unimplemented!("not used by this test")
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            unimplemented!("not used by this test")
        }
    }

    let mut ii = InvertedIndex::<SmallBlocksDummy>::new(IndexFlags_Index_DocIdsOnly);

    // After Story 1.3 the storage is a `Vec<Arc<IndexBlock>>` inside `State::pending`
    // (built per write with `Vec::with_capacity(exact)`) plus an optional `in_progress`.
    // The original "ThinVec grows by exactly 1" guarantee is replaced by "the new state's
    // total block count matches what was written" — same intent, just measured at a
    // higher level.
    assert_eq!(ii.number_of_blocks(), 0, "initially there are no blocks");

    // Test when a new block are added normally
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(11).build())
        .unwrap();
    assert_eq!(ii.number_of_blocks(), 1);

    ii.add_record(&RSIndexResult::build_virt().doc_id(12).build())
        .unwrap();
    assert_eq!(ii.number_of_blocks(), 2, "third record rolls a new block");

    // Test when a new block is added due to delta overflow
    ii.add_record(
        &RSIndexResult::build_virt()
            .doc_id(u32::MAX as u64 + 13)
            .build(),
    )
    .unwrap();
    assert_eq!(
        ii.number_of_blocks(),
        3,
        "delta-overflow forces another block"
    );

    // Make sure GC shrinks the new pending Vec to fit.
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
        ii.number_of_blocks(),
        1,
        "GC leaves only the surviving in_progress block"
    );
    // The pending Vec was shrunk to fit by `apply_gc`. Only the in_progress block
    // remains; pending is empty (capacity may still be one slot from the survivor that
    // was pop()'d into in_progress, but len is 0).
    let state = ii.state.load_full();
    assert!(state.pending.is_empty(), "no surviving pending blocks");
}

#[test]
fn adding_tracks_entries() {
    let mut ii = EntriesTrackingIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // InvertedIndex (24) + EntriesTrackingIndex's own fields (8) = 32. Down from 40 in
    // Story 1.2; Story 1.3 removed the 8-byte `blocks: ThinVec` field.
    let empty_size = 32;
    assert_eq!(ii.memory_usage(), empty_size);
    assert_eq!(ii.number_of_entries(), 0);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(ii.memory_usage(), empty_size + mem_growth);
    assert_eq!(ii.number_of_entries(), 1);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(ii.number_of_entries(), 2);
}

#[test]
fn adding_track_field_mask() {
    let mut ii = FieldMaskTrackingIndex::<Dummy>::new(IndexFlags_Index_StoreFieldFlags);

    // InvertedIndex (24) + FieldMaskTrackingIndex's own fields (16) = 40. Down from 48 in
    // Story 1.2; Story 1.3 removed the 8-byte `blocks: ThinVec` field.
    assert_eq!(ii.memory_usage(), 40);
    assert_eq!(ii.field_mask(), 0);

    let record = RSIndexResult::build_virt()
        .doc_id(10)
        .field_mask(0b101)
        .build();
    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(
        mem_growth,
        IndexBlock::STACK_SIZE + 4,
        "size of the first index block (48 bytes) plus 4 bytes for the encoded result"
    );
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::build_virt()
        .doc_id(11)
        .field_mask(0b101)
        .build();
    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    assert_eq!(mem_growth, 5);
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::build_virt()
        .doc_id(12)
        .field_mask(0b011)
        .build();
    let mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

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

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(11).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

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

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

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

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(11).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(12).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

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

    let record = RSIndexResult::build_virt().doc_id(10).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(11).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

    let record = RSIndexResult::build_virt().doc_id(12).build();
    let _mem_growth = ii.add_record(&record).unwrap().mem_growth as usize;

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
