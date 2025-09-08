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
    ptr,
};

use crate::{
    Decoder, Encoder, EntriesTrackingIndex, FieldMaskTrackingIndex, FilterGeoReader,
    FilterMaskReader, FilterNumericReader, IdDelta, IndexBlock, IndexReader, InvertedIndex,
    NumericFilter, RSAggregateResult, RSIndexResult, RSResultData, RSResultKind, RSTermRecord,
    SkipDuplicatesReader,
    debug::{BlockSummary, Summary},
};
use ffi::{GeoDistance_GEO_DISTANCE_M, GeoFilter};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_HasMultiValue, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreNumeric,
};
use pretty_assertions::assert_eq;

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
struct Dummy;

impl Encoder for Dummy {
    type Delta = u32;

    fn encode<W: std::io::Write + std::io::Seek>(
        &self,
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_be_bytes())?;

        Ok(8)
    }
}

#[test]
fn memory_usage() {
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);

    assert_eq!(ii.memory_usage(), 40);

    let record = RSIndexResult::default().doc_id(10);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.memory_usage(), 40 + mem_growth);
}

#[test]
fn adding_records() {
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);
    let record = RSIndexResult::default().doc_id(10);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth, 56,
        "size of the index block plus initial buffer capacity"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.n_unique_docs, 1);

    let record = RSIndexResult::default().doc_id(11);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 0, "buffer should not need to grow again");
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(ii.blocks[0].num_entries, 2);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 11);
    assert_eq!(ii.n_unique_docs, 2);
}

#[test]
fn adding_same_record_twice() {
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);
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
    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            &self,
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[255])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, AllowDupsDummy);

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
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: usize = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            &self,
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, SmallBlocksDummy);

    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(10)).unwrap();
    assert_eq!(
        mem_growth, 56,
        "size of the index block plus initial buffer capacity"
    );
    assert_eq!(ii.blocks.len(), 1);
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(11)).unwrap();
    assert_eq!(mem_growth, 0, "buffer does not need to grow again");
    assert_eq!(ii.blocks.len(), 1);

    // 3 entry should create a new block
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(12)).unwrap();
    assert_eq!(
        mem_growth, 56,
        "size of the new index block plus initial buffer capacity"
    );
    assert_eq!(
        ii.blocks.len(),
        2,
        "should create a new block after reaching the limit"
    );
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(13)).unwrap();
    assert_eq!(mem_growth, 0, "buffer does not need to grow again");
    assert_eq!(ii.blocks.len(), 2);

    // But duplicate entry does not go in new block even if the current block is full
    let mem_growth = ii.add_record(&RSIndexResult::default().doc_id(13)).unwrap();
    assert_eq!(mem_growth, 0, "buffer does not need to grow again");
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
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);
    let record = RSIndexResult::default().doc_id(10);

    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(
        mem_growth,
        8 + 48,
        "should write 8 bytes for delta and 48 bytes for the index block"
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
        8 + 48,
        "should write 8 bytes for delta and 48 bytes for the new index block"
    );
    assert_eq!(ii.blocks.len(), 2);
    assert_eq!(ii.blocks[1].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[1].num_entries, 1);
    assert_eq!(ii.blocks[1].first_doc_id, doc_id);
    assert_eq!(ii.blocks[1].last_doc_id, doc_id);
    assert_eq!(ii.n_unique_docs, 2);
}

#[test]
fn adding_tracks_entries() {
    let mut ii = EntriesTrackingIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);

    assert_eq!(ii.memory_usage(), 48);
    assert_eq!(ii.number_of_entries(), 0);

    let record = RSIndexResult::default().doc_id(10);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.memory_usage(), 48 + mem_growth);
    assert_eq!(ii.number_of_entries(), 1);

    let record = RSIndexResult::default().doc_id(10);
    let _mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(ii.number_of_entries(), 2);
}

#[test]
fn adding_track_field_mask() {
    let mut ii = FieldMaskTrackingIndex::new(IndexFlags_Index_StoreFieldFlags, Dummy);

    assert_eq!(ii.memory_usage(), 56);
    assert_eq!(ii.field_mask(), 0);

    let record = RSIndexResult::default().doc_id(10).field_mask(0b101);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 56);
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::default().doc_id(11).field_mask(0b101);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 0);
    assert_eq!(ii.field_mask(), 0b101);

    let record = RSIndexResult::default().doc_id(12).field_mask(0b011);
    let mem_growth = ii.add_record(&record).unwrap();

    assert_eq!(mem_growth, 8);
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
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        prev_doc_id: u64,
    ) -> std::io::Result<RSIndexResult<'index>> {
        let mut buffer = [0; 4];
        cursor.read_exact(&mut buffer)?;

        let delta = u32::from_be_bytes(buffer);
        let doc_id = prev_doc_id + (delta as u64);

        Ok(RSIndexResult::virt().doc_id(doc_id))
    }
}

#[test]
fn reading_records() {
    // Make two blocks. The first with two records and the second with one record
    let blocks = vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1],
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 0,
            first_doc_id: 100,
            last_doc_id: 100,
        },
    ];
    let mut ir = IndexReader::new(&blocks, Dummy);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(10));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(11));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(100));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer");
    assert_eq!(record, None);
}

#[test]
fn reading_over_empty_blocks() {
    // Make three blocks with the second one being empty and the other two containing one entries.
    // The second should automatically continue from the third block
    let blocks = vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 10,
            last_doc_id: 10,
        },
        IndexBlock {
            buffer: vec![],
            num_entries: 0,
            first_doc_id: 20,
            last_doc_id: 20,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
    ];
    let mut ir = IndexReader::new(&blocks, Dummy);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(10));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(30));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer");
    assert!(record.is_none(), "should not return any more records");
}

#[test]
fn read_using_the_first_block_id_as_the_base() {
    struct FirstBlockIdDummy;

    impl Decoder for FirstBlockIdDummy {
        fn decode<'index>(
            &self,
            cursor: &mut Cursor<&'index [u8]>,
            prev_doc_id: u64,
        ) -> std::io::Result<RSIndexResult<'index>> {
            let mut buffer = [0; 4];
            cursor.read_exact(&mut buffer)?;

            let delta = u32::from_be_bytes(buffer);
            let doc_id = prev_doc_id + (delta as u64);

            Ok(RSIndexResult::virt().doc_id(doc_id))
        }

        fn base_id(block: &IndexBlock, _last_doc_id: ffi::t_docId) -> ffi::t_docId {
            block.first_doc_id
        }
    }

    // Make a block with three different doc IDs
    let blocks = vec![IndexBlock {
        buffer: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 12,
    }];
    let mut ir = IndexReader::new(&blocks, FirstBlockIdDummy);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(10));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(11));
    drop(record);

    let record = ir
        .next_record()
        .expect("to be able to read from the buffer")
        .expect("to get a record");
    assert_eq!(record, RSIndexResult::virt().doc_id(12));
}

#[test]
#[should_panic(expected = "IndexReader should not be created with an empty block list")]
fn index_reader_construction_with_no_blocks() {
    let blocks: Vec<IndexBlock> = vec![];
    let _ir = IndexReader::new(&blocks, Dummy);
}

#[test]
fn index_reader_skip_to() {
    let blocks = vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 5],
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 15,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1],
            num_entries: 2,
            first_doc_id: 16,
            last_doc_id: 17,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 4],
            num_entries: 2,
            first_doc_id: 20,
            last_doc_id: 24,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 40,
            last_doc_id: 40,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 50,
            last_doc_id: 50,
        },
    ];
    let mut ir = IndexReader::new(&blocks, Dummy);

    assert_eq!(ir.current_block_idx, 0, "should start at the first block");
    assert_eq!(ir.last_doc_id, 10);

    // Skipping to an ID in the current block should not change anything
    assert!(ir.skip_to(12));
    assert_eq!(ir.current_block_idx, 0, "we are still in the first block");
    assert_eq!(ir.current_block, &blocks[0]);
    assert_eq!(ir.last_doc_id, 10);

    // Skipping to an ID in the next block should move to the next block
    assert!(ir.skip_to(16));
    assert_eq!(ir.current_block_idx, 1, "should be in the second block");
    assert_eq!(ir.current_block, &blocks[1]);
    assert_eq!(ir.last_doc_id, 16);

    // Skipping to an ID in a later block should move to that block
    assert!(ir.skip_to(30));
    assert_eq!(ir.current_block_idx, 3, "should be in the fourth block");
    assert_eq!(ir.current_block, &blocks[3]);
    assert_eq!(ir.last_doc_id, 30);

    // Skipping to an ID between blocks should give the block with the next highest ID
    assert!(ir.skip_to(45));
    assert_eq!(ir.current_block_idx, 5, "should be in the sixth block");
    assert_eq!(ir.current_block, &blocks[5]);
    assert_eq!(ir.last_doc_id, 50);

    // Skipping to an ID beyond the last block should return false and stay at the last block
    assert!(!ir.skip_to(100), "should not find a block for this ID");
    assert_eq!(
        ir.current_block_idx, 5,
        "should still be in the sixth block"
    );
    assert_eq!(ir.current_block, &blocks[5]);
    assert_eq!(ir.last_doc_id, 50);

    // Skipping to an earlier ID should do nothing
    assert!(ir.skip_to(5));
    assert_eq!(
        ir.current_block_idx, 5,
        "should still be in the sixth block"
    );
    assert_eq!(ir.current_block, &blocks[5]);
    assert_eq!(ir.last_doc_id, 50);
}

#[test]
fn read_skipping_over_duplicates() {
    // Make an iterator where the first two entries have the same doc ID and the third one is different
    let iter = vec![
        RSIndexResult::virt().doc_id(10).weight(2.0),
        RSIndexResult::virt().doc_id(10).weight(5.0),
        RSIndexResult::virt().doc_id(11),
    ];

    let reader = SkipDuplicatesReader::new(iter.into_iter());
    let records = reader.collect::<Vec<_>>();

    assert_eq!(
        records,
        vec![
            RSIndexResult::virt().doc_id(10).weight(2.0),
            RSIndexResult::virt().doc_id(11),
        ],
        "should skip duplicates"
    );
}

#[test]
fn reading_filter_based_on_field_mask() {
    // Make an iterator with three records having different field masks. The second record will be
    // filtered out based on the field mask.
    let iter = vec![
        RSIndexResult::default().doc_id(10).field_mask(0b0001),
        RSIndexResult::default().doc_id(11).field_mask(0b0010),
        RSIndexResult::default().doc_id(12).field_mask(0b0100),
    ];

    let reader = FilterMaskReader::new(0b0101 as _, iter.into_iter());
    let records = reader.collect::<Vec<_>>();

    assert_eq!(
        records,
        vec![
            RSIndexResult::default().doc_id(10).field_mask(0b0001),
            RSIndexResult::default().doc_id(12).field_mask(0b0100),
        ]
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

    let reader = FilterNumericReader::new(filter, iter.into_iter());
    let records = reader.collect::<Vec<_>>();

    assert_eq!(
        records,
        vec![
            RSIndexResult::numeric(5.0).doc_id(10),
            RSIndexResult::numeric(15.0).doc_id(12),
        ]
    );
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

    let filter = GeoFilter {
        fieldSpec: ptr::null(),
        lat: 0.0,
        lon: 0.0,
        radius: 20.0,
        unitType: GeoDistance_GEO_DISTANCE_M,
        numericFilters: ptr::null_mut(),
    };

    let reader = FilterGeoReader::new(&filter, iter.into_iter());
    let records = reader.collect::<Vec<_>>();

    assert_eq!(
        records,
        vec![
            RSIndexResult::numeric(1.0).doc_id(10),
            RSIndexResult::numeric(3.0).doc_id(11),
        ]
    );
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
    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, Dummy);

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
    let mut ii = EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric, Dummy);

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
            block_efficiency: 1.0,
            has_efficiency: true,
        }
    );
}

#[test]
fn blocks_summary() {
    /// Dummy encoder which only allows 2 entries per block for testing
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: usize = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            &self,
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, SmallBlocksDummy);

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
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type Delta = u32;

        const ALLOW_DUPLICATES: bool = true;
        const RECOMMENDED_BLOCK_ENTRIES: usize = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            &self,
            mut writer: W,
            _delta: Self::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = EntriesTrackingIndex::new(IndexFlags_Index_StoreNumeric, SmallBlocksDummy);

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
