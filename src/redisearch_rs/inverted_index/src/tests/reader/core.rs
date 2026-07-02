/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Read};

use crate::{
    Decoder, Encoder, GcScanDelta, IndexBlock, IndexReader, InvertedIndex, NumericReader,
    gc::BlockGcScanResult, gc::RepairType,
};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreTermOffsets, IndexFlags_Index_WideSchema,
};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::DocId;
use thin_vec::medium_thin_vec;

use super::super::Dummy;

#[test]
fn seeking_records() {
    // Make two blocks - the last one with four records
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1],
            num_entries: 3,
            first_doc_id: 10,
            last_doc_id: 12,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 5],
            num_entries: 4,
            first_doc_id: 100,
            last_doc_id: 108,
        },
    ];

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    let found = ir
        .seek_record(101, &mut result)
        .expect("to be able to read from the buffer");

    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(101).build());

    let found = ir
        .seek_record(105, &mut result)
        .expect("to be able to read from the buffer");

    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::build_virt().doc_id(108).build(),
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
    let mut result = RSIndexResult::build_virt().build();

    assert_eq!(ir.next_record(&mut result).unwrap(), false);
    ir.reset();
    assert_eq!(ir.seek_record(5, &mut result).unwrap(), false);
}

#[test]
fn index_reader_skip_to() {
    let blocks = medium_thin_vec![
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
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1],
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 100,
            last_doc_id: 100,
        },
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(10).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(11).build());

    // `reset` re-snapshots the index and rewinds the cursor to the first block.
    ir.reset();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(10).build());
}

/// Appends never replace `InvertedIndex::sealed` — they only extend `pending`/`in_progress`
/// beyond the snapshot — so a reader must NOT revalidate just because writes happened after
/// it took its snapshot. Its frozen view stays valid; forcing a rewind here is the churn that
/// made lock-free reads slower under concurrent writers.
///
/// We can't append while a reader borrows the index, so we simulate "same sealed, more
/// appends" by repointing the reader at a second index that shares the (empty) `sealed`
/// singleton but has extra appended blocks.
#[test]
fn reader_revalidation_ignores_appends() {
    let mut base = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    base.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();

    // More appends (extra entry + a second block), but nothing sealed: still the shared
    // empty `sealed` region.
    let mut appended = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in [10, 11, 12] {
        appended
            .add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }

    let mut ir = base.reader();
    assert!(!ir.needs_revalidation(), "index was not modified yet");
    ir.ii = &appended;
    assert!(
        !ir.needs_revalidation(),
        "appends must not force a revalidation — sealed was not replaced"
    );
}

/// GC is the only operation that rewrites already-captured blocks, and it does so by
/// replacing `InvertedIndex::sealed` wholesale with a fresh `Arc`. A reader whose snapshot
/// captured the pre-GC `sealed` must revalidate so it re-snapshots the compacted blocks.
///
/// We build the post-GC state as a separate index (running a real `apply_gc` so its `sealed`
/// becomes a fresh, non-empty `Arc`) and repoint a reader that snapshotted the shared empty
/// `sealed` at it — the pointer identities differ, so the reader must revalidate.
#[test]
fn reader_revalidation_detects_sealed_replacement() {
    let mut base = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    base.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();

    // Three blocks → `pending = [b0, b1]`, `in_progress = b2`. Deleting b0 leaves b1 (which
    // becomes the sole `sealed` block) and b2 (the new tail), so `apply_gc` swaps in a fresh,
    // non-empty `sealed` `Arc`. Buffers are dummies: a `Delete` only reads entry counts.
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 10,
            last_doc_id: 10,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
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
    let mut compacted = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let delta = GcScanDelta {
        last_block_idx: 2,
        last_block_num_entries: 1,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 1,
            },
        }],
    };
    compacted.apply_gc(delta);

    let mut ir = base.reader();
    assert!(!ir.needs_revalidation());
    ir.ii = &compacted;
    assert!(
        ir.needs_revalidation(),
        "sealed was replaced by GC — reader must revalidate"
    );
}

#[test]
fn reader_unique_docs() {
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: vec![0, 0, 0, 0, 0, 0, 0, 1],
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 100,
            last_doc_id: 100,
        },
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
            _base: DocId,
            _result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            panic!("This test won't decode anything")
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::build_virt().build()
        }
    }

    let mut ii = InvertedIndex::<AllowDupsDummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();

    {
        let ir = ii.reader();
        assert!(!ir.has_duplicates());
    }

    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    let ir = ii.reader();
    assert!(ir.has_duplicates(), "should have duplicates");
}

#[test]
fn reader_flags() {
    let mut ii = InvertedIndex::<Dummy>::new(
        IndexFlags_Index_StoreTermOffsets | IndexFlags_Index_WideSchema,
    );
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    let ir = ii.reader();

    assert_eq!(
        ir.flags(),
        IndexFlags_Index_StoreTermOffsets | IndexFlags_Index_WideSchema,
    );
}

#[test]
fn reader_is_index() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    let ir = ii.reader();

    assert!(ir.points_to_ii(&ii));

    let ii2 = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    assert!(!ir.points_to_ii(&ii2));
}

#[test]
fn reading_records() {
    // Make two blocks. The first with two records and the second with one record
    let blocks = medium_thin_vec![
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
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(10).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(11).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(100).build());

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
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 10,
            last_doc_id: 10,
        },
        IndexBlock {
            buffer: vec![0, 0, 0, 0],
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(10).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(30).build());

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
            RSIndexResult::build_virt().build()
        }

        fn base_id(block: &IndexBlock, _last_doc_id: DocId) -> DocId {
            block.first_doc_id
        }
    }

    // Make a block with three different doc IDs
    let blocks = medium_thin_vec![IndexBlock {
        buffer: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 12,
    }];
    let ii = InvertedIndex::<FirstBlockIdDummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let mut ir = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(10).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(11).build());

    let found = ir
        .next_record(&mut result)
        .expect("to be able to read from the buffer");
    assert!(found);
    assert_eq!(result, RSIndexResult::build_virt().doc_id(12).build());
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
        _doc_id: DocId,
        _result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        unimplemented!("This tests won't seek anything")
    }

    fn skip_to(&mut self, _doc_id: DocId) -> bool {
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
