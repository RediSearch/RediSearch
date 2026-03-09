/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Read};
use std::sync::atomic;

use crate::{
    Decoder, Encoder, IndexBlock, IndexReader, InvertedIndex, NumericReader, RSIndexResult,
};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreTermOffsets, IndexFlags_Index_WideSchema,
};
use pretty_assertions::assert_eq;
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
    let blocks = medium_thin_vec![IndexBlock {
        buffer: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 12,
    }];
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

    fn skip_to(&mut self, _doc_id: ffi::t_docId) -> bool {
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
