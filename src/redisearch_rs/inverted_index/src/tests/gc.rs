/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{Cursor, Read},
    marker::PhantomData,
};

use crate::{
    Decoder, Encoder, EntriesTrackingIndex, GcApplyInfo, GcScanDelta, IdDelta, IndexBlock,
    InvertedIndex, RSIndexResult, gc::BlockGcScanResult, gc::RepairType,
};
use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use pretty_assertions::assert_eq;
use smallvec::smallvec;
use thin_vec::medium_thin_vec;

use super::{Dummy, encode_ids};

#[test]
fn index_block_repair_delete() {
    // Make a block with three entries (two duplicates) which will be deleted
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 11, 11),
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 11,
                ..Default::default()
            };

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
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 11),
        num_entries: 2,
        first_doc_id: 10,
        last_doc_id: 11,
                ..Default::default()
            };

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
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 11, 12),
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 12,
                ..Default::default()
            };

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
            blocks: smallvec![IndexBlock {
                first_doc_id: 11,
                last_doc_id: 11,
                num_entries: 1,
                buffer: encode_ids!(Dummy, 11),
                ..Default::default()
            }],
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

    let block = IndexBlock {
        buffer: writer.into_inner(),
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 42,
                ..Default::default()
            };

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
                IndexBlock {
                    buffer: {
                        let mut writer = Cursor::new(Vec::new());
                        SmallDeltaDummy::encode(
                            &mut writer,
                            U5Delta(0),
                            &RSIndexResult::default().doc_id(10),
                        )
                        .unwrap();

                        writer.into_inner()
                    },
                    num_entries: 1,
                    first_doc_id: 10,
                    last_doc_id: 10,
                    ..Default::default()
                },
                IndexBlock {
                    buffer: {
                        let mut writer = Cursor::new(Vec::new());
                        SmallDeltaDummy::encode(
                            &mut writer,
                            U5Delta(0),
                            &RSIndexResult::default().doc_id(42),
                        )
                        .unwrap();

                        writer.into_inner()
                    },
                    num_entries: 1,
                    first_doc_id: 42,
                    last_doc_id: 42,
                    ..Default::default()
                }
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
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40),
            num_entries: 1,
            first_doc_id: 40,
            last_doc_id: 40,
                ..Default::default()
            },
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
                        blocks: smallvec![IndexBlock {
                            buffer: encode_ids!(Dummy, 21, 22),
                            num_entries: 2,
                            first_doc_id: 21,
                            last_doc_id: 22,
                ..Default::default()
            }],
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
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
                ..Default::default()
            },
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
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40, 71, 72),
            num_entries: 3,
            first_doc_id: 40,
            last_doc_id: 72,
                ..Default::default()
            },
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
                blocks: smallvec![IndexBlock {
                    buffer: encode_ids!(Dummy, 21),
                    num_entries: 1,
                    first_doc_id: 21,
                    last_doc_id: 21,
                ..Default::default()
            }],
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            index: 3,
            repair: RepairType::Replace {
                blocks: smallvec![
                    IndexBlock {
                        buffer: encode_ids!(Dummy, 40),
                        num_entries: 1,
                        first_doc_id: 40,
                        last_doc_id: 40,
                ..Default::default()
            },
                    IndexBlock {
                        buffer: encode_ids!(Dummy, 72),
                        num_entries: 1,
                        first_doc_id: 72,
                        last_doc_id: 72,
                ..Default::default()
            },
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
            IndexBlock {
                buffer: encode_ids!(Dummy, 21),
                num_entries: 1,
                first_doc_id: 21,
                last_doc_id: 21,
                ..Default::default()
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 30),
                num_entries: 1,
                first_doc_id: 30,
                last_doc_id: 30,
                ..Default::default()
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 40),
                num_entries: 1,
                first_doc_id: 40,
                last_doc_id: 40,
                ..Default::default()
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 72),
                num_entries: 1,
                first_doc_id: 72,
                last_doc_id: 72,
                ..Default::default()
            },
        ]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // The first, second and fourth block was removed:
            // Block 0: STACK_SIZE + 8, Block 1: STACK_SIZE + 16, Block 3: STACK_SIZE + 16
            bytes_freed: IndexBlock::STACK_SIZE * 3 + 8 + 16 + 16,
            // The third and fifth block was split making new blocks:
            // Block 1 replacement: STACK_SIZE + 8, Block 3 replacements: 2 * (STACK_SIZE + 8)
            bytes_allocated: IndexBlock::STACK_SIZE * 3 + 8 + 8 + 8,
            entries_removed: 5,
            ignored_last_block: false
        }
    );
}

#[test]
fn ii_apply_gc_last_block_updated() {
    // Create 2 blocks where the last block will have new entries since the GC scan
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            },
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
                blocks: smallvec![IndexBlock {
                    buffer: encode_ids!(Dummy, 21),
                    num_entries: 1,
                    first_doc_id: 21,
                    last_doc_id: 21,
                ..Default::default()
            }],
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
        vec![IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Freed only the first block (STACK_SIZE + 8 bytes buffer capacity)
            bytes_freed: IndexBlock::STACK_SIZE + 8,
            // Nothing new was made in the end
            bytes_allocated: 0,
            entries_removed: 2,
            // Ignored the last block
            ignored_last_block: true
        }
    );
}

#[test]
fn ii_apply_gc_last_block_updated_no_delta() {
    // Create 2 blocks where:
    // - Block 0 has a delta (entries to delete)
    // - Block 1 (last) has NO delta but gained entries post-fork
    // This tests the path where last_block_changed is true but there is no
    // stale delta to pop — ignored_last_block must still be set to true.
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
                ..Default::default()
            },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            },
    ];

    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    // Delta only for block 0 — block 1 had no deleted entries during scan.
    let gc_result = vec![BlockGcScanResult {
        index: 0,
        repair: RepairType::Delete {
            n_unique_docs_removed: 2,
        },
    }];

    let delta = GcScanDelta {
        last_block_idx: 1,
        // Simulate post-fork writes: scan saw 2 entries, but now there are 3.
        last_block_num_entries: 2,
        deltas: gc_result,
    };

    let apply_info = ii.apply_gc(delta);

    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Freed only the first block (STACK_SIZE + 8 bytes buffer capacity)
            bytes_freed: IndexBlock::STACK_SIZE + 8,
            bytes_allocated: 0,
            entries_removed: 2,
            // The key assertion: ignored_last_block must be true even without
            // a delta for the last block.
            ignored_last_block: true,
        }
    );

    // Block 0 was deleted, block 1 (unchanged) remains.
    assert_eq!(
        ii.blocks,
        vec![IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
                ..Default::default()
            }]
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
                blocks: smallvec![IndexBlock {
                    buffer: encode_ids!(AllowDupsDummy, 15, 15),
                    num_entries: 2,
                    first_doc_id: 15,
                    last_doc_id: 15,
                ..Default::default()
            }],
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
        ii.inner().blocks,
        vec![IndexBlock {
            buffer: encode_ids!(AllowDupsDummy, 15, 15),
            num_entries: 2,
            first_doc_id: 15,
            last_doc_id: 15,
                ..Default::default()
            },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Original block: STACK_SIZE + 17 (buffer capacity for 4 entries)
            bytes_freed: IndexBlock::STACK_SIZE + 17,
            // New block: STACK_SIZE + 8 (buffer capacity for 2 entries)
            bytes_allocated: IndexBlock::STACK_SIZE + 8,
            entries_removed: 2,
            ignored_last_block: false
        }
    );
}
// the memory hack below raises error in miri
#[cfg(not(miri))]
#[test]
fn test_refresh_buffer_pointers_after_reallocation() {
    use crate::IndexReader as _;

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
