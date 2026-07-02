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
    sync::Arc,
};

use crate::{
    Decoder, Encoder, EntriesTrackingIndex, GcApplyInfo, GcScanDelta, IdDelta, IndexBlock,
    InvertedIndex, gc::BlockGcScanResult, gc::RepairType, index::core::ARC_HEADER_BYTES,
};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::DocId;
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
    };

    fn cb(doc_id: DocId) -> bool {
        ![10, 11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
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
    };

    fn cb(_doc_id: DocId) -> bool {
        true
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
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
    };

    fn cb(doc_id: DocId) -> bool {
        [11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
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
            base: DocId,
            result: &mut RSIndexResult<'index>,
        ) -> std::io::Result<()> {
            let mut buffer = [0; 1];
            cursor.read_exact(&mut buffer)?;

            let delta = u8::from_be_bytes(buffer);
            result.doc_id = base + (delta as u64);

            Ok(())
        }

        fn base_result<'index>() -> RSIndexResult<'index> {
            RSIndexResult::build_virt().build()
        }
    }

    // Create an index block with three entries - the middle entry will be deleted creating a delta that is too big
    let mut writer = Cursor::new(Vec::new());
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(0),
        &RSIndexResult::build_virt().doc_id(10).build(),
    )
    .unwrap();
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(31),
        &RSIndexResult::build_virt().doc_id(41).build(),
    )
    .unwrap();
    SmallDeltaDummy::encode(
        &mut writer,
        U5Delta(1),
        &RSIndexResult::build_virt().doc_id(42).build(),
    )
    .unwrap();

    let block = IndexBlock {
        buffer: writer.into_inner(),
        num_entries: 3,
        first_doc_id: 10,
        last_doc_id: 42,
    };

    fn cb(doc_id: DocId) -> bool {
        ![41].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
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
                            &RSIndexResult::build_virt().doc_id(10).build(),
                        )
                        .unwrap();

                        writer.into_inner()
                    },
                    num_entries: 1,
                    first_doc_id: 10,
                    last_doc_id: 10,
                },
                IndexBlock {
                    buffer: {
                        let mut writer = Cursor::new(Vec::new());
                        SmallDeltaDummy::encode(
                            &mut writer,
                            U5Delta(0),
                            &RSIndexResult::build_virt().doc_id(42).build(),
                        )
                        .unwrap();

                        writer.into_inner()
                    },
                    num_entries: 1,
                    first_doc_id: 42,
                    last_doc_id: 42,
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
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40),
            num_entries: 1,
            first_doc_id: 40,
            last_doc_id: 40,
        },
    ];

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    fn cb(doc_id: DocId) -> bool {
        [21, 22, 30, 40].contains(&doc_id)
    }

    let gc_result = ii
        .scan_gc(cb, None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>)
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
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
    ];
    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    fn cb(_doc_id: DocId) -> bool {
        true
    }

    let gc_result = ii
        .scan_gc(cb, None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>)
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
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40, 71, 72),
            num_entries: 3,
            first_doc_id: 40,
            last_doc_id: 72,
        },
    ];
    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    assert_eq!(
        ii.memory_usage(),
        48 // InvertedIndex stack (sealed Arc ptr + pending Vec triple + atomics)
        + 24 // Arc<ThinVec> heap header backing `sealed`
        + 4 * std::mem::size_of::<Arc<IndexBlock>>() // pending Vec heap: 4 pointer slots
        + 4 * (ARC_HEADER_BYTES + IndexBlock::STACK_SIZE) // 4 Arc<IndexBlock> allocations
        + 8 // buffer of the first index block (doc ids 10, 11)
        + 16 // buffer of the second index block (doc ids 20, 21, 22)
        + 8 // buffer of the third index block (doc id 30)
        + 16 // buffer of the fourth index block (doc ids 40, 71, 72)
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
                    },
                    IndexBlock {
                        buffer: encode_ids!(Dummy, 72),
                        num_entries: 1,
                        first_doc_id: 72,
                        last_doc_id: 72,
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

    // After `apply_gc`, three survivors compact into `sealed` and the trailing survivor
    // stays as the lone `pending` entry — the actively-mutated tail.
    assert_eq!(
        ii.memory_usage(),
        48 // InvertedIndex stack
        + 24 // Arc<ThinVec> heap header backing `sealed`
        + 8 // ThinVec header for 3 sealed blocks
        + 3 * IndexBlock::STACK_SIZE // 3 sealed IndexBlocks
        + 8 + 8 + 8 // 3 sealed buffers (1 entry each)
        + std::mem::size_of::<Arc<IndexBlock>>() // pending Vec heap: 1 slot
        + ARC_HEADER_BYTES + IndexBlock::STACK_SIZE // pending tail's Arc<IndexBlock>
        + 8 // pending tail's buffer (1 entry)
    );

    assert_eq!(ii.unique_docs(), 4);
    assert_eq!(
        ii.blocks_snapshot(),
        vec![
            IndexBlock {
                buffer: encode_ids!(Dummy, 21),
                num_entries: 1,
                first_doc_id: 21,
                last_doc_id: 21,
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 30),
                num_entries: 1,
                first_doc_id: 30,
                last_doc_id: 30,
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 40),
                num_entries: 1,
                first_doc_id: 40,
                last_doc_id: 40,
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 72),
                num_entries: 1,
                first_doc_id: 72,
                last_doc_id: 72,
            },
        ]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Three blocks freed (sum of `mem_usage`): 56 + 64 + 64 = 184. Three were
            // replaced by smaller blocks totalling 168, so the block-level net is
            // 184 - 168 = 16 freed. On top of that, three survivors get compacted from
            // `Arc<IndexBlock>` slots in `pending` into bare `IndexBlock` entries in
            // `sealed` — each compaction saves one Arc header + one pending Vec slot
            // (24 bytes), partially offset by the new sealed `ThinVec` header. The
            // net memory_usage delta works out to 80 bytes, folded into `bytes_freed`
            // (184 + 64 adjustment = 248) so callers' running totals stay accurate.
            bytes_freed: 248,
            bytes_allocated: 168,
            entries_removed: 5,
            // Removed 3, added back (split blocks) — see `apply_gc` for the exact net delta
            block_count_delta: 0,
            ignored_last_block: false,
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
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },
    ];

    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    assert_eq!(
        ii.memory_usage(),
        48 // InvertedIndex stack
        + 24 // Arc<ThinVec> heap header backing `sealed`
        + 2 * std::mem::size_of::<Arc<IndexBlock>>() // pending Vec heap: 2 pointer slots
        + 2 * (ARC_HEADER_BYTES + IndexBlock::STACK_SIZE) // 2 Arc<IndexBlock> allocations
        + 8 // buffer of the first index block (doc ids 10, 11)
        + 16 // buffer of the second index block (doc ids 20, 21, 22)
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

    // Block 0 was deleted; block 1 was the "last block" whose Replace delta got
    // dropped because the recorded `last_block_num_entries` didn't match. The
    // single survivor lands in `pending` (the actively-mutated tail) — `sealed`
    // stays empty.
    assert_eq!(
        ii.memory_usage(),
        48 // InvertedIndex stack
        + 24 // Arc<ThinVec> heap header backing `sealed` (empty ThinVec)
        + std::mem::size_of::<Arc<IndexBlock>>() // pending Vec heap: 1 slot
        + ARC_HEADER_BYTES + IndexBlock::STACK_SIZE // pending tail's Arc<IndexBlock>
        + 16 // pending tail's buffer (doc ids 20, 21, 22)
    );

    assert_eq!(ii.unique_docs(), 3);
    assert_eq!(
        ii.blocks_snapshot(),
        vec![IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Block 0 freed: 48 STACK_SIZE + 8 buffer = 56. The surviving block 1
            // moves from `pending` (Arc + slot) to `pending` (still Arc + slot, just
            // the only entry now), so its Arc wrapper is unchanged. But the pending
            // Vec capacity drops from 2 to 1 — freeing 8 bytes for one slot. That
            // 8-byte adjustment is added below into `bytes_freed`. Net: 56 + 8 + 16
            // (the Arc<IndexBlock> for block 0) = 80.
            bytes_freed: 80,
            bytes_allocated: 0,
            entries_removed: 2,
            // Removed one block
            block_count_delta: -1,
            // Ignored the last block
            ignored_last_block: true,
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
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
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
            // Same accounting as the previous test: 56 bytes for the dropped
            // IndexBlock + 16 Arc header + 8 pending Vec slot freed = 80.
            bytes_freed: 80,
            bytes_allocated: 0,
            entries_removed: 2,
            block_count_delta: -1,
            // The key assertion: ignored_last_block must be true even without
            // a delta for the last block.
            ignored_last_block: true,
        }
    );

    // Block 0 was deleted, block 1 (unchanged) remains.
    assert_eq!(
        ii.blocks_snapshot(),
        vec![IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        }]
    );
}

#[test]
fn ii_apply_gc_last_block_updated_after_rollover() {
    // The fork-side scan saw block 1 as the tail with 2 entries. Before
    // `apply_gc` runs, the parent appends a third entry to that block and then
    // rolls over to a brand-new tail (block 2). The scanned block is therefore
    // no longer the current tail, but its contents differ from what the scan
    // observed — `apply_gc` must still detect the change and drop the stale
    // delta for it.
    let blocks = medium_thin_vec![
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
        },
        // Block 1: post-append, pre-rollover state — scan saw 2 entries here.
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },
        // Block 2: brand-new tail added by the parent after the scan.
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
    ];

    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    // Stale deltas computed by the fork-side scan. Block 0 has real deletions;
    // block 1's `Replace` was computed from the pre-append contents and must be
    // dropped. The scan never observed block 2 so no delta exists for it.
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
                }],
                n_unique_docs_removed: 2,
            },
        },
    ];

    let delta = GcScanDelta {
        last_block_idx: 1,
        // Scan saw 2 entries on block 1; the parent has since added a third.
        last_block_num_entries: 2,
        deltas: gc_result,
    };

    let apply_info = ii.apply_gc(delta);

    // Block 0's delta applies; block 1's stale `Replace` is dropped; block 2 is
    // preserved untouched.
    assert_eq!(
        ii.blocks_snapshot(),
        vec![
            IndexBlock {
                buffer: encode_ids!(Dummy, 20, 21, 22),
                num_entries: 3,
                first_doc_id: 20,
                last_doc_id: 22,
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 30),
                num_entries: 1,
                first_doc_id: 30,
                last_doc_id: 30,
            },
        ]
    );
    assert!(apply_info.ignored_last_block);
    assert_eq!(apply_info.entries_removed, 2);
    assert_eq!(apply_info.block_count_delta, -1);
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
            RSIndexResult::build_virt().build()
        }
    }

    // Create entries tracking index with two duplicate records
    let mut ii = EntriesTrackingIndex::<AllowDupsDummy>::new(IndexFlags_Index_DocIdsOnly);

    let _ = ii
        .add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    let _ = ii
        .add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    let _ = ii
        .add_record(&RSIndexResult::build_virt().doc_id(15).build())
        .unwrap();
    let _ = ii
        .add_record(&RSIndexResult::build_virt().doc_id(15).build())
        .unwrap();

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
                }],
                n_unique_docs_removed: 1,
            },
        }],
    };

    let doc_exist = |id| id == 15;

    let mut repaired = Vec::new();

    let repair =
        |result: &RSIndexResult, _ctx: &crate::RepairContext<'_>| repaired.push(result.doc_id);

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
        ii.inner().blocks_snapshot(),
        vec![IndexBlock {
            buffer: encode_ids!(AllowDupsDummy, 15, 15),
            num_entries: 2,
            first_doc_id: 15,
            last_doc_id: 15,
        },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            bytes_freed: 65,
            bytes_allocated: 56,
            entries_removed: 2,
            block_count_delta: 0,
            ignored_last_block: false,
        }
    );
}
// The pre-snapshot `test_refresh_buffer_pointers_after_reallocation` was
// removed alongside Step A. Snapshot block buffers are immutable `Vec<u8>`
// clones owned by the reader, so the in-place reallocation scenario that
// test exercised cannot happen anymore. Reader-side staleness after writes
// is covered by `tests::reader::core::reader_needs_revalidation_detects_appends_without_gc_marker_bump`
// and by `reader_reset`.
