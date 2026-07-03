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
    InvertedIndex, gc::BlockGcScanResult, gc::RepairType,
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

    // PRE-GC layout:
    //   - Empty `InvertedIndex` overhead: 56 bytes (40 stack + 16 empty sealed Arc header).
    //   - `from_blocks` puts all 4 blocks into `pending` (ThinVec: 8-byte header +
    //     capacity 4 → 40 bytes); the last one is the writable tail.
    //   - Per pending block: ARC_HEADER (16) + STACK_SIZE (48) + buffer.cap.
    assert_eq!(
        ii.memory_usage(),
        56 // empty InvertedIndex overhead
        + 40 // pending ThinVec heap (8-byte header + cap=4)
        + (16 + IndexBlock::STACK_SIZE) * 4 // ARC_HEADER + STACK_SIZE for each of the 4 pending blocks
        + 8 + 16 + 8 + 16 // buffer capacities of the 4 blocks
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

    let apply_info = ii.apply_gc(delta);

    // POST-GC layout (sealed is a single `Arc<[IndexBlock]>` allocation):
    //   - 3 sealed blocks live inline in one Arc allocation — the refcount header (16)
    //     immediately followed by 3 in-line IndexBlocks (no ThinVec header or separate
    //     backing buffer; the slice length rides in the fat pointer on the stack):
    //       Replace_block_for_1 (cap=8),
    //       survivor_of_block_2 (cap=8, moved out of its uniquely-owned sealed slot
    //       without cloning its buffer),
    //       Replace_block_for_3a (cap=8).
    //   - 1 pending tail block, Arc-wrapped: Replace_block_for_3b (cap=8).
    assert_eq!(
        ii.memory_usage(),
        40 // InvertedIndex stack (size_of::<Self>, incl. sealed fat-pointer length)
        + 16 // sealed Arc refcount header
        + IndexBlock::STACK_SIZE * 3 // sealed slots (in-line IndexBlocks)
        + 8 + 8 + 8 // sealed buffer capacities
        + 40 // pending ThinVec heap (8-byte header + cap=4) for the single tail block
        + (16 + IndexBlock::STACK_SIZE + 8) // pending tail: ARC_HEADER + STACK_SIZE + buffer cap 8
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
            // Per-block frees (184) plus the container-level overhead released during
            // compaction (Arc<IndexBlock> wrappers, sealed region reallocated as a single
            // `Arc<[IndexBlock]>`) — reconciled to match the memory_usage() delta. 8 bytes
            // more than the ThinVec-backed sealed: the compacted region no longer carries a
            // ThinVec header. Still less than pre-fold: the surviving tail keeps a pending
            // ThinVec, so that heap is not freed.
            bytes_freed: 232,
            // The third and fifth block was split making 168 new bytes
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

    // PRE-GC layout: 2 pending blocks (the last is the writable tail). See `ii_apply_gc`
    // for the breakdown.
    assert_eq!(
        ii.memory_usage(),
        56 // empty InvertedIndex overhead
        + 40 // pending ThinVec heap (8-byte header + cap=4)
        + (16 + IndexBlock::STACK_SIZE) * 2 // ARC_HEADER + STACK_SIZE for each pending block
        + 8 + 16 // buffer capacities of the two blocks
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

    let apply_info = ii.apply_gc(delta);

    // POST-GC: block 0 deleted, block 1 survives unchanged (last-block delta dropped
    // because num_entries grew since the scan). The surviving block becomes the sole
    // `pending` tail (Arc-wrapped); sealed is empty.
    assert_eq!(
        ii.memory_usage(),
        56 // empty InvertedIndex overhead
        + 40 // pending ThinVec heap (8-byte header + cap=4) for the single tail block
        + (16 + IndexBlock::STACK_SIZE + 16) // pending tail: ARC_HEADER + STACK_SIZE + buffer cap 16
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
            // Block 0 freed: its Arc<IndexBlock> wrapper (16) + IndexBlock STACK (48) +
            // buffer (8) = 72, reconciled to match the memory_usage() delta. The surviving
            // tail keeps the pending ThinVec, so no ThinVec heap is freed here.
            bytes_freed: 72,
            // Nothing new was made in the end
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
            // Block 0 freed: its Arc<IndexBlock> wrapper (16) + IndexBlock STACK (48) +
            // buffer (8) = 72, reconciled against memory_usage(). The surviving tail keeps
            // the pending ThinVec, so no ThinVec heap is freed here.
            bytes_freed: 72,
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

/// Regression for [MOD-16144]: if the block that was `in_progress` at scan time
/// rolled into `pending` before apply (gaining entries), the apply path must still
/// detect that and drop the stale tail delta. Before the fix, `last_block_changed`
/// silently returned false for the pending case.
#[test]
fn ii_apply_gc_last_block_rolled_into_pending() {
    // Three blocks → from_blocks places blocks 0, 1 in `pending` and block 2 in
    // `in_progress`. The block at logical index 1 stands in for "the block that
    // was in_progress at scan time and rolled over before apply" — its
    // `num_entries=3` is higher than what the scan saw.
    let blocks = medium_thin_vec![
        // Block 0 (pending) — scan said: delete entirely.
        IndexBlock {
            buffer: encode_ids!(Dummy, 10, 11),
            num_entries: 2,
            first_doc_id: 10,
            last_doc_id: 11,
        },
        // Block 1 (pending now, was in_progress at scan time with 1 entry).
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
        },
        // Block 2 (in_progress) — a new tail created when block 1 rolled over.
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
        },
    ];

    let mut ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    let gc_result = vec![
        BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        },
        // Stale delta for block 1: scan saw 1 entry; the post-rollover block has 3.
        // The apply path must drop this delta.
        BlockGcScanResult {
            index: 1,
            repair: RepairType::Delete {
                n_unique_docs_removed: 1,
            },
        },
    ];

    let delta = GcScanDelta {
        last_block_idx: 1,
        last_block_num_entries: 1, // scan-time value; the live pending block has 3
        deltas: gc_result,
    };

    let apply_info = ii.apply_gc(delta);

    // Block 1 must survive intact — its stale delta was dropped, NOT applied.
    assert!(
        ii.blocks_snapshot()
            .iter()
            .any(|b| b.first_doc_id == 20 && b.num_entries == 3),
        "rolled-over block was not preserved: {:?}",
        ii.blocks_snapshot()
    );

    // Block 0 was deleted; block 1 and block 2 survive.
    assert_eq!(ii.number_of_blocks(), 2);
    assert!(apply_info.ignored_last_block, "stale tail must be detected");
    assert_eq!(apply_info.entries_removed, 2, "only block 0 deleted");
    // n_unique_docs: 2+3+1 pre, minus 2 from block 0 deletion = 4 (block 1's stale
    // n_unique_docs_removed=1 must NOT be applied).
    assert_eq!(ii.unique_docs(), 4);
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

    let apply_info = ii.apply_gc(expected_delta);
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
            // bytes_freed = freed block's STACK_SIZE (48) + buffer.cap (17 after 4
            // in-place ControlledCursor writes) = 65.
            bytes_freed: 65,
            bytes_allocated: 56,
            entries_removed: 2,
            block_count_delta: 0,
            ignored_last_block: false,
        }
    );
}
#[cfg_attr(miri, ignore = "the memory hack below raises error in miri")]
#[test]
fn reader_snapshot_is_frozen_against_concurrent_writes() {
    // Epic 1, Story 1.2: the reader holds an `Arc<State>` snapshot taken at construction
    // time and reads through it. Writes that happen afterwards are invisible until the
    // caller `reset()`s, which re-snapshots. `refresh_buffer_pointers` is a no-op in this
    // model — pre-snapshot it existed to handle in-place buffer reallocation, but
    // snapshot block buffers are immutable.
    use crate::IndexReader as _;

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(11).build())
        .unwrap();

    // Bypass Rust's borrow rules to simulate the real-world scenario where writes happen
    // concurrently with a reader. In production these are on different threads under the
    // spec lock; here we use a raw pointer so we can mutate the index while a reader
    // borrow is live.
    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;

    let mut reader: crate::IndexReaderCore<'_, Dummy> = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 10);

    // Add many more records — these go into `ii.blocks` and into a new `state` revision,
    // but the reader's snapshot still references the original revision.
    unsafe {
        for i in 12..1000 {
            (*ii_ptr)
                .add_record(&RSIndexResult::build_virt().doc_id(i).build())
                .unwrap();
        }
    }

    // No-op in the snapshot model. The reader continues to see its frozen view.
    reader.refresh_buffer_pointers();

    // From the frozen snapshot, the reader sees only doc_id 11 (the second record at
    // snapshot time), then EOF.
    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 11);
    assert!(!reader.next_record(&mut result).unwrap());

    // `reset()` takes a fresh snapshot — now the reader sees everything.
    reader.reset();
    let mut count = 0;
    let mut expected = 10;
    while reader.next_record(&mut result).unwrap() {
        assert_eq!(result.doc_id, expected);
        count += 1;
        expected += 1;
    }
    // Initial 2 records + 988 added = 990.
    assert_eq!(count, 990);
}
