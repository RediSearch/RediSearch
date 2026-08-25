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
    IndexReader, InvertedIndex, gc::BlockGcScanResult, gc::RepairType,
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
        expiration_bits: Default::default(),
    };

    fn cb(doc_id: DocId) -> bool {
        ![10, 11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<Dummy>,
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
        expiration_bits: Default::default(),
    };

    fn cb(_doc_id: DocId) -> bool {
        true
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<Dummy>,
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
        expiration_bits: Default::default(),
    };

    fn cb(doc_id: DocId) -> bool {
        [11].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<Dummy>,
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
                expiration_bits: Default::default(),
            }],
            n_unique_docs_removed: 2
        })
    );
}

#[test]
fn index_block_repair_replays_surviving_prefix() {
    // Entries that survive *before* the first dead one are decoded and dropped
    // before the repair knows the block will change, so they have to be replayed
    // from the buffer. Every other repair test kills the first entry, which
    // leaves that replay with nothing to do.
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 11, 12, 13),
        num_entries: 4,
        first_doc_id: 10,
        last_doc_id: 13,
        expiration_bits: Default::default(),
    };

    fn cb(doc_id: DocId) -> bool {
        doc_id != 12
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<Dummy>,
        )
        .unwrap();

    assert_eq!(
        repair_status,
        Some(RepairType::Replace {
            blocks: smallvec![IndexBlock {
                first_doc_id: 10,
                last_doc_id: 13,
                num_entries: 3,
                buffer: encode_ids!(Dummy, 10, 11, 13),
                expiration_bits: Default::default(),
            }],
            n_unique_docs_removed: 1
        })
    );
}

#[test]
fn index_block_repair_replays_prefix_with_duplicate_doc_ids() {
    // `unique_write` is seeded by the prefix replay, and duplicates must not
    // inflate it — otherwise `n_unique_docs_removed` is wrong and `apply_gc`
    // corrupts the index's unique-document count.
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 10, 11, 12),
        num_entries: 4,
        first_doc_id: 10,
        last_doc_id: 12,
        expiration_bits: Default::default(),
    };

    fn cb(doc_id: DocId) -> bool {
        doc_id != 12
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<Dummy>,
        )
        .unwrap();

    // Three unique docs read (10, 11, 12), two written (10, 11), so exactly one
    // unique doc was removed — the repeated 10 must not be counted twice.
    //
    // The replayed block holds two entries, not three: `Dummy` sets
    // `ALLOW_DUPLICATES = false`, so `add_record` drops the repeat on re-encode.
    // That is pre-existing behavior of re-encoding through `add_record`, asserted
    // here so the prefix replay is pinned to it rather than diverging.
    assert_eq!(
        repair_status,
        Some(RepairType::Replace {
            blocks: smallvec![IndexBlock {
                first_doc_id: 10,
                last_doc_id: 11,
                num_entries: 2,
                buffer: encode_ids!(Dummy, 10, 11),
                expiration_bits: Default::default(),
            }],
            n_unique_docs_removed: 1
        })
    );
}

#[test]
fn index_block_repair_invokes_callback_once_per_survivor() {
    // The prefix replay re-decodes records the callback has already seen. It must
    // not hand them to the callback a second time: `fork_gc`'s numeric collector
    // folds every survivor into an HLL, and a double count corrupts the estimate.
    let block = IndexBlock {
        buffer: encode_ids!(Dummy, 10, 11, 12, 13),
        num_entries: 4,
        first_doc_id: 10,
        last_doc_id: 13,
        expiration_bits: Default::default(),
    };

    let mut seen = Vec::new();
    block
        .repair(
            0,
            |doc_id: DocId| doc_id != 12,
            Some(|res: &RSIndexResult, _: &crate::RepairContext<'_>| seen.push(res.doc_id)),
            PhantomData::<Dummy>,
        )
        .unwrap();

    assert_eq!(seen, vec![10, 11, 13]);
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
        expiration_bits: Default::default(),
    };

    fn cb(doc_id: DocId) -> bool {
        ![41].contains(&doc_id)
    }

    let repair_status = block
        .repair(
            0,
            cb,
            None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>,
            PhantomData::<SmallDeltaDummy>,
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
                    expiration_bits: Default::default(),
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
                    expiration_bits: Default::default(),
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
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40),
            num_entries: 1,
            first_doc_id: 40,
            last_doc_id: 40,
            expiration_bits: Default::default(),
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
                            expiration_bits: Default::default(),
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
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
            expiration_bits: Default::default(),
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
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 30),
            num_entries: 1,
            first_doc_id: 30,
            last_doc_id: 30,
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 40, 71, 72),
            num_entries: 3,
            first_doc_id: 40,
            last_doc_id: 72,
            expiration_bits: Default::default(),
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
                    expiration_bits: Default::default(),
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
                        expiration_bits: Default::default(),
                    },
                    IndexBlock {
                        buffer: encode_ids!(Dummy, 72),
                        num_entries: 1,
                        first_doc_id: 72,
                        last_doc_id: 72,
                        expiration_bits: Default::default(),
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
                expiration_bits: Default::default(),
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 30),
                num_entries: 1,
                first_doc_id: 30,
                last_doc_id: 30,
                expiration_bits: Default::default(),
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 40),
                num_entries: 1,
                first_doc_id: 40,
                last_doc_id: 40,
                expiration_bits: Default::default(),
            },
            IndexBlock {
                buffer: encode_ids!(Dummy, 72),
                num_entries: 1,
                first_doc_id: 72,
                last_doc_id: 72,
                expiration_bits: Default::default(),
            },
        ]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // The first, second and fourth block were removed.
            bytes_freed: 208,
            // The third and fifth block were split into new blocks.
            bytes_allocated: 192,
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
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
            expiration_bits: Default::default(),
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
                    expiration_bits: Default::default(),
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
        + IndexBlock::STACK_SIZE // Size of the index blocks
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
            expiration_bits: Default::default(),
        },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            // Freed only the first block
            bytes_freed: 64,
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
            expiration_bits: Default::default(),
        },
        IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
            expiration_bits: Default::default(),
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
            bytes_freed: 64,
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
        ii.blocks,
        vec![IndexBlock {
            buffer: encode_ids!(Dummy, 20, 21, 22),
            num_entries: 3,
            first_doc_id: 20,
            last_doc_id: 22,
            expiration_bits: Default::default(),
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
                    expiration_bits: Default::default(),
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
        ii.inner().blocks,
        vec![IndexBlock {
            buffer: encode_ids!(AllowDupsDummy, 15, 15),
            num_entries: 2,
            first_doc_id: 15,
            last_doc_id: 15,
            expiration_bits: Default::default(),
        },]
    );
    assert_eq!(
        apply_info,
        GcApplyInfo {
            bytes_freed: 73,
            bytes_allocated: 64,
            entries_removed: 2,
            block_count_delta: 0,
            ignored_last_block: false,
        }
    );
}
#[cfg_attr(miri, ignore = "the memory hack below raises error in miri")]
#[test]
fn test_refresh_buffer_pointers_after_reallocation() {
    use crate::IndexReader as _;

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // Add initial records
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(11).build())
        .unwrap();

    // SAFETY: We need to bypass Rust's borrowing rules to simulate the real-world
    // scenario where buffer reallocation happens while a reader is active.
    // This is safe because:
    // 1. We're not accessing the reader during the mutation
    // 2. The InvertedIndex structure remains valid
    // 3. We call refresh_buffer_pointers before using the reader again
    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;

    let mut reader: crate::IndexReaderCore<'_, Dummy> = ii.reader();
    let mut result = RSIndexResult::build_virt().build();

    // Read first record
    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 10);

    // Force buffer reallocation by adding many records to the same block
    // This should cause the buffer to grow and potentially move
    unsafe {
        for i in 12..1000 {
            (*ii_ptr)
                .add_record(&RSIndexResult::build_virt().doc_id(i).build())
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

/// `Dummy`, but two entries fill a block — enough to build a multi-block index in a
/// test without writing hundreds of records.
#[derive(Clone)]
struct TinyBlocks;

impl Encoder for TinyBlocks {
    type Delta = u32;

    const RECOMMENDED_BLOCK_ENTRIES: u16 = 2;

    fn encode<W: std::io::Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        Dummy::encode(writer, delta, record)
    }
}

impl Decoder for TinyBlocks {
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        Dummy::decode(cursor, base, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        Dummy::base_result()
    }
}

/// An index of `doc_ids`, laid out two entries per block.
fn tiny_block_index(doc_ids: impl IntoIterator<Item = DocId>) -> InvertedIndex<TinyBlocks> {
    let mut ii = InvertedIndex::<TinyBlocks>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in doc_ids {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }
    ii
}

fn doc_ids_in(ii: &InvertedIndex<TinyBlocks>) -> Vec<DocId> {
    let mut reader = ii.reader();
    let mut result = RSIndexResult::build_virt().build();
    let mut ids = Vec::new();
    while reader.next_record(&mut result).unwrap() {
        ids.push(result.doc_id);
    }
    ids
}

#[test]
fn repair_tail_block_on_empty_index_is_a_noop() {
    let mut ii = InvertedIndex::<TinyBlocks>::new(IndexFlags_Index_DocIdsOnly);
    let marker = ii.gc_marker();

    assert_eq!(ii.repair_tail_block(0, |_| true).unwrap(), None);
    assert_eq!(ii.gc_marker(), marker);
}

#[test]
fn repair_tail_block_leaves_a_clean_block_alone() {
    let mut ii = tiny_block_index([10, 11, 12, 13]);
    let marker = ii.gc_marker();

    assert_eq!(ii.repair_tail_block(0, |_| true).unwrap(), None);
    // A reader positioned in the block must not be forced to revalidate for nothing.
    assert_eq!(ii.gc_marker(), marker);
    assert_eq!(doc_ids_in(&ii), vec![10, 11, 12, 13]);
}

#[test]
fn repair_tail_block_reclaims_from_the_tail_only() {
    // Blocks are [10, 11], [12, 13], [14, 15]. Doc 10 and doc 14 are both dead, but
    // only 14 is in the tail: the write path must not widen its cost to other blocks,
    // and doc 10 stays for the fork GC.
    let mut ii = tiny_block_index([10, 11, 12, 13, 14, 15]);
    let marker = ii.gc_marker();
    let docs_before = ii.unique_docs();

    let info = ii
        .repair_tail_block(0, |doc_id| doc_id != 10 && doc_id != 14)
        .unwrap()
        .expect("the tail block holds a dead entry");

    assert_eq!(doc_ids_in(&ii), vec![10, 11, 12, 13, 15]);
    assert_eq!(ii.unique_docs(), docs_before - 1);
    assert_eq!(ii.gc_marker(), marker + 1);
    // No snapshot was taken, so there is no stale last block to skip.
    assert!(!info.ignored_last_block);
}

#[test]
fn repair_tail_block_removes_a_wholly_dead_tail() {
    let mut ii = tiny_block_index([10, 11, 12, 13]);
    let blocks_before = ii.number_of_blocks();

    let info = ii
        .repair_tail_block(0, |doc_id| doc_id < 12)
        .unwrap()
        .expect("the whole tail block is dead");

    assert_eq!(doc_ids_in(&ii), vec![10, 11]);
    assert_eq!(ii.number_of_blocks(), blocks_before - 1);
    assert_eq!(info.block_count_delta, -1);
}

#[test]
fn repair_tail_block_honours_the_minimum_reclaim() {
    // One of two entries dies: a 50% reclaim, which a 100% threshold must reject.
    let mut ii = tiny_block_index([10, 11, 12, 13]);
    let marker = ii.gc_marker();

    assert_eq!(
        ii.repair_tail_block(100, |doc_id| doc_id != 12).unwrap(),
        None
    );
    assert_eq!(ii.gc_marker(), marker);
    assert_eq!(doc_ids_in(&ii), vec![10, 11, 12, 13]);

    // The same reclaim is accepted once the threshold is at or below what it achieves.
    assert!(
        ii.repair_tail_block(50, |doc_id| doc_id != 12)
            .unwrap()
            .is_some()
    );
    assert_eq!(doc_ids_in(&ii), vec![10, 11, 13]);
}

#[test]
fn maybe_repair_tail_block_repairs_a_full_block() {
    // Blocks hold two entries. A full tail block with a dead entry is the last moment
    // that block can be repaired inline — the next new document rotates it away.
    let mut ii = tiny_block_index([10, 11, 12, 13]);

    assert!(
        ii.maybe_repair_tail_block(0, 8, |doc_id| doc_id != 12)
            .unwrap()
            .is_some()
    );
    assert_eq!(doc_ids_in(&ii), vec![10, 11, 13]);
}

#[test]
fn maybe_repair_tail_block_leaves_a_clean_block_alone() {
    // Probing a clean block costs a decode and must change nothing observable — in
    // particular the GC marker must not move, or every reader would revalidate for
    // nothing.
    let mut ii = tiny_block_index([10, 11]);
    let marker = ii.gc_marker();

    assert_eq!(ii.maybe_repair_tail_block(0, 8, |_| true).unwrap(), None);
    assert_eq!(ii.gc_marker(), marker);
    assert_eq!(doc_ids_in(&ii), vec![10, 11]);
}

/// `Dummy`, but with a realistic block capacity, so a short posting list never fills its
/// only block — the case the probe stride exists to reach.
#[derive(Clone)]
struct RoomyBlocks;

impl Encoder for RoomyBlocks {
    type Delta = u32;

    const RECOMMENDED_BLOCK_ENTRIES: u16 = 100;

    fn encode<W: std::io::Write + std::io::Seek>(
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        Dummy::encode(writer, delta, record)
    }
}

impl Decoder for RoomyBlocks {
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        Dummy::decode(cursor, base, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        Dummy::base_result()
    }
}

fn roomy_block_index(doc_ids: impl IntoIterator<Item = DocId>) -> InvertedIndex<RoomyBlocks> {
    let mut ii = InvertedIndex::<RoomyBlocks>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in doc_ids {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }
    ii
}

fn roomy_doc_ids_in(ii: &InvertedIndex<RoomyBlocks>) -> Vec<DocId> {
    let mut reader = ii.reader();
    let mut result = RSIndexResult::build_virt().build();
    let mut ids = Vec::new();
    while reader.next_record(&mut result).unwrap() {
        ids.push(result.doc_id);
    }
    ids
}

#[test]
fn maybe_repair_tail_block_reaches_a_posting_list_shorter_than_a_block() {
    // Three entries in a block that holds a hundred. This list is entirely tail, so the
    // fork GC will never repair it — it discards deltas touching the last block. If the
    // write path waited for a full block, nothing would ever reclaim this.
    let mut ii = roomy_block_index([10, 11, 12]);

    assert!(
        ii.maybe_repair_tail_block(0, 8, |doc_id| doc_id != 11)
            .unwrap()
            .is_some()
    );
    assert_eq!(roomy_doc_ids_in(&ii), vec![10, 12]);
}

#[test]
fn maybe_repair_tail_block_probes_on_a_stride_once_past_the_first_entries() {
    // Past PROBE_EVERY_WRITE_BELOW the block is probed every PROBE_STRIDE appends, which
    // is what bounds the added decode rate. At 9 entries no probe happens even though one
    // is dead; the next stride boundary reclaims it.
    let mut ii = roomy_block_index(1..=9);

    assert_eq!(
        ii.maybe_repair_tail_block(0, 8, |doc_id| doc_id != 5)
            .unwrap(),
        None,
        "9 is neither below the every-write bound nor on a stride boundary"
    );
    assert_eq!(roomy_doc_ids_in(&ii), (1..=9).collect::<Vec<_>>());

    for doc_id in 10..=16 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }

    assert!(
        ii.maybe_repair_tail_block(0, 8, |doc_id| doc_id != 5)
            .unwrap()
            .is_some(),
        "16 is on a stride boundary"
    );
    let expected: Vec<DocId> = (1..=16).filter(|d| *d != 5).collect();
    assert_eq!(roomy_doc_ids_in(&ii), expected);
}

#[test]
fn maybe_repair_tail_block_with_stride_zero_waits_for_a_full_block() {
    // Stride 0 is the cheap end of the dial: no periodic probe, so an unfilled block is
    // left alone however much of it is dead. This is the trigger the change originally
    // shipped with, kept reachable for operators who want its ~5% write cost instead of
    // the default's reclaim.
    let mut ii = roomy_block_index(1..=16);

    assert_eq!(
        ii.maybe_repair_tail_block(0, 0, |doc_id| doc_id != 5)
            .unwrap(),
        None
    );
    assert_eq!(roomy_doc_ids_in(&ii), (1..=16).collect::<Vec<_>>());

    // A block that has filled is still repaired, since that check is independent of stride.
    let mut full = tiny_block_index([10, 11, 12, 13]);
    assert!(
        full.maybe_repair_tail_block(0, 0, |doc_id| doc_id != 12)
            .unwrap()
            .is_some()
    );
    assert_eq!(doc_ids_in(&full), vec![10, 11, 13]);
}

#[test]
fn maybe_repair_tail_block_stride_sets_the_probe_cadence() {
    // The dial's whole purpose: a wider stride probes less often, so the same dead entry
    // survives longer. At 9 entries neither 8 nor 16 divides it, but 3 does.
    let mut ii = roomy_block_index(1..=9);
    assert_eq!(
        ii.maybe_repair_tail_block(0, 16, |doc_id| doc_id != 5)
            .unwrap(),
        None
    );
    assert!(
        ii.maybe_repair_tail_block(0, 3, |doc_id| doc_id != 5)
            .unwrap()
            .is_some()
    );
}

#[test]
fn maybe_repair_tail_block_stride_is_monotonic() {
    // Raising the stride must never probe *more*. It would if the every-append bound were
    // derived from the stride rather than held constant: a large stride would put every
    // short block in the probe-always region and make the dial's cheap end its expensive
    // one. Nine entries is above the constant bound, so no stride that fails to divide it
    // may fire.
    for stride in [10, 16, 64, 1024] {
        let mut ii = roomy_block_index(1..=9);
        assert_eq!(
            ii.maybe_repair_tail_block(0, stride, |doc_id| doc_id != 5)
                .unwrap(),
            None,
            "stride {stride} probed a 9-entry block"
        );
    }
}

#[test]
fn maybe_repair_tail_block_still_honours_the_threshold_on_a_short_block() {
    // The threshold is a proportion of the block, so on a short block a single dead entry
    // is already a large fraction. Ten entries with one dead is 10%, below a 20% bar.
    let mut ii = roomy_block_index(1..=16);

    assert_eq!(
        ii.maybe_repair_tail_block(20, 8, |doc_id| doc_id != 5)
            .unwrap(),
        None
    );
    assert_eq!(roomy_doc_ids_in(&ii), (1..=16).collect::<Vec<_>>());

    // Four dead of sixteen clears the same bar.
    assert!(
        ii.maybe_repair_tail_block(20, 8, |doc_id| !(5..=8).contains(&doc_id))
            .unwrap()
            .is_some()
    );
    let expected: Vec<DocId> = (1..=16).filter(|d| !(5..=8).contains(d)).collect();
    assert_eq!(roomy_doc_ids_in(&ii), expected);
}

#[test]
fn repair_tail_block_agrees_with_the_fork_gc_path() {
    // The inline path and the scan/apply path share their accounting helpers; this
    // pins them to the same observable result on an index the fork GC can fully
    // repair (single block, so no last-block skip applies).
    let dead = |doc_id: DocId| doc_id != 11;

    let mut inline = tiny_block_index([10, 11]);
    inline
        .repair_tail_block(0, dead)
        .unwrap()
        .expect("doc 11 is dead");

    let mut forked = tiny_block_index([10, 11]);
    let delta = forked
        .scan_gc(dead, None::<fn(&RSIndexResult, &crate::RepairContext<'_>)>)
        .unwrap()
        .expect("doc 11 is dead");
    forked.apply_gc(delta);

    assert_eq!(doc_ids_in(&inline), doc_ids_in(&forked));
    assert_eq!(inline.unique_docs(), forked.unique_docs());
    assert_eq!(inline.number_of_blocks(), forked.number_of_blocks());
}
