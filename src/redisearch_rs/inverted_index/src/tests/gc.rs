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
            false,
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
            false,
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
            false,
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
            false,
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
            // The three blocks before the last keep 0, 2 and 1 entries, which fit one
            // block (see `Dummy`), so they are compacted into it.
            deltas: vec![BlockGcScanResult {
                span: 3,
                index: 0,
                repair: RepairType::Replace {
                    blocks: smallvec![IndexBlock {
                        buffer: encode_ids!(Dummy, 21, 22, 30),
                        num_entries: 3,
                        first_doc_id: 21,
                        last_doc_id: 30,
                        expiration_bits: Default::default(),
                    }],
                    n_unique_docs_removed: 3
                },
            }]
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
            span: 1,
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            span: 1,
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
            span: 1,
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
            span: 1,
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        },
        BlockGcScanResult {
            span: 1,
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
        span: 1,
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
            span: 1,
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

/// A write that moves a block's buffer must be reported by
/// [`needs_revalidation`](crate::IndexReader::needs_revalidation), even though no GC ran.
///
/// The repair itself is the reader's re-seek on `revalidate`; this test only checks that a
/// caller is told to revalidate in the first place. A reader that is not told holds a pointer
/// into freed memory.
#[test]
#[cfg_attr(miri, ignore = "the memory hack below raises error in miri")]
fn test_needs_revalidation_after_block_buffer_moved() {
    use crate::IndexReader as _;

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(10).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(11).build())
        .unwrap();

    // SAFETY: mirrors what a suspended cursor does — the reader is parked, untouched, while the
    // index is written to. The reader is not used until after the writes, and the writes only ever
    // reallocate block buffers, leaving the `InvertedIndex` itself in place.
    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;

    let mut reader: crate::IndexReaderCore<'_, Dummy> = ii.reader();
    let mut result = RSIndexResult::build_virt().build();
    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 10);
    assert!(
        !reader.needs_revalidation(),
        "an untouched index must not ask for revalidation"
    );

    // SAFETY: see the `ii_ptr` construction above.
    unsafe {
        crate::test_utils::relocate_block_buffer(&mut *ii_ptr, 0);
    }

    assert_eq!(
        ii.gc_marker(),
        0,
        "no GC ran, so the marker cannot be what flags this"
    );
    assert!(
        reader.needs_revalidation(),
        "a moved block buffer must be reported: the reader's cached pointer is now dangling"
    );
}

/// A liveness source that knows exactly which IDs are gone and can count them in
/// a range, as the in-process GC's deleted set does.
#[derive(Clone)]
struct Gone(Vec<DocId>);

impl crate::DocLiveness for Gone {
    fn exists(&self, id: DocId) -> bool {
        !self.0.contains(&id)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        self.gone_in_range(first, last) != Some(0)
    }

    fn gone_in_range(&self, first: DocId, last: DocId) -> Option<usize> {
        Some(
            self.0
                .iter()
                .filter(|id| (first..=last).contains(id))
                .count(),
        )
    }
}

/// A block holding `ids`, encoded as [`encode_ids!`] does.
fn block(ids: &[DocId]) -> IndexBlock {
    let mut writer = Cursor::new(Vec::new());
    let mut last = ids[0];
    for &id in ids {
        Dummy::encode(
            &mut writer,
            (id - last) as u32,
            &RSIndexResult::build_virt().doc_id(id).build(),
        )
        .unwrap();
        last = id;
    }
    IndexBlock {
        buffer: writer.into_inner(),
        num_entries: ids.len() as u16,
        first_doc_id: ids[0],
        last_doc_id: *ids.last().unwrap(),
        expiration_bits: Default::default(),
    }
}

/// Blocks 10..=12, 20..=22 and 30..=31: two full blocks (see `Dummy`) and a last one.
fn three_blocks() -> InvertedIndex<Dummy> {
    InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[10, 11, 12]), block(&[20, 21, 22]), block(&[30, 31])],
    )
}

type NoRepair = fn(&RSIndexResult, &crate::RepairContext<'_>);

use crate::{DocLiveness, GcSnapshotScope};

/// A liveness source that can rule a range clean lets the scan skip its blocks, and
/// rules nothing out while a repair callback has to see every surviving entry.
#[test]
fn a_range_aware_liveness_skips_the_blocks_it_rules_clean() {
    let ii = three_blocks();

    let delta = ii
        .scan_gc(Gone(vec![21]), None::<NoRepair>)
        .unwrap()
        .unwrap();
    assert_eq!(
        delta.deltas,
        vec![BlockGcScanResult {
            span: 1,
            index: 1,
            repair: RepairType::Replace {
                blocks: smallvec![block(&[20, 22])],
                n_unique_docs_removed: 1,
            },
        }]
    );
    assert_eq!(ii.scan_gc(Gone(vec![]), None::<NoRepair>).unwrap(), None);

    let mut seen = Vec::new();
    ii.scan_gc(
        Gone(vec![21]),
        Some(|res: &RSIndexResult, _: &crate::RepairContext<'_>| seen.push(res.doc_id)),
    )
    .unwrap();
    assert_eq!(seen, vec![10, 11, 12, 20, 22, 30, 31]);
}

/// Looking for a dead entry before re-encoding a block changes nothing about the
/// delta, whichever way the counts compare with the blocks' entries.
#[test]
fn the_pre_check_scans_like_the_one_pass_scan() {
    let ii = three_blocks();
    for gone in [vec![11, 21], vec![10, 11], vec![31], vec![]] {
        let counting = Gone(gone.clone());
        let one_pass = |id: DocId| !gone.contains(&id);
        assert_eq!(
            ii.scan_gc(counting, None::<NoRepair>).unwrap(),
            ii.scan_gc(one_pass, None::<NoRepair>).unwrap(),
            "gone: {gone:?}"
        );
    }
}

/// A snapshot carries the data of the blocks with a gone document only, and
/// everything else a scan of it reads.
#[test]
fn a_snapshot_copies_only_the_blocks_with_a_gone_document() {
    let ii = three_blocks();
    ii.gc_marker_inc();

    let snapshot = ii
        .gc_snapshot(&Gone(vec![21]), GcSnapshotScope::DirtyBlocks)
        .unwrap();
    let dataless = |b: &IndexBlock| IndexBlock {
        buffer: Vec::new(),
        num_entries: b.num_entries,
        first_doc_id: b.first_doc_id,
        last_doc_id: b.last_doc_id,
        expiration_bits: Default::default(),
    };
    assert_eq!(
        snapshot.blocks.to_vec(),
        vec![
            dataless(&ii.blocks[0]),
            ii.blocks[1].clone(),
            dataless(&ii.blocks[2])
        ]
    );
    assert_eq!(snapshot.gc_marker(), ii.gc_marker());
    assert_eq!(snapshot.unique_id(), ii.unique_id());
    assert_eq!(snapshot.unique_docs(), ii.unique_docs());
    assert_eq!(snapshot.doc_id_range(), Some(10..=31));
    assert_eq!(snapshot.last_block_doc_id_range(), Some(30..=31));

    let full = ii
        .gc_snapshot(&Gone(vec![21]), GcSnapshotScope::AllBlocks)
        .unwrap();
    assert_eq!(full.blocks, ii.blocks);

    assert!(
        ii.gc_snapshot(&Gone(vec![]), GcSnapshotScope::DirtyBlocks)
            .is_none()
    );
}

/// A dataless block cannot be mistaken for one whose every entry is gone, even by
/// a scan with a liveness source that finds its range dirty.
#[test]
fn a_dataless_block_is_never_repaired() {
    let snapshot = three_blocks()
        .gc_snapshot(&Gone(vec![21]), GcSnapshotScope::DirtyBlocks)
        .unwrap();

    assert_eq!(
        snapshot.blocks[0]
            .repair(0, |_| false, None::<NoRepair>, false, PhantomData::<Dummy>)
            .unwrap(),
        None
    );
    let delta = snapshot
        .scan_gc(|_| false, None::<NoRepair>)
        .unwrap()
        .unwrap();
    assert_eq!(
        delta.deltas,
        vec![BlockGcScanResult {
            span: 1,
            index: 1,
            repair: RepairType::Delete {
                n_unique_docs_removed: 3
            },
        }]
    );
}

/// With a liveness source, a last block that grew since the scan is rescanned as it
/// is now instead of being denied: its dead entries go and the new ones stay.
#[test]
fn apply_gc_with_rescans_a_grown_last_block() {
    let stale_delta = || GcScanDelta {
        last_block_idx: 1,
        // The scan saw two entries in the last block; a third was appended since.
        last_block_num_entries: 2,
        deltas: vec![
            BlockGcScanResult {
                span: 1,
                index: 0,
                repair: RepairType::Delete {
                    n_unique_docs_removed: 2,
                },
            },
            BlockGcScanResult {
                span: 1,
                index: 1,
                repair: RepairType::Replace {
                    blocks: smallvec![block(&[20])],
                    n_unique_docs_removed: 1,
                },
            },
        ],
    };
    let two_blocks = || {
        InvertedIndex::<Dummy>::from_blocks(
            IndexFlags_Index_DocIdsOnly,
            medium_thin_vec![block(&[10, 11]), block(&[20, 21, 22])],
        )
    };

    let mut ii = two_blocks();
    let info = ii.apply_gc_with(stale_delta(), Some(&Gone(vec![10, 11, 21])));
    assert_eq!(ii.blocks.to_vec(), vec![block(&[20, 22])]);
    assert_eq!(ii.unique_docs(), 2);
    assert!(!info.ignored_last_block);
    assert_eq!(info.entries_removed, 3);

    // Nothing of the grown block is gone after all: it stays as it is.
    let mut ii = two_blocks();
    let info = ii.apply_gc_with(stale_delta(), Some(&Gone(vec![10, 11])));
    assert_eq!(ii.blocks.to_vec(), vec![block(&[20, 21, 22])]);
    assert_eq!(ii.unique_docs(), 3);
    assert!(!info.ignored_last_block);
    assert_eq!(info.entries_removed, 2);

    // Without a liveness source the grown block is denied, as `apply_gc` does.
    let mut ii = two_blocks();
    let info = ii.apply_gc_with(stale_delta(), None);
    assert_eq!(ii.blocks.to_vec(), vec![block(&[20, 21, 22])]);
    assert!(info.ignored_last_block);
}

/// A grown last block the rescan cannot decode is denied rather than silently kept.
#[test]
fn apply_gc_with_denies_a_grown_last_block_it_cannot_decode() {
    let mut ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![
            block(&[10, 11]),
            IndexBlock {
                // Three entries claimed, three bytes present: the decoder needs four.
                buffer: vec![0, 0, 0],
                num_entries: 3,
                first_doc_id: 20,
                last_doc_id: 22,
                expiration_bits: Default::default(),
            }
        ],
    );
    let delta = GcScanDelta {
        last_block_idx: 1,
        last_block_num_entries: 2,
        deltas: vec![BlockGcScanResult {
            span: 1,
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        }],
    };

    let info = ii.apply_gc_with(delta, Some(&Gone(vec![10, 11, 21])));

    assert!(info.ignored_last_block);
    assert_eq!(info.entries_removed, 2);
    assert_eq!(ii.blocks.len(), 1);
}

/// A repaired block's replacement holds no slack: the GC frees memory, it should
/// not leave the capacity the encoder grew along the way.
#[test]
fn a_repaired_block_has_no_spare_capacity() {
    let RepairType::Replace { blocks, .. } = block(&[20, 21, 22, 23, 24, 25, 26, 27])
        .repair(
            0,
            |id| id != 21,
            None::<NoRepair>,
            false,
            PhantomData::<Dummy>,
        )
        .unwrap()
        .unwrap()
    else {
        panic!("a partially dead block is replaced");
    };
    for b in &blocks {
        assert_eq!(b.buffer.capacity(), b.buffer.len());
    }
}

/// A gone ID inside a block's range but absent from the block is the case the
/// pre-check exists for: the block is decoded, found clean, and never re-encoded.
#[test]
fn the_pre_check_leaves_a_block_alone_whose_range_only_seems_dirty() {
    let ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[10, 12]), block(&[20, 21])],
    );
    let gone = Gone(vec![11]);

    assert_eq!(ii.scan_gc(gone.clone(), None::<NoRepair>).unwrap(), None);
    assert_eq!(
        ii.scan_gc(|id| gone.exists(id), None::<NoRepair>).unwrap(),
        None
    );
}

/// A grown last block whose new entries are gone too is deleted by the rescan,
/// even when it is the index's only block.
#[test]
fn apply_gc_with_deletes_a_grown_last_block_that_is_all_gone() {
    let mut ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[20, 21, 22])],
    );
    let delta = GcScanDelta {
        last_block_idx: 0,
        last_block_num_entries: 2,
        deltas: vec![BlockGcScanResult {
            span: 1,
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        }],
    };

    let info = ii.apply_gc_with(delta, Some(&Gone(vec![20, 21, 22])));

    assert!(ii.blocks.is_empty());
    assert_eq!(ii.unique_docs(), 0);
    assert_eq!(info.entries_removed, 3);
    assert_eq!(info.block_count_delta, -1);
    assert!(!info.ignored_last_block);
}

/// Blocks appended after the scanned last block are left as they are: the rescan
/// covers the block the scan knew as last, and the delta list stays in block order.
#[test]
fn apply_gc_with_leaves_blocks_appended_after_the_scanned_last_block_alone() {
    let mut ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[10, 11]), block(&[20, 21, 22]), block(&[30, 31])],
    );
    // The scan saw two blocks, the second with two entries; a third entry and a whole
    // block were appended since.
    let delta = GcScanDelta {
        last_block_idx: 1,
        last_block_num_entries: 2,
        deltas: vec![BlockGcScanResult {
            span: 1,
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: 2,
            },
        }],
    };

    let info = ii.apply_gc_with(delta, Some(&Gone(vec![10, 11, 21, 30])));

    assert_eq!(ii.blocks.to_vec(), vec![block(&[20, 22]), block(&[30, 31])]);
    assert_eq!(info.entries_removed, 3);
    assert!(!info.ignored_last_block);
}

/// Consecutive under-filled blocks before the last whose entries fit one block are
/// re-encoded into one, even when nothing is gone: the snapshot copies them, the scan
/// spans them in one delta, and the apply drops the originals.
#[test]
fn under_filled_runs_are_compacted_into_one_block() {
    let mut ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[1]), block(&[2]), block(&[5]), block(&[20])],
    );
    assert!(ii.has_compactable_run());

    let snapshot = ii
        .gc_snapshot(&Gone(vec![]), GcSnapshotScope::DirtyBlocks)
        .unwrap();
    assert!(snapshot.blocks[..3].iter().all(|b| !b.buffer.is_empty()));
    assert!(
        snapshot.blocks[3].buffer.is_empty(),
        "the last block is not copied"
    );

    let delta = snapshot
        .scan_gc(Gone(vec![]), None::<NoRepair>)
        .unwrap()
        .unwrap();
    assert_eq!(
        delta.deltas,
        vec![BlockGcScanResult {
            span: 3,
            index: 0,
            repair: RepairType::Replace {
                blocks: smallvec![block(&[1, 2, 5])],
                n_unique_docs_removed: 0,
            },
        }]
    );

    let unique_docs = ii.unique_docs();
    let info = ii.apply_gc(delta);
    assert_eq!(ii.blocks.to_vec(), vec![block(&[1, 2, 5]), block(&[20])]);
    assert_eq!(info.entries_removed, 0);
    assert_eq!(info.block_count_delta, -2);
    assert_eq!(ii.unique_docs(), unique_docs);
    assert!(!ii.has_compactable_run());
}

/// A run whose merged entries would not fit fewer blocks (here the codec's delta
/// cannot span the gap) is left to per-block repair, so a clean one yields no delta.
#[test]
fn a_run_that_would_not_shrink_is_not_compacted() {
    let far = 1 << 33;
    let ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[1]), block(&[far]), block(&[far + 1])],
    );
    assert!(ii.has_compactable_run());
    assert_eq!(ii.scan_gc(Gone(vec![]), None::<NoRepair>).unwrap(), None);
}

/// The last block is the one writers append to and the one the apply checks for
/// staleness by position, so it never joins a run.
#[test]
fn the_last_block_never_joins_a_run() {
    let ii = InvertedIndex::<Dummy>::from_blocks(
        IndexFlags_Index_DocIdsOnly,
        medium_thin_vec![block(&[1]), block(&[2])],
    );
    assert!(!ii.has_compactable_run());
    assert_eq!(ii.scan_gc(Gone(vec![]), None::<NoRepair>).unwrap(), None);
    assert!(
        ii.gc_snapshot(&Gone(vec![]), GcSnapshotScope::DirtyBlocks)
            .is_none()
    );
}
