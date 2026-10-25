/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the per-entry field-expiration bit, which is kept in each
//! [`IndexBlock`](crate::IndexBlock)'s `expiration_bits` side bitset (indexed by
//! entry ordinal) rather than inside the encoded entry. These tests exercise the
//! storage/round-trip layer directly by setting the bit on each record, so they
//! are agnostic to how producers derive it; they check that reads (sequential and
//! seek) and GC re-encoding recover the right bit per entry.

use crate::codec::{
    doc_ids_only::DocIdsOnly, fields_only::FieldsOnly, numeric::Numeric,
    raw_doc_ids_only::RawDocIdsOnly,
};
use crate::{IndexBlock, IndexReader, InvertedIndex, RepairContext};
use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreNumeric,
};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::DocId;

#[test]
fn block_bitset_set_and_get() {
    let mut block = IndexBlock::new(1);
    assert!(!block.expiration_bit(0));
    block.set_expiration_bit(3);
    block.set_expiration_bit(10);
    for ordinal in 0..16u16 {
        assert_eq!(
            block.expiration_bit(ordinal),
            ordinal == 3 || ordinal == 10,
            "ordinal {ordinal}"
        );
    }
    // Ordinals beyond the grown bitset read as false.
    assert!(!block.expiration_bit(1000));
}

/// Add records with the given (doc_id, has_expiration) and assert a sequential
/// read recovers both, for an encoder built from `build`.
fn assert_sequential_roundtrip<E: crate::Encoder + crate::Decoder>(
    flags: ffi::IndexFlags,
    entries: &[(DocId, bool)],
    build: impl Fn(DocId, bool) -> RSIndexResult<'static>,
) {
    let mut ii = InvertedIndex::<E>::new(flags);
    for &(doc_id, has_exp) in entries {
        ii.add_record(&build(doc_id, has_exp)).unwrap();
    }

    let mut reader = ii.reader();
    let mut result = build(0, false);
    for &(doc_id, has_exp) in entries {
        assert!(reader.next_record(&mut result).unwrap());
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(
            result.has_field_expiration, has_exp,
            "expiration bit mismatch for doc {doc_id}"
        );
    }
    assert!(!reader.next_record(&mut result).unwrap());
}

#[test]
fn doc_ids_only_roundtrips_expiration_bit() {
    assert_sequential_roundtrip::<DocIdsOnly>(
        IndexFlags_Index_DocIdsOnly,
        &[(10, false), (11, true), (20, false), (21, true), (22, true)],
        |doc_id, has_exp| {
            RSIndexResult::build_virt()
                .doc_id(doc_id)
                .has_field_expiration(has_exp)
                .build()
        },
    );
}

#[test]
fn fields_only_roundtrips_expiration_bit() {
    assert_sequential_roundtrip::<FieldsOnly>(
        IndexFlags_Index_StoreFieldFlags,
        &[(5, true), (6, false), (100, true), (101, false)],
        |doc_id, has_exp| {
            RSIndexResult::build_term()
                .doc_id(doc_id)
                .field_mask(0b1)
                .has_field_expiration(has_exp)
                .build()
        },
    );
}

#[test]
fn numeric_roundtrips_expiration_bit() {
    assert_sequential_roundtrip::<Numeric>(
        IndexFlags_Index_StoreNumeric,
        &[(1, false), (2, true), (3, true), (50, false)],
        |doc_id, has_exp| {
            RSIndexResult::build_numeric(1.5)
                .doc_id(doc_id)
                .has_field_expiration(has_exp)
                .build()
        },
    );
}

#[test]
fn raw_doc_ids_only_roundtrips_and_seeks_with_expiration_bit() {
    // RawDocIdsOnly is the fixed-stride tag encoding whose `seek` does a binary
    // search; the reader must map the search's landed ordinal back to the bitset.
    let entries: &[(DocId, bool)] = &[
        (10, false),
        (12, true),
        (15, false),
        (18, true),
        (21, true),
        (30, false),
    ];
    assert_sequential_roundtrip::<RawDocIdsOnly>(
        IndexFlags_Index_DocIdsOnly,
        entries,
        |doc_id, has_exp| {
            RSIndexResult::build_virt()
                .doc_id(doc_id)
                .has_field_expiration(has_exp)
                .build()
        },
    );

    let mut ii = InvertedIndex::<RawDocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for &(doc_id, has_exp) in entries {
        let rec = RSIndexResult::build_virt()
            .doc_id(doc_id)
            .has_field_expiration(has_exp)
            .build();
        ii.add_record(&rec).unwrap();
    }
    for &(target, expected_doc, expected_exp) in &[
        (18u64, 18u64, true),
        (15, 15, false),
        (21, 21, true),
        (29, 30, false),
    ] {
        let mut reader = ii.reader();
        let mut result = RSIndexResult::build_virt().build();
        assert!(reader.seek_record(target, &mut result).unwrap());
        assert_eq!(result.doc_id, expected_doc, "seek({target}) doc id");
        assert_eq!(
            result.has_field_expiration, expected_exp,
            "seek({target}) expiration bit"
        );
    }
}

#[test]
fn expiration_bit_survives_block_split_on_delta_overflow() {
    // A delta that does not fit the encoder forces a new block, where the record
    // becomes the block's first entry (ordinal 0). Its expiration bit must be set
    // in the new block's bitset.
    let mut ii = InvertedIndex::<FieldsOnly>::new(IndexFlags_Index_StoreFieldFlags);
    ii.add_record(
        &RSIndexResult::build_term()
            .doc_id(1)
            .field_mask(0b1)
            .build(),
    )
    .unwrap();

    let big_doc_id: DocId = (1u64 << 32) + 5;
    let second = RSIndexResult::build_term()
        .doc_id(big_doc_id)
        .field_mask(0b1)
        .has_field_expiration(true)
        .build();
    ii.add_record(&second).unwrap();

    let mut reader = ii.reader();
    let mut result = RSIndexResult::build_term().build();

    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, 1);
    assert!(!result.has_field_expiration);

    assert!(reader.next_record(&mut result).unwrap());
    assert_eq!(result.doc_id, big_doc_id);
    assert!(
        result.has_field_expiration,
        "expiration bit must survive the block split onto the new block's first entry"
    );
}

#[test]
fn expiration_bit_survives_gc() {
    // GC re-encodes surviving entries into fresh blocks; the per-entry bit must be
    // carried across the rebuild.
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let entries: &[(DocId, bool)] = &[
        (10, false),
        (11, true),
        (12, false),
        (13, true),
        (14, true),
        (15, false),
    ];
    for &(doc_id, has_exp) in entries {
        let rec = RSIndexResult::build_virt()
            .doc_id(doc_id)
            .has_field_expiration(has_exp)
            .build();
        ii.add_record(&rec).unwrap();
    }

    // Drop the even doc ids; keep 11(true), 13(true), 15(false).
    let delta = ii
        .scan_gc(
            |doc_id| doc_id % 2 == 1,
            None::<fn(&RSIndexResult, &RepairContext<'_>)>,
        )
        .expect("scan_gc ok")
        .expect("scan_gc found entries to remove");
    ii.apply_gc(delta);

    let mut reader = ii.reader();
    let mut result = RSIndexResult::build_virt().build();
    for &(doc_id, has_exp) in &[(11u64, true), (13, true), (15, false)] {
        assert!(reader.next_record(&mut result).unwrap());
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(
            result.has_field_expiration, has_exp,
            "GC must preserve the expiration bit for doc {doc_id}"
        );
    }
    assert!(!reader.next_record(&mut result).unwrap());
}

/// Numeric record for `doc_id` with the given field-expiration bit.
fn numeric(doc_id: DocId, has_exp: bool) -> RSIndexResult<'static> {
    RSIndexResult::build_numeric(1.5)
        .doc_id(doc_id)
        .has_field_expiration(has_exp)
        .build()
}

/// A block can gain its first expiration bit while a reader is suspended mid-block. After resuming,
/// the reader must report the bit of every later entry, for both sequential reads and seeks.
#[test]
#[cfg_attr(miri, ignore = "the memory hack below raises error in miri")]
fn reader_reports_bits_of_entries_appended_while_suspended() {
    use crate::{RawIndexReaderCore, RefreshOutcome, ResumableReader};
    use ref_mode::{Active, Suspended};

    for seek in [false, true] {
        let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_StoreNumeric);
        for doc_id in 1..=3 {
            ii.add_record(&numeric(doc_id, false)).unwrap();
        }
        // Spare capacity, so the appends below grow the block in place and the suspended reader
        // resumes mid-block instead of re-seeking.
        crate::test_utils::reserve_block_buffer(&mut ii, 0, 256);
        assert!(!ii.blocks[0].expiration_bits.may_contain_any());

        // SAFETY: mirrors a suspended cursor — the reader is parked and untouched while the index
        // is written to. The writes only append into the block's spare capacity, leaving the
        // `InvertedIndex` itself in place.
        let ii_ptr = &mut ii as *mut InvertedIndex<Numeric>;

        let mut reader = Box::new(ii.reader());
        let mut result = numeric(0, false);
        for doc_id in 1..=2 {
            assert!(reader.next_record(&mut result).unwrap());
            assert_eq!(
                (result.doc_id, result.has_field_expiration),
                (doc_id, false)
            );
        }

        // Suspend: the same allocation, reinterpreted as the layout-identical suspended reader.
        let active: *mut RawIndexReaderCore<Active<'_>, Numeric> = Box::into_raw(reader);
        // SAFETY: `RawIndexReaderCore`'s `Active` and `Suspended` forms are layout-identical.
        let mut suspended: Box<RawIndexReaderCore<Suspended, Numeric>> =
            unsafe { Box::from_raw(active.cast()) };

        for (doc_id, has_exp) in [(4, true), (5, false), (6, true)] {
            // SAFETY: see the `ii_ptr` construction above.
            unsafe { (*ii_ptr).add_record(&numeric(doc_id, has_exp)) }.unwrap();
        }
        assert_eq!(
            ii.number_of_blocks(),
            1,
            "the appends were meant to stay in block 0"
        );
        assert!(ii.blocks[0].expiration_bits.may_contain_any());

        // SAFETY: no writer runs during the refresh; this test is single threaded.
        assert_eq!(unsafe { suspended.refresh_pointers() }, RefreshOutcome::Ok);
        // SAFETY: the pointers were refreshed above, and the layouts are identical.
        let mut reader: Box<RawIndexReaderCore<Active<'_>, Numeric>> =
            unsafe { Box::from_raw(Box::into_raw(suspended).cast()) };

        let expected: &[(DocId, bool)] = if seek {
            assert!(reader.seek_record(4, &mut result).unwrap());
            assert_eq!((result.doc_id, result.has_field_expiration), (4, true));
            &[(5, false), (6, true)]
        } else {
            &[(3, false), (4, true), (5, false), (6, true)]
        };
        for &(doc_id, has_exp) in expected {
            assert!(reader.next_record(&mut result).unwrap());
            assert_eq!(
                (result.doc_id, result.has_field_expiration),
                (doc_id, has_exp),
                "seek={seek}"
            );
        }
        assert!(!reader.next_record(&mut result).unwrap());
    }
}

/// Seeking within and across blocks reports each entry's bit whether or not its block has a
/// bitset, including blocks entered without one.
#[test]
fn numeric_seek_reports_expiration_bit_across_blocks() {
    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_StoreNumeric);
    // Doc ids far enough apart to force several blocks; every third doc in the upper half has
    // a TTL, so some blocks have a bitset and some do not.
    let has_exp = |doc_id: DocId| doc_id > 5_000 && doc_id.is_multiple_of(3);
    for doc_id in 1..=10_000 {
        ii.add_record(&numeric(doc_id, has_exp(doc_id))).unwrap();
    }
    assert!(ii.number_of_blocks() > 2);
    assert!(!ii.blocks[0].expiration_bits.may_contain_any());
    assert!(ii.blocks.last().unwrap().expiration_bits.may_contain_any());

    let mut reader = ii.reader();
    let mut result = numeric(0, false);
    for target in (1..=10_000).step_by(7) {
        assert!(reader.seek_record(target, &mut result).unwrap());
        assert_eq!(result.doc_id, target);
        assert_eq!(
            result.has_field_expiration,
            has_exp(target),
            "seek({target})"
        );
        assert!(reader.next_record(&mut result).unwrap() || target == 10_000);
        if target < 10_000 {
            assert_eq!(
                result.has_field_expiration,
                has_exp(target + 1),
                "next after {target}"
            );
        }
    }
}
