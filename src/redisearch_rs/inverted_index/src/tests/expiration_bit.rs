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

/// Seeking within and across blocks reports each entry's bit whether or not its block has a
/// bitset, including blocks entered without one.
#[test]
fn numeric_seek_reports_expiration_bit_across_blocks() {
    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_StoreNumeric);
    // Six blocks' worth of docs, small enough for miri. Every third doc in the upper half has a
    // TTL, so the lower three blocks have no bitset and the upper three do.
    let per_block = DocId::from(<Numeric as crate::Encoder>::RECOMMENDED_BLOCK_ENTRIES);
    let last = 6 * per_block;
    let has_exp = |doc_id: DocId| doc_id > last / 2 && doc_id.is_multiple_of(3);
    for doc_id in 1..=last {
        ii.add_record(&numeric(doc_id, has_exp(doc_id))).unwrap();
    }
    assert_eq!(ii.number_of_blocks(), 6);
    for (i, block) in ii.blocks.iter().enumerate() {
        assert_eq!(
            block.expiration_bits.mem_usage() > 0,
            i >= 3,
            "bitset presence of block {i}"
        );
    }

    // Seeks within a block and into the next one, then the entry after each. The last doc is
    // included so the end of the index is checked too.
    let mut reader = ii.reader();
    let mut result = numeric(0, false);
    for target in (1..=last).step_by(7).chain([last]) {
        assert!(reader.seek_record(target, &mut result).unwrap());
        assert_eq!(
            (result.doc_id, result.has_field_expiration),
            (target, has_exp(target)),
            "seek({target})"
        );
        if target < last {
            assert!(reader.next_record(&mut result).unwrap());
            assert_eq!(
                (result.doc_id, result.has_field_expiration),
                (target + 1, has_exp(target + 1)),
                "next after {target}"
            );
        } else {
            assert!(!reader.next_record(&mut result).unwrap(), "end of index");
        }
    }

    // Seeks that skip several blocks at once, through the binary search over block bounds.
    let mut reader = ii.reader();
    for target in [1, 4 * per_block + per_block / 2, last - 1] {
        assert!(reader.seek_record(target, &mut result).unwrap());
        assert_eq!(
            (result.doc_id, result.has_field_expiration),
            (target, has_exp(target)),
            "multi-block seek({target})"
        );
    }
}
