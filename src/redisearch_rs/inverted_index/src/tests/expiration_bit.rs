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
    build: impl Fn(DocId) -> RSIndexResult<'static>,
) {
    let mut ii = InvertedIndex::<E>::new(flags);
    for &(doc_id, has_exp) in entries {
        let mut rec = build(doc_id);
        rec.has_field_expiration = has_exp;
        ii.add_record(&rec).unwrap();
    }

    let mut reader = ii.reader();
    let mut result = build(0);
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
        |doc_id| RSIndexResult::build_virt().doc_id(doc_id).build(),
    );
}

#[test]
fn fields_only_roundtrips_expiration_bit() {
    assert_sequential_roundtrip::<FieldsOnly>(
        IndexFlags_Index_StoreFieldFlags,
        &[(5, true), (6, false), (100, true), (101, false)],
        |doc_id| {
            RSIndexResult::build_term()
                .doc_id(doc_id)
                .field_mask(0b1)
                .build()
        },
    );
}

#[test]
fn numeric_roundtrips_expiration_bit() {
    assert_sequential_roundtrip::<Numeric>(
        IndexFlags_Index_StoreNumeric,
        &[(1, false), (2, true), (3, true), (50, false)],
        |doc_id| RSIndexResult::build_numeric(1.5).doc_id(doc_id).build(),
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
    assert_sequential_roundtrip::<RawDocIdsOnly>(IndexFlags_Index_DocIdsOnly, entries, |doc_id| {
        RSIndexResult::build_virt().doc_id(doc_id).build()
    });

    let mut ii = InvertedIndex::<RawDocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for &(doc_id, has_exp) in entries {
        let mut rec = RSIndexResult::build_virt().doc_id(doc_id).build();
        rec.has_field_expiration = has_exp;
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
    let mut second = RSIndexResult::build_term()
        .doc_id(big_doc_id)
        .field_mask(0b1)
        .build();
    second.has_field_expiration = true;
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
        let mut rec = RSIndexResult::build_virt().doc_id(doc_id).build();
        rec.has_field_expiration = has_exp;
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
