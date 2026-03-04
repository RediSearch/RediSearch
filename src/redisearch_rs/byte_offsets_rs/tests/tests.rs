/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use byte_offsets_rs::{ByteOffsetWriter, ByteOffsets};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `ByteOffsets` with the given fields and a shared offset stream.
///
/// `fields` is a slice of `(field_id, first_tok_pos, last_tok_pos)` tuples.
/// `offsets` is the list of *absolute* byte offsets for every token in the
/// document, in order (they will be delta-encoded internally).
fn build(fields: &[(u32, u32, u32)], offsets: &[u32]) -> ByteOffsets {
    let mut writer = ByteOffsetWriter::new();
    for &offset in offsets {
        writer.write(offset).unwrap();
    }

    let mut bo = ByteOffsets::new();
    for &(field_id, first, last) in fields {
        bo.add_field(field_id, first, last);
    }
    bo.set_offset_bytes(writer.into_bytes());
    bo
}

/// Collect all values from a field iterator into a Vec.
fn collect(bo: &ByteOffsets, field_id: u32) -> Vec<u32> {
    let mut iter = bo.iterate(field_id).expect("field not found");
    let mut result = Vec::new();
    while let Some(v) = iter.next() {
        result.push(v);
    }
    result
}

// ---------------------------------------------------------------------------
// Basic iteration
// ---------------------------------------------------------------------------

#[test]
fn single_field_single_token() {
    // One field, one token at byte offset 42.
    let bo = build(&[(0, 1, 1)], &[42]);
    assert_eq!(collect(&bo, 0), [42]);
}

#[test]
fn single_field_multiple_tokens() {
    // One field covering the full token stream.
    let bo = build(&[(0, 1, 4)], &[5, 10, 20, 40]);
    assert_eq!(collect(&bo, 0), [5, 10, 20, 40]);
}

#[test]
fn two_fields_disjoint() {
    // Field 0: tokens 1-3  (byte offsets 5, 10, 20)
    // Field 1: tokens 4-6  (byte offsets 30, 45, 60)
    let bo = build(&[(0, 1, 3), (1, 4, 6)], &[5, 10, 20, 30, 45, 60]);

    assert_eq!(collect(&bo, 0), [5, 10, 20]);
    assert_eq!(collect(&bo, 1), [30, 45, 60]);
}

#[test]
fn field_in_middle_of_stream() {
    // Three fields; only query the middle one.
    // Field 0: positions 1-2   offsets [3, 6]
    // Field 1: positions 3-5   offsets [9, 12, 20]
    // Field 2: positions 6-7   offsets [30, 50]
    let bo = build(
        &[(0, 1, 2), (1, 3, 5), (2, 6, 7)],
        &[3, 6, 9, 12, 20, 30, 50],
    );

    assert_eq!(collect(&bo, 1), [9, 12, 20]);
}

#[test]
fn field_not_found_returns_none() {
    let bo = build(&[(0, 1, 3)], &[1, 2, 3]);
    assert!(bo.iterate(99).is_none());
}

// ---------------------------------------------------------------------------
// Serialize / load round-trip
// ---------------------------------------------------------------------------

#[test]
fn serialize_load_roundtrip_empty() {
    let bo = ByteOffsets::new();

    let mut buf = Vec::new();
    bo.serialize(&mut buf).unwrap();

    let loaded = ByteOffsets::load(&buf).unwrap();
    assert_eq!(loaded.fields.len(), 0);
    assert!(loaded.iterate(0).is_none());
}

#[test]
fn serialize_load_roundtrip_single_field() {
    let original = build(&[(7, 1, 3)], &[100, 200, 300]);

    let mut buf = Vec::new();
    original.serialize(&mut buf).unwrap();

    let loaded = ByteOffsets::load(&buf).unwrap();
    assert_eq!(loaded.fields.len(), 1);
    assert_eq!(loaded.fields[0].field_id, 7);
    assert_eq!(loaded.fields[0].first_tok_pos, 1);
    assert_eq!(loaded.fields[0].last_tok_pos, 3);
    assert_eq!(collect(&loaded, 7), [100, 200, 300]);
}

#[test]
fn serialize_load_roundtrip_two_fields() {
    let original = build(&[(0, 1, 3), (1, 4, 6)], &[5, 10, 20, 30, 45, 60]);

    let mut buf = Vec::new();
    original.serialize(&mut buf).unwrap();

    let loaded = ByteOffsets::load(&buf).unwrap();
    assert_eq!(loaded.fields.len(), 2);
    assert_eq!(collect(&loaded, 0), [5, 10, 20]);
    assert_eq!(collect(&loaded, 1), [30, 45, 60]);
}

// ---------------------------------------------------------------------------
// Delta-encoding corner cases
// ---------------------------------------------------------------------------

#[test]
fn delta_encoding_large_offsets() {
    // Offsets that require multi-byte varints.
    let offsets: Vec<u32> = (0..10).map(|i| i * 10_000).collect();
    let last = offsets.len() as u32;
    let bo = build(&[(0, 1, last)], &offsets);
    assert_eq!(collect(&bo, 0), offsets);
}

#[test]
fn identical_offsets_encode_as_zero_deltas() {
    // Same offset repeated: every delta is 0.
    let bo = build(&[(0, 1, 4)], &[42, 42, 42, 42]);
    assert_eq!(collect(&bo, 0), [42, 42, 42, 42]);
}

// ---------------------------------------------------------------------------
// cur_pos tracking
// ---------------------------------------------------------------------------

#[test]
fn cur_pos_increments_correctly() {
    let bo = build(&[(0, 1, 3)], &[10, 20, 30]);
    let mut iter = bo.iterate(0).unwrap();

    // Before the first `next()`, cur_pos should reflect the decrement
    // done during construction (cur_pos = first_tok_pos - 1 = 0).
    assert_eq!(iter.cur_pos(), 0);

    iter.next();
    assert_eq!(iter.cur_pos(), 1);

    iter.next();
    assert_eq!(iter.cur_pos(), 2);

    iter.next();
    assert_eq!(iter.cur_pos(), 3);

    // Exhausted — next() returns None, cur_pos becomes 4 before the
    // end-of-range check fires.
    assert!(iter.next().is_none());
    assert_eq!(iter.cur_pos(), 4);
}

#[test]
fn cur_pos_with_non_unit_first_tok_pos() {
    // Field starts at position 3; first two positions belong to a different field.
    let bo = build(&[(0, 1, 2), (1, 3, 5)], &[10, 20, 30, 40, 50]);
    let mut iter = bo.iterate(1).unwrap();

    // After pre-advance to position 3, cur_pos was set to 2.
    assert_eq!(iter.cur_pos(), 2);

    iter.next();
    assert_eq!(iter.cur_pos(), 3);
}
