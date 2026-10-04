/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iteration over a field's offsets; see [`FieldByteOffsets`].

use byte_offsets::{ByteOffsets, FieldByteOffsets};

use super::byte_offsets;

/// The position and offset of every token the iteration over `field_id` yields.
fn yielded(offsets: &ByteOffsets, field_id: u16) -> Vec<(u32, u32)> {
    let mut iter = offsets
        .field_offsets(field_id)
        .expect("the field is recorded");
    std::iter::from_fn(|| iter.next().map(|offset| (iter.token_position(), offset))).collect()
}

/// A field yields its own tokens, whose positions continue from the fields indexed
/// before it.
#[test]
fn yields_the_tokens_of_the_field() {
    let offsets = byte_offsets(&[(7, 1, 2), (9, 3, 3)], &[0, 6, 0, 4, 9]);

    assert_eq!(yielded(&offsets, 7), [(1, 0), (2, 6)]);
    assert_eq!(yielded(&offsets, 9), [(3, 0), (4, 4), (5, 9)]);
    assert!(offsets.field_offsets(8).is_none());
}

/// The position starts just before the field's first token. Passing the field's last
/// token moves it one further, while running out of encoded offsets leaves it alone.
#[test]
fn position_around_the_field() {
    let offsets = byte_offsets(&[(1, 1, 1), (2, 2, 3)], &[3, 5, 8]);

    let mut first: FieldByteOffsets = offsets.field_offsets(1).unwrap();
    assert_eq!(first.token_position(), 0);
    assert_eq!(first.next(), Some(3));
    assert_eq!(first.next(), None);
    assert_eq!(first.token_position(), 2);

    let mut second = offsets.field_offsets(2).unwrap();
    assert_eq!(second.token_position(), 1);
    assert_eq!(second.by_ref().collect::<Vec<_>>(), [5, 8]);
    assert_eq!(second.token_position(), 3);
}

/// A field whose offsets run out early yields the ones there are, and its position stays
/// at the last one.
#[test]
fn offsets_ending_within_the_field() {
    let offsets = byte_offsets(&[(1, 1, 2), (2, 3, 3)], &[3, 4, 6]);

    let mut iter = offsets.field_offsets(2).unwrap();
    assert_eq!(iter.next(), Some(6));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.token_position(), 3);
}

/// A field that stores no offsets, such as a multi-value one, owns none even though its
/// tokens take positions, so the fields after it start past a gap.
#[test]
fn fields_after_one_without_offsets() {
    let offsets = byte_offsets(&[(1, 1, 2), (2, 3, 0), (3, 11, 2)], &[1, 2, 3, 4]);

    assert_eq!(yielded(&offsets, 2), []);
    assert_eq!(yielded(&offsets, 3), [(11, 3), (12, 4)]);
}

/// Of the fields recorded under the same id, the first one is iterated.
#[test]
fn duplicate_field_ids_iterate_the_first() {
    let offsets = byte_offsets(&[(4, 1, 1), (4, 2, 1)], &[7, 9]);

    assert_eq!(yielded(&offsets, 4), [(1, 7)]);
}

/// Fields are added in position order, so one that starts within the previous field's
/// offsets is a bug.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "overlaps the field before it")]
fn overlapping_fields_are_rejected() {
    byte_offsets(&[(1, 1, 3), (2, 2, 1)], &[1, 2, 3, 4]);
}

/// When the fields before it claim more offsets than are encoded, a field yields none.
#[test]
fn offsets_ending_before_the_field() {
    let offsets = byte_offsets(&[(1, 1, 5), (2, 6, 1)], &[3, 4]);

    assert_eq!(offsets.field_offsets(2).unwrap().next(), None);
}
