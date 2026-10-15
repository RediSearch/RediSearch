/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding of the pre-2.0 byte-offsets format; see [`ByteOffsets::load_legacy`].

use byte_offsets::{ByteOffsets, TruncatedError};
use varint::VectorWriter;

use super::byte_offsets;

/// Each as `(field_id, first, last)`, as 1.x stored them.
const FIELDS: [(u16, u32, u32); 2] = [(0, 1, 2), (3, 3, 1000)];
const TOKEN_OFFSETS: [u32; 3] = [4, 200, 0];

/// Encodes [`FIELDS`] and [`TOKEN_OFFSETS`] the way 1.x did.
fn legacy_blob() -> Vec<u8> {
    legacy_blob_of(&FIELDS)
}

/// Encodes `fields` and [`TOKEN_OFFSETS`] the way 1.x did.
fn legacy_blob_of(fields: &[(u16, u32, u32)]) -> Vec<u8> {
    let mut writer = VectorWriter::new(0);
    for offset in TOKEN_OFFSETS {
        writer.write(offset).expect("writing to a Vec cannot fail");
    }
    let encoded = writer.bytes();

    let mut blob = vec![u8::try_from(fields.len()).unwrap()];
    for &(field_id, first, last) in fields {
        blob.push(u8::try_from(field_id).unwrap());
        blob.extend(first.to_be_bytes());
        blob.extend(last.to_be_bytes());
    }
    blob.extend(u32::try_from(encoded.len()).unwrap().to_be_bytes());
    blob.extend(encoded);
    blob
}

#[test]
fn decodes_the_fields_and_their_offsets() {
    let mut blob = legacy_blob();
    // Whatever follows the offsets is not part of them.
    blob.push(0xFF);

    assert_eq!(
        ByteOffsets::load_legacy(&blob),
        Ok(byte_offsets(&[(0, 1, 2), (3, 3, 998)], &TOKEN_OFFSETS))
    );
}

/// A field whose last position precedes its first one owns no offsets.
#[test]
fn a_field_ending_before_it_starts_owns_no_offsets() {
    let blob = legacy_blob_of(&[(0, 5, 3), (1, 6, 7)]);
    let offsets = ByteOffsets::load_legacy(&blob).expect("the blob is complete");

    assert_eq!(offsets.field_offsets(0).unwrap().next(), None);
    assert_eq!(
        offsets.field_offsets(1).unwrap().collect::<Vec<_>>(),
        [4, 200]
    );
}

/// A blob that ends anywhere before its last byte, in the field count, a field, the
/// length or the offsets, is an error.
#[test]
fn truncated_input_is_an_error() {
    let blob = legacy_blob();
    for end in 0..blob.len() {
        assert_eq!(
            ByteOffsets::load_legacy(&blob[..end]),
            Err(TruncatedError),
            "truncated to {end} bytes"
        );
    }
}

/// Iteration stops at offsets that end partway through a value, and stays stopped.
#[test]
fn iteration_stops_at_incomplete_offsets() {
    // One field, id 1, at positions 1..=3, then one byte that announces a continuation.
    let mut blob = vec![1, 1];
    blob.extend(1u32.to_be_bytes());
    blob.extend(3u32.to_be_bytes());
    blob.extend(1u32.to_be_bytes());
    blob.push(0x80);
    let offsets = ByteOffsets::load_legacy(&blob).expect("the blob is complete");

    let mut iter = offsets.field_offsets(1).unwrap();
    assert_eq!(iter.next(), None);
    let position = iter.token_position();
    assert_eq!(iter.next(), None);
    assert_eq!(iter.token_position(), position);
}

/// A field spanning every position owns as many offsets as a `u32` holds, and the fields
/// after such ones still iterate, yielding the offsets that remain: none.
#[test]
fn fields_spanning_every_position() {
    let blob = legacy_blob_of(&[(0, 0, u32::MAX), (1, 0, u32::MAX), (2, 1, 1)]);
    let offsets = ByteOffsets::load_legacy(&blob).expect("the blob is complete");

    let mut first = offsets.field_offsets(0).unwrap();
    assert_eq!(first.next(), Some(4));
    assert_eq!(first.token_position(), 0);
    assert_eq!(offsets.field_offsets(2).unwrap().next(), None);
}
