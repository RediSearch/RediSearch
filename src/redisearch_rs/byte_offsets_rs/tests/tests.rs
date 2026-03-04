/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Task: implement `ByteOffsets::iterate()` in `src/lib.rs` and make the
// tests below compile and pass.
//
// Useful crates already in the workspace:
//   - `varint::VectorWriter`  — writes delta-encoded u32 offsets
//   - `varint::read::<u32, _>` — reads one varint-encoded delta
//
// Reference: `src/byte_offsets.c` (the C implementation being ported).
//
// -------------------------------------------------------------------------
// Expected behaviour
// -------------------------------------------------------------------------
//
// Document with 6 tokens:
//
//   position:    1    2    3    4    5    6
//   byte offset: 5   10   20   30   45   60
//
//   Field 0 covers positions 1–3  (byte offsets  5, 10, 20)
//   Field 1 covers positions 4–6  (byte offsets 30, 45, 60)
//
// Iterating field 0:
//   iter.next() == Some(5)
//   iter.next() == Some(10)
//   iter.next() == Some(20)
//   iter.next() == None
//
// Iterating field 1 (iterator must skip positions 1–3 before yielding):
//   iter.next() == Some(30)
//   iter.next() == Some(45)
//   iter.next() == Some(60)
//   iter.next() == None
//
// Querying a field that doesn't exist:
//   bo.iterate(99) == None
//
// -------------------------------------------------------------------------
// Tests — uncomment and make them pass
// -------------------------------------------------------------------------

use byte_offsets_rs::ByteOffsets;

/// Build a `ByteOffsets` from field specs and a list of absolute byte offsets.
///
/// `fields`  — `(field_id, first_tok_pos, last_tok_pos)` for each field
/// `offsets` — absolute byte offset for each token, in document order
///
/// The offsets are delta-encoded by `VectorWriter` before being stored, so
/// the iterator must accumulate the deltas back into absolute values.
fn build(fields: &[(u16, u32, u32)], offsets: &[u32]) -> ByteOffsets {
    let mut writer = varint::VectorWriter::new(offsets.len());
    for &offset in offsets {
        writer.write(offset).unwrap();
    }
    let mut bo = ByteOffsets::new();
    for &(field_id, first, last) in fields {
        bo.add_field(field_id, first, last);
    }
    bo.set_offset_bytes(writer.bytes().to_vec());
    bo
}
//
// #[test]
// fn iterate_first_field() {
//     let bo = build(&[(0, 1, 3), (1, 4, 6)], &[5, 10, 20, 30, 45, 60]);
//     let mut iter = bo.iterate(0).expect("field not found");
//     assert_eq!(iter.next(), Some(5));
//     assert_eq!(iter.next(), Some(10));
//     assert_eq!(iter.next(), Some(20));
//     assert_eq!(iter.next(), None);
// }
//
// #[test]
// fn iterate_second_field() {
//     let bo = build(&[(0, 1, 3), (1, 4, 6)], &[5, 10, 20, 30, 45, 60]);
//     let mut iter = bo.iterate(1).expect("field not found");
//     assert_eq!(iter.next(), Some(30));
//     assert_eq!(iter.next(), Some(45));
//     assert_eq!(iter.next(), Some(60));
//     assert_eq!(iter.next(), None);
// }
//
// #[test]
// fn field_not_found() {
//     let bo = build(&[(0, 1, 3)], &[5, 10, 20]);
//     assert!(bo.iterate(99).is_none());
// }
