/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Malformed input.
//!
//! Every one of these must produce a `DecodeError` — never a panic, never a read past the
//! buffer, and never an allocation sized from a number the block cannot back up.

use crate::harness::{Decoded, bytes, decode, decode_prefix, encode, lookup, row, try_decode};
use pretty_assertions::assert_eq;
use row_block::{Block, DecodeError, MAGIC, Tag, VERSION};
use value::SharedValue;

/// Builds a header for `ncols` columns, then whatever `rest` adds.
fn block(ncols: u16, rest: &[&[u8]]) -> Vec<u8> {
    let mut bytes = MAGIC.to_le_bytes().to_vec();
    bytes.push(VERSION);
    bytes.extend_from_slice(&ncols.to_le_bytes());
    for part in rest {
        bytes.extend_from_slice(part);
    }
    bytes
}

/// One schema entry for a single-byte column name.
fn column(name: u8) -> Vec<u8> {
    let mut bytes = 1u16.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[name, 0]);
    bytes
}

/// A valid, reasonably varied block, used as the starting point for corruption.
fn valid_block() -> Vec<u8> {
    let lookup = lookup(&["a", "bb", "ccc"]);
    encode(
        &lookup,
        &[
            row(
                &lookup,
                &[
                    ("a", SharedValue::new_num(-1.5)),
                    (
                        "ccc",
                        Decoded::Map(vec![(bytes("k"), Decoded::Array(vec![Decoded::Null]))])
                            .to_value(),
                    ),
                ],
            ),
            row(&lookup, &[]),
            row(
                &lookup,
                &[("bb", SharedValue::new_string(b"hello".to_vec()))],
            ),
        ],
    )
}

#[test]
fn a_block_that_does_not_open_with_the_magic_is_rejected() {
    let mut corrupt = valid_block();
    corrupt[0] ^= 0xff;
    let magic = u32::from_le_bytes(corrupt[..4].try_into().unwrap());
    assert_eq!(try_decode(&corrupt), Err(DecodeError::BadMagic { magic }));
}

#[test]
fn a_block_of_another_version_is_rejected() {
    // Not read as best-effort: a different version may have re-used a tag, so guessing at the
    // payloads would produce wrong values rather than an error.
    let mut corrupt = valid_block();
    corrupt[4] = VERSION.wrapping_add(1);
    assert_eq!(
        try_decode(&corrupt),
        Err(DecodeError::UnsupportedVersion {
            version: VERSION + 1
        })
    );
}

#[test]
fn a_schema_name_without_its_terminator_is_rejected() {
    let missing = block(1, &[&1u16.to_le_bytes()[..], b"a", b"a"]);
    assert_eq!(try_decode(&missing), Err(DecodeError::MalformedName));

    // An interior NUL would make the name the decoder hands on shorter than its declared
    // length, so the two sides would disagree about which column this is.
    let interior = block(1, &[&2u16.to_le_bytes()[..], b"a\0", &[0]]);
    assert_eq!(try_decode(&interior), Err(DecodeError::MalformedName));
}

#[test]
fn a_column_count_the_schema_cannot_cover_is_rejected() {
    // Every column costs at least three bytes, so this is caught before the decoder tries to
    // reserve room for 65535 names.
    assert_eq!(
        try_decode(&block(u16::MAX, &[&column(b'a')])),
        Err(DecodeError::Truncated)
    );
}

#[test]
fn rows_after_an_empty_schema_are_rejected() {
    // Such rows would be zero bytes long, so no reader could tell one from a thousand.
    assert_eq!(
        try_decode(&block(0, &[&[0u8][..]])),
        Err(DecodeError::RowsWithoutColumns)
    );
    assert_eq!(try_decode(&block(0, &[])), Ok(vec![]), "no rows is fine");
}

#[test]
fn an_unknown_value_tag_is_rejected() {
    // Its payload length is unknown, so the cursor cannot step over it: the rest of the block
    // is unreadable and must not be guessed at.
    for tag in [0u8, 6, 200, 255] {
        let corrupt = block(1, &[&column(b'a'), &[0b1, tag]]);
        assert_eq!(
            try_decode(&corrupt),
            Err(DecodeError::UnknownTag { tag }),
            "tag {tag}"
        );
    }
}

#[test]
fn a_collection_count_larger_than_the_block_is_rejected() {
    // Without this check the count would be handed to `Vec::with_capacity`, turning four
    // corrupt bytes into a multi-gigabyte allocation.
    for tag in [Tag::Array, Tag::Map] {
        let corrupt = block(
            1,
            &[
                &column(b'a'),
                &[0b1, tag as u8],
                &u32::MAX.to_le_bytes()[..],
            ],
        );
        assert_eq!(
            try_decode(&corrupt),
            Err(DecodeError::ImplausibleCount { count: u32::MAX }),
            "{tag:?}"
        );
    }
}

#[test]
fn a_string_length_larger_than_the_block_is_rejected() {
    let corrupt = block(
        1,
        &[
            &column(b'a'),
            &[0b1, Tag::String as u8],
            &1_000_000u32.to_le_bytes()[..],
            b"short",
        ],
    );
    assert_eq!(
        try_decode(&corrupt),
        Err(DecodeError::ImplausibleCount { count: 1_000_000 })
    );
}

#[test]
fn a_map_entry_count_only_half_backed_is_rejected() {
    // A map entry is a key *and* a value, so it costs two bytes at the very least. A count
    // checked against one byte per entry would let this one through and then run off the end
    // decoding the second half of the pairs.
    let corrupt = block(
        1,
        &[
            &column(b'a'),
            &[0b1, Tag::Map as u8],
            &2u32.to_le_bytes()[..],
            &[Tag::Null as u8, Tag::Null as u8, Tag::Null as u8],
        ],
    );
    assert_eq!(
        try_decode(&corrupt),
        Err(DecodeError::ImplausibleCount { count: 2 })
    );
}

#[test]
fn a_deeply_nested_value_is_rejected_before_it_exhausts_the_stack() {
    // One `TAG_ARRAY` byte plus a count of 1 buys a level of recursion, so a few kilobytes of
    // block would otherwise recurse deep enough to overflow the stack.
    let mut rest = vec![0b1u8];
    for _ in 0..10_000 {
        rest.push(Tag::Array as u8);
        rest.extend_from_slice(&1u32.to_le_bytes());
    }
    rest.push(Tag::Null as u8);

    assert_eq!(
        try_decode(&block(1, &[&column(b'a'), &rest])),
        Err(DecodeError::TooDeeplyNested)
    );
}

#[test]
fn truncating_a_valid_block_anywhere_yields_a_clean_error_or_a_shorter_block() {
    let full = valid_block();
    let want = decode(&full);

    for len in 0..full.len() {
        let (rows, error) = decode_prefix(&full[..len]);

        assert!(
            rows.len() <= want.len(),
            "truncating to {len} bytes produced more rows than the whole block"
        );
        assert_eq!(
            rows.as_slice(),
            &want[..rows.len()],
            "truncating to {len} bytes changed the rows before the cut"
        );
        assert!(
            rows.len() < want.len() || error.is_none(),
            "truncating to {len} bytes yielded every row *and* an error"
        );
        assert!(
            error.is_some() || rows.len() < want.len(),
            "truncating to {len} bytes silently dropped rows"
        );
    }
}

#[test]
fn the_row_iterator_stops_at_the_first_error() {
    // Row boundaries are implied by the values themselves, so there is nothing to resynchronise
    // to: continuing would emit garbage rows for as long as the buffer lasts.
    let corrupt = block(1, &[&column(b'a'), &[0b1, 200]]);
    let parsed = Block::parse(&corrupt).expect("the header and schema are intact");
    let outcomes: Vec<_> = parsed.rows().map(|row| row.is_ok()).collect();
    assert_eq!(outcomes, vec![false]);
}
