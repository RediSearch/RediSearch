/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Properties that must hold for every block, rather than for the hand-picked shapes the
//! other modules cover.

// proptest calls getcwd(), which Miri does not support.
#![cfg(not(miri))]

use crate::harness::{Decoded, decode_into, decode_prefix, encode, lookup, row, try_decode};
use proptest::prelude::*;
use rlookup::RLookup;
use row_block::{MAGIC, VERSION};

/// Arbitrary values of every tag, nested a few levels deep.
fn any_value() -> impl Strategy<Value = Decoded> {
    let leaf = prop_oneof![
        any::<f64>().prop_map(Decoded::Number),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(Decoded::Bytes),
        Just(Decoded::Null),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Decoded::Array),
            prop::collection::vec((inner.clone(), inner), 0..4).prop_map(Decoded::Map),
        ]
    })
}

/// Mostly valid [`ColumnKind`] bytes, plus the first byte past them.
///
/// [`ColumnKind`]: row_block::ColumnKind
fn any_kind_byte() -> impl Strategy<Value = u8> {
    0u8..=6
}

/// Arbitrary rows over `ncols` columns: `Some` where the row holds a value.
fn any_rows(ncols: usize) -> impl Strategy<Value = Vec<Vec<Option<Decoded>>>> {
    prop::collection::vec(
        prop::collection::vec(prop::option::of(any_value()), ncols),
        0..4,
    )
}

/// A block of `rows` over columns `c0`, `c1`, ..., and the rows a decoder must return for it.
fn build(rows: &[Vec<Option<Decoded>>], ncols: usize) -> (Vec<u8>, Vec<Vec<(String, Decoded)>>) {
    let names: Vec<String> = (0..ncols).map(|i| format!("c{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let lookup = lookup(&refs);

    let present = |values: &[Option<Decoded>]| -> Vec<(usize, Decoded)> {
        values
            .iter()
            .enumerate()
            .filter_map(|(col, value)| Some((col, value.clone()?)))
            .collect()
    };
    let encoded: Vec<_> = rows
        .iter()
        .map(|values| {
            let fields: Vec<_> = present(values)
                .into_iter()
                .map(|(col, value)| (refs[col], value.to_value()))
                .collect();
            row(&lookup, &fields)
        })
        .collect();
    let want = rows
        .iter()
        .map(|values| {
            present(values)
                .into_iter()
                .map(|(col, value)| (names[col].clone(), value))
                .collect()
        })
        .collect();
    (encode(&lookup, &encoded), want)
}

proptest! {
    /// Whatever the rows hold, a block decodes back to exactly what went in, through the
    /// coordinator's decoder and through the replay path's reader alike. Columns from 1 to 17
    /// put the presence bits on both sides of every bitmap byte boundary, and values of
    /// several types in one column exercise retagging at every row position.
    #[test]
    fn every_block_round_trips((ncols, rows) in (1usize..18).prop_flat_map(|n| (Just(n), any_rows(n)))) {
        let (block, want) = build(&rows, ncols);
        prop_assert_eq!(decode_prefix(&block), (want.clone(), None));
        prop_assert_eq!(decode_into(&block, &mut RLookup::new()), Ok(want));
    }

    /// Cutting a valid block short must never panic, and must never invent, corrupt or
    /// silently drop a row that was fully inside the surviving prefix.
    #[test]
    fn truncating_a_block_yields_a_clean_error_after_the_rows_before_the_cut(
        (ncols, rows) in (1usize..10).prop_flat_map(|n| (Just(n), any_rows(n))),
    ) {
        let (block, want) = build(&rows, ncols);
        for len in 0..block.len() {
            let (got, error) = decode_prefix(&block[..len]);
            prop_assert!(got.len() <= want.len());
            prop_assert_eq!(&got[..], &want[..got.len()]);
            // A cut inside the rows is either on a row boundary, leaving fewer rows, or
            // inside one, which is an error; a cut inside the schema is always an error.
            prop_assert!(error.is_some() || got.len() < want.len(), "{} bytes", len);
        }
    }

    /// The coordinator's decoder must agree with the reader on arbitrary input: the same
    /// rows where the reader decodes, the same error where it fails, and never a panic or an
    /// out-of-bounds read — the bytes come straight off the network.
    #[test]
    fn the_decoder_agrees_with_the_reader_on_arbitrary_bytes(
        tail in prop::collection::vec(any::<u8>(), 0..256),
        kinds in prop::collection::vec(any_kind_byte(), 1..12),
        garbage_header in any::<bool>(),
    ) {
        let mut coordinator = RLookup::new();
        if garbage_header {
            // Column names are arbitrary here and may repeat, which folds columns together in
            // the coordinator's lookup, so only the outcome is comparable.
            let got = decode_into(&tail, &mut coordinator).map(|rows| rows.len());
            let want = try_decode(&tail).map(|rows| rows.len());
            prop_assert_eq!(got, want);
            return Ok(());
        }

        let mut block = MAGIC.to_le_bytes().to_vec();
        block.push(VERSION);
        block.extend_from_slice(&u16::try_from(kinds.len()).unwrap().to_le_bytes());
        for (col, kind) in kinds.iter().enumerate() {
            let name = format!("c{col}");
            block.extend_from_slice(&u16::try_from(name.len()).unwrap().to_le_bytes());
            block.extend_from_slice(name.as_bytes());
            block.push(0);
            block.push(*kind);
        }
        block.extend_from_slice(&tail);

        let got = decode_into(&block, &mut coordinator);
        prop_assert_eq!(&got, &try_decode(&block));
        // Every row costs at least its bitmap byte — even one whose only values are empty
        // typed nulls — so a decoder that made no progress, yielding rows forever off a
        // fixed buffer, fails this bound rather than hanging.
        if let Ok(rows) = got {
            prop_assert!(rows.len() <= tail.len());
        }
    }
}
