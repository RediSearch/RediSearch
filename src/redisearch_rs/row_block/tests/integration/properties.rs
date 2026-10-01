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

use crate::harness::{Decoded, decode_prefix, encode, lookup, row};
use proptest::prelude::*;
use row_block::{Block, MAGIC, VERSION};

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

/// Arbitrary rows over `ncols` columns: `Some` where the row holds a value.
fn any_rows(ncols: usize) -> impl Strategy<Value = Vec<Vec<Option<Decoded>>>> {
    prop::collection::vec(
        prop::collection::vec(prop::option::of(any_value()), ncols),
        0..4,
    )
}

/// The column names a block of `ncols` columns is built with.
fn names(ncols: usize) -> Vec<String> {
    (0..ncols).map(|i| format!("c{i}")).collect()
}

proptest! {
    /// Whatever the rows hold, a block decodes back to exactly what went in. This is the
    /// property the whole format exists to provide.
    #[test]
    fn every_block_round_trips((ncols, rows) in (1usize..18).prop_flat_map(|n| (Just(n), any_rows(n)))) {
        let names = names(ncols);
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let lookup = lookup(&refs);

        let encoded: Vec<_> = rows
            .iter()
            .map(|values| {
                let present: Vec<_> = refs
                    .iter()
                    .zip(values)
                    .filter_map(|(name, value)| value.as_ref().map(|v| (*name, v.to_value())))
                    .collect();
                row(&lookup, &present)
            })
            .collect();

        let want: Vec<Vec<(String, Decoded)>> = rows
            .iter()
            .map(|values| {
                names
                    .iter()
                    .zip(values)
                    .filter_map(|(name, value)| {
                        value.as_ref().map(|v| (name.clone(), v.clone()))
                    })
                    .collect()
            })
            .collect();

        let (got, error) = decode_prefix(&encode(&lookup, &encoded));
        prop_assert_eq!(error, None);
        prop_assert_eq!(got, want);
    }

    /// Cutting a valid block short must never panic, and must never invent or corrupt a row
    /// that was fully inside the surviving prefix.
    #[test]
    fn truncating_a_block_never_panics_or_corrupts_earlier_rows(
        (ncols, rows) in (1usize..10).prop_flat_map(|n| (Just(n), any_rows(n))),
    ) {
        let names = names(ncols);
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let lookup = lookup(&refs);
        let encoded: Vec<_> = rows
            .iter()
            .map(|values| {
                let present: Vec<_> = refs
                    .iter()
                    .zip(values)
                    .filter_map(|(name, value)| value.as_ref().map(|v| (*name, v.to_value())))
                    .collect();
                row(&lookup, &present)
            })
            .collect();

        let block = encode(&lookup, &encoded);
        let (want, _) = decode_prefix(&block);

        for len in 0..block.len() {
            let (got, _) = decode_prefix(&block[..len]);
            prop_assert!(got.len() <= want.len());
            prop_assert_eq!(&got[..], &want[..got.len()]);
        }
    }

    /// Arbitrary bytes behind a valid header must be reported as malformed, not crash the
    /// decoder. A shard cannot be assumed to be the build the coordinator expects.
    #[test]
    fn arbitrary_row_bytes_decode_or_error(garbage in prop::collection::vec(any::<u8>(), 0..256)) {
        let mut block = MAGIC.to_le_bytes().to_vec();
        block.push(VERSION);
        block.extend_from_slice(&1u16.to_le_bytes());
        block.extend_from_slice(&1u16.to_le_bytes());
        block.extend_from_slice(b"c\0");
        block.extend_from_slice(&garbage);

        // Every row costs at least its bitmap byte, so a decoder that made no progress —
        // yielding rows forever off a fixed buffer — fails this bound rather than hanging.
        let rows = Block::parse(&block)
            .map(|parsed| parsed.rows().take_while(Result::is_ok).count())
            .unwrap_or(0);
        prop_assert!(rows <= garbage.len());
    }
}
