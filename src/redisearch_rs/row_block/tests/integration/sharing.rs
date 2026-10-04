/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoded strings borrowing from their block instead of copying out of it, and the bound on
//! how much memory the borrows may keep alive.

use crate::harness::{Decoded, begin, bytes, encode, lookup, row};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::{RowBlockDecoder, decoder::MAX_PINNED_BYTES};
use value::{SharedValue, Value, shared_buffer::MAX_SHARED_OFFSET};

/// A block of `rows` rows, each holding one string of `len` bytes in column `s` and a number
/// in column `n`.
fn string_block(rows: usize, len: usize) -> Vec<u8> {
    let shard = lookup(&["s", "n"]);
    let rows: Vec<_> = (0..rows)
        .map(|i| {
            let text = format!("{i:0len$}");
            row(
                &shard,
                &[
                    ("s", bytes(&text).to_value()),
                    ("n", SharedValue::new_num(i as f64)),
                ],
            )
        })
        .collect();
    encode(&shard, &rows)
}

/// Decodes every row of the active block, keeping the rows.
fn read_all(decoder: &mut RowBlockDecoder) -> Vec<RLookupRow<'static>> {
    let mut rows = Vec::new();
    while decoder.has_rows() {
        let mut row = RLookupRow::new();
        // SAFETY: every caller's coordinator lookup outlives its decoder.
        unsafe { decoder.next_row(&mut row) }.expect("the row decodes");
        rows.push(row);
    }
    rows
}

/// The values `rows` hold, in the coordinator lookup's key order.
fn values(lookup: &RLookup<'_>, rows: &[RLookupRow<'_>]) -> Vec<Vec<SharedValue>> {
    rows.iter()
        .map(|row| {
            lookup
                .iter()
                .filter_map(|key| row.get(key).cloned())
                .collect()
        })
        .collect()
}

/// Whether `value` is a string borrowed from its block.
const fn is_shared(value: &Value) -> bool {
    matches!(value, Value::String(string) if string.is_shared())
}

#[test]
fn decoded_strings_borrow_from_the_block() {
    let block = string_block(3, 4);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    assert!(decoder.shares_strings());

    let rows = read_all(&mut decoder);
    for (i, row) in values(&coordinator, &rows).iter().enumerate() {
        assert!(is_shared(&row[0]), "row {i}'s string was copied");
        assert_eq!(Decoded::from_value(&row[0]), bytes(&format!("{i:04}")));
        assert_eq!(Decoded::from_value(&row[1]), Decoded::Number(i as f64));
    }
}

#[test]
fn a_string_keeps_its_block_alive_until_it_is_dropped() {
    let block = string_block(2, 8);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let rows = read_all(&mut decoder);
    let kept = values(&coordinator, &rows)[1][0].clone();
    drop(rows);

    decoder.end();
    assert_eq!(
        decoder.live_bytes(),
        block.len(),
        "one string pins the block"
    );
    assert_eq!(Decoded::from_value(&kept), bytes("00000001"));

    drop(kept);
    assert_eq!(decoder.live_bytes(), 0, "the last string frees the block");
}

#[test]
fn a_block_whose_strings_are_all_gone_is_freed_when_it_ends() {
    let block = string_block(2, 8);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    drop(read_all(&mut decoder));
    assert_eq!(
        decoder.live_bytes(),
        block.len(),
        "the active block is held"
    );
    decoder.end();
    assert_eq!(decoder.live_bytes(), 0);
}

#[test]
fn strings_nested_in_collections_borrow_too() {
    let shard = lookup(&["v"]);
    let value = Decoded::Map(vec![(
        bytes("key"),
        Decoded::Array(vec![bytes("a"), Decoded::Number(1.0), bytes("bc")]),
    )]);
    let block = encode(&shard, &[row(&shard, &[("v", value.to_value())])]);

    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let rows = read_all(&mut decoder);
    let decoded = values(&coordinator, &rows)[0][0].clone();
    assert_eq!(Decoded::from_value(&decoded), value);

    let Value::Map(map) = &*decoded else {
        panic!("a map decodes as a map");
    };
    let (key, inner) = map.iter().next().expect("one entry");
    assert!(is_shared(key));
    let Value::Array(items) = &**inner else {
        panic!("an array decodes as an array");
    };
    assert!(is_shared(&items[0]) && is_shared(&items[2]));
}

#[test]
fn a_string_dropped_on_another_thread_frees_its_block() {
    let block = string_block(1, 16);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let rows = read_all(&mut decoder);
    let kept = values(&coordinator, &rows)[0][0].clone();
    drop(rows);
    decoder.end();

    std::thread::spawn(move || {
        assert_eq!(Decoded::from_value(&kept), bytes(&format!("{:016}", 0)));
    })
    .join()
    .expect("the thread succeeds");
    assert_eq!(decoder.live_bytes(), 0);
}

#[test]
#[cfg_attr(miri, ignore = "builds a block larger than the shareable offset")]
fn a_string_too_far_into_its_block_is_copied() {
    // Its offset would not fit the room a shared string has for it.
    let shard = lookup(&["s"]);
    let block = encode(
        &shard,
        &[
            row(
                &shard,
                &[("s", bytes(&"x".repeat(MAX_SHARED_OFFSET)).to_value())],
            ),
            row(&shard, &[("s", bytes("far").to_value())]),
        ],
    );

    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let rows = read_all(&mut decoder);
    let decoded = values(&coordinator, &rows);
    assert!(is_shared(&decoded[0][0]));
    assert!(!is_shared(&decoded[1][0]));
    assert_eq!(Decoded::from_value(&decoded[1][0]), bytes("far"));
}

#[test]
fn past_the_pinning_budget_strings_are_copied_until_blocks_are_released() {
    // One string kept per block — a `GROUPBY` key, say — pins every block it came from. Once
    // that passes the budget the decoder stops sharing, so the retained memory stays bounded
    // by the budget plus one block instead of growing with the stream.
    let block = string_block(64, 1024);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();

    let mut kept = Vec::new();
    let mut copied_blocks = 0;
    let blocks = MAX_PINNED_BYTES / block.len() + 4;
    for _ in 0..blocks {
        begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
        let sharing = decoder.shares_strings();
        let rows = read_all(&mut decoder);
        let string = values(&coordinator, &rows)[0][0].clone();
        assert_eq!(is_shared(&string), sharing);
        copied_blocks += usize::from(!sharing);
        kept.push(string);
        drop(rows);
        decoder.end();
        assert!(decoder.live_bytes() <= MAX_PINNED_BYTES + block.len());
    }
    assert!(copied_blocks > 0, "the budget was reached");

    kept.clear();
    assert_eq!(decoder.live_bytes(), 0);
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    assert!(
        decoder.shares_strings(),
        "sharing resumes once blocks are released"
    );
}
