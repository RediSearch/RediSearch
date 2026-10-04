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
            let fields = [
                ("s", bytes(&text).to_value()),
                ("n", SharedValue::new_num(i as f64)),
            ];
            row(&shard, &fields)
        })
        .collect();
    encode(&shard, &rows)
}

/// Makes `block` `decoder`'s active block and decodes all of it, returning each row's values
/// in schema order. The rows themselves are dropped, as a streaming pipeline drops them.
fn decode_values(decoder: &mut RowBlockDecoder, block: &[u8]) -> Vec<Vec<SharedValue>> {
    let mut coordinator = RLookup::new();
    begin(decoder, &mut coordinator, block).expect("the block parses");
    let mut values = Vec::new();
    while decoder.has_rows() {
        let mut row = RLookupRow::new();
        // SAFETY: `coordinator` outlives every `next_row` call.
        unsafe { decoder.next_row(&mut row) }.expect("the row decodes");
        values.push(
            coordinator
                .iter()
                .filter_map(|key| row.get(key).cloned())
                .collect(),
        );
    }
    values
}

const fn is_shared(value: &Value) -> bool {
    matches!(value, Value::String(string) if string.is_shared())
}

#[test]
fn decoded_strings_borrow_from_the_block_at_any_depth() {
    let shard = lookup(&["s", "v"]);
    let nested = Decoded::Map(vec![(
        bytes("key"),
        Decoded::Array(vec![bytes("a"), Decoded::Number(1.0)]),
    )]);
    let fields = [("s", bytes("top").to_value()), ("v", nested.to_value())];
    let block = encode(&shard, &[row(&shard, &fields)]);

    let mut decoder = RowBlockDecoder::new();
    let row = decode_values(&mut decoder, &block).remove(0);
    assert!(decoder.shares_strings());
    assert!(is_shared(&row[0]));
    assert_eq!(Decoded::from_value(&row[0]), bytes("top"));
    assert_eq!(Decoded::from_value(&row[1]), nested);

    let Value::Map(map) = &*row[1] else {
        panic!("a map decodes as a map");
    };
    let (key, inner) = map.iter().next().expect("one entry");
    let Value::Array(items) = &**inner else {
        panic!("an array decodes as an array");
    };
    assert!(is_shared(key) && is_shared(&items[0]));
}

#[test]
fn the_block_lives_until_the_decoder_and_its_last_string_let_go() {
    let block = string_block(2, 16);
    let mut decoder = RowBlockDecoder::new();
    let kept = decode_values(&mut decoder, &block).remove(1).remove(0);
    assert_eq!(
        decoder.live_bytes(),
        block.len(),
        "the active block is held"
    );

    decoder.end();
    assert_eq!(
        decoder.live_bytes(),
        block.len(),
        "one string pins the block"
    );

    // The last reference may go on any thread.
    std::thread::spawn(move || {
        assert_eq!(Decoded::from_value(&kept), bytes(&format!("{:016}", 1)))
    })
    .join()
    .expect("the thread succeeds");
    assert_eq!(decoder.live_bytes(), 0, "the last string frees the block");
}

#[test]
#[cfg_attr(miri, ignore = "builds a block larger than the shareable offset")]
fn a_string_too_far_into_its_block_is_copied() {
    // Its offset would not fit the room a shared string has for it.
    let shard = lookup(&["s"]);
    let far = bytes(&"x".repeat(MAX_SHARED_OFFSET));
    let block = encode(
        &shard,
        &[
            row(&shard, &[("s", far.to_value())]),
            row(&shard, &[("s", bytes("far").to_value())]),
        ],
    );

    let decoded = decode_values(&mut RowBlockDecoder::new(), &block);
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
    let mut decoder = RowBlockDecoder::new();

    let mut kept = Vec::new();
    let mut copied_blocks = 0;
    for _ in 0..MAX_PINNED_BYTES / block.len() + 4 {
        let string = decode_values(&mut decoder, &block).remove(0).remove(0);
        assert_eq!(is_shared(&string), decoder.shares_strings());
        copied_blocks += usize::from(!decoder.shares_strings());
        kept.push(string);
        decoder.end();
        assert!(decoder.live_bytes() <= MAX_PINNED_BYTES + block.len());
    }
    assert!(copied_blocks > 0, "the budget was reached");

    kept.clear();
    assert_eq!(decoder.live_bytes(), 0);
    decode_values(&mut decoder, &block);
    assert!(
        decoder.shares_strings(),
        "sharing resumes once blocks are released"
    );
}
