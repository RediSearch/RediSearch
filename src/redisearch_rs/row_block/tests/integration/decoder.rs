/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coordinator's decoder: blocks decoded straight into lookup rows, the way the network result processor drives it.

use crate::harness::{
    Decoded, begin, bytes, decode_into, encode, lookup, row, try_decode, valid_block,
};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::RowBlockDecoder;
use std::ffi::CString;
use value::SharedValue;

#[test]
fn rows_land_under_the_coordinator_keys_of_their_columns() {
    let block = valid_block();
    let mut coordinator = RLookup::new();
    assert_eq!(decode_into(&block, &mut coordinator), try_decode(&block));
}

#[test]
fn columns_the_coordinator_already_knows_reuse_its_keys() {
    let shard = lookup(&["a", "b"]);
    let block = encode(&shard, &[row(&shard, &[("b", SharedValue::new_num(2.0))])]);

    // The coordinator's key for `b` comes first and `a` is unknown to it, so a decoder that mapped columns by position
    // instead of by name would write `b`'s value under `a`.
    let mut coordinator = lookup(&["z", "b"]);
    let before = coordinator.iter().count();
    assert_eq!(
        decode_into(&block, &mut coordinator),
        Ok(vec![vec![("b".to_owned(), Decoded::Number(2.0))]])
    );
    assert_eq!(
        coordinator.iter().count(),
        before + 1,
        "only the unknown column `a` got a new key"
    );
}

#[test]
fn a_created_key_outlives_the_block_it_was_named_in() {
    // The lookup is handed names that point into the block, which is freed with its shard reply while the key lives on.
    // Under Miri a key still borrowing its name is a use-after-free here.
    let shard = lookup(&["dynamic"]);
    let mut block = encode(
        &shard,
        &[row(&shard, &[("dynamic", SharedValue::new_num(1.0))])],
    );
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    decoder.end();
    block.fill(0xa5);
    drop(block);

    let name = CString::new("dynamic").unwrap();
    let key = coordinator
        .find_key_by_name(&name)
        .expect("the key was created");
    assert_eq!(
        key.current().expect("found").name().as_ref(),
        name.as_c_str()
    );
}

#[test]
fn an_empty_block_is_active_until_ended_but_holds_no_rows() {
    let shard = lookup(&["a"]);
    let block = encode(&shard, &[]);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();

    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    assert!(decoder.is_active());
    assert!(!decoder.has_rows());
    assert_eq!(decoder.ncols(), 1);
    decoder.end();
    assert!(!decoder.is_active());
}

#[test]
fn every_column_counts_whether_or_not_the_row_holds_it() {
    let shard = lookup(&["a", "b", "c"]);
    let block = encode(&shard, &[row(&shard, &[("b", bytes("x").to_value())])]);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();

    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let mut target = RLookupRow::new();
    // SAFETY: `coordinator` outlives the decoder.
    unsafe { decoder.next_row(&mut target) }.expect("the row decodes");
    assert_eq!(decoder.ncols(), 3);
    assert_eq!(target.num_dyn_values(), 1);
}

#[test]
fn beginning_a_block_ends_the_one_before() {
    let shard = lookup(&["a"]);
    let first = encode(
        &shard,
        &[
            row(&shard, &[("a", SharedValue::new_num(1.0))]),
            row(&shard, &[("a", SharedValue::new_num(2.0))]),
        ],
    );
    let other = lookup(&["b"]);
    let second = encode(&other, &[row(&other, &[("b", SharedValue::new_num(3.0))])]);

    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &first).expect("the block parses");
    let mut target = RLookupRow::new();
    // SAFETY: `coordinator` outlives the decoder.
    unsafe { decoder.next_row(&mut target) }.expect("the row decodes");

    begin(&mut decoder, &mut coordinator, &second).expect("the block parses");
    let mut rows = 0;
    while decoder.has_rows() {
        let mut target = RLookupRow::new();
        // SAFETY: `coordinator` outlives the decoder.
        unsafe { decoder.next_row(&mut target) }.expect("the row decodes");
        rows += 1;
    }
    assert_eq!(rows, 1, "the first block's second row is gone");
}

#[test]
#[should_panic(expected = "no row is left")]
fn reading_past_the_last_row_panics() {
    let shard = lookup(&["a"]);
    let block = encode(&shard, &[]);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    let mut target = RLookupRow::new();
    // SAFETY: `coordinator` outlives the decoder.
    let _ = unsafe { decoder.next_row(&mut target) };
}
