/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coordinator's decoder: blocks decoded straight into lookup rows, the way the network
//! result processor drives it.

use crate::harness::{
    Decoded, begin, bytes, decode, decode_into, encode, lookup, malformed_blocks, row, try_decode,
    valid_block,
};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::{Block, RowBlockDecoder};
use std::ffi::CString;
use value::SharedValue;

#[test]
fn rows_land_under_the_coordinator_keys_of_their_columns() {
    let block = valid_block();
    let mut coordinator = RLookup::new();
    assert_eq!(decode_into(&block, &mut coordinator), Ok(decode(&block)));
}

#[test]
fn columns_the_coordinator_already_knows_reuse_its_keys() {
    let shard = lookup(&["a", "b"]);
    let block = encode(&shard, &[row(&shard, &[("b", SharedValue::new_num(2.0))])]);

    // The coordinator's key for `b` comes first and `a` is unknown to it, so a decoder that
    // mapped columns by position instead of by name would write `b`'s value under `a`.
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
    // The lookup is handed names that point into the block, which is freed once its rows are
    // read while the key lives on. Under Miri a key still borrowing its name is a
    // use-after-free here.
    let shard = lookup(&["dynamic"]);
    let block = encode(
        &shard,
        &[row(&shard, &[("dynamic", SharedValue::new_num(1.0))])],
    );
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    decoder.end();
    assert_eq!(decoder.live_bytes(), 0, "the block is freed");

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
fn an_empty_block_is_active_but_holds_no_rows() {
    let shard = lookup(&["a"]);
    let block = encode(&shard, &[]);
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();

    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    assert!(decoder.is_active());
    assert!(!decoder.has_rows());
    assert_eq!(decoder.ncols(), 1);
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
fn every_malformed_block_fails_with_the_error_the_reader_reports() {
    // The coordinator turns a failed `begin` and a failed `next_row` into different errors,
    // so beyond failing at all, each corruption must fail in the same phase as in the reader.
    for (what, block) in malformed_blocks() {
        let mut coordinator = RLookup::new();
        let got = decode_into(&block, &mut coordinator);
        assert!(got.is_err(), "{what} decoded");
        assert_eq!(got.err(), try_decode(&block).err(), "{what}");
        assert_eq!(
            begin_fails(&block),
            Block::parse(&block).is_err(),
            "{what} failed in another phase"
        );
    }
}

/// Whether [`RowBlockDecoder::begin`] rejects `block`, as opposed to a later row.
fn begin_fails(block: &[u8]) -> bool {
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, block).is_err()
}

#[test]
fn a_malformed_header_or_schema_leaves_no_block_active() {
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    let good = valid_block();
    for (what, block) in malformed_blocks() {
        // Start from an active block, so the failure has to tear it down.
        begin(&mut decoder, &mut coordinator, &good).expect("the valid block parses");
        let outcome = begin(&mut decoder, &mut coordinator, &block);
        if outcome.is_ok() {
            // Some corruptions only surface in the rows.
            continue;
        }
        assert!(!decoder.is_active(), "{what}");
        assert!(!decoder.has_rows(), "{what}");
    }
}

#[test]
fn a_truncated_row_ends_the_block() {
    let block = valid_block();
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    let truncated = &block[..block.len() - 1];

    begin(&mut decoder, &mut coordinator, truncated).expect("the schema is intact");
    let mut outcome = Ok(());
    while decoder.has_rows() {
        let mut target = RLookupRow::new();
        // SAFETY: `coordinator` outlives the decoder.
        outcome = unsafe { decoder.next_row(&mut target) };
    }
    assert!(outcome.is_err(), "the cut-off row decoded");
    assert!(!decoder.is_active());
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
fn ending_a_block_drops_its_remaining_rows() {
    let block = valid_block();
    let mut coordinator = RLookup::new();
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, &mut coordinator, &block).expect("the block parses");
    decoder.end();
    assert!(!decoder.is_active());
    assert!(!decoder.has_rows());
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
