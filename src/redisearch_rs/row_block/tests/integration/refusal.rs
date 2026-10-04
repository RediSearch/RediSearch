/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Refusing a row.
//!
//! A refused row must leave the block exactly as it was: the caller's fallback re-emits what the block holds as RESP
//! rows and then carries on down the RESP path, so a block that ended mid-row would either lose the rows before it or
//! duplicate them.

use crate::harness::{Decoded, decode, encode, lookup, row, too_deep};
use pretty_assertions::assert_eq;
use rlookup::{RLookupKeyFlags, RLookupRow};
use row_block::{ColumnFilter, RefusedRow, RowBlockWriter, TrioMember};
use std::ffi::CString;
use value::SharedValue;

#[test]
fn a_refused_row_leaves_the_block_as_if_it_was_never_written() {
    // Each refusal is found at the second column, so the bitmap and the first column's value are already in the buffer
    // when the row is abandoned: once with no row before it, once after accepted rows. The writer must then go on
    // accepting rows, since nothing about its state may depend on the caller stopping at the first refusal.
    let lookup = lookup(&["a", "b"]);
    let bad = row(
        &lookup,
        &[("a", SharedValue::new_num(9.0)), ("b", too_deep())],
    );
    let good = [
        row(&lookup, &[("a", SharedValue::new_num(1.0))]),
        row(&lookup, &[("b", SharedValue::new_num(2.0))]),
        row(&lookup, &[("a", SharedValue::new_num(3.0))]),
    ];

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .expect("encodable");
    let mut write = |row: &RLookupRow<'_>| writer.write_row(&lookup, row, TrioMember::Middle);
    assert_eq!(write(&bad), Err(RefusedRow::TooDeeplyNested));
    write(&good[0]).expect("encodable");
    write(&good[1]).expect("encodable");
    assert_eq!(write(&bad), Err(RefusedRow::TooDeeplyNested));
    write(&good[2]).expect("encodable");

    assert_eq!(
        writer.nrows(),
        good.len(),
        "only the accepted rows are counted"
    );
    assert_eq!(writer.as_bytes(), encode(&lookup, &good));
    assert_eq!(
        decode(writer.as_bytes()),
        vec![
            vec![("a".to_owned(), Decoded::Number(1.0))],
            vec![("b".to_owned(), Decoded::Number(2.0))],
            vec![("a".to_owned(), Decoded::Number(3.0))],
        ]
    );
}

#[test]
fn a_lookup_that_grew_after_the_schema_refuses_its_rows() {
    // The presence bitmap is sized from the declared column count, so a column that appeared afterwards has no bit to
    // set. Refusing beats writing a row the decoder would misread.
    let mut lookup = lookup(&["a"]);
    let mut writer = RowBlockWriter::new();
    assert_eq!(writer.write_schema(&lookup, ColumnFilter::default()), Ok(1));

    lookup
        .get_key_write(CString::new("b").unwrap(), RLookupKeyFlags::empty())
        .expect("a new column");
    let row = row(&lookup, &[("a", SharedValue::new_num(1.0))]);

    assert_eq!(
        writer.write_row(&lookup, &row, TrioMember::Middle),
        Err(RefusedRow::ColumnCountChanged)
    );
    assert_eq!(writer.nrows(), 0);
    assert_eq!(
        decode(writer.as_bytes()),
        Vec::<Vec<_>>::new(),
        "the block is still a decodable, empty one"
    );
}
