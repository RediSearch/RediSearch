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
//! A refused row must leave the block exactly as it was: the caller's fallback re-emits what
//! the block holds as RESP rows and then carries on down the RESP path, so a block that ended
//! mid-row would either lose the rows before it or duplicate them.

use crate::harness::{Decoded, decode, encode, lookup, row, try_encode_one};
use pretty_assertions::assert_eq;
use rlookup::RLookupKeyFlags;
use row_block::{ColumnFilter, MAX_NESTING_DEPTH, RefusedRow, RowBlockWriter, TrioMember};
use std::ffi::CString;
use value::SharedValue;

/// A value nesting one level deeper than the format carries.
fn too_deep() -> SharedValue {
    (0..=MAX_NESTING_DEPTH).fold(SharedValue::new_num(1.0), |inner, _| {
        SharedValue::new_array(vec![inner])
    })
}

#[test]
fn a_refused_row_leaves_only_the_schema_behind() {
    // The refusal is found at the second column, so the bitmap and the first column's value
    // are already in the buffer when the row is abandoned.
    let lookup = lookup(&["a", "b", "c"]);
    let row = row(
        &lookup,
        &[
            ("a", SharedValue::new_num(1.0)),
            ("b", too_deep()),
            ("c", SharedValue::new_num(3.0)),
        ],
    );

    let (outcome, block, nrows) = try_encode_one(&lookup, &row, TrioMember::Middle);

    assert_eq!(outcome, Err(RefusedRow::TooDeeplyNested));
    assert_eq!(nrows, 0, "a refused row is not counted");
    assert_eq!(
        block,
        encode(&lookup, &[]),
        "the buffer is back to the schema"
    );
    assert_eq!(decode(&block), Vec::<Vec<_>>::new());
}

#[test]
fn a_refused_row_leaves_the_rows_before_it_intact() {
    let lookup = lookup(&["a", "b"]);
    let good = [
        row(&lookup, &[("a", SharedValue::new_num(1.0))]),
        row(&lookup, &[("b", SharedValue::new_num(2.0))]),
    ];
    let bad = row(
        &lookup,
        &[("a", too_deep()), ("b", SharedValue::new_num(9.0))],
    );

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .expect("encodable");
    for row in &good {
        writer
            .write_row(&lookup, row, TrioMember::Middle)
            .expect("encodable");
    }
    assert_eq!(
        writer.write_row(&lookup, &bad, TrioMember::Middle),
        Err(RefusedRow::TooDeeplyNested)
    );

    assert_eq!(
        writer.nrows(),
        good.len(),
        "only the accepted rows are counted"
    );
    assert_eq!(
        writer.as_bytes(),
        encode(&lookup, &good),
        "the block is byte-identical to one that never saw the refused row"
    );
    assert_eq!(
        decode(writer.as_bytes()),
        vec![
            vec![("a".to_owned(), Decoded::Number(1.0))],
            vec![("b".to_owned(), Decoded::Number(2.0))],
        ]
    );
}

#[test]
fn a_writer_keeps_accepting_rows_after_a_refusal() {
    // The caller stops using the block on the first refusal, but nothing about the writer's
    // state should depend on that: a rolled-back row must not corrupt the next one.
    let lookup = lookup(&["a"]);
    let bad = row(&lookup, &[("a", too_deep())]);
    let good = row(&lookup, &[("a", SharedValue::new_num(5.0))]);

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .expect("encodable");
    assert!(writer.write_row(&lookup, &bad, TrioMember::Middle).is_err());
    writer
        .write_row(&lookup, &good, TrioMember::Middle)
        .expect("encodable");

    assert_eq!(writer.as_bytes(), encode(&lookup, &[good]));
}

#[test]
fn a_lookup_that_grew_after_the_schema_refuses_its_rows() {
    // The presence bitmap is sized from the declared column count, so a column that appeared
    // afterwards has no bit to set. Refusing beats writing a row the decoder would misread.
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
