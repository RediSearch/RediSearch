/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Buffer-limit refusals must leave a block replayable and the writer reusable.

use crate::harness::{Decoded, decode, lookup, row};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupKeyFlags};
use row_block::{
    Block, ColumnFilter, ColumnKind, RefusedRow, RowBlockWriter, SchemaError, TrioMember,
};
use std::ffi::CString;
use value::SharedValue;

const BUFFER_LIMIT: usize = 32 * 1024 * 1024;

#[test]
#[cfg_attr(miri, ignore)] // Full-size payloads make interpreter execution impractical.
fn exact_limit_is_accepted_and_larger_values_leave_the_block_unchanged() {
    let lookup = lookup(&["a"]);
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .unwrap();
    let schema = writer.as_bytes().to_vec();
    // Presence bitmap, string length and trailing NUL are also payload bytes.
    let string_len = BUFFER_LIMIT - schema.len() - 6;
    for len in [BUFFER_LIMIT, string_len + 1] {
        let oversized = row(&lookup, &[("a", SharedValue::new_string(vec![b'x'; len]))]);
        assert_eq!(
            writer.write_row(&lookup, &oversized, TrioMember::Middle),
            Err(RefusedRow::BufferFull)
        );
        assert_eq!(writer.as_bytes(), schema);
        assert_eq!(writer.nrows(), 0);
    }
    let exact = row(
        &lookup,
        &[("a", SharedValue::new_string(vec![b'x'; string_len]))],
    );
    writer
        .write_row(&lookup, &exact, TrioMember::Middle)
        .unwrap();
    assert_eq!(writer.as_bytes().len(), BUFFER_LIMIT);
    let before = writer.as_bytes().to_vec();
    assert_eq!(
        writer.write_row(&lookup, &row(&lookup, &[]), TrioMember::Middle),
        Err(RefusedRow::BufferFull)
    );
    assert_eq!(writer.as_bytes(), before);
    assert_eq!(writer.nrows(), 1);
    assert_eq!(
        decode(writer.as_bytes()),
        vec![vec![(
            "a".to_owned(),
            Decoded::Bytes(vec![b'x'; string_len])
        )]]
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn retagging_over_the_limit_restores_kinds_and_reset_reuses_both_buffers() {
    let lookup = lookup(&["a"]);
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .unwrap();
    let string_len = BUFFER_LIMIT - writer.as_bytes().len() - 6 - 10;
    let large = row(
        &lookup,
        &[("a", SharedValue::new_string(vec![b'x'; string_len]))],
    );
    writer
        .write_row(&lookup, &large, TrioMember::Middle)
        .unwrap();
    let before = writer.as_bytes().to_vec();
    let number = row(&lookup, &[("a", SharedValue::new_num(1.0))]);
    // The new tagged number fits; the tag inserted before the old string does not.
    assert_eq!(
        writer.write_row(&lookup, &number, TrioMember::Middle),
        Err(RefusedRow::BufferFull)
    );
    assert_eq!(writer.as_bytes(), before);
    assert_eq!(writer.nrows(), 1);
    // This succeeds only if the refused retag restored the string column kind.
    let empty_string = row(&lookup, &[("a", SharedValue::new_string(Vec::new()))]);
    writer
        .write_row(&lookup, &empty_string, TrioMember::Middle)
        .unwrap();
    assert_eq!(writer.nrows(), 2);

    writer.reset();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .unwrap();
    let exact = row(
        &lookup,
        &[("a", SharedValue::new_string(vec![b'x'; string_len - 1]))],
    );
    writer
        .write_row(&lookup, &exact, TrioMember::Middle)
        .unwrap();
    writer
        .write_row(&lookup, &number, TrioMember::Middle)
        .unwrap();
    assert_eq!(writer.as_bytes().len(), BUFFER_LIMIT);
    assert_eq!(writer.nrows(), 2);

    for _ in 0..2 {
        writer.reset();
        writer
            .write_schema(&lookup, ColumnFilter::default())
            .unwrap();
        writer
            .write_row(&lookup, &empty_string, TrioMember::Middle)
            .unwrap();
        writer
            .write_row(&lookup, &number, TrioMember::Middle)
            .unwrap();
        assert_eq!(
            decode(writer.as_bytes()),
            vec![
                vec![("a".to_owned(), Decoded::Bytes(Vec::new()))],
                vec![("a".to_owned(), Decoded::Number(1.0))],
            ]
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn giant_schema_refuses_then_accepts_a_small_schema() {
    let mut large = RLookup::new();
    let mut remaining = BUFFER_LIMIT - 7;
    for i in 0..512 {
        let name_len = usize::from(u16::MAX).min(remaining - 4);
        let name = format!("{i:04}{}", "x".repeat(name_len - 4));
        large
            .get_key_write(CString::new(name).unwrap(), RLookupKeyFlags::empty())
            .unwrap();
        remaining -= name_len + 4;
    }
    assert_eq!(remaining, 0);
    let mut writer = RowBlockWriter::new();
    assert_eq!(
        writer.write_schema(&large, ColumnFilter::default()),
        Ok(512)
    );
    assert_eq!(writer.as_bytes().len(), BUFFER_LIMIT);
    writer.reset();
    large
        .get_key_write(CString::new("overflow").unwrap(), RLookupKeyFlags::empty())
        .unwrap();
    assert_eq!(
        writer.write_schema(&large, ColumnFilter::default()),
        Err(SchemaError::BufferFull)
    );
    assert!(writer.as_bytes().is_empty());
    assert_eq!(writer.nrows(), 0);
    let small = lookup(&["a"]);
    assert_eq!(writer.write_schema(&small, ColumnFilter::default()), Ok(1));
    writer
        .write_row(
            &small,
            &row(&small, &[("a", SharedValue::new_num(2.0))]),
            TrioMember::Middle,
        )
        .unwrap();
    assert_eq!(
        decode(writer.as_bytes()),
        vec![vec![("a".to_owned(), Decoded::Number(2.0))]]
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn sparse_retagging_accepts_the_limit_when_its_reservation_estimate_exceeds_it() {
    let lookup = lookup(&["a"]);
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .unwrap();
    let string_len = BUFFER_LIMIT - writer.as_bytes().len() - 18;
    let large = row(
        &lookup,
        &[("a", SharedValue::new_string(vec![b'x'; string_len]))],
    );
    writer
        .write_row(&lookup, &large, TrioMember::Middle)
        .unwrap();
    writer
        .write_row(&lookup, &row(&lookup, &[]), TrioMember::Middle)
        .unwrap();
    // The estimate includes a tag for the absent field; only the present string needs one.
    let number = row(&lookup, &[("a", SharedValue::new_num(1.0))]);
    writer
        .write_row(&lookup, &number, TrioMember::Middle)
        .unwrap();
    assert_eq!(writer.as_bytes().len(), BUFFER_LIMIT);
    assert_eq!(writer.nrows(), 3);
    assert_eq!(
        Block::parse(writer.as_bytes()).unwrap().kinds(),
        &[ColumnKind::Tagged]
    );
    assert_eq!(
        decode(writer.as_bytes()),
        vec![
            vec![("a".to_owned(), Decoded::Bytes(vec![b'x'; string_len]))],
            vec![],
            vec![("a".to_owned(), Decoded::Number(1.0))],
        ]
    );
}
