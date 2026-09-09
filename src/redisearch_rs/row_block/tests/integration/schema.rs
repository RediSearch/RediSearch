/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The header and schema: the exact bytes, which keys become columns, and the cases a caller
//! must fall back to RESP for.

use crate::harness::{columns_of, decode, encode, lookup, lookup_with_flags, row};
use pretty_assertions::assert_eq;
use rlookup::{RLookupKeyFlag, RLookupKeyFlags};
use row_block::{ColumnFilter, MAGIC, RowBlockWriter, SchemaError, TrioMember};
use value::SharedValue;

#[test]
fn header_and_schema_bytes_are_exactly_the_documented_layout() {
    // The decoder on the other end of the internal path parses these bytes by hand, so this
    // is the test that fails if the layout is ever changed by accident.
    let lookup = lookup(&["ab", "c"]);
    let block = encode(&lookup, &[]);

    #[rustfmt::skip]
    let want: Vec<u8> = [
        &MAGIC.to_le_bytes()[..],       // magic
        &[2],                           // version
        &2u16.to_le_bytes()[..],        // ncols
        &2u16.to_le_bytes()[..], b"ab", &[0],  // "ab" plus its terminator
        &1u16.to_le_bytes()[..], b"c",  &[0],  // "c" plus its terminator
    ]
    .concat();

    assert_eq!(block, want);
}

#[test]
fn a_row_is_a_bitmap_followed_by_tagged_values() {
    let lookup = lookup(&["a", "b"]);
    let schema = encode(&lookup, &[]);
    let block = encode(
        &lookup,
        &[row(&lookup, &[("b", SharedValue::new_num(1.0))])],
    );

    #[rustfmt::skip]
    let want_row: Vec<u8> = [
        &[0b10][..],                    // presence bitmap: column 1 only
        &[1],                           // ROW_BLOCK_TAG_NUM
        &1.0f64.to_le_bytes()[..],
    ]
    .concat();

    assert_eq!(
        &block[..schema.len()],
        &schema[..],
        "the schema is unchanged"
    );
    assert_eq!(&block[schema.len()..], &want_row[..]);
}

#[test]
fn a_block_with_no_rows_is_just_its_schema() {
    let lookup = lookup(&["a"]);
    let block = encode(&lookup, &[]);

    assert_eq!(columns_of(&block), vec!["a".to_owned()]);
    assert_eq!(decode(&block), Vec::<Vec<_>>::new());
}

#[test]
fn a_lookup_with_no_visible_keys_declares_no_columns() {
    // Rows would be zero bytes long, which no reader can count, so the caller has to reply in
    // RESP. `write_schema` reporting zero is how it finds out.
    let mut writer = RowBlockWriter::new();
    assert_eq!(
        writer.write_schema(&lookup(&[]), ColumnFilter::default()),
        Ok(0)
    );
}

#[test]
fn the_filter_selects_the_same_columns_the_resp_serializer_would() {
    let columns = [
        ("plain", RLookupKeyFlags::empty()),
        ("hidden", RLookupKeyFlag::Hidden.into()),
        ("returned", RLookupKeyFlag::ExplicitReturn.into()),
    ];
    let lookup = lookup_with_flags(&columns);

    let schema = |filter| {
        let mut writer = RowBlockWriter::new();
        writer.write_schema(&lookup, filter).expect("encodable");
        columns_of(writer.as_bytes())
    };

    assert_eq!(
        schema(ColumnFilter {
            required: RLookupKeyFlags::empty(),
            excluded: RLookupKeyFlag::Hidden.into(),
        }),
        vec!["plain".to_owned(), "returned".to_owned()],
    );
    // An explicit RETURN list narrows the reply to the keys that asked for it.
    assert_eq!(
        schema(ColumnFilter {
            required: RLookupKeyFlag::ExplicitReturn.into(),
            excluded: RLookupKeyFlag::Hidden.into(),
        }),
        vec!["returned".to_owned()],
    );
    assert_eq!(
        schema(ColumnFilter::default()).len(),
        3,
        "no filter, no exclusions"
    );
}

#[test]
fn a_filtered_out_column_is_absent_from_rows_too() {
    // The schema and the rows must agree on the columns, or the presence bitmap describes a
    // different set of columns than the schema declares.
    let lookup = lookup_with_flags(&[
        ("plain", RLookupKeyFlags::empty()),
        ("hidden", RLookupKeyFlag::Hidden.into()),
    ]);
    let row = row(
        &lookup,
        &[
            ("plain", SharedValue::new_num(1.0)),
            ("hidden", SharedValue::new_num(2.0)),
        ],
    );

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(
            &lookup,
            ColumnFilter {
                required: RLookupKeyFlags::empty(),
                excluded: RLookupKeyFlag::Hidden.into(),
            },
        )
        .expect("encodable");
    writer
        .write_row(&lookup, &row, TrioMember::Middle)
        .expect("encodable");

    assert_eq!(
        decode(writer.as_bytes())
            .into_iter()
            .flatten()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        vec!["plain".to_owned()],
    );
}

#[test]
fn a_column_name_too_long_for_its_length_field_is_refused() {
    // The length is a `u16` but the full name plus its terminator follows it, so writing a
    // truncated length would leave the decoder reading the name's tail as the next field —
    // and every field after it in the block. Reachable through `LOAD ... AS <alias>`.
    let over = "x".repeat(usize::from(u16::MAX) + 1);
    let mut writer = RowBlockWriter::new();
    assert_eq!(
        writer.write_schema(&lookup(&[&over]), ColumnFilter::default()),
        Err(SchemaError::NameTooLong { len: over.len() })
    );
    assert!(
        writer.as_bytes().is_empty(),
        "a refused schema leaves nothing for the caller to send"
    );

    // The longest name that does fit is still encodable, so the boundary is not off by one.
    let fits = "x".repeat(usize::from(u16::MAX));
    let mut writer = RowBlockWriter::new();
    assert_eq!(
        writer.write_schema(&lookup(&[&fits]), ColumnFilter::default()),
        Ok(1)
    );
    assert_eq!(columns_of(writer.as_bytes()), vec![fits]);
}

#[test]
fn reset_discards_the_block_and_lets_a_new_schema_be_written() {
    let first = lookup(&["a"]);
    let second = lookup(&["x", "y"]);

    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&first, ColumnFilter::default())
        .expect("encodable");
    writer
        .write_row(
            &first,
            &row(&first, &[("a", SharedValue::new_num(1.0))]),
            TrioMember::Middle,
        )
        .expect("encodable");
    assert_eq!(writer.nrows(), 1);

    writer.reset();
    assert_eq!(writer.nrows(), 0);
    assert_eq!(
        writer.write_schema(&second, ColumnFilter::default()),
        Ok(2),
        "the reset writer accepts an unrelated schema"
    );
    assert_eq!(
        columns_of(writer.as_bytes()),
        vec!["x".to_owned(), "y".to_owned()]
    );
}
