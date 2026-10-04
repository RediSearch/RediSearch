/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Column kinds: typed columns that drop the per-value tag, and the columns whose values
//! disagree on a type and have to keep it.

use crate::harness::{Decoded, bytes, decode, encode, lookup, row};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::{
    Block, ColumnFilter, ColumnKind, MAX_NESTING_DEPTH, RefusedRow, RowBlockWriter, Tag, TrioMember,
};
use value::SharedValue;

/// One value of every tag, each in a column of its own.
fn one_of_each() -> Vec<(&'static str, Decoded, Tag)> {
    vec![
        ("num", Decoded::Number(1.5), Tag::Number),
        ("str", bytes("abc"), Tag::String),
        ("null", Decoded::Null, Tag::Null),
        (
            "arr",
            Decoded::Array(vec![Decoded::Number(1.0), bytes("x")]),
            Tag::Array,
        ),
        (
            "map",
            Decoded::Map(vec![(bytes("k"), Decoded::Null)]),
            Tag::Map,
        ),
    ]
}

/// A single-column block holding `values`, one per row, and the column kind it declares.
fn single_column(values: &[Decoded]) -> (Vec<u8>, ColumnKind) {
    let lookup = lookup(&["v"]);
    let rows: Vec<_> = values
        .iter()
        .map(|value| row(&lookup, &[("v", value.to_value())]))
        .collect();
    let block = encode(&lookup, &rows);
    let kind = Block::parse(&block).expect("the block parses").kinds()[0];
    (block, kind)
}

/// `values`, one per row, as [`decode`] reports a single-column block of them.
fn rows_of(values: &[Decoded]) -> Vec<Vec<(String, Decoded)>> {
    values
        .iter()
        .map(|value| vec![("v".to_owned(), value.clone())])
        .collect()
}

#[test]
fn a_column_whose_values_share_a_tag_is_typed_with_it() {
    for (name, value, tag) in one_of_each() {
        let values = vec![value; 3];
        let (block, kind) = single_column(&values);
        assert_eq!(kind, ColumnKind::Typed(tag), "{name}");
        assert_eq!(decode(&block), rows_of(&values), "{name}");
    }
}

#[test]
fn every_typed_column_round_trips_side_by_side() {
    let columns = one_of_each();
    let names: Vec<&str> = columns.iter().map(|(name, ..)| *name).collect();
    let lookup = lookup(&names);
    let present: Vec<_> = columns
        .iter()
        .map(|(name, value, _)| (*name, value.to_value()))
        .collect();
    let block = encode(
        &lookup,
        &[
            row(&lookup, &present),
            row(&lookup, &[]),
            row(&lookup, &present),
        ],
    );

    let parsed = Block::parse(&block).expect("the block parses");
    let want_kinds: Vec<_> = columns
        .iter()
        .map(|(.., tag)| ColumnKind::Typed(*tag))
        .collect();
    assert_eq!(parsed.kinds(), &want_kinds[..]);

    let full: Vec<_> = columns
        .iter()
        .map(|(name, value, _)| ((*name).to_owned(), value.clone()))
        .collect();
    assert_eq!(decode(&block), vec![full.clone(), vec![], full]);
}

#[test]
fn a_typed_column_spends_no_byte_on_tags() {
    // The point of the kinds: a number column costs its bitmap and its payload per row, and
    // the tagged layout's extra byte per value is the difference.
    let rows = 100;
    let numbers = vec![Decoded::Number(7.0); rows];
    let (typed, kind) = single_column(&numbers);
    assert_eq!(kind, ColumnKind::Typed(Tag::Number));

    let schema = single_column(&[]).0.len();
    assert_eq!(typed.len(), schema + rows * (1 + size_of::<f64>()));

    let mut mixed = numbers;
    mixed[0] = bytes("");
    let (tagged, kind) = single_column(&mixed);
    assert_eq!(kind, ColumnKind::Tagged);
    assert_eq!(
        tagged.len(),
        schema + (rows - 1) * (1 + 1 + size_of::<f64>()) + (1 + 1 + size_of::<u32>() + 1),
        "a tagged column pays one byte per value on top of the typed layout"
    );
}

#[test]
fn a_column_with_values_of_different_types_is_tagged() {
    // A `LOAD` field can be numeric in one document and a string in another, and a field can
    // hold an array in one row and a scalar in the next: neither forces a RESP fallback.
    for values in [
        vec![Decoded::Number(1.0), bytes("one")],
        vec![Decoded::Array(vec![bytes("a")]), bytes("a")],
        vec![Decoded::Map(vec![]), Decoded::Array(vec![])],
        vec![Decoded::Null, Decoded::Number(0.0)],
        vec![Decoded::Number(0.0), Decoded::Null],
    ] {
        let (block, kind) = single_column(&values);
        assert_eq!(kind, ColumnKind::Tagged, "{values:?}");
        assert_eq!(decode(&block), rows_of(&values), "{values:?}");
    }
}

#[test]
fn a_conflict_anywhere_re_encodes_every_row_before_it() {
    let rows = 7;
    for conflict_at in 1..rows {
        let values: Vec<_> = (0..rows)
            .map(|i| {
                if i < conflict_at {
                    Decoded::Number(i as f64)
                } else {
                    bytes(&format!("s{i}"))
                }
            })
            .collect();
        let (block, kind) = single_column(&values);
        assert_eq!(kind, ColumnKind::Tagged, "conflict at row {conflict_at}");
        assert_eq!(
            decode(&block),
            rows_of(&values),
            "conflict at row {conflict_at}"
        );
    }
}

#[test]
fn a_conflict_mid_row_keeps_the_columns_around_it_intact() {
    // The retagged column sits between typed ones, and the conflicting row has already
    // written the column before it when the conflict is found.
    let lookup = lookup(&["a", "b", "c"]);
    let typed_row = |i: f64| {
        row(
            &lookup,
            &[
                ("a", SharedValue::new_num(i)),
                ("b", SharedValue::new_num(-i)),
                ("c", bytes("c").to_value()),
            ],
        )
    };
    let mut rows: Vec<_> = (0..3).map(|i| typed_row(f64::from(i))).collect();
    rows.push(row(
        &lookup,
        &[
            ("a", SharedValue::new_num(3.0)),
            ("b", bytes("b").to_value()),
            ("c", bytes("c").to_value()),
        ],
    ));
    rows.push(row(&lookup, &[("b", SharedValue::new_num(4.0))]));
    rows.push(typed_row(5.0));

    let block = encode(&lookup, &rows);
    let parsed = Block::parse(&block).expect("the block parses");
    assert_eq!(
        parsed.kinds(),
        &[
            ColumnKind::Typed(Tag::Number),
            ColumnKind::Tagged,
            ColumnKind::Typed(Tag::String)
        ]
    );

    let field = |name: &str, value: Decoded| (name.to_owned(), value);
    let typed = |i: f64| {
        vec![
            field("a", Decoded::Number(i)),
            field("b", Decoded::Number(-i)),
            field("c", bytes("c")),
        ]
    };
    assert_eq!(
        decode(&block),
        vec![
            typed(0.0),
            typed(1.0),
            typed(2.0),
            vec![
                field("a", Decoded::Number(3.0)),
                field("b", bytes("b")),
                field("c", bytes("c")),
            ],
            vec![field("b", Decoded::Number(4.0))],
            typed(5.0),
        ]
    );
}

#[test]
fn several_columns_retagged_by_one_row_are_re_encoded_together() {
    let lookup = lookup(&["a", "b"]);
    let numbers = row(
        &lookup,
        &[
            ("a", SharedValue::new_num(1.0)),
            ("b", SharedValue::new_num(2.0)),
        ],
    );
    let strings = row(
        &lookup,
        &[("a", bytes("a").to_value()), ("b", bytes("b").to_value())],
    );
    let block = encode(&lookup, &[numbers, strings]);

    assert_eq!(
        Block::parse(&block).expect("the block parses").kinds(),
        &[ColumnKind::Tagged, ColumnKind::Tagged]
    );
    assert_eq!(
        decode(&block),
        vec![
            vec![
                ("a".to_owned(), Decoded::Number(1.0)),
                ("b".to_owned(), Decoded::Number(2.0)),
            ],
            vec![("a".to_owned(), bytes("a")), ("b".to_owned(), bytes("b"))],
        ]
    );
}

#[test]
fn a_column_no_row_holds_keeps_its_placeholder_kind() {
    let lookup = lookup(&["empty", "v"]);
    let block = encode(
        &lookup,
        &[
            row(&lookup, &[("v", SharedValue::new_num(1.0))]),
            row(&lookup, &[]),
        ],
    );
    assert_eq!(
        Block::parse(&block).expect("the block parses").kinds()[0],
        ColumnKind::Typed(Tag::Null)
    );
    assert_eq!(
        decode(&block),
        vec![vec![("v".to_owned(), Decoded::Number(1.0))], vec![]]
    );
}

#[test]
fn a_column_of_nulls_costs_only_its_presence_bits() {
    // A null is a value, not an absence — the coordinator replies it as a null field — so it
    // sets its presence bit, but in a column of nothing else it has no payload at all.
    let values = vec![Decoded::Null; 10];
    let (block, kind) = single_column(&values);
    assert_eq!(kind, ColumnKind::Typed(Tag::Null));
    assert_eq!(block.len(), single_column(&[]).0.len() + values.len());
    assert_eq!(decode(&block), rows_of(&values));
}

#[test]
fn a_refused_row_does_not_change_any_column_kind() {
    // The refused row would fix one column and retag another before its last column is found
    // unencodable; the block afterwards must be the one it would be without that row.
    let lookup = lookup(&["fresh", "typed", "bad"]);
    let good = row(&lookup, &[("typed", SharedValue::new_num(1.0))]);
    let too_deep = (0..=MAX_NESTING_DEPTH).fold(SharedValue::new_num(1.0), |inner, _| {
        SharedValue::new_array(vec![inner])
    });
    let refused = row(
        &lookup,
        &[
            ("fresh", SharedValue::new_num(2.0)),
            ("typed", bytes("x").to_value()),
            ("bad", too_deep),
        ],
    );
    let after = row(&lookup, &[("typed", SharedValue::new_num(3.0))]);

    let mut writer = writer_for(&lookup);
    write(&mut writer, &lookup, &good).expect("encodable");
    assert_eq!(
        write(&mut writer, &lookup, &refused),
        Err(RefusedRow::TooDeeplyNested)
    );
    write(&mut writer, &lookup, &after).expect("encodable");

    assert_eq!(writer.as_bytes(), encode(&lookup, &[good, after]));
}

#[test]
fn a_block_with_a_retagged_column_is_still_replayable_after_a_refusal() {
    // The fallback path replays what the block holds through the reader when a later row is
    // refused, so a block that went through a retag must still read back whole.
    let lookup = lookup(&["v"]);
    let rows = [
        row(&lookup, &[("v", SharedValue::new_num(1.0))]),
        row(&lookup, &[("v", bytes("two").to_value())]),
    ];
    let too_deep = (0..=MAX_NESTING_DEPTH).fold(SharedValue::new_num(1.0), |inner, _| {
        SharedValue::new_array(vec![inner])
    });

    let mut writer = writer_for(&lookup);
    for row in &rows {
        write(&mut writer, &lookup, row).expect("encodable");
    }
    assert!(write(&mut writer, &lookup, &row(&lookup, &[("v", too_deep)])).is_err());

    let parsed = Block::parse(writer.as_bytes()).expect("the block parses");
    assert_eq!(parsed.kinds(), &[ColumnKind::Tagged]);
    assert_eq!(parsed.rows().count(), writer.nrows());
    assert_eq!(
        decode(writer.as_bytes()),
        rows_of(&[Decoded::Number(1.0), bytes("two")])
    );
}

#[test]
fn reset_forgets_the_previous_chunks_column_kinds() {
    let lookup = lookup(&["v"]);
    let mut writer = writer_for(&lookup);
    write(
        &mut writer,
        &lookup,
        &row(&lookup, &[("v", bytes("s").to_value())]),
    )
    .expect("ok");

    writer.reset();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .expect("encodable");
    let number = row(&lookup, &[("v", SharedValue::new_num(1.0))]);
    write(&mut writer, &lookup, &number).expect("encodable");
    assert_eq!(writer.as_bytes(), encode(&lookup, &[number]));
}

/// A writer with `lookup`'s schema written.
fn writer_for(lookup: &RLookup<'_>) -> RowBlockWriter {
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(lookup, ColumnFilter::default())
        .expect("encodable");
    writer
}

fn write(
    writer: &mut RowBlockWriter,
    lookup: &RLookup<'_>,
    row: &RLookupRow<'_>,
) -> Result<(), RefusedRow> {
    writer.write_row(lookup, row, TrioMember::Middle)
}
