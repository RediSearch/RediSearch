/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Column kinds: typed columns that drop the per-value tag, and the columns whose values disagree on a type and have to
//! keep it.

use crate::harness::{Decoded, bytes, decode, encode, lookup, row, too_deep};
use pretty_assertions::assert_eq;
use rlookup::{RLookup, RLookupRow};
use row_block::{Block, ColumnFilter, ColumnKind, RefusedRow, RowBlockWriter, Tag, TrioMember};
use value::SharedValue;

fn one_of_each() -> Vec<(Decoded, Tag)> {
    vec![
        (Decoded::Number(1.5), Tag::Number),
        (bytes("abc"), Tag::String),
        (Decoded::Null, Tag::Null),
        (
            Decoded::Array(vec![Decoded::Number(1.0), bytes("x")]),
            Tag::Array,
        ),
        (Decoded::Map(vec![(bytes("k"), Decoded::Null)]), Tag::Map),
    ]
}

/// A block of `values`, one per row, in column `v`, and that column's kind.
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

/// What [`decode`] reports for [`single_column`] of `values`.
fn rows_of(values: &[Decoded]) -> Vec<Vec<(String, Decoded)>> {
    values
        .iter()
        .map(|value| vec![("v".to_owned(), value.clone())])
        .collect()
}

#[test]
fn a_columns_kind_follows_the_types_of_its_values() {
    let mut cases: Vec<(Vec<Decoded>, ColumnKind)> = one_of_each()
        .into_iter()
        .map(|(value, tag)| (vec![value; 3], ColumnKind::Typed(tag)))
        .collect();
    // A `LOAD` field can be numeric in one document and a string in another, and a field can hold an array in one row
    // and a scalar in the next: neither forces a RESP fallback.
    for values in [
        vec![Decoded::Array(vec![bytes("a")]), bytes("a")],
        vec![Decoded::Map(vec![]), Decoded::Array(vec![])],
        vec![Decoded::Null, Decoded::Number(0.0)],
        vec![Decoded::Number(0.0), Decoded::Null],
    ] {
        cases.push((values, ColumnKind::Tagged));
    }
    // The conflict on every row position, each re-encoding a different number of rows.
    for conflict_at in 1..7 {
        let values = (0..7)
            .map(|i| match i < conflict_at {
                true => Decoded::Number(f64::from(i)),
                false => bytes(&format!("s{i}")),
            })
            .collect();
        cases.push((values, ColumnKind::Tagged));
    }

    for (values, want) in cases {
        let (block, kind) = single_column(&values);
        assert_eq!(kind, want, "{values:?}");
        assert_eq!(decode(&block), rows_of(&values), "{values:?}");
    }
}

#[test]
fn a_conflict_mid_row_keeps_the_columns_around_it_intact() {
    // The conflicting row retags two columns at once, one of them right after a typed column it has already written
    // when the first conflict is found.
    let lookup = lookup(&["a", "b", "c"]);
    let num = |n: f64| Decoded::Number(n);
    let rows: Vec<[Option<Decoded>; 3]> = vec![
        [Some(num(0.0)), Some(num(-0.0)), Some(bytes("c"))],
        [Some(num(1.0)), Some(num(-1.0)), Some(bytes("c"))],
        [Some(num(3.0)), Some(bytes("b")), Some(num(3.0))],
        [None, Some(num(4.0)), None],
        [Some(num(5.0)), Some(num(-5.0)), Some(bytes("c"))],
    ];
    let names = ["a", "b", "c"];
    let encoded: Vec<_> = rows
        .iter()
        .map(|values| {
            let fields: Vec<_> = names
                .iter()
                .zip(values)
                .filter_map(|(name, value)| Some((*name, value.as_ref()?.to_value())))
                .collect();
            row(&lookup, &fields)
        })
        .collect();
    let block = encode(&lookup, &encoded);

    assert_eq!(
        Block::parse(&block).expect("the block parses").kinds(),
        &[
            ColumnKind::Typed(Tag::Number),
            ColumnKind::Tagged,
            ColumnKind::Tagged
        ]
    );
    let want: Vec<Vec<_>> = rows
        .iter()
        .map(|values| {
            names
                .iter()
                .zip(values)
                .filter_map(|(name, value)| Some(((*name).to_owned(), value.clone()?)))
                .collect()
        })
        .collect();
    assert_eq!(decode(&block), want);
}

#[test]
fn columns_without_values_or_with_only_nulls_cost_only_presence_bits() {
    // A null is a value, not an absence — the coordinator replies it as a null field — so it sets its presence bit, but
    // in a column of nothing else it has no payload at all. A column no row holds keeps the placeholder kind it was
    // declared with.
    let lookup = lookup(&["empty", "nulls"]);
    let rows: Vec<_> = (0..10)
        .map(|_| row(&lookup, &[("nulls", SharedValue::null_static())]))
        .collect();
    let block = encode(&lookup, &rows);

    assert_eq!(
        Block::parse(&block).expect("the block parses").kinds(),
        &[ColumnKind::Typed(Tag::Null); 2]
    );
    assert_eq!(block.len(), encode(&lookup, &[]).len() + rows.len());
    assert_eq!(
        decode(&block),
        vec![vec![("nulls".to_owned(), Decoded::Null)]; 10]
    );
}

#[test]
fn a_refused_row_does_not_change_any_column_kind() {
    // The refused row would fix one column and retag another before its last column is found unencodable; the block
    // afterwards must be the one it would be without that row. The replay fallback then reads it back through the
    // reader, retagged column included.
    let lookup = lookup(&["fresh", "typed", "mixed", "bad"]);
    let num = SharedValue::new_num;
    let good = [
        row(&lookup, &[("typed", num(1.0)), ("mixed", num(1.0))]),
        row(
            &lookup,
            &[("typed", num(2.0)), ("mixed", bytes("x").to_value())],
        ),
    ];
    let refused = row(
        &lookup,
        &[
            ("fresh", num(2.0)),
            ("typed", bytes("x").to_value()),
            ("bad", too_deep()),
        ],
    );

    let mut writer = writer_for(&lookup);
    for row in &good {
        write(&mut writer, &lookup, row).expect("encodable");
    }
    assert_eq!(
        write(&mut writer, &lookup, &refused),
        Err(RefusedRow::TooDeeplyNested)
    );

    assert_eq!(writer.as_bytes(), encode(&lookup, &good));
    let parsed = Block::parse(writer.as_bytes()).expect("the block parses");
    assert_eq!(
        parsed.kinds()[1..3],
        [ColumnKind::Typed(Tag::Number), ColumnKind::Tagged]
    );
    assert_eq!(parsed.rows().filter(Result::is_ok).count(), writer.nrows());
}

#[test]
fn reset_forgets_the_previous_chunks_column_kinds() {
    let lookup = lookup(&["v"]);
    let mut writer = writer_for(&lookup);
    let string = row(&lookup, &[("v", bytes("s").to_value())]);
    write(&mut writer, &lookup, &string).expect("encodable");

    writer.reset();
    writer
        .write_schema(&lookup, ColumnFilter::default())
        .expect("encodable");
    let number = row(&lookup, &[("v", SharedValue::new_num(1.0))]);
    write(&mut writer, &lookup, &number).expect("encodable");
    assert_eq!(writer.as_bytes(), encode(&lookup, &[number]));
}

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
