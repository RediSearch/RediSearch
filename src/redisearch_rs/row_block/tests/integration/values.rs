/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Values the encoder maps onto another tag or resolves before writing, and the limits on what it writes. Plain round
//! trips of every tag are the property tests' job.

use crate::harness::{Decoded, decode, encode, lookup, row};
use pretty_assertions::assert_eq;
use row_block::{MAX_NESTING_DEPTH, RefusedRow, RowBlockWriter, TrioMember};
use value::{SharedValue, Value};

fn round_trip(value: SharedValue) -> Decoded {
    let lookup = lookup(&["v"]);
    let block = encode(&lookup, &[row(&lookup, &[("v", value)])]);
    let mut rows = decode(&block);
    assert_eq!(rows.len(), 1);
    let mut fields = rows.pop().expect("one row");
    assert_eq!(fields.len(), 1);
    fields.pop().expect("one field").1
}

#[test]
fn null_and_undefined_both_encode_as_null() {
    // `Undefined` is what the RESP path replies as null too, so flattening it loses nothing.
    assert_eq!(round_trip(SharedValue::null_static()), Decoded::Null);
    assert_eq!(
        round_trip(SharedValue::new(Value::Undefined)),
        Decoded::Null
    );
}

#[test]
fn references_resolve_to_their_target() {
    let inner = SharedValue::new_num(7.0);
    let reference = SharedValue::new(Value::Ref(SharedValue::new(Value::Ref(inner))));
    assert_eq!(round_trip(reference), Decoded::Number(7.0));
}

#[test]
fn nesting_up_to_the_limit_round_trips_and_beyond_it_is_refused() {
    fn nest(depth: u32) -> Decoded {
        (0..depth).fold(Decoded::Number(1.0), |inner, _| Decoded::Array(vec![inner]))
    }

    let deepest = nest(MAX_NESTING_DEPTH);
    assert_eq!(round_trip(deepest.to_value()), deepest);

    let lookup = lookup(&["v"]);
    let too_deep = row(&lookup, &[("v", nest(MAX_NESTING_DEPTH + 1).to_value())]);
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(&lookup, Default::default())
        .expect("one column");
    assert_eq!(
        writer.write_row(&lookup, &too_deep, TrioMember::Middle),
        Err(RefusedRow::TooDeeplyNested)
    );
}

#[test]
fn a_row_carries_every_column_in_schema_order() {
    let lookup = lookup(&["b", "a", "c"]);
    let block = encode(
        &lookup,
        &[row(
            &lookup,
            &[
                ("c", SharedValue::new_num(3.0)),
                ("a", SharedValue::new_num(1.0)),
                ("b", SharedValue::new_num(2.0)),
            ],
        )],
    );

    // Schema order, not the order the row was populated in: the presence bitmap indexes columns positionally, so a
    // reordering would silently mislabel every value.
    assert_eq!(
        decode(&block),
        vec![vec![
            ("b".to_owned(), Decoded::Number(2.0)),
            ("a".to_owned(), Decoded::Number(1.0)),
            ("c".to_owned(), Decoded::Number(3.0)),
        ]]
    );
}
