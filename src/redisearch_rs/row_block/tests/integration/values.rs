/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Round trips for every value tag, and for the values the encoder maps onto one.

use crate::harness::{Decoded, bytes, decode, encode, lookup, row};
use pretty_assertions::assert_eq;
use row_block::{MAX_NESTING_DEPTH, RefusedRow, RowBlockWriter, TrioMember};
use value::{SharedValue, Value};

/// Round trips one value stored in a single-column row.
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
fn numbers_survive_by_bit_pattern() {
    // An integral value must not come back as a string, a fractional one must not be
    // rounded, and the payload is a raw `f64` so the oddities have to survive too.
    for number in [
        0.0,
        -0.0,
        1.0,
        -1.0,
        42.0,
        0.5,
        -1.0 / 3.0,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::EPSILON,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        9_007_199_254_740_993.0,
    ] {
        assert_eq!(
            round_trip(SharedValue::new_num(number)),
            Decoded::Number(number),
            "{number} did not survive the round trip"
        );
    }
}

#[test]
fn strings_are_carried_as_opaque_bytes() {
    // The format gives a string an explicit length and no encoding, so an embedded NUL and
    // invalid UTF-8 must come back untouched — a decoder that used `strlen` or validated
    // UTF-8 would truncate or reject these.
    for input in [
        b"".to_vec(),
        b"plain".to_vec(),
        b"with\0embedded\0nuls".to_vec(),
        vec![0xff, 0xfe, 0x80, 0x00, 0x41],
        vec![b'x'; 100_000],
    ] {
        assert_eq!(
            round_trip(SharedValue::new_string(input.clone())),
            Decoded::Bytes(input.clone()),
            "a {} byte string did not survive the round trip",
            input.len()
        );
    }
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
fn nested_arrays_and_maps_round_trip() {
    let nested = Decoded::Map(vec![
        (
            bytes("nums"),
            Decoded::Array(vec![Decoded::Number(1.0), Decoded::Null]),
        ),
        (
            bytes("inner"),
            Decoded::Array(vec![Decoded::Map(vec![(bytes("k"), bytes("v"))])]),
        ),
        // A non-string map key is representable: keys are tagged values like any other.
        (Decoded::Number(3.0), Decoded::Array(vec![])),
    ]);
    assert_eq!(round_trip(nested.to_value()), nested);
}

#[test]
fn empty_collections_round_trip() {
    assert_eq!(
        round_trip(Decoded::Array(vec![]).to_value()),
        Decoded::Array(vec![])
    );
    assert_eq!(
        round_trip(Decoded::Map(vec![]).to_value()),
        Decoded::Map(vec![])
    );
}

#[test]
fn nesting_up_to_the_limit_round_trips_and_beyond_it_is_refused() {
    /// An array nesting `depth` levels below the row field.
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

    // Schema order, not the order the row was populated in: the presence bitmap indexes
    // columns positionally, so a reordering would silently mislabel every value.
    assert_eq!(
        decode(&block),
        vec![vec![
            ("b".to_owned(), Decoded::Number(2.0)),
            ("a".to_owned(), Decoded::Number(1.0)),
            ("c".to_owned(), Decoded::Number(3.0)),
        ]]
    );
}
