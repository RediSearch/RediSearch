/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Trio resolution.
//!
//! The RESP row serializer resolves a trio in two different ways depending on where it sits,
//! and a block has to agree with it in both places or distributed aggregation returns
//! different values than a single shard would. The rule this pins down:
//!
//! - a field the row stores as a trio *directly* gets the three-way choice, once;
//! - a trio anywhere else — inside an array or map, behind a reference, or reached through
//!   another trio — resolves to its middle member, whatever was asked for.

use crate::harness::{Decoded, bytes, decode, encode_with_trio, lookup, row};
use pretty_assertions::assert_eq;
use row_block::TrioMember;
use value::{SharedValue, Value};

/// Numbers standing in for the three members, so a wrong choice names itself.
const LEFT: f64 = 1.0;
const MIDDLE: f64 = 2.0;
const RIGHT: f64 = 3.0;

/// A trio of [`LEFT`], [`MIDDLE`] and [`RIGHT`].
fn trio() -> SharedValue {
    SharedValue::new_trio(
        SharedValue::new_num(LEFT),
        SharedValue::new_num(MIDDLE),
        SharedValue::new_num(RIGHT),
    )
}

/// Encodes `value` as the only field of the only row, and decodes it back.
fn field(value: SharedValue, trio: TrioMember) -> Decoded {
    let lookup = lookup(&["v"]);
    let block = encode_with_trio(&lookup, &[row(&lookup, &[("v", value)])], trio);
    decode(&block)
        .pop()
        .expect("one row")
        .pop()
        .expect("one field")
        .1
}

#[test]
fn a_top_level_trio_takes_the_member_the_caller_chose() {
    for (choice, want) in [
        (TrioMember::Left, LEFT),
        (TrioMember::Middle, MIDDLE),
        (TrioMember::Right, RIGHT),
    ] {
        assert_eq!(field(trio(), choice), Decoded::Number(want), "{choice:?}");
    }
}

#[test]
fn a_trio_inside_an_array_always_takes_its_middle_member() {
    // This is the case `FORMAT EXPAND` gets wrong if the three-way choice is applied at every
    // depth: `REDUCE TOLIST` over a JSON multi-value field puts trios inside an array, and
    // shipping the right member there would disagree with what RESP sends for the same row.
    for choice in [TrioMember::Left, TrioMember::Middle, TrioMember::Right] {
        let array = SharedValue::new_array(vec![trio()]);
        assert_eq!(
            field(array, choice),
            Decoded::Array(vec![Decoded::Number(MIDDLE)]),
            "{choice:?}"
        );
    }
}

#[test]
fn a_trio_inside_a_map_always_takes_its_middle_member() {
    for choice in [TrioMember::Left, TrioMember::Right] {
        let map = SharedValue::new_map(vec![(trio(), trio())]);
        assert_eq!(
            field(map, choice),
            Decoded::Map(vec![(Decoded::Number(MIDDLE), Decoded::Number(MIDDLE))]),
            "{choice:?}"
        );
    }
}

#[test]
fn a_trio_reached_through_a_top_level_trio_takes_its_middle_member() {
    // The three-way choice happens exactly once. The chosen member is then serialized like
    // any other nested value, so a trio found there takes its middle.
    let nested = SharedValue::new_trio(
        SharedValue::new_array(vec![]),
        SharedValue::new_array(vec![]),
        trio(),
    );
    assert_eq!(
        field(nested, TrioMember::Right),
        Decoded::Number(MIDDLE),
        "the right member's own middle, not its right"
    );
}

#[test]
fn a_trio_behind_a_top_level_reference_takes_its_middle_member() {
    // The RESP path's trio test does not follow references, so a row holding a reference to a
    // trio never reaches the three-way choice at all — the generic serializer dereferences it
    // and takes the middle.
    let behind_reference = SharedValue::new(Value::Ref(trio()));
    assert_eq!(
        field(behind_reference, TrioMember::Right),
        Decoded::Number(MIDDLE)
    );
}

#[test]
fn a_top_level_trio_member_is_serialized_in_full() {
    // The chosen member is not restricted to a scalar: `FORMAT EXPAND`'s right member is
    // typically the whole multi-value array.
    let expanded = SharedValue::new_trio(
        SharedValue::new_num(LEFT),
        SharedValue::new_num(MIDDLE),
        SharedValue::new_array(vec![
            SharedValue::new_string(b"a".to_vec()),
            SharedValue::new_string(b"b".to_vec()),
        ]),
    );
    assert_eq!(
        field(expanded, TrioMember::Right),
        Decoded::Array(vec![bytes("a"), bytes("b")])
    );
}
