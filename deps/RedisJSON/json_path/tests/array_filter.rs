/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#[macro_use]
extern crate serde_json;

use common::{read_json, select_and_then_compare, setup};

mod common;

#[test]
fn array_range_default() {
    setup();

    select_and_then_compare(
        "$.school.friends[1, 2]",
        read_json("./json_examples/data_obj.json"),
        json!([
            {"id": 1, "name": "Vincent Cannon" },
            {"id": 2, "name": "Gray Berry"}
        ]),
    );
}

#[test]
fn array_range_all() {
    setup();

    select_and_then_compare(
        "$[ : ]",
        json!(["first", "second"]),
        json!(["first", "second"]),
    );
}

#[test]
fn array_range_step_all() {
    setup();

    select_and_then_compare(
        "$[::]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["first", "second", "third", "forth", "fifth"]),
    );
}

#[test]
fn array_range_step_only_step_value() {
    setup();

    select_and_then_compare(
        "$[::2]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["first", "third", "fifth"]),
    );
}

#[test]
fn array_range_step_only_start_index() {
    setup();

    select_and_then_compare(
        "$[1::]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["second", "third", "forth", "fifth"]),
    );
}

#[test]
fn array_range_step_empty_step_value() {
    setup();

    select_and_then_compare(
        "$[1:2:]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["second"]),
    );
}

#[test]
fn array_range_step_empty_end_index() {
    setup();

    select_and_then_compare(
        "$[1::2]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["second", "forth"]),
    );
}

#[test]
fn array_range_step_by_1() {
    setup();

    select_and_then_compare(
        "$[0:3:1]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["first", "second", "third"]),
    );
}

#[test]
fn array_range_step_by_2() {
    setup();

    select_and_then_compare(
        "$[0:3:2]",
        json!(["first", "second", "third", "forth", "fifth"]),
        json!(["first", "third"]),
    );
}

#[test]
fn array_range_only_negative_index() {
    setup();

    select_and_then_compare(
        "$[-4:]",
        json!(["first", "second", "third"]),
        json!(["first", "second", "third"]),
    );
}

#[test]
fn array_range_only_end_index() {
    setup();

    select_and_then_compare(
        "$[:4]",
        json!(["first", "second", "third"]),
        json!(["first", "second", "third"]),
    );
}

#[test]
fn array_range_only_from_index() {
    setup();

    select_and_then_compare(
        "$.school.friends[1: ]",
        read_json("./json_examples/data_obj.json"),
        json!([
            {"id": 1, "name": "Vincent Cannon" },
            {"id": 2, "name": "Gray Berry"}
        ]),
    );
}

#[test]
fn array_range_only_negative_end_index() {
    setup();

    select_and_then_compare(
        "$.school.friends[:-2]",
        read_json("./json_examples/data_obj.json"),
        json!([
            {"id": 0, "name": "Millicent Norman"}
        ]),
    );
}

#[test]
fn array_index() {
    setup();

    select_and_then_compare(
        "$..friends[2].name",
        read_json("./json_examples/data_obj.json"),
        json!(["Gray Berry", "Gray Berry"]),
    );
}

#[test]
fn array_all_index() {
    setup();

    select_and_then_compare(
        "$..friends[*].name",
        read_json("./json_examples/data_obj.json"),
        json!([
            "Vincent Cannon",
            "Gray Berry",
            "Millicent Norman",
            "Vincent Cannon",
            "Gray Berry"
        ]),
    );
}

#[test]
fn array_all_and_then_key() {
    setup();

    select_and_then_compare(
        "$['school']['friends'][*].['name']",
        read_json("./json_examples/data_obj.json"),
        json!(["Millicent Norman", "Vincent Cannon", "Gray Berry"]),
    );
}

#[test]
fn array_index_and_then_key() {
    setup();

    select_and_then_compare(
        "$['school']['friends'][0].['name']",
        read_json("./json_examples/data_obj.json"),
        json!(["Millicent Norman"]),
    );
}

#[test]
fn array_multiple_key() {
    setup();

    select_and_then_compare(
        r#"$.["eyeColor", "name"]"#,
        read_json("./json_examples/data_obj.json"),
        json!(["blue", "Leonor Herman"]),
    );
}

#[test]
fn bugs40_bracket_notation_after_recursive_descent() {
    setup();

    select_and_then_compare(
        "$..[0]",
        json!([
            "first",
            {
                "key": [
                    "first nested",
                    {
                        "more": [
                            {"nested": ["deepest", "second"]},
                            ["more", "values"]
                        ]
                    }
                ]
            }
        ]),
        json!([
           "first",
           "first nested",
           {
              "nested" : [
                 "deepest",
                 "second"
              ]
           },
           "deepest",
           "more"
        ]),
    );
}
