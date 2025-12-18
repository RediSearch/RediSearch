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
fn quote() {
    setup();

    select_and_then_compare(
        r#"$['single\'quote']"#,
        json!({"single'quote":"value"}),
        json!(["value"]),
    );
    select_and_then_compare(
        r#"$["double\"quote"]"#,
        json!({"double\"quote":"value"}),
        json!(["value"]),
    );
}

#[test]
fn filter_next_all() {
    setup();

    for path in &[r#"$.*"#, r#"$[*]"#] {
        select_and_then_compare(
            path,
            json!(["string", 42, { "key": "value" }, [0, 1]]),
            json!(["string", 42, { "key": "value" }, [0, 1]]),
        );
    }
}

#[test]
fn filter_all() {
    setup();

    for path in &[r#"$..*"#, r#"$..[*]"#] {
        select_and_then_compare(
            path,
            json!(["string", 42, { "key": "value" }, [0, 1]]),
            json!([ "string", 42, { "key" : "value" }, [ 0, 1 ], "value", 0, 1 ]),
        );
    }
}

#[test]
fn filter_array_next_all() {
    setup();

    for path in &[r#"$.*.*"#, r#"$[*].*"#, r#"$.*[*]"#, r#"$[*][*]"#] {
        select_and_then_compare(
            path,
            json!(["string", 42, { "key": "value" }, [0, 1]]),
            json!(["value", 0, 1]),
        );
    }
}

#[test]
fn filter_all_complex() {
    setup();

    for path in &[r#"$..friends.*"#, r#"$[*].friends.*"#] {
        select_and_then_compare(
            path,
            read_json("./json_examples/data_array.json"),
            json!([
               { "id" : 0, "name" : "Millicent Norman" },
               { "id" : 1, "name" : "Vincent Cannon" },
               { "id" : 2, "name" : "Gray Berry" },
               { "id" : 0, "name" : "Tillman Mckay" },
               { "id" : 1, "name" : "Rivera Berg" },
               { "id" : 2, "name" : "Rosetta Erickson" }
            ]),
        );
    }
}

#[test]
fn filter_parent_with_matched_child() {
    setup();

    select_and_then_compare(
        "$[?(@.b.c == 1)]",
        json!({
            "a": {
                "b": {
                    "c": 1
                }
            }
        }),
        json!([
           {
              "b" : {
                 "c" : 1
              }
           }
        ]),
    );

    select_and_then_compare(
        "$.a[?(@.b.c == 1)]",
        json!({
            "a": {
                "b": {
                    "c": 1
                }
            }
        }),
        json!([]),
    );
}

#[test]
fn filter_parent_exist_child() {
    setup();

    select_and_then_compare(
        "$[?(@.b.c)]",
        json!({
            "a": {
                "b": {
                    "c": 1
                }
            }
        }),
        json!([
           {
              "b" : {
                 "c" : 1
              }
           }
        ]),
    );
}

#[test]
fn filter_parent_paths() {
    setup();

    select_and_then_compare(
        "$[?(@.key.subKey == 'subKey2')]",
        json!([
           {"key": {"seq": 1, "subKey": "subKey1"}},
           {"key": {"seq": 2, "subKey": "subKey2"}},
           {"key": 42},
           {"some": "value"}
        ]),
        json!([{"key": {"seq": 2, "subKey": "subKey2"}}]),
    );
}

#[test]
fn bugs33_exist_in_all() {
    setup();

    select_and_then_compare(
        "$..[?(@.first.second)]",
        json!({
            "foo": {
                "first": { "second": "value" }
            },
            "foo2": {
                "first": {}
            },
            "foo3": {
            }
        }),
        json!([
            {
                "first": {
                    "second": "value"
                }
            }
        ]),
    );
}

#[test]
fn bugs33_exist_left_in_all_with_and_condition() {
    setup();

    select_and_then_compare(
        "$..[?(@.first && @.first.second)]",
        json!({
            "foo": {
                "first": { "second": "value" }
            },
            "foo2": {
                "first": {}
            },
            "foo3": {
            }
        }),
        json!([
            {
                "first": {
                    "second": "value"
                }
            }
        ]),
    );
}

#[test]
fn bugs33_exist_right_in_all_with_and_condition() {
    setup();

    select_and_then_compare(
        "$..[?(@.b.c.d && @.b)]",
        json!({
            "a": {
                "b": {
                    "c": {
                        "d" : {
                            "e" : 1
                        }
                    }
                }
            }
        }),
        json!([
           {
              "b" : {
                "c" : {
                   "d" : {
                      "e" : 1
                   }
                }
              }
           }
        ]),
    );
}

#[test]
fn bugs38_array_notation_in_filter() {
    setup();

    select_and_then_compare(
        "$[?(@['key']==42)]",
        json!([
           {"key": 0},
           {"key": 42},
           {"key": -1},
           {"key": 41},
           {"key": 43},
           {"key": 42.0001},
           {"key": 41.9999},
           {"key": 100},
           {"some": "value"}
        ]),
        json!([{"key": 42}]),
    );

    select_and_then_compare(
        "$[?(@['key'].subKey == 'subKey2')]",
        json!([
           {"key": {"seq": 1, "subKey": "subKey1"}},
           {"key": {"seq": 2, "subKey": "subKey2"}},
           {"key": 42},
           {"some": "value"}
        ]),
        json!([{"key": {"seq": 2, "subKey": "subKey2"}}]),
    );

    select_and_then_compare(
        "$[?(@['key']['subKey'] == 'subKey2')]",
        json!([
           {"key": {"seq": 1, "subKey": "subKey1"}},
           {"key": {"seq": 2, "subKey": "subKey2"}},
           {"key": 42},
           {"some": "value"}
        ]),
        json!([{"key": {"seq": 2, "subKey": "subKey2"}}]),
    );

    select_and_then_compare(
        "$..key[?(@['subKey'] == 'subKey2')]",
        json!([
           {"key": [{"seq": 1, "subKey": "subKey1"}]},
           {"key": [{"seq": 2, "subKey": "subKey2"}]},
           {"key": [42]},
           {"some": "value"}
        ]),
        json!([{"seq": 2, "subKey": "subKey2"}]),
    );
}

#[test]
fn unimplemented_in_filter() {
    setup();

    let json = json!([{
       "store": {
           "book": [
             {"authors": [
                 {"firstName": "Nigel",
                   "lastName": "Rees"},
                 {"firstName": "Evelyn",
                   "lastName": "Waugh"}
               ],
               "title": "SayingsoftheCentury"},
             {"authors": [
                 {"firstName": "Herman",
                   "lastName": "Melville"},
                 {"firstName": "Somebody",
                   "lastName": "Else"}
               ],
               "title": "MobyDick"}
           ]}
    }]);

    // Should not panic
    //  unimplemented!("range syntax in filter")
    select_and_then_compare("$.store.book[?(@.authors[0:1])]", json.clone(), json!([]));

    // Should not panic
    //  unimplemented!("union syntax in filter")
    select_and_then_compare("$.store.book[?(@.authors[0,1])]", json.clone(), json!([]));

    // Should not panic
    //  unimplemented!("keys in filter");
    select_and_then_compare("$.store[?(@.book['authors', 'title'])]", json, json!([]));
}

#[test]
fn filter_nested() {
    setup();

    select_and_then_compare(
        "$.store.book[?(@.authors[?(@.lastName == 'Rees')])].title",
        json!({
            "store": {
                "book": [
                    {
                        "authors": [
                            {
                                "firstName": "Nigel",
                                "lastName": "Rees"
                            },
                            {
                                "firstName": "Evelyn",
                                "lastName": "Waugh"
                            }
                        ],
                        "title": "Sayings of the Century"
                    },
                    {
                        "authors": [
                            {
                                "firstName": "Herman",
                                "lastName": "Melville"
                            },
                            {
                                "firstName": "Somebody",
                                "lastName": "Else"
                            }
                        ],
                        "title": "Moby Dick"
                    }
                ]
            }
        }),
        json!(["Sayings of the Century"]),
    );
}

#[test]
fn filter_inner() {
    setup();

    select_and_then_compare(
        "$[?(@.inner.for.inner=='u8')].id",
        json!(
        {
            "a": {
              "id": "0:4",
              "inner": {
                "for": {"inner": "u8", "kind": "primitive"}
              }
            }
        }),
        json!(["0:4"]),
    );
}

#[test]
fn op_object_or_nonexisting_default() {
    setup();

    select_and_then_compare(
        "$.friends[?(@.id >= 2 || @.id == 4 || @.id == 6)]",
        read_json("./json_examples/data_obj.json"),
        json!([
            { "id" : 2, "name" : "Gray Berry" }
        ]),
    );
}
