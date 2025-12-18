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
fn example_authros_of_all_books() {
    setup();

    select_and_then_compare(
        r#"$.store.book[*].author"#,
        read_json("./json_examples/example.json"),
        json!([
            "Nigel Rees",
            "Evelyn Waugh",
            "Herman Melville",
            "J. R. R. Tolkien"
        ]),
    );
}

#[test]
fn all_authors() {
    setup();

    select_and_then_compare(
        r#"$..author"#,
        read_json("./json_examples/example.json"),
        json!([
            "Nigel Rees",
            "Evelyn Waugh",
            "Herman Melville",
            "J. R. R. Tolkien"
        ]),
    );
}

#[test]
fn all_things_both_books_and_bicycles() {
    setup();

    select_and_then_compare(
        r#"$.store.*"#,
        read_json("./json_examples/example.json"),
        json!([
            [
                {"category" : "reference", "author" : "Nigel Rees","title" : "Sayings of the Century", "price" : 8.95},
                {"category" : "fiction", "author" : "Evelyn Waugh","title" : "Sword of Honour","price" : 12.99},
                {"category" : "fiction", "author" : "Herman Melville","title" : "Moby Dick","isbn" : "0-553-21311-3","price" : 8.99},
                {"category" : "fiction", "author" : "J. R. R. Tolkien","title" : "The Lord of the Rings","isbn" : "0-395-19395-8","price" : 22.99}
            ],
            {"color" : "red","price" : 19.95},
        ]),
    );
}

#[test]
fn the_price_of_everything() {
    setup();

    select_and_then_compare(
        r#"$.store..price"#,
        read_json("./json_examples/example.json"),
        json!([8.95, 12.99, 8.99, 22.99, 19.95]),
    );
}

#[test]
fn the_third_book() {
    setup();

    select_and_then_compare(
        r#"$..book[2]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
            "category" : "fiction",
            "author" : "Herman Melville",
            "title" : "Moby Dick",
            "isbn" : "0-553-21311-3",
            "price" : 8.99
            }
        ]),
    );
}

#[test]
fn the_second_to_last_book() {
    setup();

    select_and_then_compare(
        r#"$..book[-2]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "fiction",
                "author" : "Herman Melville",
                "title" : "Moby Dick",
                "isbn" : "0-553-21311-3",
                "price" : 8.99
            }
        ]),
    );
}

#[test]
fn the_first_two_books() {
    setup();

    select_and_then_compare(
        r#"$..book[0, 1]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "reference",
                "author" : "Nigel Rees",
                "title" : "Sayings of the Century",
                "price" : 8.95
            },
            {
                "category" : "fiction",
                "author" : "Evelyn Waugh",
                "title" : "Sword of Honour",
                "price" : 12.99
            }
        ]),
    );
}

#[test]
fn all_books_from_index_0_inclusive_until_index_2_exclusive() {
    setup();

    select_and_then_compare(
        r#"$..book[:2]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "reference",
                "author" : "Nigel Rees",
                "title" : "Sayings of the Century",
                "price" : 8.95
            },
            {
                "category" : "fiction",
                "author" : "Evelyn Waugh",
                "title" : "Sword of Honour",
                "price" : 12.99
            }
        ]),
    );
}

#[test]
fn all_books_from_index_1_inclusive_until_index_2_exclusive() {
    setup();

    select_and_then_compare(
        r#"$..book[2:]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "fiction",
                "author" : "Herman Melville",
                "title" : "Moby Dick",
                "isbn" : "0-553-21311-3",
                "price" : 8.99
           },
           {
                "category" : "fiction",
                "author" : "J. R. R. Tolkien",
                "title" : "The Lord of the Rings",
                "isbn" : "0-395-19395-8",
                "price" : 22.99
           }
        ]),
    );
}

#[test]
fn all_books_with_an_isbn_number() {
    setup();

    select_and_then_compare(
        r#"$..book[?(@.isbn)]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "fiction",
                "author" : "Herman Melville",
                "title" : "Moby Dick",
                "isbn" : "0-553-21311-3",
                "price" : 8.99
           },
           {
                "category" : "fiction",
                "author" : "J. R. R. Tolkien",
                "title" : "The Lord of the Rings",
                "isbn" : "0-395-19395-8",
                "price" : 22.99
           }
        ]),
    );
}

#[test]
fn all_books_in_store_cheaper_than_10() {
    setup();

    select_and_then_compare(
        r#"$.store.book[?(@.price < 10)]"#,
        read_json("./json_examples/example.json"),
        json!([
            {
                "category" : "reference",
                "author" : "Nigel Rees",
                "title" : "Sayings of the Century",
                "price" : 8.95
           },
           {
                "category" : "fiction",
                "author" : "Herman Melville",
                "title" : "Moby Dick",
                "isbn" : "0-553-21311-3",
                "price" : 8.99
           }
        ]),
    );
}

#[test]
fn give_me_every_thing() {
    setup();

    select_and_then_compare(
        r#"$..*"#,
        read_json("./json_examples/example.json"),
        read_json("./json_examples/giveme_every_thing_result.json"),
    );
}
