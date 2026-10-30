/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding of the pre-2.0 sorting-vector RDB format; see
//! [`RSSortingVector::load_legacy_rdb`].

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::io::Cursor;

use rdb_io::RdbIO;
use sorting_vector::{RS_SORTABLES_MAX, RSSortingVector};
use value::Value;

const TAG_NUMBER: u64 = 1;
const TAG_STRING: u64 = 2;
const TAG_NULL: u64 = 3;

/// Written after every encoded vector, so a decoder that reads too little or
/// too much is caught by what it leaves behind.
const SENTINEL: u64 = 0xFEED_FACE;

#[derive(Debug, Clone)]
enum Element {
    Number(f64),
    /// A string saved the way 1.x did: its bytes followed by a NUL terminator.
    String(Vec<u8>),
    /// A string element whose saved buffer is given verbatim.
    RawString(Vec<u8>),
    /// A tag that carries no payload.
    Bare(u64),
}

fn encode(len: u64, elements: &[Element]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);
    let mut io = &mut cursor;
    io.write_u64(len);
    for element in elements {
        match element {
            Element::Number(num) => {
                io.write_u64(TAG_NUMBER);
                io.write_f64(*num);
            }
            Element::String(content) => {
                io.write_u64(TAG_STRING);
                io.write_buffer(&[content.as_slice(), b"\0"].concat());
            }
            Element::RawString(buffer) => {
                io.write_u64(TAG_STRING);
                io.write_buffer(buffer);
            }
            Element::Bare(tag) => io.write_u64(*tag),
        }
    }
    io.write_u64(SENTINEL);
    bytes
}

/// Decodes `bytes`, asserting that exactly the trailing [`SENTINEL`] is left.
fn decode(mut bytes: Vec<u8>) -> RSSortingVector {
    let mut cursor = Cursor::new(&mut bytes);
    let mut io = &mut cursor;
    let vector = RSSortingVector::load_legacy_rdb(&mut io).expect("decoding failed");
    assert_eq!(io.read_u64().ok(), Some(SENTINEL), "stream out of sync");
    vector
}

fn decode_elements(elements: &[Element]) -> RSSortingVector {
    decode(encode(elements.len() as u64, elements))
}

#[test]
fn decodes_numbers_strings_and_nulls() {
    let vector = decode_elements(&[
        Element::Number(-1.5),
        Element::String(b"hello".to_vec()),
        Element::Bare(TAG_NULL),
    ]);

    assert_eq!(vector.len(), 3);
    assert!(matches!(*vector[0], Value::Number(n) if n == -1.5));
    assert_eq!(vector[1].as_str_bytes(), Some(&b"hello"[..]));
    assert!(vector[2].is_null_static());
}

/// Tags other than number and string carry no payload and decode as null.
#[test]
fn other_tags_decode_as_null() {
    let vector = decode_elements(&[
        Element::Bare(0),
        Element::Bare(4),
        Element::Bare(5),
        Element::Number(7.0),
    ]);

    assert!(vector.iter().take(3).all(|value| value.is_null_static()));
    assert!(matches!(*vector[3], Value::Number(n) if n == 7.0));
}

/// The terminator is stripped and the rest is read up to the first NUL; an empty
/// buffer decodes as null.
#[test]
fn strings_are_read_as_c_strings() {
    let vector = decode_elements(&[
        Element::RawString(b"ab\0cd\0".to_vec()),
        Element::RawString(b"\0".to_vec()),
        Element::RawString(Vec::new()),
    ]);

    assert_eq!(vector[0].as_str_bytes(), Some(&b"ab"[..]));
    assert_eq!(vector[1].as_str_bytes(), Some(&b""[..]));
    assert!(vector[2].is_null_static());
}

/// A length outside the supported range yields an empty vector, and the
/// elements that follow it are left unread.
#[test]
fn out_of_range_lengths_decode_as_empty() {
    for len in [0, RS_SORTABLES_MAX as u64 + 1, u64::MAX] {
        let vector = decode(encode(len, &[]));
        assert!(vector.is_empty(), "length {len}");
    }
}

#[test]
fn accepts_the_maximum_length() {
    let elements = vec![Element::Number(1.0); RS_SORTABLES_MAX];
    assert_eq!(decode_elements(&elements).len(), RS_SORTABLES_MAX);
}

/// A read failing anywhere in the vector, in the length, a tag or a payload, is an
/// error.
#[test]
fn truncated_input_is_an_error() {
    let mut full = encode(
        3,
        &[
            Element::Number(1.0),
            Element::String(b"abc".to_vec()),
            Element::Bare(TAG_NULL),
        ],
    );
    full.truncate(full.len() - size_of_val(&SENTINEL));

    for end in 0..full.len() {
        let mut bytes = full[..end].to_vec();
        let mut cursor = Cursor::new(&mut bytes);
        assert!(
            RSSortingVector::load_legacy_rdb(&mut &mut cursor).is_err(),
            "truncated to {end} bytes"
        );
    }
}

#[cfg(not(miri))]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn element() -> impl Strategy<Value = Element> {
        prop_oneof![
            any::<f64>().prop_map(Element::Number),
            proptest::collection::vec(1u8.., 0..32).prop_map(Element::String),
            Just(Element::Bare(TAG_NULL)),
        ]
    }

    proptest! {
        #[test]
        fn round_trips(elements in proptest::collection::vec(element(), 1..64)) {
            let vector = decode_elements(&elements);

            prop_assert_eq!(vector.len(), elements.len());
            for (value, element) in vector.iter().zip(&elements) {
                match element {
                    Element::Number(num) => {
                        prop_assert!(matches!(**value, Value::Number(n) if n.to_bits() == num.to_bits()));
                    }
                    Element::String(content) => {
                        prop_assert_eq!(value.as_str_bytes(), Some(content.as_slice()));
                    }
                    _ => prop_assert!(value.is_null_static()),
                }
            }
        }
    }
}
