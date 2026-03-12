/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{Array, Map, RsString, RsValue, RsValueTrio, SharedRsValue};

fn debug(value: &RsValue) -> String {
    format!("{:?}", value.debug_formatter(false))
}

fn debug_obfuscated(value: &RsValue) -> String {
    format!("{:?}", value.debug_formatter(true))
}

#[test]
fn debug_null() {
    assert_eq!(debug(&RsValue::Null), "NULL");
}

#[test]
fn debug_undefined() {
    assert_eq!(debug(&RsValue::Undefined), "<Undefined>");
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn debug_number() {
    assert_eq!(debug(&RsValue::Number(42.0)), "42");
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn debug_number_obfuscated() {
    assert_eq!(debug_obfuscated(&RsValue::Number(42.0)), "Number");
}

#[test]
fn debug_string() {
    assert_eq!(
        debug(&RsValue::String(RsString::from_vec(b"Hello".to_vec()))),
        "\"Hello\""
    );
}

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn debug_string_obfuscated() {
    assert_eq!(
        debug_obfuscated(&RsValue::String(RsString::from_vec(b"Hello".to_vec()))),
        "\"Text\""
    );
}

#[test]
fn debug_string_invalid_utf8() {
    assert_eq!(
        debug(&RsValue::String(RsString::from_vec(vec![255]))),
        "<non-utf8-data>"
    );
}

#[test]
fn debug_array() {
    assert_eq!(
        debug(&RsValue::Array(Array::new(
            vec![
                SharedRsValue::new(RsValue::String(RsString::from_vec(b"foo".to_vec()))),
                SharedRsValue::new(RsValue::String(RsString::from_vec(b"bar".to_vec()))),
                SharedRsValue::new(RsValue::String(RsString::from_vec(b"baz".to_vec())))
            ]
            .into_boxed_slice()
        )),),
        "[\"foo\", \"bar\", \"baz\"]"
    );
}

#[test]
fn debug_map() {
    assert_eq!(
        debug(&RsValue::Map(Map::new(
            vec![
                (
                    SharedRsValue::new(RsValue::String(RsString::from_vec(b"foo".to_vec()))),
                    SharedRsValue::new(RsValue::Null)
                ),
                (
                    SharedRsValue::new(RsValue::String(RsString::from_vec(b"bar".to_vec()))),
                    SharedRsValue::new(RsValue::Undefined)
                ),
            ]
            .into_boxed_slice()
        )),),
        "{\"foo\": NULL, \"bar\": <Undefined>}"
    );
}

#[test]
fn debug_ref() {
    assert_eq!(
        debug(&RsValue::Ref(SharedRsValue::new(RsValue::Null))),
        "NULL"
    );
}

#[test]
fn debug_trio() {
    assert_eq!(
        debug(&RsValue::Trio(RsValueTrio::new(
            SharedRsValue::new(RsValue::String(RsString::from_vec(b"foo".to_vec()))),
            SharedRsValue::new(RsValue::String(RsString::from_vec(b"bar".to_vec()))),
            SharedRsValue::new(RsValue::String(RsString::from_vec(b"baz".to_vec())))
        )),),
        "\"foo\""
    );
}
