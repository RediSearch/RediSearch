/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{Array, Map, SharedValue, String, Trio, Value};

fn debug(value: &Value) -> std::string::String {
    format!("{:?}", value.debug_formatter(false))
}

fn debug_obfuscated(value: &Value) -> std::string::String {
    format!("{:?}", value.debug_formatter(true))
}

#[test]
fn debug_null() {
    assert_eq!(debug(&Value::Null), "NULL");
}

#[test]
fn debug_undefined() {
    assert_eq!(debug(&Value::Undefined), "<Undefined>");
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `snprintf`")]
fn debug_number() {
    assert_eq!(debug(&Value::Number(42.0)), "42");
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `Obfuscate_Number`")]
fn debug_number_obfuscated() {
    assert_eq!(debug_obfuscated(&Value::Number(42.0)), "Number");
}

#[test]
fn debug_string() {
    assert_eq!(
        debug(&Value::String(String::from_vec(b"Hello".to_vec()))),
        "\"Hello\""
    );
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `Obfuscate_Text`")]
fn debug_string_obfuscated() {
    assert_eq!(
        debug_obfuscated(&Value::String(String::from_vec(b"Hello".to_vec()))),
        "\"Text\""
    );
}

#[test]
fn debug_string_invalid_utf8() {
    assert_eq!(
        debug(&Value::String(String::from_vec(vec![255]))),
        "<non-utf8-data>"
    );
}

#[test]
fn debug_array() {
    assert_eq!(
        debug(&Value::Array(Array::new(
            vec![
                SharedValue::new(Value::String(String::from_vec(b"foo".to_vec()))),
                SharedValue::new(Value::String(String::from_vec(b"bar".to_vec()))),
                SharedValue::new(Value::String(String::from_vec(b"baz".to_vec())))
            ]
            .into_boxed_slice()
        )),),
        "[\"foo\", \"bar\", \"baz\"]"
    );
}

#[test]
fn debug_map() {
    assert_eq!(
        debug(&Value::Map(Map::new(
            vec![
                (
                    SharedValue::new(Value::String(String::from_vec(b"foo".to_vec()))),
                    SharedValue::new(Value::Null)
                ),
                (
                    SharedValue::new(Value::String(String::from_vec(b"bar".to_vec()))),
                    SharedValue::new(Value::Undefined)
                ),
            ]
            .into_boxed_slice()
        )),),
        "{\"foo\": NULL, \"bar\": <Undefined>}"
    );
}

#[test]
fn debug_ref() {
    assert_eq!(debug(&Value::Ref(SharedValue::new(Value::Null))), "NULL");
}

#[test]
fn debug_trio() {
    assert_eq!(
        debug(&Value::Trio(Trio::new(
            SharedValue::new(Value::String(String::from_vec(b"foo".to_vec()))),
            SharedValue::new(Value::String(String::from_vec(b"bar".to_vec()))),
            SharedValue::new(Value::String(String::from_vec(b"baz".to_vec())))
        )),),
        "\"foo\""
    );
}
