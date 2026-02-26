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
fn debug_number() {
    assert_eq!(debug(&RsValue::Number(42.0)), "42");
}

#[test]
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
                SharedRsValue::new(RsValue::Number(12.0)),
                SharedRsValue::new(RsValue::Number(34.0)),
                SharedRsValue::new(RsValue::Number(56.0))
            ]
            .into_boxed_slice()
        )),),
        "[12, 34, 56]"
    );
}

#[test]
fn debug_map() {
    assert_eq!(
        debug(&RsValue::Map(Map::new(
            vec![
                (
                    SharedRsValue::new(RsValue::Number(12.0)),
                    SharedRsValue::new(RsValue::Null)
                ),
                (
                    SharedRsValue::new(RsValue::Number(34.0)),
                    SharedRsValue::new(RsValue::Undefined)
                ),
            ]
            .into_boxed_slice()
        )),),
        "{12: NULL, 34: <Undefined>}"
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
            SharedRsValue::new(RsValue::Number(12.0)),
            SharedRsValue::new(RsValue::Number(34.0)),
            SharedRsValue::new(RsValue::Number(56.0))
        )),),
        "12"
    );
}
