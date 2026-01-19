/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

//! Property tests for [`rlookup::JsonDocumentFormat`] — the JSON document-loading path.
//!
//! These tests encode the **intended** ("should") behavior described in
//! `src/load_document/BASELINE.md`, focusing on what only a Rust unit test can
//! see: the internal `Value` representation (Trio / Map / Array, the scalar
//! type table) and version-specific branches. Observable end-to-end behavior
//! (a present field loads, a JSONPath miss keeps the document, the root loads
//! when no `RETURN` is given) is covered by `tests/pytests/test_json_partial_load.py`.
//!
//! Each test drives the real `JsonDocumentFormat` against an in-memory RedisJSON API
//! ([`redis_json_api::mock`]) whose vtable resolves JSONPath queries against a
//! [`serde_json::Value`].

use proptest::prelude::{Just, Strategy, any, prop_oneof};
use proptest::proptest;
use redis_json_api::mock::with_json_api;
use redis_module::RedisString;
use rlookup::{
    DocumentFormat, FieldLoader, JsonDocumentFormat, LoadAllError, RLookup, RLookupKeyFlags,
    RLookupRow,
};
use serde_json::json;
use std::ffi::{CStr, CString};
use value::{SharedValue, Value};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// The first API version that returns the multi-value `Trio`. Below it, the
/// loader takes only the first match as a plain value.
const MULTI: u32 = ffi::APIVERSION_RETURN_MULTI_CMP_FIRST;
const PRE_MULTI: u32 = MULTI - 1;

/// Construct a `RedisString` from a `CStr` the caller keeps alive. The mock
/// `RedisModule_CreateString` stores the pointer rather than copying, so the
/// bytes must outlive the returned `RedisString`.
fn make_redis_string(bytes: &CStr) -> RedisString {
    unsafe { RedisString::from_raw_parts(None, bytes.as_ptr(), bytes.to_bytes().len()) }
}

/// Per-field load `path` from `doc` (opened under key name `doc:1`), returning
/// the value written to the row, if any.
fn load_field_value(
    doc: serde_json::Value,
    api_version: u32,
    path: &'static CStr,
) -> Option<SharedValue> {
    redis_mock::init_redis_module_mock();
    with_json_api(Some(doc), |japi, ctx| {
        let format = JsonDocumentFormat::new(ctx, &japi, api_version);
        let key_name = make_redis_string(c"doc:1");

        let mut rlookup = RLookup::new();
        let key = rlookup
            .get_key_load(path, path, RLookupKeyFlags::empty())
            .unwrap();

        let mut row = RLookupRow::new();
        format
            .open(&key_name)
            .unwrap()
            .load_field(key, &mut row)
            .unwrap();
        row.get(key).cloned()
    })
}

/// Run `load_all` against `doc` (`None` means missing key) and return the value
/// written to the `$` key, or the propagated [`LoadAllError`].
fn load_all_dollar(
    doc: Option<serde_json::Value>,
    api_version: u32,
) -> Result<Option<SharedValue>, LoadAllError> {
    redis_mock::init_redis_module_mock();
    with_json_api(doc, |japi, ctx| {
        let format = JsonDocumentFormat::new(ctx, &japi, api_version);
        let key_name = make_redis_string(c"doc:1");

        let mut rlookup = RLookup::new();
        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name)?;

        let cursor = rlookup
            .find_key_by_name(c"$")
            .expect("load_all must create the $ key");
        let key = cursor.current().unwrap();
        Ok(row.get(key).cloned())
    })
}

/// Asserts the `actual` shared RSValue value matches the `expected` JSON object
fn assert_value_matches(actual: &SharedValue, expected: &serde_json::Value) {
    match expected {
        serde_json::Value::Null => {
            assert!(
                matches!(&**actual, Value::Null),
                "expected Null, got {actual:?}"
            );
        }
        serde_json::Value::Bool(b) => assert_eq!(actual.as_num(), Some(if *b { 1.0 } else { 0.0 })),
        serde_json::Value::Number(n) => assert_eq!(actual.as_num(), n.as_f64()),
        serde_json::Value::String(s) => assert_eq!(actual.as_str_bytes(), Some(s.as_bytes())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let expected = serde_json::to_string(expected).unwrap();
            assert_eq!(actual.as_str_bytes(), Some(expected.as_bytes()));
        }
    }
}

fn assert_expanded_matches(actual: &SharedValue, expected: &serde_json::Value) {
    match expected {
        serde_json::Value::Object(map) => {
            let Value::Map(m) = &**actual else {
                panic!("expected Map, got {actual:?}")
            };
            assert_eq!(m.len(), map.len());
            for (k, v) in map {
                let got = m
                    .get(k.as_bytes())
                    .unwrap_or_else(|| panic!("expanded Map missing key {k:?}"));
                assert_expanded_matches(got, v);
            }
        }
        serde_json::Value::Array(arr) => {
            let Value::Array(a) = &**actual else {
                panic!("expected Array, got {actual:?}")
            };
            assert_eq!(a.len(), arr.len());
            for (got, v) in a.iter().zip(arr) {
                assert_expanded_matches(got, v);
            }
        }
        _ => assert_value_matches(actual, expected),
    }
}

/// JSON scalars. Integers stay within `i64` (RedisJSON's `getInt`); floats finite.
fn arb_json_scalar() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|i| json!(i)),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(|f| json!(f)),
        "[ -~]{0,16}".prop_map(serde_json::Value::String),
    ]
}

/// Recursive JSON values. Object keys come from a `HashMap`, so uniqueness is
/// baked into the strategy rather than filtered at runtime.
fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
    arb_json_scalar().prop_recursive(3, 32, 5, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..5).prop_map(serde_json::Value::Array),
            proptest::collection::hash_map("[a-z]{1,6}", inner, 0..5)
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
        ]
    })
}

/// Assert that `load_all` creates the `$` rlookup key when absent, and reuses it when
/// present but writes the whole-document value either way.
#[test]
#[cfg_attr(miri, ignore)]
fn load_all_writes_root_value() {
    // Create: no `$` key yet → the harness asserts one is created on the fly.
    let created = load_all_dollar(Some(json!("hello")), PRE_MULTI).unwrap();
    assert_eq!(created.unwrap().as_str_bytes(), Some(&b"hello"[..]));

    // Reuse: a pre-registered `$` key is written in place, not duplicated.
    redis_mock::init_redis_module_mock();
    with_json_api(Some(json!("world")), |japi, ctx| {
        let format = JsonDocumentFormat::new(ctx, &japi, PRE_MULTI);
        let key_name = make_redis_string(c"doc:1");

        let mut rlookup = RLookup::new();
        rlookup
            .get_key_load(c"$", c"$", RLookupKeyFlags::empty())
            .unwrap();

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        let cursor = rlookup.find_key_by_name(c"$").unwrap();
        let key = cursor.current().unwrap();
        assert_eq!(row.get(key).unwrap().as_str_bytes(), Some(&b"world"[..]));
    });
}

/// Assert that `load_all` maps a missing key to `KeyNotFound`, and a present-but-empty
/// root (`$` resolving to an empty array) to `JsonRootMissing`.
#[test]
#[cfg_attr(miri, ignore)]
fn load_all_reports_missing_root_values() {
    assert!(matches!(
        load_all_dollar(None, MULTI),
        Err(LoadAllError::OpenKeyFailed),
    ));
    assert!(matches!(
        load_all_dollar(Some(json!([])), MULTI),
        Err(LoadAllError::JsonRootMissing),
    ));
}

proptest! {
    /// Assert that the sentinel path `__key` resolves to the document key name, written as
    /// a string value.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn underscore_key_path_loads_key_name_as_string(key_name_bytes in any::<CString>()) {
        redis_mock::init_redis_module_mock();
        with_json_api(Some(json!({})), |japi, ctx| {
            let format = JsonDocumentFormat::new(ctx, &japi, PRE_MULTI);
            let key_name = make_redis_string(&key_name_bytes);

            let mut rlookup = RLookup::new();
            let key = rlookup
                .get_key_load(c"__key", c"__key", RLookupKeyFlags::empty())
                .unwrap();

            let mut row = RLookupRow::new();
            format.open(&key_name).unwrap().load_field(key, &mut row).unwrap();

            assert_eq!(
                row.get(key).expect("__key must load the key name").as_str_bytes(),
                Some(key_name_bytes.as_bytes()),
            );
        });
    }

    /// Assert that with `apiVersion < MULTI`, only the first matched value is taken, as a
    /// plain single value.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn pre_multi_apiversion_takes_first_value_only(values in proptest::collection::vec(arb_json_scalar(), 1..6)) {
        let val = load_field_value(json!(values.clone()), PRE_MULTI, c"$[*]")
            .expect("first value should be loaded");
        assert_value_matches(&val, &values[0]);
    }

    /// Assert that with `apiVersion >= MULTI` and a single non-array match, the value is a
    /// `Trio(first_value, full_serialized_string, expanded_array)`.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn multi_apiversion_yields_trio(scalar in arb_json_scalar()) {
        let val = load_field_value(json!({ "name": scalar.clone() }), MULTI, c"$.name")
            .expect("value should be loaded");
        let Value::Trio(trio) = &*val else {
            panic!("multi apiVersion must yield a Trio, got {val:?}");
        };
        // .0 — first match, mapped flat.
        assert_value_matches(trio.left(), &scalar);
        // .1 — full serialized form of the match. A JSONPath query is always
        // array-wrapped by RedisJSON's `getJSONFromIter`, even for a single match.
        let serialized = serde_json::to_string(&json!([scalar])).unwrap();
        assert_eq!(trio.middle().as_str_bytes(), Some(serialized.as_bytes()));
        // .2 — expanded array over the matches (a single scalar here).
        let Value::Array(expanded) = &**trio.right() else { panic!("Trio.2 must be an Array") };
        assert_eq!(expanded.len(), 1);
        assert_value_matches(&expanded[0], &scalar);
    }

    /// Assert that when the match is an array, element `[0]` supplies the Trio's
    /// first-value. An empty array is treated as absent (per-field skip).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn multi_apiversion_array_first_uses_element_zero_else_absent(elems in proptest::collection::vec(arb_json_scalar(), 0..6)) {
        let val = load_field_value(json!({ "arr": elems.clone() }), MULTI, c"$.arr");
        match elems.first() {
            None => assert!(val.is_none(), "an empty array match must be treated as absent"),
            Some(first) => {
                let val = val.expect("non-empty array should load");
                let Value::Trio(trio) = &*val else { panic!("expected Trio") };
                assert_value_matches(trio.left(), first);
            }
        }
    }

    /// Assert leaf mapping: String→string, Int/Double→number, Bool→number(0/1),
    /// Null→null, Object/Array→serialized string.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn scalar_value_mapping(value in arb_json_value()) {
        let val = load_field_value(json!({ "x": value.clone() }), PRE_MULTI, c"$.x")
            .expect("value should be loaded");
        assert_value_matches(&val, &value);
    }

    /// Assert expanded mapping: Object→Map, Array→Array. Must preserve
    /// empty containers (empty Map / empty Array, not absent).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn expanded_mapping_recurses_and_preserves_empty_containers(map in proptest::collection::hash_map("[a-z]{1,6}", arb_json_value(), 1..5)) {
        let value = serde_json::Value::Object(map.into_iter().collect());
        let val = load_field_value(json!({ "x": value.clone() }), MULTI, c"$.x")
            .expect("value should be loaded");
        let Value::Trio(trio) = &*val else { panic!("expected Trio") };
        let Value::Array(expanded) = &**trio.right() else { panic!("Trio.2 must be an Array") };
        assert_eq!(expanded.len(), 1);
        assert_expanded_matches(&expanded[0], &value);
    }
}
