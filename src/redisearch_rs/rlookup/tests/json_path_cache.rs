/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_json_api::mock::{json_api_calls, reset_json_api_calls, with_json_api};
use redis_module::RedisString;
use rlookup::{
    DocumentFormat, FieldLoader, JsonDocumentFormat, JsonPathCache, LoadAllError, RLookup,
    RLookupKeyFlags, RLookupRow,
};
use serde_json::{Value as JsonValue, json};
use std::ffi::{CStr, CString};
use value::SharedValue;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const MULTI: u8 = ffi::APIVERSION_RETURN_MULTI_CMP_FIRST as u8;
const PRE_MULTI: u8 = MULTI - 1;

fn key_name() -> RedisString {
    // SAFETY: the initialized mock copies these static bytes and ignores the context.
    unsafe { RedisString::from_raw_parts(None, c"doc:1".as_ptr(), 5) }
}

fn load_fields(
    doc: JsonValue,
    paths: &[Option<&CStr>],
    cache: &JsonPathCache,
    version: u8,
) -> Vec<Option<SharedValue>> {
    with_json_api(Some(doc), |api, ctx| {
        let mut lookup = RLookup::new();
        for (index, path) in paths.iter().enumerate() {
            let name = CString::new(format!("field{index}")).unwrap();
            match path {
                Some(path) => {
                    lookup
                        .get_key_load(name, path, RLookupKeyFlags::empty())
                        .unwrap();
                }
                None => {
                    lookup
                        .get_key_write(name, RLookupKeyFlags::empty())
                        .unwrap();
                }
            }
        }
        let format = JsonDocumentFormat::new(ctx, &api, version).with_path_cache(cache);
        let name = key_name();
        let loader = format.open(&name).unwrap();
        let mut row = RLookupRow::new();
        for (index, key) in lookup.iter().enumerate() {
            loader.load_field_at(index, key, &mut row).unwrap();
        }
        lookup.iter().map(|key| row.get(key).cloned()).collect()
    })
}

fn load_root(doc: JsonValue, cache: &JsonPathCache, version: u8) -> SharedValue {
    with_json_api(Some(doc), |api, ctx| {
        let format = JsonDocumentFormat::new(ctx, &api, version).with_path_cache(cache);
        let mut lookup = RLookup::new();
        let mut row = RLookupRow::new();
        format.load_all(&mut lookup, &mut row, &key_name()).unwrap();
        let key = lookup
            .find_key_by_name(c"$")
            .unwrap()
            .into_current()
            .unwrap();
        row.get(key).unwrap().clone()
    })
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn compiles_once_reuses_across_documents_and_frees_on_drop() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    let paths = [Some(c"$.name"), Some(c"$.price")];
    with_json_api(None, |api, _ctx| {
        // SAFETY: the mock implements V10 and the path bytes outlive the cache.
        let cache = unsafe { JsonPathCache::new(&api, paths.into_iter()) };
        for (name, price) in [("a", 10), ("b", 20), ("c", 30)] {
            let row = load_fields(
                json!({"name": name, "price": price}),
                &paths,
                &cache,
                PRE_MULTI,
            );
            assert_eq!(
                row[0].as_ref().unwrap().as_str_bytes(),
                Some(name.as_bytes())
            );
            assert_eq!(row[1].as_ref().unwrap().as_num(), Some(f64::from(price)));
        }
        let calls = json_api_calls();
        assert_eq!(calls.path_parse, 0);
        assert_eq!(
            (
                calls.path_parse_no_error_msg,
                calls.get_with_path,
                calls.get,
                calls.path_free
            ),
            (2, 6, 0, 0)
        );
        drop(cache);
        assert_eq!(json_api_calls().path_free, 2);
    });
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn empty_entries_preserve_positions_and_malformed_paths_are_not_retried() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    let paths = [
        None,
        Some(c"__key"),
        Some(c"$.broken["),
        Some(c"$.price"),
        Some(c"$.missing"),
    ];
    with_json_api(None, |api, _ctx| {
        // SAFETY: the mock implements V10 and the path bytes outlive the cache.
        let cache = unsafe { JsonPathCache::new(&api, paths.into_iter()) };
        let row = load_fields(json!({"price": 42}), &paths, &cache, PRE_MULTI);
        assert!(row[0].is_none());
        assert_eq!(
            row[1].as_ref().unwrap().as_str_bytes(),
            Some(b"doc:1".as_slice())
        );
        assert!(row[2].is_none());
        assert_eq!(row[3].as_ref().unwrap().as_num(), Some(42.0));
        assert!(row[4].is_none());
        let calls = json_api_calls();
        assert_eq!(calls.path_parse, 0);
        assert_eq!(
            (
                calls.path_parse_no_error_msg,
                calls.get_with_path,
                calls.get
            ),
            (3, 2, 0)
        );
        drop(cache);
        assert_eq!(json_api_calls().path_free, 2);
    });
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn root_load_uses_compiled_path_and_preserves_whole_document() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    with_json_api(None, |api, _ctx| {
        // SAFETY: the mock implements V10 and the path bytes outlive the cache.
        let cache = unsafe { JsonPathCache::new(&api, [Some(c"$")].into_iter()) };
        for version in [PRE_MULTI, MULTI] {
            let doc = json!({"name": "alice", "price": 12});
            assert_eq!(
                load_root(doc, &cache, version)
                    .fully_dereferenced_ref_and_trio()
                    .as_str_bytes(),
                Some(br#"{"name":"alice","price":12}"#.as_slice())
            );
        }
        let calls = json_api_calls();
        assert_eq!(
            (
                calls.path_parse_no_error_msg,
                calls.get_with_path,
                calls.get
            ),
            (1, 2, 0)
        );
    });
    assert_eq!(json_api_calls().path_free, 1);
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn cached_root_load_preserves_errors_without_writing_a_value() {
    redis_mock::init_redis_module_mock();
    for (doc, expected) in [
        (None, LoadAllError::OpenKeyFailed),
        (Some(json!([])), LoadAllError::JsonRootMissing),
    ] {
        with_json_api(doc, |api, ctx| {
            // SAFETY: the mock implements V10 and the path is static.
            let cache = unsafe { JsonPathCache::new(&api, [Some(c"$")].into_iter()) };
            for cached in [false, true] {
                let mut format = JsonDocumentFormat::new(ctx, &api, MULTI);
                if cached {
                    format = format.with_path_cache(&cache);
                }
                let mut lookup = RLookup::new();
                let mut row = RLookupRow::new();
                let error = format
                    .load_all(&mut lookup, &mut row, &key_name())
                    .unwrap_err();
                assert_eq!(
                    std::mem::discriminant(&error),
                    std::mem::discriminant(&expected),
                    "cached={cached}: expected {expected:?}, got {error:?}"
                );
                assert!(lookup.find_key_by_name(c"$").is_none());
            }
        });
    }
}
