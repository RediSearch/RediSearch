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
    DocumentFormat, FieldLoader, JsonDocumentFormat, JsonPathCache, RLookup, RLookupKeyFlags,
    RLookupRow,
};
use serde_json::{Value as JsonValue, json};
use std::{
    ffi::{CStr, CString, c_void},
    ptr,
};
use value::{SharedValue, Value};

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
    cache: Option<&JsonPathCache>,
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
        let mut format = JsonDocumentFormat::new(ctx, &api, version);
        if let Some(cache) = cache {
            format = format.with_path_cache(cache);
        }
        let name = key_name();
        let loader = format.open(&name).unwrap();
        let mut row = RLookupRow::new();
        for (index, key) in lookup.iter().enumerate() {
            loader.load_field_at(index, key, &mut row).unwrap();
        }
        lookup.iter().map(|key| row.get(key).cloned()).collect()
    })
}

fn load_root(doc: JsonValue, cache: Option<&JsonPathCache>, version: u8) -> SharedValue {
    with_json_api(Some(doc), |api, ctx| {
        let mut format = JsonDocumentFormat::new(ctx, &api, version);
        if let Some(cache) = cache {
            format = format.with_path_cache(cache);
        }
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

// Preserve all three Trio components: comparing only the scalar would miss
// differences in serialized multi-value replies and their expanded representation.
fn snapshot(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Number(n) => json!(n),
        Value::String(_) | Value::RedisString(_) => {
            json!(std::str::from_utf8(value.as_str_bytes().unwrap()).unwrap())
        }
        Value::Array(a) => JsonValue::Array(a.iter().map(|v| snapshot(v)).collect()),
        Value::Map(m) => JsonValue::Array(
            m.iter()
                .map(|(k, v)| json!([snapshot(k), snapshot(v)]))
                .collect(),
        ),
        Value::Ref(v) => snapshot(v),
        Value::Trio(t) => {
            json!({"trio": [snapshot(t.left()), snapshot(t.middle()), snapshot(t.right())]})
        }
        Value::Undefined => panic!("unexpected undefined loaded value"),
    }
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn compiles_once_reuses_across_documents_and_frees_on_drop() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    let paths = [Some(c"$.name"), Some(c"$.price")];
    with_json_api(None, |api, ctx| {
        // SAFETY: the mock context is live and its vtable implements V9.
        let cache = unsafe { JsonPathCache::new(ctx.as_ptr(), &api, paths.into_iter()) };
        for (name, price) in [("a", 10), ("b", 20), ("c", 30)] {
            let row = load_fields(
                json!({"name": name, "price": price}),
                &paths,
                Some(&cache),
                PRE_MULTI,
            );
            assert_eq!(
                row[0].as_ref().unwrap().as_str_bytes(),
                Some(name.as_bytes())
            );
            assert_eq!(row[1].as_ref().unwrap().as_num(), Some(f64::from(price)));
        }
        let calls = json_api_calls();
        assert_eq!(
            (
                calls.path_parse,
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
fn empty_entries_preserve_positions_and_malformed_paths_fall_back() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    let paths = [
        None,
        Some(c"__key"),
        Some(c"$.broken["),
        Some(c"$.price"),
        Some(c"$.missing"),
    ];
    with_json_api(None, |api, ctx| {
        // SAFETY: the mock context is live and its vtable implements V9.
        let cache = unsafe { JsonPathCache::new(ctx.as_ptr(), &api, paths.into_iter()) };
        let row = load_fields(json!({"price": 42}), &paths, Some(&cache), PRE_MULTI);
        assert!(row[0].is_none());
        assert_eq!(
            row[1].as_ref().unwrap().as_str_bytes(),
            Some(b"doc:1".as_slice())
        );
        assert!(row[2].is_none());
        assert_eq!(row[3].as_ref().unwrap().as_num(), Some(42.0));
        assert!(row[4].is_none());
        let calls = json_api_calls();
        assert_eq!(
            (calls.path_parse, calls.get_with_path, calls.get),
            (3, 2, 1)
        );
        drop(cache);
        assert_eq!(json_api_calls().path_free, 2);
    });
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn cached_and_uncached_fields_preserve_scalar_and_multi_value_results() {
    redis_mock::init_redis_module_mock();
    let cases = [
        (
            json!({"name": "alice", "price": 12, "tags": ["red", "blue"]}),
            vec![
                Some(c"$.name"),
                Some(c"$.price"),
                Some(c"$.tags"),
                Some(c"$.missing"),
            ],
        ),
        (json!(["red", "blue"]), vec![Some(c"$[*]")]),
        (json!({"tags": []}), vec![Some(c"$.tags")]),
    ];
    for (doc, paths) in cases {
        with_json_api(None, |api, ctx| {
            // SAFETY: the mock context is live and its vtable implements V9.
            let cache = unsafe { JsonPathCache::new(ctx.as_ptr(), &api, paths.iter().copied()) };
            for version in [PRE_MULTI, MULTI] {
                let cached = load_fields(doc.clone(), &paths, Some(&cache), version);
                let uncached = load_fields(doc.clone(), &paths, None, version);
                let values = |row: Vec<Option<SharedValue>>| {
                    row.iter()
                        .map(|v| v.as_ref().map(|v| snapshot(v)))
                        .collect::<Vec<_>>()
                };
                assert_eq!(values(cached), values(uncached));
            }
        });
    }
}

#[test]
#[cfg_attr(miri, ignore)] // RedisModule_CreateString crosses the C FFI boundary.
fn root_load_uses_compiled_path_and_preserves_whole_document() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    with_json_api(None, |api, ctx| {
        // SAFETY: the mock context is live and its vtable implements V9.
        let cache = unsafe { JsonPathCache::new(ctx.as_ptr(), &api, [Some(c"$")].into_iter()) };
        for version in [PRE_MULTI, MULTI] {
            let doc = json!({"name": "alice", "price": 12});
            assert_eq!(
                snapshot(&load_root(doc.clone(), Some(&cache), version)),
                snapshot(&load_root(doc, None, version))
            );
        }
        let calls = json_api_calls();
        assert_eq!(
            (calls.path_parse, calls.get_with_path, calls.get),
            (1, 2, 2)
        );
    });
    assert_eq!(json_api_calls().path_free, 1);
}

unsafe extern "C" {
    fn JsonPathCache_New(
        ctx: *mut redis_module::RedisModuleCtx,
        keys: *const *const ffi::RLookupKey,
        nkeys: usize,
    ) -> *mut c_void;
    fn JsonPathCache_Free(cache: *mut c_void);
}

struct ApiGlobals {
    api: *mut ffi::RedisJSONAPI,
    version: i32,
}

impl ApiGlobals {
    fn save() -> Self {
        // SAFETY: only the compatibility test in this binary changes these globals.
        let api = unsafe { ffi::japi };
        // SAFETY: only the compatibility test in this binary changes these globals.
        let version = unsafe { ffi::japi_ver };
        Self { api, version }
    }
}

impl Drop for ApiGlobals {
    fn drop(&mut self) {
        // SAFETY: restore the globals before the mock vtable leaves scope, also on panic.
        unsafe { ffi::japi = self.api };
        // SAFETY: restore the negotiated version alongside the saved API pointer.
        unsafe { ffi::japi_ver = self.version };
    }
}

#[test]
#[cfg_attr(miri, ignore)] // The exported cache constructor calls foreign C APIs.
fn ffi_constructor_checks_version_and_function_before_compiling() {
    redis_mock::init_redis_module_mock();
    reset_json_api_calls();
    with_json_api(None, |api, ctx| {
        // SAFETY: the mock vtable is initialized and copying its function pointers is safe.
        let mut table = unsafe { api.vtable().as_ptr().read() };
        let _restore = ApiGlobals::save();
        // SAFETY: only this test uses the globals; the table stays live through restoration.
        unsafe {
            ffi::japi = &raw mut table;
        }
        for version in [7, 8] {
            // SAFETY: only this test changes the negotiated version.
            unsafe {
                ffi::japi_ver = version;
            }
            // SAFETY: live mock context; zero keys requests root loading.
            let cache = unsafe { JsonPathCache_New(ctx.as_ptr(), ptr::null(), 0) };
            assert!(cache.is_null());
            assert_eq!(
                load_fields(
                    json!({"name": "alice"}),
                    &[Some(c"$.name")],
                    None,
                    PRE_MULTI
                )[0]
                .as_ref()
                .unwrap()
                .as_str_bytes(),
                Some(b"alice".as_slice())
            );
        }
        // SAFETY: the stack vtable is exclusively owned by this test.
        unsafe {
            ffi::japi_ver = 9;
        }
        let get_with_path = table.getWithPath.take();
        // SAFETY: live mock context; zero keys requests root loading.
        assert!(unsafe { JsonPathCache_New(ctx.as_ptr(), ptr::null(), 0) }.is_null());
        assert_eq!(json_api_calls().path_parse, 0);
        table.getWithPath = get_with_path;
        // SAFETY: publish the restored table before calling the constructor again.
        unsafe { ffi::japi = &raw mut table };
        // SAFETY: live V9 mock, now with a compiled-path callback.
        let cache = unsafe { JsonPathCache_New(ctx.as_ptr(), ptr::null(), 0) };
        assert!(!cache.is_null());
        // SAFETY: the returned cache is live and only borrowed for this root load.
        let value = load_root(
            json!({"name": "alice"}),
            Some(unsafe { &*cache.cast::<JsonPathCache>() }),
            PRE_MULTI,
        );
        assert_eq!(
            value.as_str_bytes(),
            Some(br#"{"name":"alice"}"#.as_slice())
        );
        // SAFETY: transfer the sole cache owner back to its destructor.
        unsafe { JsonPathCache_Free(cache) };
        // SAFETY: the destructor explicitly accepts null.
        unsafe { JsonPathCache_Free(ptr::null_mut()) };
        assert_eq!(
            (json_api_calls().path_parse, json_api_calls().path_free),
            (1, 1)
        );

        reset_json_api_calls();
        let paths = [Some(c"$.name"), Some(c"$.price")];
        let mut lookup = RLookup::new();
        let keys: Vec<_> = [c"$.name", c"$.price"]
            .into_iter()
            .map(|path| {
                lookup
                    .get_key_load_ptr(path, path, RLookupKeyFlags::empty())
                    .unwrap()
                    .as_ptr()
                    .cast::<ffi::RLookupKey>()
                    .cast_const()
            })
            .collect();
        // SAFETY: V9 mock; all lookup keys and the pointer array outlive this call.
        let cache = unsafe { JsonPathCache_New(ctx.as_ptr(), keys.as_ptr(), keys.len()) };
        assert!(!cache.is_null());
        // SAFETY: the cache is live and its entries match `paths` in load order.
        let row = load_fields(
            json!({"name": "bob", "price": 42}),
            &paths,
            Some(unsafe { &*cache.cast::<JsonPathCache>() }),
            PRE_MULTI,
        );
        assert_eq!(
            row[0].as_ref().unwrap().as_str_bytes(),
            Some(b"bob".as_slice())
        );
        assert_eq!(row[1].as_ref().unwrap().as_num(), Some(42.0));
        // SAFETY: all loads have finished and this test owns the cache exactly once.
        unsafe { JsonPathCache_Free(cache) };
        let calls = json_api_calls();
        assert_eq!(
            (calls.path_parse, calls.get_with_path, calls.path_free),
            (2, 2, 2)
        );
    });
}
