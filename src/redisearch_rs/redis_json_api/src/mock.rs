/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! An in-memory implementation of the RedisJSON module API, for use in tests.

use crate::RedisJsonApi;
use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_int, c_longlong, c_void};
use std::ptr::{self, NonNull};

/// Per-invocation mock state, owned by the [`with_json_api`] stack frame.
struct MockState {
    /// The document `openKeyWithFlags` resolves to. `None` models a missing document.
    doc: Option<serde_json::Value>,
    /// Owns the serialized buffers (`getJSON` / `getJSONFromIter`) minted during
    /// the mock's lifetime. `redis_mock`'s `RedisModule_CreateString` copies its
    /// input, so retaining them here is not required for correctness; it simply
    /// gives each transient buffer a well-defined owner for the call.
    strings: RefCell<Vec<Box<[u8]>>>,
}

/// Run `f` with a [`RedisJsonApi`] backed by an in-memory `doc`.
///
/// `doc` is the value `openKeyWithFlags` resolves to; pass `None` to model a
/// missing document. The returned context pointer must be handed to whatever opens
/// the document (e.g. `JsonFormat::new(ctx, &api, api_version)`); it is a
/// disguised pointer to the mock state and is only valid for the duration of
/// `f`.
pub fn with_json_api<R>(
    doc: Option<serde_json::Value>,
    f: impl FnOnce(RedisJsonApi, NonNull<ffi::RedisModuleCtx>) -> R,
) -> R {
    let state = MockState {
        doc,
        strings: RefCell::new(Vec::new()),
    };
    // Derive the handle from a shared reference: we never form `&mut state`
    // afterwards, and all interior mutation goes through `RefCell`.
    let ctx = NonNull::from(&state).cast::<ffi::RedisModuleCtx>();
    let api = RedisJsonApi::from_vtable(&VTABLE);

    f(api, ctx)
}

static VTABLE: ffi::RedisJSONAPI = ffi::RedisJSONAPI {
    openKey: None,
    openKeyFromStr: None,
    get: Some(get),
    next: Some(next),
    len: Some(len),
    freeIter: Some(free_iter),
    getLen: Some(get_len),
    getType: Some(get_type),
    getInt: Some(get_int),
    getDouble: Some(get_double),
    getBoolean: Some(get_boolean),
    getString: Some(get_string),
    getJSON: Some(get_json),
    isJSON: None,
    pathParse: None,
    pathFree: None,
    pathIsSingle: None,
    pathHasDefinedOrder: None,
    getJSONFromIter: Some(get_json_from_iter),
    resetIter: Some(reset_iter),
    getKeyValues: Some(get_key_values),
    freeKeyValuesIter: Some(free_key_values_iter),
    openKeyWithFlags: Some(open_key_with_flags),
    allocJson: Some(alloc_json),
    getAt: Some(get_at),
    nextKeyValue: Some(next_key_value),
    freeJson: Some(free_json),
    getArray: None,
    getJsonFromHandle: Some(get_json_from_handle),
};

/// View a `RedisJSON` handle as the `serde_json::Value` node it points at.
///
/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON value created by this module
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
const unsafe fn node<'a>(json: ffi::RedisJSON) -> &'a serde_json::Value {
    // Safety: ensured by caller (1.)
    unsafe { &*json.cast::<serde_json::Value>() }
}

/// Create a `RedisModuleString` over `bytes`.
///
/// The buffer is parked in the mock's string arena so each transient buffer has
/// a well-defined owner, but `RedisModule_CreateString` copies its input, so the
/// arena is not load-bearing for correctness.
fn arena_string(state: &MockState, bytes: Vec<u8>) -> *mut ffi::RedisModuleString {
    let buf = bytes.into_boxed_slice();
    let ptr = buf.as_ptr();
    let len = buf.len();
    state.strings.borrow_mut().push(buf);
    module_string(ptr, len)
}

/// Mint a `RedisModuleString` over `len` bytes at `ptr`. The mock
/// `RedisModule_CreateString` copies the bytes, so `ptr` only needs to stay
/// valid for the duration of this call.
fn module_string(ptr: *const u8, len: usize) -> *mut ffi::RedisModuleString {
    // SAFETY: initialized by `redis_mock::init_redis_module_mock`.
    let create = unsafe { redis_module::raw::RedisModule_CreateString }
        .expect("RedisModule_CreateString not initialized — call init_redis_module_mock()");
    // SAFETY: the mock ignores `ctx`; `ptr`/`len` describe a caller-owned buffer.
    let s = unsafe { create(ptr::null_mut(), ptr.cast::<c_char>(), len) };
    s.cast()
}

/// Evaluate the minimal JSONPath subset the loader uses against `root`.
fn eval_path(root: &serde_json::Value, path: &str) -> Vec<*const serde_json::Value> {
    let rest = path
        .strip_prefix('$')
        .expect("mock: unsupported JSON path `{path}` (must start with `$`)");

    if rest.is_empty() {
        return vec![ptr::from_ref(root)];
    }

    if rest == "[*]" {
        return match root {
            serde_json::Value::Array(a) => a.iter().map(ptr::from_ref).collect(),
            _ => Vec::new(),
        };
    }

    // A plain `$.key`: a present key matches, an absent key (or non-object root)
    // is a missing field. Anything carrying further path syntax is a multi-level
    // or wildcard path this mock does not model.
    if let Some(key) = rest.strip_prefix('.')
        && !key.contains(|c| matches!(c, '.' | '[' | ']' | '*'))
    {
        return match root {
            serde_json::Value::Object(m) => m.get(key).map(ptr::from_ref).into_iter().collect(),
            _ => Vec::new(),
        };
    }

    unimplemented!("mock: unsupported JSON path `{path}`");
}

/// # Safety
///
/// 1. `ctx` must be a [valid], non-null pointer to a mock context created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn open_key_with_flags(
    ctx: *mut ffi::RedisModuleCtx,
    _key_name: *mut ffi::RedisModuleString,
    _flags: c_int,
) -> ffi::RedisJSON {
    // Safety: ensured by caller (1.)
    let state = unsafe { &*ctx.cast::<MockState>() };
    match &state.doc {
        Some(value) => ptr::from_ref(value).cast(),
        None => ptr::null(),
    }
}

/// Resolve an already-open key handle to the mock document's JSON root. The mock
/// represents the handle by the same [`MockState`] pointer as the context.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to a mock context created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_json_from_handle(key: *mut ffi::RedisModuleKey) -> ffi::RedisJSON {
    // Safety: ensured by caller (1.)
    let state = unsafe { &*key.cast::<MockState>() };
    match &state.doc {
        Some(value) => ptr::from_ref(value).cast(),
        None => ptr::null(),
    }
}

struct Results {
    items: Vec<*const serde_json::Value>,
    pos: usize,
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `path` must be a [valid], non-null pointer to a null-terminated c string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get(json: ffi::RedisJSON, path: *const c_char) -> ffi::JSONResultsIterator {
    // SAFETY: ensured by caller (2.)
    let Ok(path) = unsafe { CStr::from_ptr(path) }.to_str() else {
        return ptr::null();
    };
    // SAFETY: ensured by caller (1.)
    let json = unsafe { node(json) };
    let items = eval_path(json, path);
    if items.is_empty() {
        // A null iterator signals "no match"; the wrapper maps it to absence.
        return ptr::null();
    }
    Box::into_raw(Box::new(Results { items, pos: 0 })).cast()
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a results iterator created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn next(iter: ffi::JSONResultsIterator) -> ffi::RedisJSON {
    // Safety: ensured by caller (1.)
    let it = unsafe { &mut *iter.cast::<Results>().cast_mut() };
    match it.items.get(it.pos) {
        Some(&node) => {
            it.pos += 1;
            node.cast()
        }
        None => ptr::null(),
    }
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a results iterator created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
const unsafe extern "C" fn len(iter: ffi::JSONResultsIterator) -> usize {
    // Safety: ensured by caller (1.)
    unsafe { &*iter.cast::<Results>() }.items.len()
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a results iterator created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn reset_iter(iter: ffi::JSONResultsIterator) {
    // Safety: ensured by caller (1.)
    unsafe { &mut *iter.cast::<Results>().cast_mut() }.pos = 0;
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a results iterator created by this module.
/// 2. this function must be called exactly once, for a given `ptr`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn free_iter(iter: ffi::JSONResultsIterator) {
    // Safety: ensured by caller (1., 2.)
    drop(unsafe { Box::from_raw(iter.cast::<Results>().cast_mut()) });
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a results iterator created by this module.
/// 2. `ctx` must be a [valid], non-null pointer to a mock context created by this module.
/// 3. `str` must be a [valid], non-null pointer to a `*mut RedisModuleString`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_json_from_iter(
    iter: ffi::JSONResultsIterator,
    ctx: *mut ffi::RedisModuleCtx,
    str: *mut *mut ffi::RedisModuleString,
) -> c_int {
    // Safety: ensured by caller (1.)
    let it = unsafe { &*iter.cast::<Results>() };
    // Safety: ensured by caller (2.)
    let state = unsafe { &*ctx.cast::<MockState>() };

    // RedisJSON's `getJSONFromIter` always wraps matches in a JSON array, even
    // when there is exactly one match.
    let arr = it
        .items
        .iter()
        // Safety: `with_json_api` ensures everything within its scope has the same lifetime.
        // therefore it is safe to dereference the item here: the iterator cannot ordinarily outlive it.
        .map(|p| unsafe { (**p).clone() })
        .collect();
    let text = serde_json::to_string(&serde_json::Value::Array(arr)).unwrap();

    let s = arena_string(state, text.into_bytes());

    // Safety: ensured by caller (3.)
    unsafe { *str = s };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_type(json: ffi::RedisJSON) -> ffi::JSONType {
    // SAFETY: ensured by caller (1.)
    match unsafe { node(json) } {
        serde_json::Value::String(_) => ffi::JSONType_JSONType_String,
        serde_json::Value::Number(n) if n.is_f64() => ffi::JSONType_JSONType_Double,
        serde_json::Value::Number(_) => ffi::JSONType_JSONType_Int,
        serde_json::Value::Bool(_) => ffi::JSONType_JSONType_Bool,
        serde_json::Value::Object(_) => ffi::JSONType_JSONType_Object,
        serde_json::Value::Array(_) => ffi::JSONType_JSONType_Array,
        serde_json::Value::Null => ffi::JSONType_JSONType_Null,
    }
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `count` must be a [valid], non-null pointer to a `usize`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_len(json: ffi::RedisJSON, count: *mut usize) -> c_int {
    // Safety: ensured by caller (1.)
    let value = match unsafe { node(json) } {
        serde_json::Value::Object(m) => m.len(),
        serde_json::Value::Array(a) => a.len(),
        _ => return ffi::REDISMODULE_ERR as i32,
    };

    // Safety: ensured by caller (2.)
    unsafe { *count = value };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `integer` must be a [valid], non-null pointer to a `c_longlong`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_int(json: ffi::RedisJSON, integer: *mut c_longlong) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(i) = unsafe { node(json) }.as_i64() else {
        return ffi::REDISMODULE_ERR as i32;
    };

    // Safety: ensured by caller (2.)
    unsafe { *integer = i };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `dbl` must be a [valid], non-null pointer to a `f64`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_double(json: ffi::RedisJSON, dbl: *mut f64) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(d) = unsafe { node(json) }.as_f64() else {
        return ffi::REDISMODULE_ERR as i32;
    };

    // Safety: ensured by caller (2.)
    unsafe { *dbl = d };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `boolean` must be a [valid], non-null pointer to a `c_int`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_boolean(json: ffi::RedisJSON, boolean: *mut c_int) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(b) = unsafe { node(json) }.as_bool() else {
        return ffi::REDISMODULE_ERR as i32;
    };

    // Safety: ensured by caller (2.)
    unsafe { *boolean = c_int::from(b) };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `str` must be a [valid], non-null pointer to a `*const c_char`.
/// 3. `len` must be a [valid], non-null pointer to a `usize`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_string(
    json: ffi::RedisJSON,
    str: *mut *const c_char,
    len: *mut usize,
) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(s) = unsafe { node(json) }.as_str() else {
        return ffi::REDISMODULE_ERR as i32;
    };

    // The bytes live in the document, valid for the mock's lifetime.
    // Safety: ensured by caller (2.)
    unsafe { *str = s.as_ptr().cast::<c_char>() };

    // Safety: ensured by caller (3.)
    unsafe { *len = s.len() };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `ctx` must be a [valid], non-null pointer to a mock context created by this module.
/// 3. `str` must be a [valid] pointer to a `*mut RedisModuleString`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_json(
    json: ffi::RedisJSON,
    ctx: *mut ffi::RedisModuleCtx,
    str: *mut *mut ffi::RedisModuleString,
) -> c_int {
    // Safety: ensured by caller (1.)
    let state = unsafe { ctx.cast::<MockState>().as_ref().unwrap() };
    // Safety: ensured by caller (2.)
    let text = serde_json::to_string(unsafe { node(json) }).unwrap();

    let s = arena_string(state, text.into_bytes());

    // Safety: ensured by caller (3.)
    unsafe { *str = s };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. `ptr` must be a [valid], non_null pointer to a mock JSON object created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_at(json: ffi::RedisJSON, index: usize, ptr: ffi::RedisJSONPtr) -> c_int {
    // Safety: ensured by caller (1.)
    let Some(elem) = unsafe { node(json) }.as_array().and_then(|a| a.get(index)) else {
        return ffi::REDISMODULE_ERR as i32;
    };

    // Safety: ensured by caller (2.)
    unsafe { *ptr = ptr::from_ref(elem).cast() };

    ffi::REDISMODULE_OK as i32
}

struct KeyValues {
    entries: Vec<(*const c_char, usize, *const serde_json::Value)>,
    pos: usize,
}

/// # Safety
///
/// 1. `json` must be a [valid], non-null pointer to a mock JSON object created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn get_key_values(json: ffi::RedisJSON) -> ffi::JSONKeyValuesIterator {
    // Safety: ensured by caller (1.)
    let serde_json::Value::Object(map) = (unsafe { node(json) }) else {
        return ptr::null();
    };
    let entries = map
        .iter()
        .map(|(k, v)| (k.as_ptr().cast::<c_char>(), k.len(), ptr::from_ref(v)))
        .collect();
    Box::into_raw(Box::new(KeyValues { entries, pos: 0 })).cast()
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a mock key-values iterator created by this module.
/// 2. `key_name` must be a [valid], non-null pointer to a `*mut RedisModuleString`.
/// 3. `ptr` must be a [valid], non-null pointer to a JSON object created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn next_key_value(
    iter: ffi::JSONKeyValuesIterator,
    key_name: *mut *mut ffi::RedisModuleString,
    ptr: ffi::RedisJSONPtr,
) -> c_int {
    // Safety: ensured by caller (1.)
    let it = unsafe { &mut *iter.cast::<KeyValues>().cast_mut() };
    let Some(&(key_ptr, key_len, value)) = it.entries.get(it.pos) else {
        return ffi::REDISMODULE_ERR as i32;
    };
    it.pos += 1;

    let key = module_string(key_ptr.cast::<u8>(), key_len);

    // Safety: ensured by caller (2.)
    unsafe { *key_name = key };

    // Safety: ensured by caller (3.)
    unsafe { *ptr = value.cast() };

    ffi::REDISMODULE_OK as i32
}

/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to a mock key-values iterator created by this module.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn free_key_values_iter(iter: ffi::JSONKeyValuesIterator) {
    // Safety: ensured by caller (1.)
    drop(unsafe { Box::from_raw(iter.cast::<KeyValues>().cast_mut()) });
}

extern "C" fn alloc_json() -> ffi::RedisJSONPtr {
    Box::into_raw(Box::new(ptr::null::<c_void>()))
}

/// # Safety
///
/// 1. `ptr` must be a [valid], non-null pointer to a mock JSON object created by this module.
/// 2. this function must be called exactly once, for a given `ptr`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn free_json(ptr: ffi::RedisJSONPtr) {
    // Safety: ensured by caller (1., 2.)
    drop(unsafe { Box::from_raw(ptr) });
}
