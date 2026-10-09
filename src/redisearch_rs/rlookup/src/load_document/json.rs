/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    RLookup, RLookupKey, RLookupKeyFlags, RLookupRow,
    load_document::{
        DOCUMENT_OPEN_KEY_QUERY_FLAGS, DocumentFormat, FieldLoader, LoadAllError, LoadFieldError,
        UNDERSCORE_KEY,
    },
};
use lending_iterator::LendingIterator;
use redis_json_api::{JsonPath, JsonType, JsonValueRef, RedisJsonApi, ResultsIter, SerializeError};
use redis_module::RedisString;
use std::ffi::CStr;
use std::ptr::{self, NonNull};
use value::{SharedValue, Value};

const JSON_ROOT: &CStr = c"$";

/// Owns the compiled [`JsonPath`]s for one query loader, in field-load order.
pub struct JsonPathCache {
    paths: Vec<Option<JsonPath>>,
}

impl JsonPathCache {
    /// Compiles JSON paths, leaving sentinels and malformed paths on the string-loading path.
    ///
    /// # Safety
    ///
    /// `ctx` must be a [valid] Redis module context.
    /// The negotiated API must support V9 and provide `getWithPath`.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new<'p>(
        ctx: *mut redis_module::RedisModuleCtx,
        api: &RedisJsonApi,
        paths: impl Iterator<Item = Option<&'p CStr>>,
    ) -> Self {
        let paths = paths
            .map(|path| {
                let path = path.filter(|path| path.to_bytes().starts_with(b"$"))?;
                // SAFETY: the caller guarantees a valid context.
                unsafe { JsonPath::parse(path, ctx, api) }.ok()
            })
            .collect();
        Self { paths }
    }

    fn get(&self, index: usize) -> Option<&JsonPath> {
        self.paths.get(index).and_then(Option::as_ref)
    }
}

pub struct JsonDocumentFormat<'a> {
    ctx: NonNull<redis_module::RedisModuleCtx>,
    japi: &'a RedisJsonApi,
    api_version: u8,
    path_cache: Option<&'a JsonPathCache>,
}

pub struct JsonFieldLoader<'a> {
    ctx: NonNull<redis_module::RedisModuleCtx>,
    value: JsonValueRef<'a>,
    key_name: &'a RedisString,
    api_version: u8,
    path_cache: Option<&'a JsonPathCache>,
}

impl<'a> JsonDocumentFormat<'a> {
    pub const fn new(
        ctx: NonNull<redis_module::RedisModuleCtx>,
        japi: &'a RedisJsonApi,
        api_version: u8,
    ) -> Self {
        Self {
            ctx,
            japi,
            api_version,
            path_cache: None,
        }
    }

    /// Uses a query-owned [`JsonPathCache`] for field or root loading.
    pub const fn with_path_cache(mut self, path_cache: &'a JsonPathCache) -> Self {
        self.path_cache = Some(path_cache);
        self
    }

    fn open_key(&self, key_name: &RedisString) -> Option<JsonValueRef<'a>> {
        // SAFETY: `self.ctx` is a valid Redis module context held for the lifetime of `JsonFormat`.
        unsafe {
            self.japi.open_key_with_flags(
                self.ctx.cast().as_ptr(),
                key_name,
                DOCUMENT_OPEN_KEY_QUERY_FLAGS,
            )
        }
    }

    fn load_root_without_path_cache(
        &self,
        key_name: &RedisString,
    ) -> Result<SharedValue, LoadAllError> {
        let json_root = self.open_key(key_name).ok_or(LoadAllError::OpenKeyFailed)?;
        self.load_root_from_iter(json_root.get(JSON_ROOT))
    }

    fn load_root_with_path(
        &self,
        key_name: &RedisString,
        path: &JsonPath,
    ) -> Result<SharedValue, LoadAllError> {
        let json_root = self.open_key(key_name).ok_or(LoadAllError::OpenKeyFailed)?;
        // SAFETY: the cache requires V9; it owns the path throughout iteration.
        self.load_root_from_iter(unsafe { json_root.get_with_path(path) })
    }

    fn load_root_from_iter(
        &self,
        iter: Option<ResultsIter<'_>>,
    ) -> Result<SharedValue, LoadAllError> {
        let iter = iter.ok_or(LoadAllError::JsonRootMissing)?;
        // Unlike per-field loading, an absent root is a document-level failure.
        json_iter_to_value(self.ctx, iter, self.api_version)?.ok_or(LoadAllError::JsonRootMissing)
    }
}

impl DocumentFormat for JsonDocumentFormat<'_> {
    type FieldLoader<'key>
        = JsonFieldLoader<'key>
    where
        Self: 'key;

    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
    ) -> Result<Self::FieldLoader<'key>, LoadFieldError> {
        let value = self.open_key(key_name).ok_or(LoadFieldError::KeyNotFound)?;

        Ok(JsonFieldLoader {
            ctx: self.ctx,
            value,
            key_name,
            api_version: self.api_version,
            path_cache: self.path_cache,
        })
    }

    fn borrow<'key>(
        &'key self,
        open_key: &'key redis_module::RedisModuleKey,
        key_name: &'key RedisString,
    ) -> Result<Self::FieldLoader<'key>, LoadFieldError> {
        // Safety: the `&'key` reference guarantees `open_key` is valid for `'key`.
        let value = unsafe {
            self.japi
                .open_from_handle(ptr::from_ref(open_key).cast_mut().cast())
        }
        // If we fail to open the JSON root from the borrowed handle: fall back to
        // open the document by name.
        .or_else(|| self.open_key(key_name))
        .ok_or(LoadFieldError::KeyNotFound)?;

        Ok(JsonFieldLoader {
            ctx: self.ctx,
            value,
            key_name,
            api_version: self.api_version,
            path_cache: self.path_cache,
        })
    }

    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
    ) -> Result<(), LoadAllError> {
        let value = match self.path_cache.and_then(|cache| cache.get(0)) {
            Some(path) => self.load_root_with_path(key_name, path)?,
            None => self.load_root_without_path_cache(key_name)?,
        };

        let rlk = if let Some(rlk) = rlookup.find_key_by_name(JSON_ROOT) {
            rlk.into_current().unwrap()
        } else {
            rlookup
                .get_key_load(JSON_ROOT, JSON_ROOT, RLookupKeyFlags::empty())
                .unwrap()
        };

        dst_row.write_key(rlk, value);

        Ok(())
    }
}

impl FieldLoader for JsonFieldLoader<'_> {
    fn load_field(&self, key: &RLookupKey, dst_row: &mut RLookupRow) -> Result<(), LoadFieldError> {
        self.load_field_without_path_cache(key, dst_row)
    }

    fn load_field_at(
        &self,
        index: usize,
        key: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadFieldError> {
        match self.path_cache.and_then(|cache| cache.get(index)) {
            Some(path) => self.load_field_with_path(key, dst_row, path),
            None => self.load_field_without_path_cache(key, dst_row),
        }
    }
}

impl JsonFieldLoader<'_> {
    fn load_field_with_path(
        &self,
        key: &RLookupKey,
        dst_row: &mut RLookupRow,
        path: &JsonPath,
    ) -> Result<(), LoadFieldError> {
        // SAFETY: the cache requires V9; it owns the path throughout iteration.
        let iter = unsafe { self.value.get_with_path(path) };
        self.load_field_from_iter(key, dst_row, iter)
    }

    fn load_field_without_path_cache(
        &self,
        key: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadFieldError> {
        let path = match key.path() {
            Some(p) => p.as_ref(),
            // No path set — nothing to load.
            None => return Ok(()),
        };

        // A path starting with `$` is a JSONPath expression to evaluate against the document.
        // Anything else is a sentinel — currently only `__key`, which resolves to the document key.
        // For per-field loads, "field absent" is not an error — we just leave it unset
        // and continue. Only hard failures bubble up as `Err`.
        let val = if path.to_bytes().starts_with(JSON_ROOT.to_bytes()) {
            return self.load_field_from_iter(key, dst_row, self.value.get(path));
        } else if path == UNDERSCORE_KEY {
            SharedValue::new_string(self.key_name.to_vec())
        } else {
            // Path is neither a JSONPath nor a recognized sentinel — nothing to load.
            return Ok(());
        };

        dst_row.write_key(key, val);

        Ok(())
    }

    fn load_field_from_iter(
        &self,
        key: &RLookupKey,
        dst_row: &mut RLookupRow,
        iter: Option<ResultsIter<'_>>,
    ) -> Result<(), LoadFieldError> {
        let Some(iter) = iter else {
            return Ok(());
        };
        if let Ok(Some(value)) = json_iter_to_value(self.ctx, iter, self.api_version) {
            dst_row.write_key(key, value);
        }
        Ok(())
    }
}

/// Consume the iterator and produce a single [`SharedValue`].
///
/// Returns:
/// - `Ok(Some(value))` — a value was extracted.
/// - `Ok(None)` — the iterator (or the first matched array) had no values; the
///   caller decides whether absence is acceptable.
/// - `Err(_)` — a hard failure (e.g. serialization).
///
/// Multi-value is supported with `apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST`.
fn json_iter_to_value(
    ctx: NonNull<redis_module::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
    api_version: u8,
) -> Result<Option<SharedValue>, SerializeError> {
    if u32::from(api_version) < ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
        // Preserve single value behavior for backward compatibility
        let Some(json) = iter.next() else {
            return Ok(None);
        };
        return Ok(Some(json_val_to_value(ctx, json)));
    }

    if iter.is_empty() {
        return Ok(None);
    }

    // SAFETY: `ctx` is a valid Redis module context by construction of `JsonFormat`.
    // First get the JSON serialized value (since it does not consume the iterator)
    let serialized = unsafe { iter.serialize(ctx.cast().as_ptr())? };

    // Second, get the first JSON value. `is_empty()` returned false above, so the
    // iterator is contractually obligated to yield at least one value here.
    let json = iter
        .next()
        .expect("ResultsIter::is_empty()/next() disagree");

    let val = if matches!(json.get_type(), JsonType::Array) {
        // If the value is an array, we currently try using the first element.
        // An empty array means there's no value to surface — return absence.
        let Some(first) = json.get_at(0) else {
            return Ok(None);
        };
        json_val_to_value(ctx, first.as_ref())
    } else {
        json_val_to_value(ctx, json)
    };

    let otherval = SharedValue::new_string(serialized.to_vec());

    // NB: make sure the iterator is reset to the beginning, so we correctly
    // get the full expanded value.
    iter.reset();
    let expand = json_iter_to_value_expanded(ctx, iter);

    Ok(Some(SharedValue::new_trio(val, otherval, expand)))
}

// Return an array of expanded values from an iterator.
// The iterator is being reset and is not being freed.
// Required japi_ver >= 4
fn json_iter_to_value_expanded(
    ctx: NonNull<redis_module::RedisModuleCtx>,
    iter: redis_json_api::ResultsIter<'_>,
) -> SharedValue {
    debug_assert!(!iter.is_empty(), "should be checked by caller");

    let values: Box<_> = iter
        .map_into_iter(|json_val| json_val_to_value_expanded(ctx, json_val))
        .collect();

    SharedValue::new_array(values)
}

fn json_val_to_value_expanded(
    ctx: NonNull<redis_module::RedisModuleCtx>,
    json: JsonValueRef,
) -> SharedValue {
    match json.get_type() {
        JsonType::Object => {
            // SAFETY: `ctx` is a valid Redis module context propagated from the caller.
            let iter = unsafe { json.key_values(ctx.cast().as_ptr()).unwrap() };

            let values = iter.map(|(key, value)| {
                let key = SharedValue::new_string(key.to_vec());
                let value = json_val_to_value_expanded(ctx, value.as_ref());

                (key, value)
            });

            SharedValue::new_map(values)
        }
        JsonType::Array => {
            let len = json.len().unwrap();

            let values = (0..len).map(|i| {
                let json = json.get_at(i).unwrap();

                json_val_to_value_expanded(ctx, json.as_ref())
            });

            SharedValue::new_array(values)
        }
        // Scalar
        _ => json_val_to_value(ctx, json),
    }
}

fn json_val_to_value(
    ctx: NonNull<redis_module::RedisModuleCtx>,
    json: JsonValueRef<'_>,
) -> SharedValue {
    // Currently `getJSON` cannot fail here also the other japi APIs below
    match json.get_type() {
        JsonType::String => {
            let v = json.get_str().unwrap();
            SharedValue::new_string(v.to_string().into_bytes())
        }
        JsonType::Int => {
            let v = json.get_int().unwrap();
            SharedValue::new_num(v as f64)
        }
        JsonType::Double => {
            let v = json.get_double().unwrap();
            SharedValue::new_num(v)
        }
        JsonType::Bool => {
            let v = json.get_bool().unwrap();
            SharedValue::new_num(v as u8 as f64)
        }
        JsonType::Object | JsonType::Array => {
            // SAFETY: `ctx` is a valid Redis module context propagated from the caller.
            let v = unsafe { json.serialize(ctx.cast().as_ptr()).unwrap() };
            redis_module::raw::string_retain_string(ptr::null_mut(), v.inner);
            // SAFETY: `v` is a valid Redis string and we retained it above to
            // transfer one owned reference into the RSValue.
            let v = unsafe { value::RedisString::from_raw(v.inner.cast()) };
            SharedValue::new(Value::RedisString(v))
        }
        JsonType::Null => SharedValue::null_static(),
    }
}
