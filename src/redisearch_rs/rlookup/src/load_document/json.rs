/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    RLookup, RLookupKey, RLookupKeyFlag, RLookupRow,
    load_document::{
        DOCUMENT_OPEN_KEY_QUERY_FLAGS, DocumentFormat, FieldLoader, LoadAllError, LoadFieldError,
        UNDERSCORE_KEY,
    },
};
use lending_iterator::LendingIterator;
use redis_json_api::{JsonPath, JsonType, JsonValueRef, RedisJsonApi, SerializeError};
use redis_module::RedisString;
use std::ffi::CStr;
use std::ptr::{self, NonNull};
use value::{SharedValue, Value};

const JSON_ROOT: &CStr = c"$";

enum CachedJsonPath {
    Broken,
    DocumentKey,
    Path(JsonPath),
}

/// Owns the compiled [`JsonPath`]s for one load operation, in field-load order.
pub struct JsonPathCache {
    paths: Vec<CachedJsonPath>,
}

impl JsonPathCache {
    /// Classifies each field as a compiled [`JsonPath`], a document key, or a broken path.
    ///
    /// # Safety
    ///
    /// The path bytes must remain [valid] until this cache is dropped.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new<'p>(
        api: &RedisJsonApi,
        paths: impl Iterator<Item = Option<&'p CStr>>,
    ) -> Self {
        let paths = paths
            .map(|path| match path {
                Some(path) if path == UNDERSCORE_KEY => CachedJsonPath::DocumentKey,
                Some(path) if path.to_bytes().starts_with(b"$") => {
                    // SAFETY: the caller keeps the path bytes alive throughout the cache's lifetime.
                    unsafe { JsonPath::parse_no_error_msg(path, api) }
                        .map_or(CachedJsonPath::Broken, CachedJsonPath::Path)
                }
                _ => CachedJsonPath::Broken,
            })
            .collect();
        Self { paths }
    }

    fn get(&self, index: usize) -> &CachedJsonPath {
        &self.paths[index]
    }
}

pub struct JsonDocumentFormat<'a> {
    ctx: NonNull<redis_module::RedisModuleCtx>,
    japi: &'a RedisJsonApi,
    api_version: u8,
    path_cache: &'a JsonPathCache,
}

pub struct JsonFieldLoader<'a> {
    ctx: NonNull<redis_module::RedisModuleCtx>,
    value: JsonValueRef<'a>,
    key_name: &'a RedisString,
    api_version: u8,
    path_cache: &'a JsonPathCache,
}

impl<'a> JsonDocumentFormat<'a> {
    /// Uses `path_cache` in the same order as the fields to load, or a single root entry
    /// for [`DocumentFormat::load_all`].
    pub const fn new(
        ctx: NonNull<redis_module::RedisModuleCtx>,
        japi: &'a RedisJsonApi,
        api_version: u8,
        path_cache: &'a JsonPathCache,
    ) -> Self {
        Self {
            ctx,
            japi,
            api_version,
            path_cache,
        }
    }

    fn open_key(&self, key_name: &RedisString) -> Option<JsonValueRef<'a>> {
        // SAFETY: `self.ctx` is a valid Redis module context held for the lifetime of `JsonFormat`.
        unsafe {
            self.japi
                .open_key_with_flags(self.ctx.cast(), key_name, DOCUMENT_OPEN_KEY_QUERY_FLAGS)
        }
    }

    fn load_root(&self, key_name: &RedisString) -> Result<SharedValue, LoadAllError> {
        let json_root = self.open_key(key_name).ok_or(LoadAllError::OpenKeyFailed)?;
        let CachedJsonPath::Path(path) = self.path_cache.get(0) else {
            return Err(LoadAllError::JsonRootMissing);
        };
        // SAFETY: the cache owns the path throughout iteration.
        let iter = unsafe { json_root.get_with_path(path) }.ok_or(LoadAllError::JsonRootMissing)?;
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
                .open_from_handle(Some(NonNull::from_ref(open_key).cast()))
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
        let value = self.load_root(key_name)?;

        let rlk = if let Some(rlk) = rlookup.find_key_by_name(JSON_ROOT) {
            rlk.into_current().unwrap()
        } else {
            // A SORTABLE schema field on `$` makes the key value-available from
            // the sorting vector, in which case `get_key_load` returns `None`
            // unless forced.
            rlookup
                .get_key_load(JSON_ROOT, JSON_ROOT, RLookupKeyFlag::ForceLoad.into())
                .unwrap()
        };

        dst_row.write_key(rlk, value);

        Ok(())
    }
}

impl FieldLoader for JsonFieldLoader<'_> {
    fn load_field_at(
        &self,
        index: usize,
        key: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadFieldError> {
        match self.path_cache.get(index) {
            CachedJsonPath::Broken => {}
            CachedJsonPath::DocumentKey => {
                dst_row.write_key(key, SharedValue::new_string(self.key_name.to_vec()));
            }
            CachedJsonPath::Path(path) => {
                // SAFETY: the cache owns the path throughout iteration.
                if let Some(iter) = unsafe { self.value.get_with_path(path) }
                    && let Ok(Some(value)) = json_iter_to_value(self.ctx, iter, self.api_version)
                {
                    dst_row.write_key(key, value);
                }
            }
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
    let serialized = unsafe { iter.serialize(ctx.cast())? };

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
            let iter = unsafe { json.key_values(ctx.cast()).unwrap() };

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
            let v = unsafe { json.serialize(ctx.cast()).unwrap() };
            redis_module::raw::string_retain_string(ptr::null_mut(), v.inner);
            // SAFETY: `v` is a valid Redis string and we retained it above to
            // transfer one owned reference into the RSValue.
            let v = unsafe { value::RedisString::from_raw(v.inner.cast()) };
            SharedValue::new(Value::RedisString(v))
        }
        JsonType::Null => SharedValue::null_static(),
    }
}
