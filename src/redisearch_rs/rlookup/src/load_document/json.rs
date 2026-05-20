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
        DOCUMENT_OPEN_KEY_QUERY_FLAGS, DocumentFormat, LoadAllError, LoadFieldError, OpenDocument,
        UNDERSCORE_KEY,
    },
};
use lending_iterator::LendingIterator;
use redis_json_api::{JsonType, JsonValueRef, RedisJsonApi, SerializeError};
use redis_module::RedisString;
use std::ffi::CStr;
use std::ptr::NonNull;
use value::{Array, Map, SharedValue, Trio, Value};

const JSON_ROOT: &CStr = c"$";

pub struct JsonFormat<'a> {
    ctx: NonNull<ffi::RedisModuleCtx>,
    japi: &'a RedisJsonApi,
    api_version: u32,
}

pub struct JsonDoc<'a> {
    ctx: NonNull<ffi::RedisModuleCtx>,
    value: JsonValueRef<'a>,
    key_name: &'a RedisString,
    api_version: u32,
}

impl<'a> JsonFormat<'a> {
    pub const fn new(
        ctx: NonNull<ffi::RedisModuleCtx>,
        japi: &'a RedisJsonApi,
        api_version: u32,
    ) -> Self {
        Self {
            ctx,
            japi,
            api_version,
        }
    }
}

impl DocumentFormat for JsonFormat<'_> {
    type Document<'a>
        = JsonDoc<'a>
    where
        Self: 'a;

    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
    ) -> Result<Self::Document<'key>, LoadFieldError> {
        // SAFETY: `self.ctx` is a valid Redis module context held for the lifetime of `JsonFormat`.
        let key = unsafe {
            self.japi.open_key_with_flags(
                self.ctx.cast().as_ptr(),
                key_name,
                DOCUMENT_OPEN_KEY_QUERY_FLAGS,
            )
        };

        let Some(value) = key else {
            return Err(LoadFieldError::KeyNotFound);
        };

        Ok(JsonDoc {
            ctx: self.ctx,
            value,
            key_name,
            api_version: self.api_version,
        })
    }

    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
    ) -> Result<(), LoadAllError> {
        // SAFETY: `self.ctx` is a valid Redis module context held for the lifetime of `JsonFormat`.
        let json_root = unsafe {
            self.japi
                .open_key_with_flags(
                    self.ctx.cast().as_ptr(),
                    key_name,
                    DOCUMENT_OPEN_KEY_QUERY_FLAGS,
                )
                .ok_or(LoadAllError::KeyNotFound)?
        };

        let json_iter = json_root
            .get(JSON_ROOT)
            .ok_or(LoadAllError::JsonRootMissing)?;

        // For `load_all` an absent root is a document-level failure: a JSON document
        // is expected to have a `$` value, so collapse `Ok(None)` to `Err`.
        let value = json_iter_to_value(self.ctx, json_iter, self.api_version)?
            .ok_or(LoadAllError::JsonRootMissing)?;

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

impl OpenDocument for JsonDoc<'_> {
    fn load_field(&self, kk: &RLookupKey, dst_row: &mut RLookupRow) -> Result<(), LoadFieldError> {
        let path = kk
            .path()
            .as_ref()
            .expect("JSON RLookupKey must have a path set");

        // A path starting with `$` is a JSONPath expression to evaluate against the document.
        // Anything else is a sentinel — currently only `__key`, which resolves to the document key.
        // For per-field loads, "field absent" is not an error — we just leave it unset
        // and continue. Only hard failures bubble up as `Err`.
        let val = if path.to_bytes().starts_with(JSON_ROOT.to_bytes()) {
            let Some(iter) = self.value.get(path) else {
                // JSONPath did not match any value in the document — skip silently.
                return Ok(());
            };
            match json_iter_to_value(self.ctx, iter, self.api_version) {
                Ok(Some(v)) => v,
                Ok(None) | Err(_) => return Ok(()),
            }
        } else if path == UNDERSCORE_KEY {
            SharedValue::new_string(self.key_name.to_vec())
        } else {
            // Path is neither a JSONPath nor a recognized sentinel — nothing to load.
            return Ok(());
        };

        dst_row.write_key(kk, val);

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
    ctx: NonNull<ffi::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
    api_version: u32,
) -> Result<Option<SharedValue>, SerializeError> {
    if api_version < ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
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
    let Some(json) = iter.next() else {
        debug_assert!(false, "ResultsIter::is_empty()/next() disagree");
        return Ok(None);
    };

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
    let expand = json_iter_to_value_expanded(ctx, iter);

    Ok(Some(SharedValue::new(Value::Trio(Trio::new(
        val, otherval, expand,
    )))))
}

// Return an array of expanded values from an iterator.
// The iterator is being reset and is not being freed.
// Required japi_ver >= 4
fn json_iter_to_value_expanded(
    ctx: NonNull<ffi::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
) -> SharedValue {
    debug_assert!(!iter.is_empty(), "should be checked by caller");
    iter.reset();

    let values: Box<_> = iter
        .map_into_iter(|json_val| json_val_to_value_expanded(ctx, json_val))
        .collect();

    SharedValue::new(Value::Array(Array::new(values)))
}

fn json_val_to_value_expanded(
    ctx: NonNull<ffi::RedisModuleCtx>,
    json: JsonValueRef,
) -> SharedValue {
    match json.get_type() {
        JsonType::Object => {
            let len = json.len().unwrap();

            if len > 0 {
                // SAFETY: `ctx` is a valid Redis module context propagated from the caller.
                let iter = unsafe { json.key_values(ctx.cast().as_ptr()).unwrap() };

                let values: Box<_> = iter
                    .map(|(key, value)| {
                        let key = SharedValue::new_string(key.to_vec());
                        let value = json_val_to_value_expanded(ctx, value.as_ref());

                        (key, value)
                    })
                    .collect();

                SharedValue::new(Value::Map(Map::new(values)))
            } else {
                SharedValue::new(Value::Map(Map::new(Box::new([]))))
            }
        }
        JsonType::Array => {
            let len = json.len().unwrap();

            if len > 0 {
                let values: Box<_> = (0..len)
                    .map(|i| {
                        let json = json.get_at(i).unwrap();

                        json_val_to_value_expanded(ctx, json.as_ref())
                    })
                    .collect();

                SharedValue::new(Value::Array(Array::new(values)))
            } else {
                SharedValue::new(Value::Array(Array::new(Box::new([]))))
            }
        }
        // Scalar
        _ => json_val_to_value(ctx, json),
    }
}

fn json_val_to_value(ctx: NonNull<ffi::RedisModuleCtx>, json: JsonValueRef<'_>) -> SharedValue {
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
            SharedValue::new_string(v.to_string_lossy().into_bytes())
        }
        JsonType::Null => SharedValue::null_static(),
    }
}
