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
    load_document::{DocumentFormat, LoadDocumentError, OpenDocument, UNDERSCORE_KEY},
};
use lending_iterator::LendingIterator;
use query_error::QueryError;
use redis_json_api::{JsonType, JsonValueRef, RedisJsonApi};
use redis_module::RedisString;
use std::ffi::CStr;
use std::ptr::NonNull;
use value::{Array, Map, RsValue, RsValueTrio, SharedRsValue};

const DOCUMENT_OPEN_KEY_QUERY_FLAGS: u32 = ffi::REDISMODULE_READ
    | ffi::REDISMODULE_OPEN_KEY_NOEFFECTS
    | ffi::REDISMODULE_OPEN_KEY_NOEXPIRE
    | ffi::REDISMODULE_OPEN_KEY_ACCESS_EXPIRED;

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
    ) -> Result<Self, LoadDocumentError> {
        Ok(Self {
            ctx,
            japi,
            api_version,
        })
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
        status: &mut QueryError,
    ) -> Result<Self::Document<'key>, LoadDocumentError> {
        let key = unsafe {
            self.japi.open_key_with_flags(
                self.ctx.cast().as_ptr(),
                key_name,
                DOCUMENT_OPEN_KEY_QUERY_FLAGS as i32,
            )
        };

        let Some(value) = key else {
            status.set_code(query_error::QueryErrorCode::NoDoc);
            return Err(LoadDocumentError {});
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
        _status: &mut QueryError,
    ) -> Result<(), LoadDocumentError> {
        let json_root = unsafe {
            self.japi
                .open_key_with_flags(
                    self.ctx.cast().as_ptr(),
                    key_name,
                    DOCUMENT_OPEN_KEY_QUERY_FLAGS as i32,
                )
                .ok_or(LoadDocumentError {})?
        };

        let json_iter = json_root.get(JSON_ROOT).ok_or(LoadDocumentError {})?;

        let value = json_iter_to_value(self.ctx, json_iter, self.api_version)?;

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
    fn load_field(
        &self,
        kk: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadDocumentError> {
        // Get the actual json value
        // TODO verify if this requires PATH to be different to NAME
        let path = kk.path().as_ref().unwrap();
        let val = if path == JSON_ROOT
            && let Some(iter) = self.value.get(path)
        {
            json_iter_to_value(self.ctx, iter, self.api_version)?
        } else {
            // The field does not exist and and it isn't `__key`
            if path != UNDERSCORE_KEY {
                return Ok(());
            }

            SharedRsValue::new_string(self.key_name.to_vec())
        };

        dst_row.write_key(kk, val);

        Ok(())
    }
}

// Get the value from an iterator and free the iterator
// Return REDISMODULE_OK, and set rsv to the value, if value exists
// Return REDISMODULE_ERR otherwise
//
// Multi value is supported with apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST
fn json_iter_to_value(
    ctx: NonNull<ffi::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
    api_version: u32,
) -> Result<SharedRsValue, LoadDocumentError> {
    if api_version < ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
        // Preserve single value behavior for backward compatibility
        let json = iter.next().ok_or(LoadDocumentError {})?;

        let rsv = json_val_to_value(ctx, json);

        Ok(rsv)
    } else {
        let len = iter.len();

        if len > 0 {
            // First get the JSON serialized value (since it does not consume the iterator)
            let serialized = unsafe { iter.serialize(ctx.cast().as_ptr())? };

            // Second, get the first JSON value
            if let Some(json) = iter.next() {
                let val = if matches!(json.get_type(), JsonType::Array) {
                    // If the value is an array, we currently try using the first element
                    let json = json.get_at(0).unwrap();
                    json_val_to_value(ctx, json.as_ref())
                } else {
                    json_val_to_value(ctx, json)
                };

                let otherval = SharedRsValue::new_string(serialized.to_vec());

                let expand = json_iter_to_value_expanded(ctx, iter);

                let rsv =
                    SharedRsValue::new(RsValue::Trio(RsValueTrio::new(val, otherval, expand)));

                Ok(rsv)
            } else {
                todo!("error??")
            }
        } else {
            todo!("error??")
        }
    }
}

// Return an array of expanded values from an iterator.
// The iterator is being reset and is not being freed.
// Required japi_ver >= 4
fn json_iter_to_value_expanded(
    ctx: NonNull<ffi::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
) -> SharedRsValue {
    debug_assert!(!iter.is_empty(), "should be checked by caller");
    iter.reset();

    let values: Box<_> = iter
        .map_into_iter(|json_val| json_val_to_value_expanded(ctx, json_val))
        .collect();

    SharedRsValue::new(RsValue::Array(Array::new(values)))
}

fn json_val_to_value_expanded(
    ctx: NonNull<ffi::RedisModuleCtx>,
    json: JsonValueRef,
) -> SharedRsValue {
    match json.get_type() {
        JsonType::Object => {
            let len = json.len().unwrap();

            if len > 0 {
                let iter = unsafe { json.key_values(ctx.cast().as_ptr()).unwrap() };

                let values: Box<_> = iter
                    .map(|(key, value)| {
                        let key = SharedRsValue::new_string(key.to_vec());
                        let value = json_val_to_value_expanded(ctx, value.as_ref());

                        (key, value)
                    })
                    .collect();

                SharedRsValue::new(RsValue::Map(Map::new(values)))
            } else {
                SharedRsValue::new(RsValue::Map(Map::new(Box::new([]))))
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

                SharedRsValue::new(RsValue::Array(Array::new(values)))
            } else {
                SharedRsValue::new(RsValue::Array(Array::new(Box::new([]))))
            }
        }
        // Scalar
        _ => json_val_to_value(ctx, json),
    }
}

fn json_val_to_value(ctx: NonNull<ffi::RedisModuleCtx>, json: JsonValueRef<'_>) -> SharedRsValue {
    // Currently `getJSON` cannot fail here also the other japi APIs below
    match json.get_type() {
        JsonType::String => {
            let v = json.get_str().unwrap();
            SharedRsValue::new_string(v.to_string().into_bytes())
        }
        JsonType::Int => {
            let v = json.get_int().unwrap();
            SharedRsValue::new_num(v as f64)
        }
        JsonType::Double => {
            let v = json.get_double().unwrap();
            SharedRsValue::new_num(v)
        }
        JsonType::Bool => {
            let v = json.get_bool().unwrap();
            SharedRsValue::new_num(v as u8 as f64)
        }
        JsonType::Object | JsonType::Array => {
            let v = unsafe { json.serialize(ctx.cast().as_ptr()).unwrap() };
            SharedRsValue::new_string(v.to_string_lossy().into_bytes())
        }
        JsonType::Null => SharedRsValue::null_static(),
    }
}
