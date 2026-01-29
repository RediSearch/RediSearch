use crate::{
    RLookup, RLookupKey, RLookupKeyFlags, RLookupRow,
    load_document::{LoadDocumentError, UNDERSCORE_KEY},
};
use query_error::QueryError;
use redis_json_api::{JsonType, JsonValueRef, RedisJsonApi};
use redis_module::RedisString;
use std::ptr::NonNull;
use std::{collections::HashMap, ffi::CStr};
use value::{RSValueFFI, RSValueTrait};

const DOCUMENT_OPEN_KEY_QUERY_FLAGS: u32 = ffi::REDISMODULE_READ
    | ffi::REDISMODULE_OPEN_KEY_NOEFFECTS
    | ffi::REDISMODULE_OPEN_KEY_NOEXPIRE
    | ffi::REDISMODULE_OPEN_KEY_ACCESS_EXPIRED;

const JSON_ROOT: &CStr = c"$";

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    let Some(japi) = (unsafe { RedisJsonApi::get() }) else {
        //   QueryError_SetCode(options->status, QUERY_ERROR_CODE_UNSUPP_TYPE);
        tracing::warn!("cannot operate on a JSON index as RedisJSON is not loaded");
        return Err(LoadDocumentError {});
    };

    // if (isValueAvailable(kk, dst, options)) {
    //   return REDISMODULE_OK;
    // }
    // TODO move this to mod-level

    let key = unsafe {
        japi.open_key_with_flags(
            ctx.cast().as_ptr(),
            &key_name,
            DOCUMENT_OPEN_KEY_QUERY_FLAGS as i32,
        )
        //     QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
        .ok_or(LoadDocumentError {})?
    };

    // Get the actual json value
    // TODO verify if this requires PATH to be different to NAME
    let path = kk.path().as_ref().unwrap();
    let val = if path == JSON_ROOT
        && let Some(iter) = key.get(path)
    {
        json_iter_to_value(ctx, iter, api_version)?
    } else {
        // The field does not exist and and it isn't `__key`
        if path != UNDERSCORE_KEY {
            return Ok(());
        }

        RSValueFFI::create_string(key_name.to_vec())
    };

    dst_row.write_key(kk, val);

    Ok(())
}

pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    //   int rc = REDISMODULE_ERR;
    //   if (!japi) {
    //     return rc;
    //   }
    let japi = unsafe { RedisJsonApi::get() }.ok_or_else(|| LoadDocumentError {})?;

    //   RedisJSON jsonRoot = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    //   RedisModule_FreeString(ctx, keyName);
    //   if (!jsonRoot) {
    //     goto done;
    //   }
    let json_root = unsafe {
        japi.open_key_with_flags(
            ctx.cast().as_ptr(),
            key_name,
            DOCUMENT_OPEN_KEY_QUERY_FLAGS as i32,
        )
        .ok_or_else(|| LoadDocumentError {})?
    };

    //   jsonIter = japi->get(jsonRoot, JSON_ROOT);
    //   if (jsonIter == NULL) {
    //     goto done;
    //   }
    let json_iter = json_root
        .get(JSON_ROOT)
        .ok_or_else(|| LoadDocumentError {})?;

    //   RSValue *vptr;
    //   int res = jsonIterToValue(ctx, jsonIter, options->sctx->apiVersion, &vptr);
    //   japi->freeIter(jsonIter);
    //   if (res == REDISMODULE_ERR) {
    //     goto done;
    //   }
    let value = json_iter_to_value(ctx, json_iter, api_version)?;

    //   RLookupKey *rlk = RLookup_FindKey(it, JSON_ROOT, strlen(JSON_ROOT));
    let rlk = if let Some(rlk) = rlookup.find_key_by_name(JSON_ROOT) {
        rlk.into_current().unwrap()
    } else {
        //   if (!rlk) {
        //     // First returned document, create the key.
        //     rlk = RLookup_GetKey_LoadEx(it, JSON_ROOT, strlen(JSON_ROOT), JSON_ROOT, RLOOKUP_F_NOFLAGS);
        //   }

        rlookup
            .get_key_load(JSON_ROOT, JSON_ROOT, RLookupKeyFlags::empty())
            .unwrap()
    };

    //   RLookup_WriteOwnKey(rlk, dst, vptr);
    dst_row.write_key(rlk, value);

    //   rc = REDISMODULE_OK;
    Ok(())
}

// // Get the value from an iterator and free the iterator
// // Return REDISMODULE_OK, and set rsv to the value, if value exists
// // Return REDISMODULE_ERR otherwise
// //
// // Multi value is supported with apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST
fn json_iter_to_value(
    ctx: NonNull<ffi::RedisModuleCtx>,
    mut iter: redis_json_api::ResultsIter<'_>,
    api_version: u32,
) -> Result<RSValueFFI, LoadDocumentError> {
    if api_version < ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
        // Preserve single value behavior for backward compatibility
        let json = iter.next().ok_or(LoadDocumentError {})?;
        //     *rsv = jsonValToValue(ctx, json);

        let rsv = json_val_to_value(ctx, json);
        //     res = REDISMODULE_OK;
        //     goto done;
        Ok(rsv)
    } else {
        //   size_t len = japi->len(iter);
        let len = iter.len();

        //   if (len > 0) {
        if len > 0 {
            //     if (japi->getJSONFromIter(iter, ctx, &serialized) == REDISMODULE_ERR) {
            //       goto done;
            //     }
            // First get the JSON serialized value (since it does not consume the iterator)
            let serialized = unsafe { iter.serialize(ctx.cast().as_ptr())? };

            //     RedisJSON json = japi->next(iter);

            // // Second, get the first JSON value
            if let Some(json) = iter.next() {
                // RSValue *val = jsonValToValue(ctx, json);

                let val = if matches!(json.get_type(), JsonType::Array) {
                    // If the value is an array, we currently try using the first element
                    let json = json.get_at(0).unwrap();
                    json_val_to_value(ctx, json.as_ref())
                } else {
                    json_val_to_value(ctx, json)
                };

                // RSValue *otherval = RSValue_NewRedisString(serialized);
                let otherval = RSValueFFI::create_string(serialized.to_vec());

                // RSValue *expand = jsonIterToValueExpanded(ctx, iter);
                let expand = json_iter_to_value_expanded(ctx, iter);

                // *rsv = RSValue_NewTrio(val, otherval, expand);
                // TODO let rsv = RSValueFFI::create_trio(val, otherval, expand);
                todo!();

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
) -> RSValueFFI {
    debug_assert!(iter.len() > 0, "should be checked by caller");
    iter.reset();

    let mut arr = Vec::with_capacity(iter.len());
    while let Some(json) = iter.next() {
        arr.push(json_val_to_value_expanded(ctx, json))
    }

    // TODO RSValueFFI::create_array(arr)
    todo!()
}

fn json_val_to_value_expanded(ctx: NonNull<ffi::RedisModuleCtx>, json: JsonValueRef) -> RSValueFFI {
    match json.get_type() {
        JsonType::Object => {
            let len = json.len().unwrap();

            if len > 0 {
                //       JSONKeyValuesIterator iter = japi->getKeyValues(json);
                let iter = unsafe { json.key_values(ctx.cast().as_ptr()).unwrap() };

                //       RSValueMap map = RSValueMap_AllocUninit(len);
                //       for (; (japi->nextKeyValue(iter, &keyName, value_ptr) == REDISMODULE_OK); ++i) {
                //         value = *value_ptr;
                //       }
                let map: HashMap<RSValueFFI, RSValueFFI> = iter
                    .map(|(key, value)| {
                        let key = RSValueFFI::create_string(key.to_vec());
                        let value = json_val_to_value_expanded(ctx, value.as_ref());

                        (key, value)
                    })
                    .collect();
                debug_assert_eq!(map.len(), len);
                //       RS_ASSERT(i == len);

                //       ret = RSValue_NewMap(map);
                // TODO RSValueFFI::create_map(map)
                todo!()
            } else {
                //       ret = RSValue_NewMap(RSValueMap_AllocUninit(0));
                // TODO RSValueFFI::create_map(HashMap::new())
                todo!()
            }
        }
        JsonType::Array => {
            let len = json.len().unwrap();

            if len > 0 {
                let mut arr = Vec::with_capacity(len);
                for i in 0..len {
                    //         japi->getAt(json, i, value_ptr);
                    let json = json.get_at(i).unwrap();
                    //         arr[i] = jsonValToValueExpanded(ctx, value);
                    arr.push(json_val_to_value_expanded(ctx, json.as_ref()));
                }

                //       ret = RSValue_NewArray(arr, len);
                // RSValueFFI::create_array(arr)
                todo!()
            } else {
                //       // Empty array
                //       ret = RSValue_NewArray(NULL, 0);
                // RSValueFFI::create_array(vec![])
                todo!()
            }
        }
        // Scalar
        _ => json_val_to_value(ctx, json),
    }
}

fn json_val_to_value(ctx: NonNull<ffi::RedisModuleCtx>, json: JsonValueRef<'_>) -> RSValueFFI {
    // Currently `getJSON` cannot fail here also the other japi APIs below
    match json.get_type() {
        JsonType::String => {
            // japi->getString(json, &constStr, &len);
            // str = rm_strndup(constStr, len);
            // return RSValue_NewString(str, len);
            let v = json.get_str().unwrap();
            RSValueFFI::create_string(v.to_string().into_bytes())
        }
        JsonType::Int => {
            // japi->getInt(json, &ll);
            // return RSValue_NewNumberFromInt64(ll);
            let v = json.get_int().unwrap();
            RSValueFFI::create_num(v as f64)
        }
        JsonType::Double => {
            // japi->getDouble(json, &dd);
            // return RSValue_NewNumber(dd);
            let v = json.get_double().unwrap();
            RSValueFFI::create_num(v)
        }
        JsonType::Bool => {
            // japi->getBoolean(json, &i);
            // return RSValue_NewNumberFromInt64(i);
            let v = json.get_bool().unwrap();
            RSValueFFI::create_num(v as u8 as f64)
        }
        JsonType::Object | JsonType::Array => {
            // japi->getJSON(json, ctx, &rstr);
            // return RSValue_NewStolenRedisString(rstr);
            let v = unsafe { json.serialize(ctx.cast().as_ptr()).unwrap() };
            RSValueFFI::create_string(v.to_string_lossy().into_bytes())
        }
        JsonType::Null => {
            // return RSValue_NullStatic();
            return RSValueFFI::create_null();
        }
    }
}
