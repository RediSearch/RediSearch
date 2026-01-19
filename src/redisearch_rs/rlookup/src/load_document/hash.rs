use crate::RLookupKeyFlag;
use crate::bindings::RLookupCoerceType;
use crate::load_document::UNDERSCORE_KEY;
use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use query_error::{QueryError, QueryErrorCode};
use redis_module::RedisString;
use redis_module::{
    KeyType,
    key::{KeyFlags, RedisKey},
};
use std::ptr::NonNull;
use value::{RSValueFFI, RSValueTrait};

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    _api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    let key = RedisKey::open_with_flags(
        ctx.cast().as_ptr(),
        key_name,
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );

    if key.is_null() {
        status.set_code(QueryErrorCode::NoDoc);
        return Err(LoadDocumentError {});
    }

    if key.key_type() != KeyType::Hash {
        status.set_code(QueryErrorCode::RedisKeyType);
        return Err(LoadDocumentError {});
    }

    let path = kk.path().as_ref().unwrap().as_ref();
    let val = if let Some(val) = key.hash_get(path.to_str().unwrap())? {
        let coerce_type = if kk.flags.contains(RLookupKeyFlag::Numeric) {
            RLookupCoerceType::Dbl
        } else {
            RLookupCoerceType::Str
        };

        hval_to_value(&val, coerce_type)
    } else {
        if path == UNDERSCORE_KEY {
            return Ok(());
        }

        // const RedisModuleString *keyName = RedisModule_GetKeyNameFromModuleKey(*keyobj);
        //     rsv = hvalToValue(keyName, RLOOKUP_C_STR);
        todo!()
    };

    dst_row.write_key(kk, val);

    Ok(())
}

fn hval_to_value(src: &RedisString, coerce_type: RLookupCoerceType) -> RSValueFFI {
    match coerce_type {
        // RLookupCoerceType::Bool | RLookupCoerceType::Int => {
        //     let mut ll: ::std::ffi::c_longlong = 0;
        //     let ret =
        //         unsafe { (ffi::RedisModule_StringToLongLong.unwrap())(src.inner.cast(), &mut ll) };
        //     debug_assert_eq!(ret, ffi::REDISMODULE_OK as i32);

        //     RSValueFFI::create_num(ll as f64)
        // }
        RLookupCoerceType::Dbl => {
            let mut dd: f64 = 0.0;
            let ret =
                unsafe { (ffi::RedisModule_StringToDouble.unwrap())(src.inner.cast(), &mut dd) };
            debug_assert_eq!(ret, ffi::REDISMODULE_OK as i32);

            RSValueFFI::create_num(dd)
        }
        _ => {
            // let src = src.safe_clone(&redis_module::Context::dummy());
            RSValueFFI::create_string(src.to_vec())
        }
    }
}

pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    force_string: bool,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    todo!()
}
