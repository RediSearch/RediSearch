use crate::RLookupKeyFlag;
use crate::bindings::RLookupCoerceType;
use crate::load_document::UNDERSCORE_KEY;
use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use query_error::{QueryError, QueryErrorCode};
use redis_module::{
    KeyType, RedisString, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use std::ffi::CStr;
use std::ptr::NonNull;
use std::slice;
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

// // When loading the document we are after the iterators phase, where we already verified the expiration time of the field and document
// // We don't allow any lazy expiration to happen here
// const DOCUMENT_OPEN_KEY_QUERY_FLAGS: KeyFlags = /* KeyFlags::READ | */
pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    force_string: bool,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    //   int rc = REDISMODULE_ERR;
    //   RedisModuleCtx *ctx = options->sctx->redisCtx;
    //   RedisModuleString *krstr =
    //       RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));

    // 1. open key
    //
    //   RedisModuleKey *key = RedisModule_OpenKey(ctx, krstr, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    let key = RedisKey::open_with_flags(
        ctx.cast().as_ptr(),
        key_name,
        // TODO make this a constant
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );

    //   if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
    //     goto done;
    //   }

    // TODO this is stupid why would you do it like this??
    if key.is_null() || key.key_type() != KeyType::Hash {
        return Err(LoadDocumentError {}); // TODO error
    }

    // 2. Create a scan cursor

    //   RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
    //   RLookup_HGETALL_privdata pd = {
    //     .it = it,
    //     .dst = dst,
    //     .options = options,
    //   };
    let scan_cursor = ScanKeyCursor::new(key);

    // while(RedisModule_ScanKey(key, cursor, RLookup_HGETALL_scan_callback, &pd));
    scan_cursor.for_each(|_key, field, value| {
        //   size_t fieldCStrLen;
        //   const char *fieldCStr = RedisModule_StringPtrLen(field, &fieldCStrLen);
        let field_cstr = {
            let (field_ptr, field_len) = field.as_cstr_ptr_and_len();

            let bytes = unsafe { slice::from_raw_parts(field_ptr.cast::<u8>(), field_len) };

            CStr::from_bytes_with_nul(bytes).unwrap()
        };

        let key = if let Some(c) = rlookup.find_key_by_name(field_cstr) {
            // /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/) {

            if c.current()
                .unwrap()
                .flags
                .contains(RLookupKeyFlag::QuerySrc)
            {
                // Key name is already taken by a query key.
                return;
            } else {
                c.into_current().unwrap()
            }
        } else {
            //     // First returned document, create the key.
            //     rlk = RLookup_GetKey_LoadEx(pd->it, fieldCStr, fieldCStrLen, fieldCStr, RLOOKUP_F_FORCE_LOAD | RLOOKUP_F_NAMEALLOC);

            rlookup
                .get_key_load(
                    field_cstr.to_owned(),
                    field_cstr,
                    RLookupKeyFlag::ForceLoad | RLookupKeyFlag::NameAlloc,
                )
                .unwrap()
        };

        //   RLookupCoerceType ctype = RLOOKUP_C_STR;
        //   if (!pd->options->forceString && rlk->flags & RLOOKUP_T_NUMERIC) {
        //     ctype = RLOOKUP_C_DBL;
        //   }
        let coerce_to_type = if key.flags.contains(RLookupKeyFlag::Numeric) && !force_string {
            ffi::RLookupCoerceType_RLOOKUP_C_DBL
        } else {
            ffi::RLookupCoerceType_RLOOKUP_C_STR
        };

        //   RSValue *vptr = hvalToValue(value, ctype);

        // This function will retain the value if it's a string. This is thread-safe because
        // the value was created just before calling this callback and will be freed right after
        // the callback returns, so this is a thread-local operation that will take ownership of
        // the string value.
        let value = rsvalue_from_hash_value(value, coerce_to_type);

        //   RLookup_WriteOwnKey(rlk, pd->dst, vptr);
        dst_row.write_key(key, value);
    });

    Ok(())
}

fn rsvalue_from_hash_value(
    value: &RedisString,
    coerce_to_type: ffi::RLookupCoerceType,
) -> RSValueFFI {
    let value = unsafe {
        ffi::hvalToValue(
            value.inner.cast::<ffi::RedisModuleString>().cast_const(),
            coerce_to_type,
        )
    };
    unsafe { RSValueFFI::from_raw(NonNull::new(value).unwrap()) }
}
