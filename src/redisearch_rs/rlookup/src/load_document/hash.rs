use std::ffi::CStr;
use std::ptr::NonNull;
use std::slice;

use crate::RLookupKeyFlag;
use crate::load_document::UNDERSCORE_KEY;
use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use ffi::DocumentMetadata;
use redis_module::RedisString;
use redis_module::{
    KeyType, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use value::RSValueFFI;

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: redis_module::Context,
    key_name: &RedisString,
    api_version: u32,
) -> Result<(), LoadDocumentError> {
    let key = ctx.open_key_with_flags(
        key_name,
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );
    if key.is_null() {
        // QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
        return Err(LoadDocumentError {});
    }

    if key.key_type() != KeyType::Hash {
        // QueryError_SetCode(options->status, QUERY_ERROR_CODE_REDIS_KEY_TYPE);
        return Err(LoadDocumentError {});
    }

    let path = kk.path().as_ref().unwrap().as_ref();
    let val = if let Some(val) = key.hash_get(path.to_str().unwrap())? {
        hval_to_value(val)
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

// // When loading the document we are after the iterators phase, where we already verified the expiration time of the field and document
// // We don't allow any lazy expiration to happen here
// const DOCUMENT_OPEN_KEY_QUERY_FLAGS: KeyFlags = /* KeyFlags::READ | */
pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: redis_module::Context,
    dmd: &DocumentMetadata,
    force_string: bool,
) -> Result<(), LoadDocumentError> {
    //   int rc = REDISMODULE_ERR;
    //   RedisModuleCtx *ctx = options->sctx->redisCtx;
    //   RedisModuleString *krstr =
    //       RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));

    // 1. open key

    // Safety: We assume the caller provided options with a key pointer containing a sds string.
    let sds_len = unsafe { ffi::sdslen__(dmd.keyPtr) };

    // Safety: The sds string is prefixed with its length, key_ptr directly points to the string data.
    let key_str =
        unsafe { RedisString::from_raw_parts(NonNull::new(ctx.ctx), dmd.keyPtr, sds_len) };

    //   RedisModuleKey *key = RedisModule_OpenKey(ctx, krstr, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    let key = RedisKey::open_with_flags(
        ctx.ctx,
        &key_str,
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
