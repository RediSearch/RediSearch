use std::ffi::CStr;
use std::ptr::NonNull;
use std::slice;

use crate::RLookupKeyFlag;
use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use ffi::DocumentMetadata;
use redis_module::RedisString;
use redis_module::{
    KeyType, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use value::RSValueFFI;
use value::{RSValueFFI, strings::RedisString};

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

    dst.write_key(kk, val);

    Ok(())
}

pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: redis_module::Context,
    dmd: &DocumentMetadata,
    force_string: bool,
) -> Result<(), LoadDocumentError> {
    todo!()
}
