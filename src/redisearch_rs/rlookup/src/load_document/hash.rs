use std::ptr::NonNull;

use ffi::DocumentMetadata;
use redis_module::RedisString;
use value::RSValueFFI;

use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    api_version: u32,
) -> Result<(), LoadDocumentError> {
    todo!()
}

pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    force_string: bool,
) -> Result<(), LoadDocumentError> {
    todo!()
}
