use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use query_error::QueryError;
use redis_module::RedisString;
use std::ptr::NonNull;
use value::RSValueFFI;

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    todo!()
}
pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    key_name: &RedisString,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    todo!()
}
