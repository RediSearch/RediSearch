/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};
use query_error::QueryError;
use redis_module::RedisString;
use std::ptr::NonNull;

/// Opaque handle to an opened JSON key.
///
/// In the C code this is `RedisJSON` (a `void*` returned by `japi->openKeyWithFlags`).
/// The Rust equivalent will wrap this once the JSON API bindings are ported.
pub struct JsonKeyHandle {
    _private: (),
}

pub fn open_key(
    _ctx: NonNull<ffi::RedisModuleCtx>,
    _key_name: &RedisString,
    _status: &mut QueryError,
) -> Result<JsonKeyHandle, LoadDocumentError> {
    todo!()
}

pub fn load_field(
    _kk: &RLookupKey<'_>,
    _dst_row: &mut RLookupRow<'_>,
    _key: &JsonKeyHandle,
    _key_name: &RedisString,
    _api_version: u32,
) -> Result<(), LoadDocumentError> {
    todo!()
}

pub fn load_all_keys(
    _rlookup: &mut RLookup<'_>,
    _dst_row: &mut RLookupRow<'_>,
    _ctx: NonNull<ffi::RedisModuleCtx>,
    _key_name: &RedisString,
    _api_version: u32,
    _status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    todo!()
}
