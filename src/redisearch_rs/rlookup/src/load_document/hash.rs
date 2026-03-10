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
use redis_module::{RedisString, key::RedisKey};
use std::ptr::NonNull;

/// Open a handle to the `RedisKey` with the given name.
pub fn open_key(
    _ctx: NonNull<ffi::RedisModuleCtx>,
    _key_name: &RedisString,
    _status: &mut QueryError,
) -> Result<RedisKey, LoadDocumentError> {
    todo!()
}

pub fn load_field(
    _kk: &RLookupKey<'_>,
    _dst_row: &mut RLookupRow<'_>,
    _key: &RedisKey,
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
    _force_string: bool,
    _api_version: u32,
    _status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    todo!()
}
