use ffi::DocumentMetadata;
use value::{RSValueFFI, strings::RedisString};

use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};

pub fn load_key(
    kk: &RLookupKey,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: redis_module::Context,
    key_name: &RedisString,
    api_version: u32,
) -> Result<(), LoadDocumentError> {
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
