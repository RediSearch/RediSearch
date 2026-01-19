use ffi::DocumentMetadata;
use value::RSValueFFI;

use crate::{RLookup, RLookupKey, RLookupRow, load_document::LoadDocumentError};

pub fn load_key(
    kk: &RLookupKey,
    dst: &mut RLookupRow<RSValueFFI>,
) -> Result<(), LoadDocumentError> {
    todo!()
}
pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    ctx: redis_module::Context,
    dmd: &DocumentMetadata,
    api_version: u32,
) -> Result<(), LoadDocumentError> {
    todo!()
}
