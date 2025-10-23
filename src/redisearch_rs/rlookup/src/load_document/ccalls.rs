/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::REDISMODULE_OK;
use value::RSValueFFI;

use crate::{
    RLookup, RLookupRow,
    load_document::{LoadDocumentError, LoadDocumentOptions},
};

#[allow(unused)]
#[allow(unreachable_code)]
#[allow(clippy::diverging_sub_expression)]
pub(super) fn json_get_all(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // Get a raw pointer to the RLookupRow
    let dst_row = std::ptr::from_mut(dst_row);
    // RLookupRow is not compatible with ffi::RLookupRow as we use another type in Rust
    // todo: this works only after the swap of RLookupRow and if the C side is only using it as an opaque pointer
    let dst_row = todo!("Merge RLookupRow swap");
    todo!("Remove allows above function signature");

    // Get a raw pointer to the RLookup
    let lookup = std::ptr::from_mut(lookup);
    // Safety: RLookups first field of type RLookupHeader is compatible with ffi::RLookup
    let lookup = unsafe { std::mem::transmute::<*mut RLookup<'_>, *mut ffi::RLookup>(lookup) };

    let options = options
        .tmp_cstruct
        .ok_or(LoadDocumentError::invalid_arguments(Some(
            "Rust Code calls back to C but lacks RLookupOptions from C".to_owned(),
        )))?;

    let options = options.as_ptr();

    // Safety: Calling a unsafe C function to provide JSON loading functionality.
    let res = unsafe { ffi::RLookup_JSON_GetAll(lookup, dst_row, options) as u32 };
    if res != REDISMODULE_OK {
        return Err(LoadDocumentError::FromCCode);
    }
    Ok(())
}

#[allow(unused)]
#[allow(unreachable_code)]
#[allow(clippy::diverging_sub_expression)]
pub(super) fn load_individual_keys(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // Get a raw pointer to the RLookupRow
    let dst_row = std::ptr::from_mut(dst_row);
    // RLookupRow is not compatible with ffi::RLookupRow as we use another type in Rust
    // todo: this works only after the swap of RLookupRow and if the C side is only using it as an opaque pointer
    let dst_row = todo!("Merge RLookuRow swap");
    todo!("Remove allows above function signature");

    // Get a raw pointer to the RLookup
    let lookup = std::ptr::from_mut(lookup);
    // Safety: RLookups first field of type RLookupHeader is compatible with ffi::RLookup
    let lookup = unsafe { std::mem::transmute::<*mut RLookup<'_>, *mut ffi::RLookup>(lookup) };

    let options = options
        .tmp_cstruct
        .ok_or(LoadDocumentError::invalid_arguments(Some(
            "Rust Code calls back to C but lacks RLookupOptions from C".to_owned(),
        )))?;

    let options = options.as_ptr();

    // Safety: Calling a unsafe C function to provide JSON loading functionality.
    // The types `RLookup` and `RLookupRow` are only used as opaque pointers in the C code, so that is safe.
    let res = unsafe { ffi::loadIndividualKeys(lookup, dst_row, options) as u32 };
    if res != REDISMODULE_OK {
        return Err(LoadDocumentError::FromCCode);
    }
    Ok(())
}
