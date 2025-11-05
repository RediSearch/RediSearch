/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    mem,
    ptr::{self, NonNull},
    slice,
};

use ffi::REDISMODULE_OK;
use value::RSValueFFI;

use crate::{
    RLookup, RLookupRow,
    load_document::{LoadDocumentError, LoadDocumentOptions},
};

fn with_temp_ffi_types<R>(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &mut LoadDocumentOptions<'_>,
    cb: impl FnOnce(*mut ffi::RLookup, *mut ffi::RLookupRow, *mut ffi::RLookupLoadOptions) -> R,
) -> R {
    // todo: The following will be removed in MOD-10405 when there is only one RLookupRow type.
    let mut temp_dst_row = ffi::RLookupRow {
        // TODO this is not correct, should use the real RSSortingVector but the FFI type
        // is dynamically sized which makes this tricky
        sv: ptr::null(),
        ndyn: dst_row.dyn_values().len(),
        // Safety: this is safe to do because RSValueFFI is `NonNull<ffi::RSValue>`
        // and due to niche optimization `Option<NonNull<ffi::RSValue>>` has the same layout
        // and bit pattern  as `*mut ffi::RSValue`. Getting the  base pointer of
        // `&mut [Option<NonNull<ffi::RSValue>>]` must therefore be safe to coerce to `*mut *mut ffi::RSValue`.
        dyn_: dst_row
            .dyn_values_mut()
            .as_mut_ptr()
            .cast::<*mut ffi::RSValue>(),
    };

    let options = options
        .tmp_cstruct
        .expect("We are calling from C and the C type got provided")
        .as_ptr();

    let res = cb(
        // RLookups first field of type RLookupHeader is compatible with ffi::RLookup
        ptr::from_mut(lookup).cast::<ffi::RLookup>(),
        &raw mut temp_dst_row,
        options,
    );

    // write back any potential changes made to the FFI RLookupRow

    // we first need to resize the Row to accomodate the new values
    dst_row.set_dyn_capacity(dst_row.dyn_values().len());

    // then we iterate through the FFI dyn values, convert them into RSValueFFI types and write each
    // to the Rust RLookupRow
    let temp_values = unsafe { slice::from_raw_parts_mut(temp_dst_row.dyn_, temp_dst_row.ndyn) };
    for (src, dst) in temp_values.iter_mut().zip(dst_row.dyn_values_mut()) {
        let ptr = mem::replace(src, ptr::null_mut());
        *dst = NonNull::new(ptr).map(|ptr| unsafe { RSValueFFI::from_raw(ptr) });
    }

    res
}

pub(super) fn json_get_all(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &mut LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // Safety: Calling a unsafe C function to provide JSON loading functionality.
    let res = with_temp_ffi_types(
        lookup,
        dst_row,
        options,
        |lookup, dst_row, options| unsafe {
            ffi::RLookup_JSON_GetAll(lookup, dst_row, options) as u32
        },
    );

    if res != REDISMODULE_OK {
        return Err(LoadDocumentError::FromCCode);
    }

    Ok(())
}

pub(super) fn load_individual_keys(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &mut LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // Safety: Calling a unsafe C function to provide JSON loading functionality.
    // The types `RLookup` and `RLookupRow` are only used as opaque pointers in the C code, so that is safe.
    let res = with_temp_ffi_types(
        lookup,
        dst_row,
        options,
        |lookup, dst_row, options| unsafe {
            ffi::loadIndividualKeys(lookup, dst_row, options) as u32
        },
    );
    if res != REDISMODULE_OK {
        return Err(LoadDocumentError::FromCCode);
    }
    Ok(())
}
