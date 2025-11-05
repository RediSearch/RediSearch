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

struct SizedSortingVectorFFI {
    len: u16,
    values: *mut *mut ffi::RSValue,
}

fn with_temp_ffi_types<R>(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
    cb: impl FnOnce(*mut ffi::RLookup, *mut ffi::RLookupRow, *mut ffi::RLookupLoadOptions) -> R,
) -> R {
    let sv = dst_row
        .sorting_vector()
        .expect("each rlookup row should have a sorting vector");

    // rust side sorting vector short typename:
    type SortingVectorRust = sorting_vector::RSSortingVector<RSValueFFI>;
    // Safety: We need mutable access
    #[allow(invalid_reference_casting)]
    let sv: &mut SortingVectorRust =
        unsafe { &mut *(sv as *const SortingVectorRust as *mut SortingVectorRust) };
    let vec = sv.iter().collect::<Vec<&RSValueFFI>>().into_boxed_slice();
    let vec = Box::into_raw(vec);

    let ssvf = SizedSortingVectorFFI {
        len: sv.len() as u16,
        values: vec.cast(),
    };

    let sv_heap = Box::new(ssvf);
    let sv_heap = Box::into_raw(sv_heap);

    // todo: The following will be removed in MOD-10405 when there is only one RLookupRow type.
    let mut temp_dst_row = ffi::RLookupRow {
        // TODO this is not correct, should use the real RSSortingVector but the FFI type
        // is dynamically sized which makes this tricky
        sv: sv_heap.cast(),
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

    let (options, _) = if let Some(options) = &options.tmp_cstruct {
        (options.as_ptr(), None)
    } else {
        let keys = options.keys.as_ref().map_or(std::ptr::null_mut(), |keys| {
            let reval = keys.as_ptr();
            let mut reval = reval as *const ffi::RLookupKey;
            &mut reval
        });
        let nkeys = options.keys.as_ref().map_or(0, |keys| keys.len());

        println!("Num Keys: {}", nkeys);

        let mut options = ffi::RLookupLoadOptions {
            sctx: options.context.map_or(ptr::null_mut(), |ctx| {
                let rm_ctx = ctx.as_ptr();
                let ffi_ctx: *mut ffi::RedisSearchCtx = rm_ctx.cast();
                ffi_ctx
            }),
            dmd: std::ptr::null_mut(),
            keyPtr: options.key_ptr.map_or(ptr::null(), |ptr| ptr.as_ptr()),
            type_: DocumentType_DocumentType_Json, // todo: either JSON or HASH
            keys,
            nkeys,
            mode: match options.mode {
                RLookupLoadMode::KeyList => ffi::RLookupLoadFlags_RLOOKUP_LOAD_KEYLIST,
                RLookupLoadMode::SortingVectorKeys => ffi::RLookupLoadFlags_RLOOKUP_LOAD_SVKEYS,
                RLookupLoadMode::AllKeys => ffi::RLookupLoadFlags_RLOOKUP_LOAD_ALLKEYS,
            },
            forceLoad: options.force_load,
            forceString: options.force_string,
            status: std::ptr::null_mut(),
        };
        let options: *mut ffi::RLookupLoadOptions = &mut options;
        (options, Some(options))
    };

    let res = cb(
        // RLookups first field of type RLookupHeader is compatible with ffi::RLookup
        ptr::from_mut(lookup).cast::<ffi::RLookup>(),
        &raw mut temp_dst_row,
        options,
    );

    // write back any potential changes made to the FFI RLookupRow

    // replace sorting vector:
    // Safety: We used that pointer as raw pointer for calling C and just generate the Box here again
    let sv_heap = unsafe { Box::from_raw(sv_heap) };
    let sv_new_len = sv_heap.len as usize;
    // Safety: We assume the c callbacks give back a valid RSSortingVector so we can access the static RSValues with that slice safety.
    let temp_values = unsafe { std::slice::from_raw_parts_mut(sv_heap.values, sv_new_len) };
    for (src, dst) in temp_values.iter_mut().zip(sv.iter_mut()) {
        let ptr = mem::replace(src, ptr::null_mut());
        *dst = NonNull::new(ptr)
            // Safety: We assume the c callbacks keep valid RSValues then the we can generate the RSValueFFI wrapper around the pointer.
            .map(|ptr| unsafe { RSValueFFI::from_raw(ptr) })
            .expect("The pointers aren't null");
    }
    // we use unsafe to access in a mutable way, this is not the actual ownership semantic as the LookupRow holds a readonly reference.

    // we first need to resize the Row to accomodate the new values
    dst_row.set_dyn_capacity(dst_row.dyn_values().len());

    // then we iterate through the FFI dyn values, convert them into RSValueFFI types and write each
    // to the Rust RLookupRow
    // Safety: We assume the c callbacks give back a valid RLookupRow so we can access the dynamic RSValues with that slice safety.
    let temp_values = unsafe { slice::from_raw_parts_mut(temp_dst_row.dyn_, temp_dst_row.ndyn) };
    for (src, dst) in temp_values.iter_mut().zip(dst_row.dyn_values_mut()) {
        let ptr = mem::replace(src, ptr::null_mut());
        // Safety: We assume the c callbacks keep valid RSValues then the we can generate the RSValueFFI wrapper around the pointer.
        *dst = NonNull::new(ptr).map(|ptr| unsafe { RSValueFFI::from_raw(ptr) });
    }

    res
}

pub(super) fn json_get_all(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
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
    options: &LoadDocumentOptions<'_>,
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
