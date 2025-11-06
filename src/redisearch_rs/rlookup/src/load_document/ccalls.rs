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

use ffi::{
    __BindgenBitfieldUnit, DocumentType_DocumentType_Hash, DocumentType_DocumentType_Json,
    DocumentType_DocumentType_Unsupported, REDISMODULE_OK, RSDocumentMetadata_s,
};
use value::RSValueFFI;

use crate::{
    RLookup, RLookupRow,
    bindings::RLookupLoadMode,
    load_document::{LoadDocumentError, LoadDocumentOptions},
};

/// Helper type that mimics [`ffi::RSSortingVector`] bbut with the correct C representation
#[repr(C)]
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
    // function contains of three parts:
    // 1. Transform the Rust datatypes `RSSortingVector` and `RLookupRow` to C compatible types.
    // 2. Call the callback into the C functions
    // 3. Write back the changes from the C compatible types to the `RSSortingVector` and `RLookupRow``

    // 1.a: Prepare RSSortingVector (todo remove as soon as MOD-10714 lands)
    let sv = dst_row
        .sorting_vector()
        .expect("each rlookup row should have a sorting vector");

    // rust side sorting vector short typename:
    type SortingVectorRust = sorting_vector::RSSortingVector<RSValueFFI>;
    // Safety: We captured the ownership semantic that a SortingVector is created once and then is readonly in a
    // RLookupRow. Here we need a mutable reference to update raw pointers if they are changed
    // by the c-side.
    #[allow(invalid_reference_casting)]
    let sv: &mut SortingVectorRust =
        unsafe { &mut *(sv as *const SortingVectorRust as *mut SortingVectorRust) };

    // Extract raw pointers directly to avoid borrowing from sv
    let sv_len = sv.len();
    let sv_value_ptrs: Vec<*mut ffi::RSValue> = (0..sv_len).map(|i| sv[i].as_ptr()).collect();
    let mut sv_value_slice = sv_value_ptrs.into_boxed_slice();
    let sv_data_ptr = sv_value_slice.as_mut_ptr();

    // the type SizedSortingVectorFFI uses the same layout as
    // ffi::RSSortingVector
    let ssvf = SizedSortingVectorFFI {
        len: sv.len() as u16,
        values: sv_data_ptr,
    };

    let sv_heap = Box::new(ssvf);
    let sv_heap = Box::into_raw(sv_heap);

    // todo: The following will be removed when MOD-10714 and MOD-10405 landed and when there is only one RLookupRow and RSSortingVector type.
    // 1.b: Generate the C compatible RLookupRow
    let mut temp_dst_row = ffi::RLookupRow {
        // Use a heap object that has the same memory layout as the dynamically sized ffi::RSSortingVector type.
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

    // generate the ffi options from the Rust type
    let keys_ptr_storage;
    let keys = match options.keys.as_ref() {
        Some(keys) => {
            keys_ptr_storage = keys.as_ptr() as *const ffi::RLookupKey;
            &keys_ptr_storage as *const _ as *mut *const ffi::RLookupKey
        }
        None => std::ptr::null_mut(),
    };
    let nkeys = options.keys.as_ref().map_or(0, |keys| keys.len());

    // generate a stack based dmd is used to access the keyptr
    let dmd = RSDocumentMetadata_s {
        id: 0,
        keyPtr: options.key_ptr.map_or(ptr::null_mut(), |kp| kp.as_ptr()),
        score: 0.0,
        _bitfield_align_1: [],
        _bitfield_1: __BindgenBitfieldUnit::new([0u8; 8]),
        ref_count: 1,
        sortVector: std::ptr::null_mut(), // null is ok as this is handled in `LoadDocument` which is in Rust
        byteOffsets: std::ptr::null_mut(), // null is ok, isn't used in rlookup.c
        llnode: ffi::DLLIST2_node {
            // null is ok, isn't used in rlookup.c
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        },
        payload: std::ptr::null_mut(), // null is ok, isn't used in rlookup.c
    };

    // todo:
    //let status = todo!("wait on Rust query error pr");

    let mut options = ffi::RLookupLoadOptions {
        sctx: options.context.map_or(ptr::null_mut(), |ctx| {
            let rm_ctx = ctx.as_ptr();
            let ffi_ctx: *mut ffi::RedisSearchCtx = rm_ctx.cast();
            ffi_ctx
        }),
        dmd: &dmd as *const RSDocumentMetadata_s as *mut RSDocumentMetadata_s,
        keyPtr: options.key_ptr.map_or(ptr::null(), |ptr| ptr.as_ptr()),
        type_: match options.document_type {
            crate::bindings::DocumentType::Hash => DocumentType_DocumentType_Hash,
            crate::bindings::DocumentType::Json => DocumentType_DocumentType_Json,
            crate::bindings::DocumentType::Unsupported => DocumentType_DocumentType_Unsupported,
        },
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

    // 2. Call the c function with the compatible C types
    let res = cb(
        // RLookups first field of type RLookupHeader is compatible with ffi::RLookup
        ptr::from_mut(lookup).cast::<ffi::RLookup>(),
        &raw mut temp_dst_row,
        options,
    );

    // 3. write back any potential changes made to the FFI RLookupRow

    // 3.a: replace sorting vector:
    // Safety: We used that pointer as raw pointer for calling C and just generate the Box here again
    let sv_heap = unsafe { Box::from_raw(sv_heap) };
    let sv_new_len = sv_heap.len as usize;
    assert_eq!(
        sv_new_len,
        sv.len(),
        "The sorting vector length should not have changed",
    );
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

    // 3.b: Replace the RLookupRow
    // we first need to resize the Row to accommodate the new values
    dst_row.set_dyn_capacity(temp_dst_row.ndyn);

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
