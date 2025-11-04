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

/// Generates a [`ffi::RLookupRow`] on the stack and fills the pointers with reasonable values from the Rust types.
///
/// We only require this temporary to close the circle with MOD-10405. At the moment we have Rust and C types that
/// are not compatible with each other:
///
/// - RSSortingVector: [sorting_vector::RSSortingVector] and [ffi::RSSortingVector].
/// - RLookupRow: [crate::row::RLookupRow] and [ffi::RLookupRow].
///
/// The RLookupRow is readonly, the SortingVector is empty. Both cannot be mutated by C-Code.
///
/// # Safety
///
/// Don't use in production this is unsafe Rust and C types differ in memory layout and cannot be simply
/// converted forth and back.
unsafe fn ffi_param_from_row(
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
) -> (ffi::RLookupRow, Option<ffi::RSSortingVector>) {
    // we need a ffi::RSSortingVector on the stack
    let stack_sv = if let Some(sv) = dst_row.sorting_vector() {
        // access the data in the sorting vector:
        // let values = sv.access_inner();
        // Safety: We know a RSValueFFI wraps a NonNull<ffi::RSValue> so this is safe:
        // let _values: &[*mut ffi::RSValue] = unsafe { std::mem::transmute(values) };
        // deadend, we cannot generate a ffi::__IncompleteArrayField using the values

        // generate empty sorting vector:
        let stack_sv = ffi::RSSortingVector {
            len: sv.len() as u16,
            values: ffi::__IncompleteArrayField::new(),
        };
        Some(stack_sv)
    } else {
        None
    };

    let sv = if let Some(sv) = &stack_sv {
        let ptr: *const ffi::RSSortingVector = sv;
        ptr
    } else {
        std::ptr::null()
    };

    // access values over pointer
    let dyn_ = dst_row.dyn_values().as_ptr();
    let mut dyn_: *mut ffi::RSValue = dyn_ as *mut ffi::RSValue;
    let dyn_: *mut *mut ffi::RSValue = (&mut dyn_) as *mut *mut ffi::RSValue;

    // access length
    let ndyn = dst_row.len();

    (ffi::RLookupRow { sv, dyn_, ndyn }, stack_sv)
}

pub(super) fn json_get_all(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // todo: The following will be removed in MOD-10405 when there is only one RLookupRow type.
    // Safety: Generating a ffi::RLookupRow.
    let (mut dst_row, _stack_sv) = unsafe { ffi_param_from_row(dst_row) };

    // Get a raw pointer to the RLookup
    let lookup = std::ptr::from_mut(lookup);
    // Safety: RLookups first field of type RLookupHeader is compatible with ffi::RLookup
    let lookup: *mut ffi::RLookup = lookup.cast();

    let options = options
        .tmp_cstruct
        .ok_or(LoadDocumentError::invalid_arguments(Some(
            "Rust Code calls back to C but lacks RLookupOptions from C".to_owned(),
        )))?;

    let options = options.as_ptr();

    // Safety: Calling a unsafe C function to provide JSON loading functionality.
    let res = unsafe { ffi::RLookup_JSON_GetAll(lookup, &mut dst_row, options) as u32 };
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
    // todo: The following will be removed in MOD-10405 when there is only one RLookupRow type.
    // Safety: Generating a ffi::RLookupRow.
    let (mut dst_row, _stack_sv) = unsafe { ffi_param_from_row(dst_row) };

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
    let res = unsafe { ffi::loadIndividualKeys(lookup, &mut dst_row, options) as u32 };
    if res != REDISMODULE_OK {
        return Err(LoadDocumentError::FromCCode);
    }
    Ok(())
}
