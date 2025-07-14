/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use rlookup::{RLookupKey, row::RLookupRow};

use value::RSValueTrait;

// placeholder gets removed when PR #
#[derive(Debug, Clone)]
pub struct RSValueFFI {
    // Placeholder for the actual value type
    // This should be replaced with the actual implementation
}

impl RSValueTrait for RSValueFFI {
    fn create_null() -> Self {
        todo!()
    }

    fn create_string(_s: String) -> Self {
        todo!()
    }

    fn create_num(_num: f64) -> Self {
        todo!()
    }

    fn create_ref(_value: Self) -> Self {
        todo!()
    }

    fn is_null(&self) -> bool {
        todo!()
    }

    fn get_ref(&self) -> Option<&Self> {
        todo!()
    }

    fn get_ref_mut(&mut self) -> Option<&mut Self> {
        todo!()
    }

    fn as_str(&self) -> Option<&str> {
        todo!()
    }

    fn as_num(&self) -> Option<f64> {
        todo!()
    }

    fn get_type(&self) -> ffi::RSValueType {
        todo!()
    }

    fn is_ptr_type() -> bool {
        todo!()
    }

    fn increment(&mut self) {
        todo!()
    }

    fn decrement(&mut self) {
        todo!()
    }
}

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    mut row: NonNull<RLookupRow<RSValueFFI>>,
    value: NonNull<ffi::RSValue>,
) {
    let key = unsafe { key.as_ref() }.expect("Key must not be null");
    let row = unsafe { row.as_mut() };
    let val: &ffi::RSValue = unsafe { value.as_ref() };

    todo!("generate RSValueFFI from ffi::RSValue");
}

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_WriteOwnKey(
    key: *const RLookupKey,
    mut row: NonNull<RLookupRow<RSValueFFI>>,
    value: NonNull<ffi::RSValue>,
) {
    let key = unsafe { key.as_ref() }.expect("Key must not be null");
    let row = unsafe { row.as_mut() };
    let val: &ffi::RSValue = unsafe { value.as_ref() };

    todo!("generate RSValueFFI from ffi::RSValue");
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Wipe(mut vec: NonNull<RLookupRow<RSValueFFI>>) {
    let vec = unsafe { vec.as_mut() };
    vec.wipe();
}

/// Cleanup a RLookupRow by wiping it (see [`RLookupRow_Wipe`]) and deallocating the memory.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Cleanup(vec: NonNull<RLookupRow<RSValueFFI>>) {
    todo!("Implement RLookupRow_Cleanup");
}
