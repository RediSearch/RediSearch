/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::RsValue;

/// Get a reference to an [`RsValue`] from a pointer.
///
/// Checks for null in debug mode, does an unwrap_unchecked in release mode.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
pub(crate) const unsafe fn expect_value<'a>(value: *const RsValue) -> &'a RsValue {
    // Safety: ensured by caller (1.)
    let value = unsafe { value.as_ref() };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // Safety: ensured by caller (1.)
        unsafe { value.unwrap_unchecked() }
    }
}
