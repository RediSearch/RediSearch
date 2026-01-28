/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::RsValue;

/// Get a reference to an RsValue object from a pointer.
///
/// # SAFETY
///
/// value must point to a valid RsValue object.
pub(crate) const unsafe fn expect_value<'a>(value: *const RsValue) -> &'a RsValue {
    // SAFETY: value points to a valid RsValue object.
    let value = unsafe { value.as_ref() };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // SAFETY: value points to a valid RsValue object.
        unsafe { value.unwrap_unchecked() }
    }
}
