/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use fnv::Fnv64;
use std::hash::Hasher;
use value::Value;

/// Computes a 64-bit FNV-1a hash of an [`RsValue`], using `hval` as the initial offset basis.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(value: *const Value, hval: u64) -> u64 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let mut hasher = Fnv64::with_offset_basis(hval);
    value::hash::hash_value(value, &mut hasher);
    hasher.finish()
}
