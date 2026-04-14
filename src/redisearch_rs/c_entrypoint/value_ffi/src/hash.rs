/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fnv::Fnv64;
use std::hash::Hasher;
use value::SharedValueRef;

/// Computes a 64-bit FNV-1a hash of an [`RSValue`], using `hval` as the initial offset basis.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(value: SharedValueRef, hval: u64) -> u64 {
    let mut hasher = Fnv64::with_offset_basis(hval);
    value::hash::hash_value(&value, &mut hasher);
    hasher.finish()
}
