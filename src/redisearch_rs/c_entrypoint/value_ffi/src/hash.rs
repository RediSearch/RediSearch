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
use value::RsValue;

/// Computes a 64-bit FNV-1a hash of an [`RsValue`], using `hval` as the initial offset basis.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(value: *const RsValue, hval: u64) -> u64 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    hash_value(value, hval)
}

/// Computes a 64-bit FNV-1a hash of an [`RsValue`], using `hval` as the initial offset basis.
fn hash_value(value: &RsValue, hval: u64) -> u64 {
    match value {
        RsValue::Undefined => 0,
        RsValue::Null => hval + 1,
        RsValue::Number(num) => fnv_hash(&num.to_ne_bytes(), hval),
        RsValue::String(str) => fnv_hash(str.as_bytes(), hval),
        RsValue::RedisString(str) => fnv_hash(str.as_bytes(), hval),
        RsValue::Array(arr) => arr.iter().fold(hval, |h, elem| hash_value(elem.value(), h)),
        RsValue::Map(map) => map.iter().fold(hval, |h, (key, val)| {
            let h = hash_value(key.value(), h);
            hash_value(val.value(), h)
        }),
        RsValue::Trio(trio) => hash_value(trio.left().value(), hval),
        RsValue::Ref(ref_value) => hash_value(ref_value.value(), hval),
    }
}

/// Computes a 64-bit FNV-1a hash of `bytes` using `hval` as the offset basis.
fn fnv_hash(bytes: &[u8], hval: u64) -> u64 {
    let mut fnv = Fnv64::with_offset_basis(hval);
    fnv.write(bytes);
    fnv.finish()
}
