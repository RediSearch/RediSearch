/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RsValue;
use fnv::Fnv64;
use std::hash::Hasher;

/// Hashes an [`RsValue`] into the given [`Fnv64`] hasher.
///
/// The `Undefined` and `Null` variants require FNV-specific state resets.
pub fn hash_value(value: &RsValue, fnv64: &mut Fnv64) {
    match value {
        RsValue::Undefined => *fnv64 = Fnv64::with_offset_basis(0),
        RsValue::Null => *fnv64 = Fnv64::with_offset_basis(fnv64.finish() + 1),
        RsValue::Number(num) => fnv64.write(&num.to_ne_bytes()),
        RsValue::String(str) => fnv64.write(str.as_bytes()),
        RsValue::RedisString(str) => fnv64.write(str.as_bytes()),
        RsValue::Array(arr) => arr.iter().for_each(|elem| hash_value(elem.value(), fnv64)),
        RsValue::Map(map) => map.iter().for_each(|(key, val)| {
            hash_value(key.value(), fnv64);
            hash_value(val.value(), fnv64);
        }),
        RsValue::Trio(trio) => hash_value(trio.left().value(), fnv64),
        RsValue::Ref(ref_value) => hash_value(ref_value.value(), fnv64),
    }
}
