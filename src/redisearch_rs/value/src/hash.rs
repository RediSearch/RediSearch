/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Value;
use fnv::Fnv64;
use std::hash::Hasher;

/// Hashes a [`Value`] into the given [`Fnv64`] hasher.
///
/// The `Undefined` and `Null` variants bypass normal `fnv` hashing by directly
/// resetting the hasher state via [`Fnv64::with_offset_basis`].
pub fn hash_value(value: &Value, fnv64: &mut Fnv64) {
    match value {
        Value::Undefined => *fnv64 = Fnv64::with_offset_basis(0),
        Value::Null => *fnv64 = Fnv64::with_offset_basis(fnv64.finish().wrapping_add(1)),
        Value::Number(num) => fnv64.write(&num.to_ne_bytes()),
        Value::String(str) => fnv64.write(str.as_bytes()),
        Value::RedisString(str) => fnv64.write(str.as_bytes()),
        Value::Array(arr) => arr.iter().for_each(|elem| hash_value(elem, fnv64)),
        Value::Map(map) => map.iter().for_each(|(key, val)| {
            hash_value(key, fnv64);
            hash_value(val, fnv64);
        }),
        Value::Trio(trio) => hash_value(trio.left(), fnv64),
        Value::Ref(ref_value) => hash_value(ref_value, fnv64),
    }
}
