/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Value;
use std::hash::{Hash, Hasher};

/// Hashes a [`Value`] into the given [`Hasher`].
///
/// [`Value::Ref`] and [`Value::Trio`] are transparent: the value is first
/// reduced via [`Value::fully_dereferenced_ref_and_trio`], so they hash
/// identically to their dereferenced inner value / left element respectively.
///
/// Every other variant first hashes its [`core::mem::discriminant`] before its
/// payload (if any). This gives `Undefined` and `Null` - which carry no
/// payload - a fixed, well-mixed contribution of their own, and ensures
/// distinct variants can never alias each other's encoding (e.g. `Undefined`
/// can't hash the same as `Number(0.0)`).
pub fn hash_value<H: Hasher>(value: &Value, hasher: &mut H) {
    let value = value.fully_dereferenced_ref_and_trio();
    core::mem::discriminant(value).hash(hasher);

    match value {
        Value::Undefined | Value::Null => {}
        Value::Number(num) => num.to_ne_bytes().hash(hasher),
        Value::String(str) => str.as_bytes().hash(hasher),
        Value::RedisString(str) => str.as_bytes().hash(hasher),
        Value::Array(arr) => arr.iter().for_each(|elem| hash_value(elem, hasher)),
        Value::Map(map) => map.iter().for_each(|(key, val)| {
            hash_value(key, hasher);
            hash_value(val, hasher);
        }),
        Value::Ref(_) | Value::Trio(_) => unreachable!("fully dereferenced above"),
    }
}
