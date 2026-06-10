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

/// Canonical tag hashed before a [`Value`]'s payload (if any).
///
/// [`Value::String`] and [`Value::RedisString`] share [`Tag::String`], so
/// that values [`compare`](crate::comparison::compare) treats as equal (e.g.
/// a `String` and a `RedisString` holding identical bytes) also hash
/// identically.
#[derive(Hash)]
enum Tag {
    Undefined,
    Null,
    Number,
    String,
    Array,
    Map,
}

/// Hashes a [`Value`] into the given [`Hasher`].
///
/// [`Value::Ref`] and [`Value::Trio`] are transparent: the value is first
/// reduced via [`Value::fully_dereferenced_ref_and_trio`], so they hash
/// identically to their dereferenced inner value / left element respectively.
pub fn hash_value<H: Hasher>(value: &Value, hasher: &mut H) {
    let value = value.fully_dereferenced_ref_and_trio();

    match value {
        Value::Undefined => Tag::Undefined.hash(hasher),
        Value::Null => Tag::Null.hash(hasher),
        Value::Number(num) => {
            Tag::Number.hash(hasher);
            num.to_ne_bytes().hash(hasher);
        }
        Value::String(str) => {
            Tag::String.hash(hasher);
            str.as_bytes().hash(hasher);
        }
        Value::RedisString(str) => {
            Tag::String.hash(hasher);
            str.as_bytes().hash(hasher);
        }
        Value::Array(arr) => {
            Tag::Array.hash(hasher);
            arr.iter().for_each(|elem| hash_value(elem, hasher));
        }
        Value::Map(map) => {
            Tag::Map.hash(hasher);
            map.iter().for_each(|(key, val)| {
                hash_value(key, hasher);
                hash_value(val, hasher);
            });
        }
        Value::Ref(_) | Value::Trio(_) => unreachable!("fully dereferenced above"),
    }
}
