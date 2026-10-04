/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Value;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::LazyLock;

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

/// Per-process random keys used to seed [`hash`]'s hasher.
///
/// Generated once from OS randomness on first use. Without a secret,
/// unpredictable key, an attacker who controls indexed field values (and
/// therefore the inputs hashed into the GROUPBY/COUNT_DISTINCT/TOLIST hash
/// tables) could craft values that collide under a known hash function,
/// degrading those hash tables to linked lists (a "HashDoS" attack). Keying
/// the hash with a value the attacker cannot know prevents this.
///
/// The seed is fixed for the lifetime of the process, so hashes remain
/// stable within and across queries on the same instance, but differ across
/// restarts.
static HASH_SEED: LazyLock<RandomState> = LazyLock::new(RandomState::new);

/// Computes a HashDoS-resistant 64-bit hash of a [`Value`], mixing in `hval` so that
/// the hashes of multiple values (e.g. the fields making up a GROUPBY key) can be
/// combined into a single hash by chaining: `hash(b, hash(a, hval))`.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// Because the hasher is keyed with a per-process secret, the result is only
/// meaningful within the current process: it must not be compared, merged, or
/// persisted across processes (e.g. across cluster shards). For that, use
/// [`hash_stable`] instead.
pub fn hash(value: &Value, hval: u64) -> u64 {
    let mut hasher = HASH_SEED.build_hasher();
    hasher.write_u64(hval);
    hash_value(value, &mut hasher);
    hasher.finish()
}

/// Computes a deterministic 64-bit hash of a [`Value`], mixing in `hval` as
/// described in [`hash`].
///
/// Unlike [`hash`], this is *not* keyed with a per-process secret, so
/// the same value hashes identically across processes and restarts. Use this
/// where the hash is compared, merged, or persisted across processes - e.g.
/// the per-shard HyperLogLog registers fed by `COUNT_DISTINCTISH`, which are
/// merged across shards by `HLL_SUM` and therefore must agree on how values
/// map to register/rank pairs.
pub fn hash_stable(value: &Value, hval: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(hval);
    hash_value(value, &mut hasher);
    hasher.finish()
}
