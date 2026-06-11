/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use crate::util::expect_value;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::LazyLock;

/// Per-process random keys used to seed [`RSValue_Hash`]'s hasher.
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

/// Computes a HashDoS-resistant 64-bit hash of an [`RSValue`], mixing in `hval` so that
/// the hashes of multiple values (e.g. the fields making up a GROUPBY key) can be
/// combined into a single hash by chaining: `RSValue_Hash(b, RSValue_Hash(a, hval))`.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(value: *const RSValue, hval: u64) -> u64 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let mut hasher = HASH_SEED.build_hasher();
    hasher.write_u64(hval);
    value::hash::hash_value(value, &mut hasher);
    hasher.finish()
}
