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

/// Computes a HashDoS-resistant 64-bit hash of an [`RSValue`], mixing in `hval` so that
/// the hashes of multiple values (e.g. the fields making up a GROUPBY key) can be
/// combined into a single hash by chaining: `RSValue_Hash(b, RSValue_Hash(a, hval))`.
///
/// The hashing is recursive for composite types (arrays, maps, references, trios).
///
/// Because the hasher is keyed with a per-process secret, the result is only
/// meaningful within the current process: it must not be compared, merged, or
/// persisted across processes (e.g. across cluster shards). For that, use
/// [`RSValue_HashStable`] instead.
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

    value::hash::hash(value, hval)
}

/// Computes a deterministic 64-bit hash of an [`RSValue`], mixing in `hval` as
/// described in [`RSValue_Hash`].
///
/// Unlike [`RSValue_Hash`], this is *not* keyed with a per-process secret, so
/// the same value hashes identically across processes and restarts. Use this
/// where the hash is compared, merged, or persisted across processes - e.g.
/// the per-shard HyperLogLog registers fed by `COUNT_DISTINCTISH`, which are
/// merged across shards by `HLL_SUM` and therefore must agree on how values
/// map to register/rank pairs.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_HashStable(value: *const RSValue, hval: u64) -> u64 {
    // Safety: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    value::hash::hash_stable(value, hval)
}
