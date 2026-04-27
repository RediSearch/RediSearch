/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared workload helpers for the head-to-head TTL-table benchmark.
//!
//! The benchmark in `benches/operations.rs` compares three implementations
//! of the same TTL-table contract:
//!
//! - [`ttl_table`] — idiomatic `HashMap<u64, …>` Rust port.
//! - [`ttl_table_2`] — direct-modulo bucket-array Rust port, faithful to
//!   the C layout.
//! - The C module under `src/ttl_table/`, accessed through the `ffi`
//!   bindings (see [`c_adapter`]).
//!
//! Only the operations supported by the C public API are exercised — the
//! Rust [`ttl_table`] crate has a wider surface (`has_doc_expired`, an
//! extra `doc_expiration` argument on `add`) but we restrict to the C
//! intersection so the comparison stays fair.

use libc::timespec;
use thin_vec::ThinVec;

pub use field::FieldExpirationPredicate;

// The C codebase expects a Redis allocator to be linked; we provide one
// that delegates to Rust's global allocator. The macro also stubs out a
// handful of unrelated SSL/RDB symbols whose call sites are not reached
// from any benchmarked path.
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

pub mod c_adapter;

/// Workload knobs shared by the benchmark groups.
#[derive(Debug, Clone, Copy)]
pub struct Workload {
    /// Modulus for [`ttl_table_2`]'s direct-modulo slot formula. Must be
    /// `>= n_docs` so doc ids `1..=n_docs` land in distinct slots when
    /// inserted in monotonic order.
    pub max_size: usize,
    /// Number of documents to insert.
    pub n_docs: u64,
    /// Number of per-field expirations attached to each document.
    pub k_fields: u16,
}

/// Zero `timespec`. The TTL tables treat a field point of zero as "no
/// expiration set", so this doubles as the `doc_expiration` argument of
/// [`ttl_table::TimeToLiveTable::add`] — semantically equivalent to the C
/// `Add` (which has no doc-level TTL parameter).
#[must_use]
pub const fn zero_ts() -> timespec {
    timespec {
        tv_sec: 0,
        tv_nsec: 0,
    }
}

/// A `timespec` far enough in the future that any realistic query time
/// is strictly less than it. Used as the `point` of every field
/// expiration so `did_expire` returns `false` consistently.
#[must_use]
pub const fn far_future_ts() -> timespec {
    timespec {
        tv_sec: i64::MAX,
        tv_nsec: 0,
    }
}

/// A `timespec` strictly less than [`far_future_ts`]. Used as the `now`
/// argument to the verify routines so all per-field expirations resolve
/// to "not expired" — combined with [`FieldExpirationPredicate::Missing`]
/// this forces the verify loop to walk the full mask without early exit.
#[must_use]
pub const fn query_now_ts() -> timespec {
    timespec {
        tv_sec: 1,
        tv_nsec: 0,
    }
}

/// Builds a `Vec<ttl_table::FieldExpiration>` of length `k`, sorted by
/// index, with every field set to [`far_future_ts`].
#[must_use]
pub fn fields_v1(k: u16) -> Vec<ttl_table::FieldExpiration> {
    let point = far_future_ts();
    (0..k)
        .map(|i| ttl_table::FieldExpiration { index: i, point })
        .collect()
}

/// Builds a `ThinVec<ttl_table_2::FieldExpiration>` of length `k`,
/// sorted by index, with every field set to [`far_future_ts`].
///
/// Returns the [`ThinVec`] shape that [`ttl_table_2::TimeToLiveTable::add`]
/// requires.
#[must_use]
pub fn fields_v2(k: u16) -> ThinVec<ttl_table_2::FieldExpiration> {
    let point = far_future_ts();
    (0..k)
        .map(|i| ttl_table_2::FieldExpiration { index: i, point })
        .collect()
}

/// Builds a `Vec<ffi::FieldExpiration>` of length `k`, sorted by index,
/// with every field set to [`far_future_ts`]. The buffer is materialized
/// in Rust and copied into a C-allocated `arrayof(FieldExpiration)` by
/// [`c_adapter::c_add`] before being handed to `TimeToLiveTable_Add`.
///
/// Note: `ffi::timespec` is a bindgen-generated newtype distinct from
/// `libc::timespec` even though the two have identical layout — we
/// rebuild the timespec literal directly here.
#[must_use]
pub fn fields_c(k: u16) -> Vec<ffi::FieldExpiration> {
    let point = ffi::timespec {
        tv_sec: i64::MAX,
        tv_nsec: 0,
    };
    (0..k)
        .map(|i| ffi::FieldExpiration { index: i, point })
        .collect()
}

/// Translation table mapping bit position `b` in a field mask to field
/// index `b` (identity / monotonic). The verify routines require the
/// mapping to be monotonic so the two-pointer scan over the sorted
/// per-field list only advances forward.
#[must_use]
pub fn ftid_table(width: u32) -> Vec<u16> {
    (0..width as u16).collect()
}

/// Pre-populates a [`ttl_table::TimeToLiveTable`] with `workload.n_docs`
/// documents (ids `1..=n_docs`, monotonic), each carrying
/// `workload.k_fields` field expirations.
#[must_use]
pub fn build_ttl1(workload: &Workload) -> ttl_table::TimeToLiveTable {
    let mut t = ttl_table::TimeToLiveTable::new();
    for doc_id in 1..=workload.n_docs {
        t.add(doc_id, zero_ts(), fields_v1(workload.k_fields));
    }
    t
}

/// Pre-populates a [`ttl_table_2::TimeToLiveTable`] with `workload.n_docs`
/// documents (ids `1..=n_docs`, monotonic), each carrying
/// `workload.k_fields` field expirations.
#[must_use]
pub fn build_ttl2(workload: &Workload) -> ttl_table_2::TimeToLiveTable {
    let mut t = ttl_table_2::TimeToLiveTable::new(workload.max_size);
    for doc_id in 1..=workload.n_docs {
        t.add(doc_id, fields_v2(workload.k_fields));
    }
    t
}

/// Returns a `u32` mask with the lowest `popcount` bits set.
#[must_use]
pub const fn mask_u32(popcount: u32) -> u32 {
    if popcount == 0 {
        0
    } else if popcount >= u32::BITS {
        u32::MAX
    } else {
        (1u32 << popcount) - 1
    }
}

/// Returns a `u128` mask with the lowest `popcount` bits set.
#[must_use]
pub const fn mask_u128(popcount: u32) -> u128 {
    if popcount == 0 {
        0
    } else if popcount >= u128::BITS {
        u128::MAX
    } else {
        (1u128 << popcount) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_v1_and_v2_have_matching_payload() {
        let a = fields_v1(8);
        let b = fields_v2(8);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.index, y.index);
            assert_eq!(x.point.tv_sec, y.point.tv_sec);
            assert_eq!(x.point.tv_nsec, y.point.tv_nsec);
        }
    }

    #[test]
    fn build_ttl1_and_ttl2_are_populated() {
        let w = Workload {
            max_size: 1024,
            n_docs: 500,
            k_fields: 4,
        };
        let t1 = build_ttl1(&w);
        let t2 = build_ttl2(&w);
        assert!(!t1.is_empty());
        assert!(!t2.is_empty());
    }

    #[test]
    fn mask_helpers_set_lowest_bits() {
        assert_eq!(mask_u32(0), 0);
        assert_eq!(mask_u32(1), 1);
        assert_eq!(mask_u32(4), 0b1111);
        assert_eq!(mask_u32(32), u32::MAX);
        assert_eq!(mask_u128(0), 0);
        assert_eq!(mask_u128(8), 0xFF);
        assert_eq!(mask_u128(128), u128::MAX);
    }
}
