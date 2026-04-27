/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A per-document time-to-live table — faithful port of the C module under
//! `src/ttl_table/`.
//!
//! Unlike the sibling [`ttl_table`](../ttl_table/index.html) crate (which uses
//! a [`std::collections::HashMap`]), this crate mirrors the C implementation's
//! data layout and growth strategy:
//!
//! - A direct-modulo bucket array (`slot = doc_id % max_size`) — no hashing,
//!   so monotonically allocated docIds map to sequential slots and the CPU
//!   prefetcher can stream upcoming bucket headers into L1.
//! - Per-bucket contiguous-vec collision chains (a [`ThinVec`] of entries,
//!   matching the role `arr.h` plays in the C version).
//! - Lazy growth: the bucket array starts empty and grows geometrically
//!   (+1.5×, capped at `1 << 20` and clamped to `max_size`) only as `add`
//!   demands more slots.
//! - No shrink-on-delete: empty buckets are released, but the bucket array
//!   itself keeps its high-water-mark length so churning indexes don't
//!   thrash on realloc.
//!
//! The table holds only field-level (HEXPIRE) expirations; document-level
//! TTL lives directly on `RSDocumentMetadata::expirationTimeNs` so the
//! result-processor hot path avoids a lookup here.
//!
//! This crate is a Rust-only sibling of the C implementation, intended for
//! benchmarking the two head-to-head before any decision is made to retire
//! the C version.

pub use field::FieldExpirationPredicate;
use libc::timespec;
use thin_vec::ThinVec;

/// Initial bucket-array length the first time we grow from zero.
///
/// Chosen so an index that only ever holds a handful of TTL docs pays a
/// single small allocation and no further reallocs.
const TTL_BUCKET_INITIAL_CAP: usize = 64;

/// Upper bound on the geometric +1.5× step once the bucket array is
/// non-empty.
///
/// Mirrors `DocTable_Set`'s cap so very large tables don't take a single
/// multi-MiB realloc hit and so the two allocators scale in lockstep.
const TTL_BUCKET_MAX_GROW_STEP: usize = 1 << 20;

/// The expiration time recorded for a single field of a document.
///
/// Callers of [`TimeToLiveTable::add`] are responsible for sorting these
/// by `index` in ascending order — the verification routines rely on the
/// sort order to scan the list in linear time.
#[derive(Debug, Clone, Copy)]
pub struct FieldExpiration {
    /// The field index this expiration applies to.
    pub index: u16,
    /// The point in time at which the field expires.
    pub point: timespec,
}

#[derive(Debug)]
struct TimeToLiveEntry {
    doc_id: u64,
    /// Owned, sorted by field index, never empty.
    field_expirations: ThinVec<FieldExpiration>,
}

/// Direct-modulo bucket array with contiguous-vec collision chains.
///
/// See the module-level documentation for the rationale. The bucket array
/// length (`buckets.len()`) plays the role of `cap` in the C version and
/// always satisfies `buckets.len() <= max_size`.
#[derive(Debug)]
pub struct TimeToLiveTable {
    buckets: Vec<ThinVec<TimeToLiveEntry>>,
    /// Modulus for the slot formula. Captured at construction and never
    /// changes — growing `buckets` would otherwise relocate every entry.
    max_size: usize,
    count: usize,
}

impl TimeToLiveTable {
    /// Creates an empty table with `max_size` as the fixed modulus for
    /// the slot formula.
    ///
    /// The bucket array starts empty (`cap = 0` in C terms) and grows on
    /// demand — equivalent to the C `TimeToLiveTable_VerifyInit` followed
    /// by zero `Add` calls.
    ///
    /// # Panics
    ///
    /// Panics if `max_size == 0`, which would cause a divide-by-zero in
    /// the slot formula. The C version asserts the same precondition; the
    /// `DocTable` caller floors its own `maxSize` to 1 on load.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        assert!(max_size >= 1, "TTL table max_size must be >= 1");
        Self {
            buckets: Vec::new(),
            max_size,
            count: 0,
        }
    }

    /// Returns `true` if the table holds no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Inserts a document's per-field expirations.
    ///
    /// `sorted_by_id` must be non-empty and sorted by `index` in ascending
    /// order. Ownership transfers to the table.
    ///
    /// # Panics
    ///
    /// Panics if `sorted_by_id` is empty (the C version asserts the same).
    /// In debug builds, also panics if `doc_id` is already present in the
    /// table — the upstream spec write lock is expected to prevent this.
    pub fn add(&mut self, doc_id: u64, sorted_by_id: ThinVec<FieldExpiration>) {
        assert!(
            !sorted_by_id.is_empty(),
            "TTL table add requires at least one field expiration"
        );

        let slot = self.slot(doc_id);
        self.grow_to(slot);

        // docIds are monotonically assigned in DocTable_Put under the spec
        // write lock, so duplicates should not reach here; the assert
        // catches a broken locking discipline during development before it
        // corrupts the table.
        debug_assert!(
            !self.buckets[slot].iter().any(|e| e.doc_id == doc_id),
            "duplicate docId in TTL table"
        );

        self.buckets[slot].push(TimeToLiveEntry {
            doc_id,
            field_expirations: sorted_by_id,
        });
        self.count += 1;
    }

    /// Removes the entry for `doc_id`, if any. No-op if absent.
    ///
    /// Uses swap-last deletion (O(1)) and does not shrink the bucket — the
    /// allocation is kept at its high-water mark to avoid realloc churn.
    pub fn remove(&mut self, doc_id: u64) {
        let slot = self.slot(doc_id);
        if slot >= self.buckets.len() {
            return;
        }
        let bucket = &mut self.buckets[slot];
        if let Some(pos) = bucket.iter().position(|e| e.doc_id == doc_id) {
            bucket.swap_remove(pos);
            self.count -= 1;
        }
    }

    /// Test-only: number of buckets currently allocated (lazy-growth
    /// high-water mark). Mirrors the C `TimeToLiveTable_DebugAllocatedBuckets`.
    #[doc(hidden)]
    #[must_use]
    pub const fn debug_allocated_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Checks the expiration state of a single field of a document under
    /// `predicate`.
    ///
    /// Mirrors the semantics of `TimeToLiveTable_VerifyDocAndField` in the
    /// C module:
    /// - If the document has no entry in the table, returns `true`
    ///   (no expiration is recorded for it, so the field is trivially
    ///   valid for `Default` and trivially absent for `Missing`).
    /// - If the document has an entry but no per-field expirations,
    ///   returns `true` for the same reason.
    /// - Otherwise scans the per-field list for a matching index and
    ///   returns the result that respects `predicate`.
    #[must_use]
    pub fn verify_doc_and_field(
        &self,
        doc_id: u64,
        field: u16,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
    ) -> bool {
        let Some(entry) = self.find_entry(doc_id) else {
            return true;
        };
        if entry.field_expirations.is_empty() {
            return true;
        }

        for fe in entry.field_expirations.iter() {
            if fe.index == field {
                let expired = did_expire(&fe.point, expiration_point);
                return if expired {
                    predicate == FieldExpirationPredicate::Missing
                } else {
                    predicate != FieldExpirationPredicate::Missing
                };
            }
        }

        // Field not tracked: valid for `Default`, not actually missing for
        // `Missing`.
        predicate != FieldExpirationPredicate::Missing
    }

    /// Checks the expiration state of a set of fields described by a
    /// 32-bit `field_mask`.
    ///
    /// `ft_id_to_field_index[bit]` translates a bit position in the mask
    /// into the `t_fieldIndex` recorded in the table; it must contain at
    /// least as many entries as the highest set bit of `field_mask`. The
    /// translation is required to be monotonic — bits scanned low-to-high
    /// must produce non-decreasing field indices — because the two-pointer
    /// scan over the sorted per-field list only advances forward.
    ///
    /// Mirrors `TimeToLiveTable_VerifyDocAndFieldMask` in the C module.
    ///
    /// # Panics
    ///
    /// Panics if `ft_id_to_field_index` is shorter than `highest_set_bit + 1`.
    /// The verify loop indexes the slice without per-iteration bounds checks
    /// — the entry assertion is what licences that.
    #[must_use]
    pub fn verify_doc_and_field_mask(
        &self,
        doc_id: u64,
        field_mask: u32,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
        ft_id_to_field_index: &[u16],
    ) -> bool {
        verify_mask::<u32>(
            self.find_entry(doc_id),
            field_mask,
            predicate,
            expiration_point,
            ft_id_to_field_index,
        )
    }

    /// Checks the expiration state of a set of fields described by a
    /// 128-bit `field_mask` (the wide-schema variant).
    ///
    /// Mirrors `TimeToLiveTable_VerifyDocAndWideFieldMask` in the C
    /// module. Kept as a separate method from
    /// [`Self::verify_doc_and_field_mask`] to match the C public API
    /// shape — see the C TODO at `src/ttl_table/ttl_table.c:305` for the
    /// rationale behind keeping the two distinct here. The translation
    /// table follows the same monotonicity contract.
    ///
    /// # Panics
    ///
    /// Panics if `ft_id_to_field_index` is shorter than
    /// `highest_set_bit + 1` of `field_mask`. See
    /// [`Self::verify_doc_and_field_mask`] for the rationale.
    #[must_use]
    pub fn verify_doc_and_wide_field_mask(
        &self,
        doc_id: u64,
        field_mask: u128,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
        ft_id_to_field_index: &[u16],
    ) -> bool {
        verify_mask::<u128>(
            self.find_entry(doc_id),
            field_mask,
            predicate,
            expiration_point,
            ft_id_to_field_index,
        )
    }

    /// Direct-modulo slot formula — mirrors `ttl_slot` in the C source.
    #[inline]
    const fn slot(&self, doc_id: u64) -> usize {
        if doc_id < self.max_size as u64 {
            doc_id as usize
        } else {
            (doc_id % self.max_size as u64) as usize
        }
    }

    /// Ensures `buckets[slot]` is allocated. Mirrors the growth curve of
    /// `ttl_grow` in the C source: the first grow seeds at
    /// `TTL_BUCKET_INITIAL_CAP`, subsequent grows are `+1 + min(cap/2,
    /// TTL_BUCKET_MAX_GROW_STEP)`, all clamped to `max_size` and rounded
    /// up to cover the requested `slot`.
    fn grow_to(&mut self, slot: usize) {
        debug_assert!(slot < self.max_size);
        let cap = self.buckets.len();
        if slot < cap {
            return;
        }
        debug_assert!(cap < self.max_size);

        let mut newcap = if cap == 0 {
            TTL_BUCKET_INITIAL_CAP
        } else {
            cap + 1 + std::cmp::min(cap / 2, TTL_BUCKET_MAX_GROW_STEP)
        };
        if newcap > self.max_size {
            newcap = self.max_size;
        }
        if newcap < slot + 1 {
            newcap = slot + 1;
        }

        self.buckets.resize_with(newcap, ThinVec::new);
    }

    fn find_entry(&self, doc_id: u64) -> Option<&TimeToLiveEntry> {
        let slot = self.slot(doc_id);
        let bucket = self.buckets.get(slot)?;
        bucket.iter().find(|e| e.doc_id == doc_id)
    }
}

/// Bit-mask abstraction shared by the 32-bit and wide-mask helpers.
///
/// The trait splits the mask into a slice of `u64` halves (low to high)
/// so [`verify_mask`] can iterate bits with native `u64` ops — single
/// `rbit`+`clz` for `trailing_zeros`, single `lsl`, single `bic` for the
/// AND-NOT. This mirrors the C source's manual `fieldMask64[2]` split
/// (`src/ttl_table/ttl_table.c:315-322`); using `u128` ops directly
/// expands each step into a multi-instruction sequence with branchless
/// half-selection because no major ISA has native 128-bit integer
/// support.
///
/// Kept private (not part of the crate's public API) so the public
/// surface mirrors the C two-function signature exactly.
trait BitMask: Copy {
    /// Owned array holding the mask split into `u64` halves. The actual
    /// length is `1` for `u32` and `2` for `u128` — encoded in the
    /// concrete type so the verify loop fully unrolls / specializes per
    /// instantiation.
    type Halves: AsRef<[u64]>;

    /// Splits the mask into a sequence of `u64` halves, low to high.
    fn halves(self) -> Self::Halves;
}

impl BitMask for u32 {
    type Halves = [u64; 1];

    #[inline]
    fn halves(self) -> Self::Halves {
        [self as u64]
    }
}

impl BitMask for u128 {
    type Halves = [u64; 2];

    #[inline]
    fn halves(self) -> Self::Halves {
        [self as u64, (self >> 64) as u64]
    }
}

/// Shared body for the two mask-checking entry points.
///
/// The C version inlines this twice (once for `uint32_t`, once for the
/// wide mask) — here we share the body via a private generic, but the
/// inner bit-pop walks `u64` halves so the codegen matches C even on
/// the wide path.
fn verify_mask<M: BitMask>(
    entry: Option<&TimeToLiveEntry>,
    mask: M,
    predicate: FieldExpirationPredicate,
    expiration_point: &timespec,
    ft_id_to_field_index: &[u16],
) -> bool {
    let Some(entry) = entry else {
        return true;
    };

    let field_with_expiration_count = entry.field_expirations.len();
    // A view into the mask as a slice of `u64` halves, low to high.
    let halves_storage = mask.halves();
    let halves: &[u64] = halves_storage.as_ref();
    let field_count: usize = halves.iter().map(|h| h.count_ones() as usize).sum();

    if field_with_expiration_count == 0 {
        return true;
    }
    if field_with_expiration_count < field_count && predicate == FieldExpirationPredicate::Default {
        // Fewer recorded expirations than mask bits ⇒ at least one mask
        // bit must hit a field with no recorded expiration ⇒ trivially
        // valid for `Default`.
        return true;
    }

    // Hoisted bounds check on the translation table. The bit indices
    // we generate are exactly the set bits of the mask, so we only
    // need the slice to cover the *highest* set bit. Doing the check
    // once here lets us drop the per-iteration bounds check on
    // `ft_id_to_field_index` below. The check is plain `assert!` (not
    // `debug_assert!`) so a contract-violating caller still hits a
    // panic instead of UB in release builds.
    let highest_bit_plus_one: usize = halves
        .iter()
        .enumerate()
        .map(|(i, &h)| {
            if h == 0 {
                0
            } else {
                i * (u64::BITS as usize) + (u64::BITS as usize) - (h.leading_zeros() as usize)
            }
        })
        .max()
        .unwrap_or(0);
    assert!(
        ft_id_to_field_index.len() >= highest_bit_plus_one,
        "ft_id_to_field_index must cover the highest set bit of the mask"
    );

    let mut predicate_misses: usize = 0;
    let mut current_field_index: usize = 0;

    // Outer loop walks halves; inner walks bits within a half using
    // native `u64` ops. With `[u64; 1]` (u32 mask) or `[u64; 2]` (u128
    // mask) baked into the concrete type, the outer loop fully unrolls.
    // We `return` from the inner loop instead of using a labeled
    // `break` — the latter forced a CFG shape under which LLVM dropped
    // the post-index `ldrh` it generates for the cursor advance loop.
    for (half_idx, &half_init) in halves.iter().enumerate() {
        let mut half = half_init;
        let bit_offset = (half_idx as u32) * u64::BITS;
        while half != 0 {
            let bit_in_half = half.trailing_zeros();
            half &= !(1u64 << bit_in_half);
            let bit_index = bit_in_half + bit_offset;

            // SAFETY: the function-entry assertion proved
            // `ft_id_to_field_index.len() >= halves.len() * 64`. The
            // outer loop bounds `half_idx < halves.len()` and
            // `bit_in_half < 64`, so
            // `bit_index = half_idx * 64 + bit_in_half < halves.len() * 64
            //            <= ft_id_to_field_index.len()`.
            let field_index_to_check =
                *unsafe { ft_id_to_field_index.get_unchecked(bit_index as usize) };

            // Advance the cursor over fields strictly less than the one
            // we are checking.
            while current_field_index < field_with_expiration_count {
                // SAFETY: the `while` condition above proved
                // `current_field_index < field_with_expiration_count`,
                // which equals `entry.field_expirations.len()`.
                let candidate_index = unsafe {
                    entry
                        .field_expirations
                        .get_unchecked(current_field_index)
                        .index
                };
                if field_index_to_check <= candidate_index {
                    break;
                }
                current_field_index += 1;
            }
            if current_field_index >= field_with_expiration_count {
                return match predicate {
                    FieldExpirationPredicate::Default => predicate_misses < field_count,
                    FieldExpirationPredicate::Missing => false,
                };
            }
            // SAFETY: the `if` above branched out for the out-of-bounds
            // case, so here `current_field_index < field_with_expiration_count
            //   == entry.field_expirations.len()`.
            let entry_field = unsafe { entry.field_expirations.get_unchecked(current_field_index) };
            if field_index_to_check < entry_field.index {
                // Field not tracked by this entry — treat as absent.
                continue;
            }
            debug_assert_eq!(field_index_to_check, entry_field.index);

            let expired = did_expire(&entry_field.point, expiration_point);
            if !expired && predicate == FieldExpirationPredicate::Default {
                return true;
            }
            if expired && predicate == FieldExpirationPredicate::Missing {
                return true;
            }
            predicate_misses += 1;
        }
    }

    match predicate {
        // For `Default` we still return true if at least one mask bit
        // never hit a tracked expiration — see the early return above
        // for the cheap version of this check.
        FieldExpirationPredicate::Default => predicate_misses < field_count,
        FieldExpirationPredicate::Missing => false,
    }
}

/// Returns `true` if `field` has elapsed by `now`.
///
/// A `field` with both `tv_sec` and `tv_nsec` zero is treated as "no
/// expiration set" and never expires — matches the C `DidExpire` helper.
#[inline]
const fn did_expire(field: &timespec, now: &timespec) -> bool {
    if field.tv_sec == 0 && field.tv_nsec == 0 {
        return false;
    }
    !((field.tv_sec > now.tv_sec) || (field.tv_sec == now.tv_sec && field.tv_nsec > now.tv_nsec))
}
