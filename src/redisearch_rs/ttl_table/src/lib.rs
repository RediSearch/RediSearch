/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A per-(document-field) time-to-live table
//!
//! Data layout and growth strategy:
//!
//! - A direct-modulo bucket array (`slot = doc_id % max_size`) — no hashing,
//!   so monotonically allocated docIds map to sequential slots and the CPU
//!   prefetcher can stream upcoming bucket headers into L1.
//! - Per-bucket contiguous-vec collision chains (a [`ThinVec`] of entries).
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

#[cfg(feature = "test-utils")]
pub mod test_utils;

use std::{iter::Chain, num::NonZeroUsize, ops::Deref};

pub use ffi::FieldExpiration;
use ffi::{FieldMask, t_expirationTimePoint as timespec};
pub use field::FieldExpirationPredicate;
use thin_vec::ThinVec;

use ffi::t_docId;

/// An ascending-by-`index`, duplicate-free list of [`FieldExpiration`] entries.
///
/// `#[repr(transparent)]` over [`ThinVec<FieldExpiration>`] — the FFI crate
/// defines a layout-compatible sibling type so the wire format is identical.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct FieldExpirations {
    inner: ThinVec<FieldExpiration>,
}

impl FieldExpirations {
    /// Creates an empty `FieldExpirations`.
    ///
    /// No heap allocation is performed until the first insertion.
    pub const fn new() -> Self {
        Self {
            inner: ThinVec::new(),
        }
    }

    /// Creates an empty `FieldExpirations` with backing storage pre-sized for
    /// at least `cap` entries.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: ThinVec::with_capacity(cap),
        }
    }

    /// Inserts `fe` into its sorted-by-`index` position.
    ///
    /// # Returns
    ///
    /// `Ok(&fe)` — a reference to the newly inserted entry inside the list.
    ///
    /// # Errors
    ///
    /// Returns `Err(fe)` if an entry with `fe.index` already exists; the list
    /// is left unchanged and `fe` is handed back to the caller untouched.
    pub fn add(
        &mut self,
        fe: FieldExpiration,
    ) -> std::result::Result<&FieldExpiration, FieldExpiration> {
        let result = self.inner.binary_search_by_key(&fe.index, |a| a.index);

        match result {
            Ok(_) => Err(fe),
            Err(index) => {
                self.inner.insert(index, fe);
                Ok(&self.inner[index])
            }
        }
    }

    /// Appends `fe` to the end of the list without searching for the
    /// insertion point.
    ///
    /// Use when the caller already knows that `fe.index` is strictly greater
    /// than every index currently in `self` — typically when building the
    /// list from an already-sorted source iterator. Equivalent in result to
    /// [`Self::add`] but avoids the binary search and the shift.
    ///
    /// # Safety
    ///
    /// `fe.index` must be **strictly greater** than the `index` of every
    /// entry already present in `self`.
    pub unsafe fn push_unchecked(&mut self, fe: FieldExpiration) {
        // Elided in release mode
        if let Some(last) = self.last() {
            debug_assert!(last.index < fe.index);
        }
        self.inner.push(fe);
    }

    /// Wraps a pre-built [`ThinVec<FieldExpiration>`] without re-validating
    /// its contents.
    ///
    /// # Safety
    ///
    /// `v` must be sorted ascending by `index` with no duplicate indices.
    /// The same invariants are checked via `debug_assert!` and elided in
    /// release.
    pub unsafe fn from_thin_vec_unchecked(v: ThinVec<FieldExpiration>) -> Self {
        debug_assert!(
            v.is_sorted_by(|a, b| a.index < b.index),
            "FieldExpirations must be sorted ascending by index with no duplicates"
        );
        Self { inner: v }
    }
}

impl Default for FieldExpirations {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only access to the underlying `ThinVec`.
impl Deref for FieldExpirations {
    type Target = ThinVec<FieldExpiration>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Initial bucket-array length the first time we grow from zero.
///
/// Chosen so an index that only ever holds a handful of TTL docs pays a
/// single small allocation and no further reallocs.
const TTL_BUCKET_INITIAL_CAP: usize = 64;

/// Upper bound on the geometric +1.5× step once the bucket array is
/// non-empty.
///
/// Very large tables don't take a single
/// multi-MiB realloc hit and so the two allocators scale in lockstep.
const TTL_BUCKET_MAX_GROW_STEP: usize = 1 << 20;

/// A document's record in a [`TimeToLiveTable`] bucket's collision chain.
#[derive(Debug)]
pub struct TimeToLiveEntry {
    /// The document id
    pub doc_id: t_docId,
    /// Owned, sorted by field index, never empty.
    pub field_expirations: FieldExpirations,
}

/// Direct-modulo bucket array with contiguous-vec collision chains.
///
/// See the module-level documentation for the rationale. The bucket array
/// length (`buckets.len()`) always satisfies `buckets.len() <= max_size`.
#[derive(Debug)]
pub struct TimeToLiveTable {
    /// The bucket
    buckets: Vec<ThinVec<TimeToLiveEntry>>,
    /// Modulus for the slot formula. Captured at construction and never
    /// changes.
    max_size: usize,
    /// Number of stored document
    count: usize,
}

impl TimeToLiveTable {
    /// Creates an empty table with `max_size` as the fixed modulus for
    /// the slot formula.
    ///
    /// The bucket array starts empty and grows on demand.
    pub fn new(max_size: NonZeroUsize) -> Self {
        Self {
            buckets: Vec::new(),
            max_size: max_size.into(),
            count: 0,
        }
    }

    /// Returns `true` if the table holds no entries.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Inserts a document's per-field expirations.
    ///
    /// Ownership of `sorted_by_id` transfers to the table.
    ///
    /// # Safety
    ///
    /// The caller must guarantee:
    /// - `sorted_by_id` is non-empty.
    /// - `doc_id` is not already present in the table.
    ///
    /// Sortedness and duplicate-free-by-index of the field expirations are
    /// carried by the [`FieldExpirations`] type itself.
    ///
    /// These invariants are load-bearing for the lookup hot paths
    /// ([`verify_doc_and_field`](Self::verify_doc_and_field),
    /// [`verify_doc_and_field_mask`](Self::verify_doc_and_field_mask),
    /// [`verify_doc_and_wide_field_mask`](Self::verify_doc_and_wide_field_mask)),
    /// which assume them when scanning the per-bucket chain and the
    /// per-entry field-expiration list.
    ///
    /// In debug builds these preconditions are checked via `debug_assert!`
    /// and will panic on violation; in release builds the checks are
    /// elided.
    pub unsafe fn add(&mut self, doc_id: t_docId, field_expirations: FieldExpirations) {
        debug_assert!(
            !field_expirations.is_empty(),
            "TTL table add requires at least one field expiration"
        );

        let slot = self.slot(doc_id);
        self.grow_to(slot);

        debug_assert!(
            !self.buckets[slot].iter().any(|e| e.doc_id == doc_id),
            "duplicate docId in TTL table"
        );

        self.buckets[slot].push(TimeToLiveEntry {
            doc_id,
            field_expirations,
        });
        self.count += 1;
    }

    /// Removes the entry for `doc_id`, if any. No-op if absent.
    ///
    /// Uses swap-last deletion (O(1)) and does not shrink the bucket — the
    /// allocation is kept at its high-water mark to avoid realloc churn.
    pub fn remove(&mut self, doc_id: t_docId) -> Option<TimeToLiveEntry> {
        let slot = self.slot(doc_id);
        if slot >= self.buckets.len() {
            return None;
        }
        let bucket = &mut self.buckets[slot];
        if let Some(pos) = bucket.iter().position(|e| e.doc_id == doc_id) {
            let removed = bucket.swap_remove(pos);
            self.count -= 1;

            Some(removed)
        } else {
            None
        }
    }

    /// Return the number of buckets currently allocated
    #[cfg(feature = "test-utils")]
    pub const fn n_allocated_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the per-field expiration list stored for `doc_id`, or `None`
    /// if no entry exists.
    ///
    /// The slice aliases storage owned by the table and is invalidated by any
    /// subsequent [`add`](Self::add) / [`remove`](Self::remove) on this table.
    pub fn field_expirations(&self, doc_id: t_docId) -> Option<&FieldExpirations> {
        self.find_entry(doc_id)
            .map(|e| &e.field_expirations)
    }

    /// Checks the expiration state of a single field of a document under
    /// `predicate`.
    ///
    /// The result then respects `predicate`:
    /// - [`FieldExpirationPredicate::Default`] returns `true` iff the
    ///   field is not expired ("valid").
    /// - [`FieldExpirationPredicate::Missing`] returns `true` iff the
    ///   field is expired ("considered missing").
    ///
    /// A field is considered *expired* when it has a recorded expiration
    /// point that has elapsed by `expiration_point`.
    /// `(0, 0)` is a special value that means it never expires.
    /// Untracked fields are likewise treated as not expired.
    ///
    /// As a special case, when no expiration information is recorded for
    /// the document at all, the function returns `true` regardless of `predicate`,
    /// because none of the document's fields is expired, so the query trivially passes
    /// under either predicate.
    pub fn verify_doc_and_field(
        &self,
        doc_id: t_docId,
        field: u16,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
    ) -> bool {
        let Some(entry) = self.find_entry(doc_id) else {
            // the document did not have a ttl for itself or its fields
            // if predicate is FieldExpirationPredicate::Default, at least one field is valid
            // if predicate is FieldExpirationPredicate::Missing, the field is indeed missing since the document has no expiration for it
            return true;
        };

        debug_assert!(
            !entry.field_expirations.is_empty(),
            "field_expirations is guaranteed to not be empty"
        );

        entry
            .field_expirations
            .iter()
            // Find the field in the chain
            .find(|fe| fe.index == field)
            .map(|fe| {
                let expired = did_expire(&fe.point, expiration_point);
                if expired {
                    // the document is invalid (should return `false`), unless we look for missing fields
                    predicate == FieldExpirationPredicate::Missing
                } else {
                    // the document is valid (should return `true`), unless we look for missing fields
                    predicate != FieldExpirationPredicate::Missing
                }
            })
            // Field not tracked: valid for `Default`, not actually missing for
            // `Missing`.
            .unwrap_or(predicate != FieldExpirationPredicate::Missing)
    }

    /// Checks the expiration state of a set of fields described by a
    /// 32-bit `field_mask`.
    ///
    /// `ft_id_to_field_index[bit]` translates a bit position in the mask
    /// into the `t_fieldIndex` recorded in the table; it must contain at
    /// least as many entries as the highest set bit of `field_mask`. The
    /// translation is required to be monotonic, bits scanned low-to-high
    /// must produce non-decreasing field indices.
    ///
    /// # Panics
    ///
    /// Panics if `ft_id_to_field_index` is shorter than `highest_set_bit + 1`.
    ///
    /// Callers must guarantee that `ft_id_to_field_index.len()` is at
    /// least `highest_set_bit + 1` of `field_mask`. The bit-walk reads
    /// the translation slice via `_unchecked` once per set bit;
    /// violating the bound is undefined behavior in release builds.
    pub fn verify_doc_and_field_mask(
        &self,
        doc_id: t_docId,
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

    /// Checks the expiration state of a set of fields described by a wide
    /// [`FieldMask`] (the wide-schema variant).
    ///
    /// `FieldMask` is a compile-time alias chosen by the C build: it can be
    /// either a 64-bit or a 128-bit unsigned integer. This function
    /// dispatches to the matching bit-walk at compile time, based on the
    /// target pointer width.
    ///
    /// See also [`TimeToLiveTable::verify_doc_and_field_mask`].
    ///
    /// # Panics
    ///
    /// See [`TimeToLiveTable::verify_doc_and_field_mask`].
    pub fn verify_doc_and_wide_field_mask(
        &self,
        doc_id: t_docId,
        field_mask: FieldMask,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
        ft_id_to_field_index: &[u16],
    ) -> bool {
        #[cfg(target_pointer_width = "64")]
        {
            verify_mask::<u128>(
                self.find_entry(doc_id),
                field_mask,
                predicate,
                expiration_point,
                ft_id_to_field_index,
            )
        }
        #[cfg(target_pointer_width = "32")]
        {
            verify_mask::<u64>(
                self.find_entry(doc_id),
                field_mask,
                predicate,
                expiration_point,
                ft_id_to_field_index,
            )
        }
    }

    /// Direct-modulo slot formula
    const fn slot(&self, doc_id: t_docId) -> usize {
        if doc_id < self.max_size as u64 {
            doc_id as usize
        } else {
            (doc_id % self.max_size as u64) as usize
        }
    }

    /// Ensures `buckets[slot]` is allocated.
    ///
    /// The first grow seeds at `TTL_BUCKET_INITIAL_CAP`,
    /// subsequent grows are `+1 + min(cap/2, TTL_BUCKET_MAX_GROW_STEP)`,
    /// all clamped to `max_size` and rounded up to cover the requested `slot`.
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

    fn find_entry(&self, doc_id: t_docId) -> Option<&TimeToLiveEntry> {
        let slot = self.slot(doc_id);
        let bucket = self.buckets.get(slot)?;
        bucket.iter().find(|e| e.doc_id == doc_id)
    }
}

/// Bit-mask abstraction shared by the 32-bit and wide-mask helpers.
trait BitMask: Copy {
    /// Concrete iterator type returned by [`Self::iter`]; yields the
    /// positions of set bits as [`u32`] values.
    type Iter: Iterator<Item = u32>;

    /// Returns the number of `1` bits in the mask.
    fn count_ones(self) -> usize;

    /// Returns the highest set-bit position plus one — i.e. the minimum
    /// number of bits needed to represent the value, or `0` when the mask
    /// is zero.
    fn higher_bit_position(self) -> usize;

    /// Returns an iterator over the positions of the set bits, yielded
    /// low-to-high.
    fn iter(self) -> Self::Iter;
}

impl BitMask for u32 {
    type Iter = BitU64Iter;

    fn iter(self) -> Self::Iter {
        BitU64Iter::new(self as u64)
    }

    fn count_ones(self) -> usize {
        u32::count_ones(self) as usize
    }

    fn higher_bit_position(self) -> usize {
        (Self::BITS - self.leading_zeros()) as usize
    }
}

impl BitMask for u64 {
    type Iter = BitU64Iter;

    fn iter(self) -> Self::Iter {
        BitU64Iter::new(self)
    }

    fn count_ones(self) -> usize {
        u64::count_ones(self) as usize
    }

    fn higher_bit_position(self) -> usize {
        (Self::BITS - self.leading_zeros()) as usize
    }
}

impl BitMask for u128 {
    type Iter = BitU128Iter;

    fn iter(self) -> Self::Iter {
        BitU128Iter::new(self)
    }

    fn count_ones(self) -> usize {
        u128::count_ones(self) as usize
    }

    fn higher_bit_position(self) -> usize {
        (Self::BITS - self.leading_zeros()) as usize
    }
}

/// Iterator over the indices of the set bits of a [`u64`], yielded low to high.
///
/// Each item is the zero-based position of a `1` bit (`0..64`).
///
/// # Example
///
/// ```
/// use ttl_table::BitU64Iter;
///
/// let bits: Vec<u32> = BitU64Iter::new(0b1010_u64).collect();
/// assert_eq!(bits, vec![1, 3]);
/// ```
pub struct BitU64Iter {
    current: u64,
    base: u32,
}

impl BitU64Iter {
    /// Builds an iterator over the set bits of `mask`.
    #[inline]
    pub const fn new(mask: u64) -> Self {
        Self::with_base(mask, 0)
    }

    #[inline]
    pub const fn with_base(mask: u64, base: u32) -> Self {
        Self {
            current: mask,
            base,
        }
    }
}

impl Iterator for BitU64Iter {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.current == 0 {
            return None;
        }
        // `bit ∈ [0, 64)`, 64 excluded because of `self.current != 0`.
        let bit = self.current.trailing_zeros();
        // Clear the lowest set bit.
        self.current &= self.current - 1;
        Some(bit + self.base)
    }
}

/// Iterator over the indices of the set bits of a [`u128`], yielded low to high.
///
/// Each item is the zero-based position of a `1` bit (`0..128`).
///
/// # Example
///
/// ```
/// use ttl_table::BitU128Iter;
///
/// let bits: Vec<u32> = BitU128Iter::new(0b1010_u128).collect();
/// assert_eq!(bits, vec![1, 3]);
/// ```
pub struct BitU128Iter {
    iter: Chain<BitU64Iter, BitU64Iter>,
}
impl BitU128Iter {
    /// Builds an iterator over the set bits of `mask`.
    #[inline]
    pub fn new(mask: u128) -> Self {
        let first = mask as u64;
        let second = (mask >> 64) as u64;

        let iter = // Yield from [0, 64)
            BitU64Iter::with_base(first, 0)
            // Yield from [64, 127)
            .chain(BitU64Iter::with_base(second, 64));
        Self { iter }
    }
}
impl Iterator for BitU128Iter {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        self.iter.next()
    }
}

fn verify_mask<M: BitMask>(
    entry: Option<&TimeToLiveEntry>,
    mask: M,
    predicate: FieldExpirationPredicate,
    expiration_point: &timespec,
    ft_id_to_field_index: &[u16],
) -> bool {
    let Some(entry) = entry else {
        // The document did not have a ttl for itself or its fields.
        // Therefore:
        // - if predicate is default, then we know at least one field is valid
        // - if predicate is missing, then we know the field is indeed missing since the document has no expiration for it
        return true;
    };

    let field_expirations: &[FieldExpiration] = &entry.field_expirations;
    debug_assert!(
        !entry.field_expirations.is_empty(),
        "field_expirations is guaranteed to not be empty"
    );
    let field_with_expiration_length = field_expirations.len();

    let field_count: usize = mask.count_ones();
    if field_with_expiration_length < field_count && predicate == FieldExpirationPredicate::Default
    {
        // The document has less fields with expiration times than the fields we are checking.
        // So, at least one field is valid
        return true;
    }

    // Hoisted bound, so the loop can use `get_unchecked`.
    let highest_bit_plus_one: usize = mask.higher_bit_position();
    assert!(
        ft_id_to_field_index.len() >= highest_bit_plus_one,
        "ft_id_to_field_index must cover the highest set bit of the mask"
    );

    /// Reads `&arr[index]`,
    ///
    /// # Safety
    ///
    /// The caller must guarantee `index < arr.len()`.
    #[inline]
    unsafe fn get_unchecked<T>(arr: &[T], index: usize) -> &T {
        debug_assert!(
            index < arr.len(),
            "Safety violation: try to access to an out-of-bounds index, {index}. Length {}",
            arr.len(),
        );
        // SAFETY: Function safety guarantees
        unsafe { arr.get_unchecked(index) }
    }

    let mut predicate_misses: usize = 0;
    let mut current_field_index: usize = 0;

    // Visits set bits low-to-high. Order matters: `current_field_index` only
    // moves forward, so monotonic field indices amortize the cursor walk
    // across bits (especially across the u128 halves).
    for bit_index in mask.iter() {
        // Load-bearing: `bit_index <= highest_bit_plus_one - 1`, which
        // underwrites the safety proof of `get_unchecked` below.

        // SAFETY: `bit_index` is the position of a set bit in `mask`.
        // Because:
        // - `bit_index <= highest_bit_plus_one - 1`
        // - `highest_bit_plus_one <= ft_id_to_field_index.len()`
        // hence `bit_index < ft_id_to_field_index.len()`.
        let field_index_to_check =
            unsafe { *get_unchecked(ft_id_to_field_index, bit_index as usize) };

        // Advance the cursor over fields strictly less than the one
        // we are checking.
        while current_field_index < field_with_expiration_length {
            // SAFETY: the `while` condition above proves it
            let candidate_index =
                unsafe { get_unchecked(field_expirations, current_field_index).index };
            if field_index_to_check <= candidate_index {
                break;
            }
            current_field_index += 1;
        }
        if current_field_index >= field_with_expiration_length {
            // No more fields with expiration times to check.
            break;
        }

        // SAFETY: the `if … break` above ensures we only get
        // here when `current_field_index < field_with_expiration_length`
        let entry_field = unsafe { get_unchecked(field_expirations, current_field_index) };
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

    match predicate {
        FieldExpirationPredicate::Default => {
            // At least one valid field
            predicate_misses < field_count
        }
        FieldExpirationPredicate::Missing => {
            // If we are checking for the missing predicate, we need at least one expired field
            // If we reached here, it means we did not find any expired fields
            false
        }
    }
}

/// Returns `true` if `field` has elapsed by `now`.
///
/// A `field` with both `tv_sec` and `tv_nsec` zero is treated as "no
/// expiration set" and never expires.
#[inline]
const fn did_expire(field: &timespec, now: &timespec) -> bool {
    if field.tv_sec == 0 && field.tv_nsec == 0 {
        return false;
    }
    !((field.tv_sec > now.tv_sec) || (field.tv_sec == now.tv_sec && field.tv_nsec > now.tv_nsec))
}

#[cfg(test)]
#[expect(
    clippy::undocumented_unsafe_blocks,
    reason = "every `t.add` site below passes a non-empty, fresh-doc-id pair by inspection"
)]
mod tests {
    use super::test_utils::*;
    use super::*;

    #[test]
    fn verify_field_returns_true_for_doc_colliding_with_a_known_doc() {
        // doc_id 1 is in the table; doc_id 9 also hashes to slot 1 but is
        // absent. Per docs: if the document has no entry, both predicates
        // return true.
        const MAX: usize = 8;
        let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        // SAFETY: `DOC_ID_1` is fresh; the single-entry list is non-empty.
        unsafe { t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)])) };

        let unknown_collider: u64 = DOC_ID_1 + MAX as u64;
        // Be sure it is a collider
        assert_eq!(t.slot(DOC_ID_1), t.slot(unknown_collider));

        assert!(t.verify_doc_and_field(
            unknown_collider,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        assert!(t.verify_doc_and_field(
            unknown_collider,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Missing,
            &NOW,
        ));
    }

    #[test]
    fn remove_first_of_three_collider_bucket_keeps_others_findable() {
        const MAX: usize = 8;

        const DOC_ID_1_COLLIDER_1: u64 = DOC_ID_1 + MAX as u64;
        const DOC_ID_1_COLLIDER_2: u64 = DOC_ID_1 + (MAX as u64) * 2;

        let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());

        // ensure collisions
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER_1));
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER_2));

        // SAFETY (each call below): the `doc_id` is unique across this test
        // and each single-element list is non-empty.
        unsafe { t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)])) };
        unsafe { t.add(DOC_ID_1_COLLIDER_1, fes([fe(FIELD_INDEX_1, PAST)])) };
        unsafe { t.add(DOC_ID_1_COLLIDER_2, fes([fe(FIELD_INDEX_1, FUTURE)])) };
        t.remove(DOC_ID_1);
        // Doc 9: PAST ⇒ Default false.
        assert!(!t.verify_doc_and_field(
            DOC_ID_1_COLLIDER_1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        // Doc 17: FUTURE ⇒ Default true.
        assert!(t.verify_doc_and_field(
            DOC_ID_1_COLLIDER_2,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        t.remove(DOC_ID_1_COLLIDER_1);
        assert!(t.verify_doc_and_field(
            DOC_ID_1_COLLIDER_2,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        assert!(!t.is_empty());
        t.remove(DOC_ID_1_COLLIDER_2);
        assert!(t.is_empty());
    }

    #[test]
    fn remove_doc_absent_from_existing_bucket_is_noop() {
        const MAX: usize = 8;
        const DOC_ID_1_COLLIDER: u64 = DOC_ID_1 + MAX as u64;

        let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        // SAFETY: `DOC_ID_1` is fresh; the single-element list is non-empty.
        unsafe { t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)])) };

        // Slot collider
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER));
        t.remove(DOC_ID_1_COLLIDER);

        assert!(!t.is_empty());
        assert!(t.verify_doc_and_field(
            DOC_ID_1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
    }

    #[test]
    fn max_size_one_collapses_every_doc_to_slot_zero() {
        const MAX: usize = 1;
        let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        // SAFETY (each call below): doc_ids 0, 1, 2 are distinct; each
        // single-element list is non-empty.
        unsafe { t.add(0, fes([fe(FIELD_INDEX_1, FUTURE)])) };
        unsafe { t.add(1, fes([fe(FIELD_INDEX_1, PAST)])) };
        unsafe { t.add(2, fes([fe(FIELD_INDEX_1, FUTURE)])) };
        assert_eq!(t.n_allocated_buckets(), 1);
        assert!(t.verify_doc_and_field(0, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW,));
        assert!(
            !t.verify_doc_and_field(1, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW,)
        );
        assert!(t.verify_doc_and_field(2, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW,));

        assert_eq!(t.slot(0), t.slot(1));
        assert_eq!(t.slot(0), t.slot(2));
    }

    #[test]
    #[should_panic(expected = "field_expirations is guaranteed to not be empty")]
    fn verify_mask_panics_when_entry_has_no_field_expirations() {
        let entry = TimeToLiveEntry {
            doc_id: DOC_ID_1,
            field_expirations: empty_fields(),
        };
        let map = identity_ft_id();
        verify_mask(
            Some(&entry),
            mask_bit(&[0, 1, 2]),
            FieldExpirationPredicate::Default,
            &NOW,
            &map,
        );
    }

    #[test]
    fn fast_path_and_modulo_path_doc_ids_coexist_in_same_bucket() {
        // docId `x` uses the fast path (`x < max_size`); `x + CAP` and
        // `x + 2*CAP` take the modulo path (`>= max_size`). All three hash to
        // the same slot. Verifies the two `slot()` arms route to the same
        // bucket and stay distinguishable, then removes the middle layer to
        // confirm swap-last does not corrupt the outer entries.
        const CAP: u64 = 32;
        let mut t = TimeToLiveTable::new(NonZeroUsize::new(CAP as usize).unwrap());

        for x in 1u64..8 {
            assert_eq!(t.slot(x), t.slot(x + CAP));
            assert_eq!(t.slot(x), t.slot(x + 2 * CAP));
            // SAFETY (each call below): `x`, `x + CAP`, `x + 2 * CAP` are
            // distinct and never repeat across loop iterations; each
            // single-element list is non-empty.
            unsafe { t.add(x, fes([fe(0, PAST)])) };
            unsafe { t.add(x + CAP, fes([fe(0, FUTURE)])) };
            unsafe { t.add(x + 2 * CAP, fes([fe(0, PAST)])) };
        }
        for x in 1u64..8 {
            assert!(!t.verify_doc_and_field(x, 0, FieldExpirationPredicate::Default, &NOW));
            assert!(t.verify_doc_and_field(x + CAP, 0, FieldExpirationPredicate::Default, &NOW));
            assert!(!t.verify_doc_and_field(
                x + 2 * CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW,
            ));
            // A never-inserted docId hashing to the same slot must report "no TTL".
            assert!(t.verify_doc_and_field(
                x + 3 * CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW,
            ));
        }

        for x in 1u64..8 {
            t.remove(x + CAP);
        }
        for x in 1u64..8 {
            assert!(!t.verify_doc_and_field(x, 0, FieldExpirationPredicate::Default, &NOW));
            // Removed docs report "no TTL" ⇒ Default true.
            assert!(t.verify_doc_and_field(x + CAP, 0, FieldExpirationPredicate::Default, &NOW));
            assert!(!t.verify_doc_and_field(
                x + 2 * CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW,
            ));
        }
    }

    #[test]
    fn verify_mask_u64_walks_bits_above_31() {
        // High-bit field (bit 40) is the only one tracked, and it is
        // expired. Under Default this must report `false` (no valid
        // field); under Missing it must report `true`.
        const HIGH_BIT: u16 = 40;
        let entry = TimeToLiveEntry {
            doc_id: DOC_ID_1,
            field_expirations: fes([fe(HIGH_BIT, PAST)]),
        };
        let map = identity_ft_id();
        let mask = mask_bit_u64(&[HIGH_BIT]);

        assert!(!verify_mask::<u64>(
            Some(&entry),
            mask,
            FieldExpirationPredicate::Default,
            &NOW,
            &map,
        ));
        assert!(verify_mask::<u64>(
            Some(&entry),
            mask,
            FieldExpirationPredicate::Missing,
            &NOW,
            &map,
        ));
    }

    #[test]
    fn verify_mask_u64_default_returns_true_when_any_selected_field_is_valid() {
        // Bit 2 → PAST, bit 35 → FUTURE. Default must return `true`.
        let entry = TimeToLiveEntry {
            doc_id: DOC_ID_1,
            field_expirations: fes([fe(2, PAST), fe(35, FUTURE)]),
        };
        let map = identity_ft_id();
        let mask = mask_bit_u64(&[2, 35]);

        assert!(verify_mask::<u64>(
            Some(&entry),
            mask,
            FieldExpirationPredicate::Default,
            &NOW,
            &map,
        ));
    }
}
