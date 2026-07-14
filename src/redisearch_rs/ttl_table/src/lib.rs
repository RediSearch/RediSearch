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
//! Data layout and concurrency:
//!
//! - A direct-modulo bucket array (`slot = doc_id % max_size`) — no hashing,
//!   so monotonically allocated docIds map to sequential slots and the CPU
//!   prefetcher can stream upcoming bucket slots into L1.
//! - Each slot is an atomic pointer to an **immutable** bucket (a boxed slice
//!   of entries). A slot is never mutated in place: `add`/`remove` build a new
//!   bucket (copy-on-write of that one slot's short collision chain), publish
//!   it with a release store, and retire the old bucket for safe reclamation.
//! - Lazy growth: the bucket array starts empty and grows geometrically
//!   (+1.5×, capped at `1 << 20` and clamped to `max_size`) only as `add`
//!   demands more slots. Growth allocates a fresh array, copies the slot
//!   pointers (buckets are shared, not copied), publishes it, and retires the
//!   old array — never `realloc`, so a concurrent reader is never left holding
//!   a freed array.
//!
//! This makes reads **lock-free**: a reader loads the published array and slot
//! pointers with acquire ordering and walks an immutable bucket without any
//! lock, while a single writer mutates under the spec write lock. Reclamation
//! of retired buckets/arrays is deferred until no reader is in flight (see
//! [`reclaim`]). Methods take `&self`; callers must guarantee a single writer
//! (the spec write lock provides this).
//!
//! The table holds only field-level (HEXPIRE) expirations; document-level
//! TTL lives directly on `RSDocumentMetadata::expirationTimeNs` so the
//! result-processor hot path avoids a lookup here.

#[cfg(feature = "test-utils")]
pub mod test_utils;

mod reclaim;

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::{iter::Chain, num::NonZeroUsize, ops::Deref, ptr};

use ffi::t_expirationTimePoint as timespec;
pub use field::FieldExpirationPredicate;
use rqe_core::{DocId, FieldIndex, FieldMask};
use thin_vec::{AlignedU32, ThinVec};

/// A single field's expiration record.
///
/// Pairs a field identifier ([`index`](Self::index)) with the
/// wall-clock instant ([`point`](Self::point)) at which that field expires.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FieldExpiration {
    /// The zero-based field index identifying which field carries this expiration.
    pub index: FieldIndex,
    /// The absolute wall-clock instant at which the field expires.
    ///
    /// When both `tv_sec` and `tv_nsec` are `0` the entry is treated as
    /// "never expires".
    pub point: timespec,
}

/// An ascending-by-`index`, duplicate-free list of [`FieldExpiration`] entries.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct FieldExpirations {
    /// Backing store for the field list.
    ///
    /// We use a `ThinVec` with an over-aligned [`AlignedU32`] capacity rather than
    /// the default `ThinVec` for performance: the over-alignment lets `data_raw`
    /// elide its `capacity == 0` guard on the verify hot path.
    inner: ThinVec<FieldExpiration, AlignedU32>,
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
    pub fn add(&mut self, fe: FieldExpiration) -> Result<&FieldExpiration, FieldExpiration> {
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
    /// # Panics
    ///
    /// `fe.index` must be **strictly greater** than the `index` of every
    /// entry already present in `self`.
    pub fn push(&mut self, fe: FieldExpiration) {
        if let Some(last) = self.last() {
            assert!(
                last.index < fe.index,
                "pushed index {} must be strictly greater than the last index {}",
                fe.index,
                last.index
            );
        }
        // Safety: checked above
        unsafe {
            self.push_unchecked(fe);
        }
    }

    /// Appends `fe` to the end of the list without searching for the
    /// insertion point.
    ///
    /// Use when the caller already knows that `fe.index` is strictly greater
    /// than every index currently in `self`.
    ///
    /// # Safety
    ///
    /// `fe.index` must be **strictly greater** than the `index` of every
    /// entry already present in `self`.
    pub unsafe fn push_unchecked(&mut self, fe: FieldExpiration) {
        #[cfg(debug_assertions)]
        {
            if let Some(last) = self.last() {
                assert!(
                    last.index < fe.index,
                    "pushed index {} must be strictly greater than the last index {}",
                    fe.index,
                    last.index
                );
            }
        }

        // `MediumThinVec::push` grows the chain exponentially when full.
        self.inner.push(fe);
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Shrinks the backing store so its capacity equals its length.
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }
}

impl Default for FieldExpirations {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only access to the underlying data.
impl Deref for FieldExpirations {
    type Target = [FieldExpiration];

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
///
/// `Clone` is used for the copy-on-write of a slot's chain on `add`/`remove`.
#[derive(Debug, Clone)]
pub struct TimeToLiveEntry {
    /// The document id
    pub doc_id: DocId,
    /// Owned, sorted by field index, never empty.
    pub field_expirations: FieldExpirations,
}

/// An immutable published bucket: one slot's collision chain.
///
/// Never mutated after publication; `add`/`remove` build a fresh `Bucket` and
/// swap the slot pointer, retiring the old one.
#[derive(Debug)]
struct Bucket {
    entries: Box<[TimeToLiveEntry]>,
}

/// An immutable published bucket array. Grown by allocating a fresh array and
/// copying the (shared) slot pointers, then retiring the old array — so a
/// concurrent reader is never left holding a freed array. Each slot is either
/// null (empty) or owns one `Box<Bucket>`.
#[derive(Debug)]
struct BucketArray {
    slots: Box<[AtomicPtr<Bucket>]>,
}

/// Reclamation destructor for a retired `Box<Bucket>`.
///
/// # Safety
/// `p` must be a `*mut Bucket` from `Box::into_raw`, retired exactly once.
unsafe fn drop_bucket(p: usize) {
    // SAFETY: caller contract — `p` is a once-retired `Box<Bucket>` raw pointer.
    drop(unsafe { Box::from_raw(p as *mut Bucket) });
}

/// Reclamation destructor for a retired `Box<BucketArray>`. Frees only the slot
/// array; the buckets it referenced are shared with the successor array.
///
/// # Safety
/// `p` must be a `*mut BucketArray` from `Box::into_raw`, retired exactly once.
unsafe fn drop_bucket_array(p: usize) {
    // SAFETY: caller contract — `p` is a once-retired `Box<BucketArray>` raw
    // pointer. `AtomicPtr`'s drop does not touch the pointees, so shared buckets
    // are left intact.
    drop(unsafe { Box::from_raw(p as *mut BucketArray) });
}

/// Direct-modulo, lock-free-read time-to-live table.
///
/// See the module-level documentation. Reads are lock-free; a single writer
/// (the spec write lock) mutates via `&self` using atomic publish + deferred
/// reclamation.
pub struct TimeToLiveTable {
    /// Atomically-published bucket array. Never null after construction.
    array: AtomicPtr<BucketArray>,
    /// Modulus for the slot formula. Captured at construction; never changes.
    max_size: usize,
    /// Number of stored documents.
    count: AtomicUsize,
}

// `TimeToLiveTable` is `Send + Sync` automatically (`AtomicPtr`/`AtomicUsize`
// are both). That is sound here because all shared state is accessed through
// atomics and immutable published buckets: the single-writer invariant (spec
// write lock) rules out write-write races, and readers only ever observe
// published, retire-protected memory.

impl TimeToLiveTable {
    /// Creates an empty table with `max_size` as the fixed modulus for
    /// the slot formula.
    ///
    /// The bucket array starts empty and grows on demand.
    pub fn new(max_size: NonZeroUsize) -> Self {
        let empty = Box::new(BucketArray {
            slots: Box::new([]),
        });
        Self {
            array: AtomicPtr::new(Box::into_raw(empty)),
            max_size: max_size.into(),
            count: AtomicUsize::new(0),
        }
    }

    /// Returns `true` if the table holds no entries.
    pub fn is_empty(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }

    /// Load the currently-published bucket array.
    ///
    /// The returned reference is valid for as long as the caller holds a
    /// [`reclaim`] pin (readers) or the single-writer invariant holds (writer).
    #[inline]
    fn array(&self) -> &BucketArray {
        // SAFETY: `array` is published at construction and only ever replaced
        // (never nulled) by the single writer; the old array is retired, not
        // freed inline, so this pointer is valid under a reader pin / on the
        // writer thread.
        unsafe { &*self.array.load(Ordering::Acquire) }
    }

    /// Inserts a document's per-field expirations.
    ///
    /// # Panics
    ///
    /// Panics if `field_expirations` is empty or if `doc_id` is already present
    /// in the table.
    pub fn add(&self, doc_id: DocId, field_expirations: FieldExpirations) {
        assert!(
            !field_expirations.is_empty(),
            "TTL table add requires at least one field expiration"
        );
        assert!(
            self.find_entry(doc_id).is_none(),
            "duplicate docId in TTL table"
        );
        // SAFETY: both preconditions of `add_unchecked` were just verified —
        // `field_expirations` is non-empty and `doc_id` is absent from the table.
        unsafe { self.add_unchecked(doc_id, field_expirations) };
    }

    /// Inserts a document's per-field expirations.
    ///
    /// # Safety
    ///
    /// The caller must guarantee:
    /// - `field_expirations` is non-empty.
    /// - `doc_id` is not already present in the table.
    ///
    /// These invariants are load-bearing for the lookup hot paths
    /// ([`field_satisfies_predicate`](Self::field_satisfies_predicate),
    /// [`field_mask_satisfies_predicate`](Self::field_mask_satisfies_predicate)),
    /// which assume them when scanning the per-bucket chain and the
    /// per-entry field-expiration list.
    pub unsafe fn add_unchecked(&self, doc_id: DocId, field_expirations: FieldExpirations) {
        debug_assert!(
            !field_expirations.is_empty(),
            "TTL table add requires at least one field expiration"
        );

        let slot = self.slot(doc_id);
        self.grow_to(slot);

        let array = self.array();
        let old = array.slots[slot].load(Ordering::Acquire);

        // Copy-on-write the slot's short collision chain: clone the existing
        // entries, append the new one, and publish as a fresh immutable bucket.
        let mut entries: Vec<TimeToLiveEntry> = if old.is_null() {
            Vec::with_capacity(1)
        } else {
            // SAFETY: `old` is a published bucket; single-writer means it is not
            // being mutated, and it is not freed while we hold the writer role.
            let old_bucket = unsafe { &*old };
            debug_assert!(
                !old_bucket.entries.iter().any(|e| e.doc_id == doc_id),
                "duplicate docId in TTL table"
            );
            old_bucket.entries.to_vec()
        };
        entries.push(TimeToLiveEntry {
            doc_id,
            field_expirations,
        });
        let new_bucket = Box::into_raw(Box::new(Bucket {
            entries: entries.into_boxed_slice(),
        }));

        array.slots[slot].store(new_bucket, Ordering::Release);
        if !old.is_null() {
            // SAFETY: `old` was just unlinked from the slot (RETIRE-AFTER-UNLINK)
            // and is retired exactly once here.
            unsafe { reclaim::retire(old as usize, drop_bucket) };
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes the entry for `doc_id`, if any. No-op if absent.
    ///
    /// Copy-on-write: publishes a fresh bucket without the entry (or null when
    /// the chain empties) and retires the old bucket. Returns a clone of the
    /// removed entry, since the old bucket is immutable and shared.
    pub fn remove(&self, doc_id: DocId) -> Option<TimeToLiveEntry> {
        let slot = self.slot(doc_id);
        let array = self.array();
        if slot >= array.slots.len() {
            return None;
        }
        let old = array.slots[slot].load(Ordering::Acquire);
        if old.is_null() {
            return None;
        }
        // SAFETY: `old` is a published bucket; single-writer means it is stable
        // and not freed while we hold the writer role.
        let old_bucket = unsafe { &*old };
        let pos = old_bucket.entries.iter().position(|e| e.doc_id == doc_id)?;

        let removed = old_bucket.entries[pos].clone();
        if old_bucket.entries.len() == 1 {
            // Chain empties: publish null so the slot is free again.
            array.slots[slot].store(ptr::null_mut(), Ordering::Release);
        } else {
            let entries: Vec<TimeToLiveEntry> = old_bucket
                .entries
                .iter()
                .enumerate()
                .filter_map(|(i, e)| (i != pos).then(|| e.clone()))
                .collect();
            let new_bucket = Box::into_raw(Box::new(Bucket {
                entries: entries.into_boxed_slice(),
            }));
            array.slots[slot].store(new_bucket, Ordering::Release);
        }
        // SAFETY: `old` was just unlinked from the slot (RETIRE-AFTER-UNLINK)
        // and is retired exactly once here.
        unsafe { reclaim::retire(old as usize, drop_bucket) };
        self.count.fetch_sub(1, Ordering::Relaxed);
        Some(removed)
    }

    /// Return the number of bucket slots currently allocated (the published
    /// array length — the lazy-growth high-water mark).
    pub fn n_allocated_buckets(&self) -> usize {
        self.array().slots.len()
    }

    /// Returns the per-field expiration list stored for `doc_id`, or `None`
    /// if no entry exists.
    ///
    /// The slice aliases storage owned by the table and is invalidated by any
    /// subsequent [`add`](Self::add) / [`add_unchecked`](Self::add_unchecked) /
    /// [`remove`](Self::remove) on this table.
    pub fn field_expirations(&self, doc_id: DocId) -> Option<&FieldExpirations> {
        self.find_entry(doc_id).map(|e| &e.field_expirations)
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
    pub fn field_satisfies_predicate(
        &self,
        doc_id: DocId,
        field: u16,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
    ) -> bool {
        // Lock-free read section: keeps the bucket `find_entry` returns alive
        // for the whole scan (no borrow escapes this call).
        let _guard = reclaim::pin();
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

    /// Checks the expiration state of a set of fields described by
    /// `field_mask` under `predicate`.
    ///
    /// `field_mask` is always taken as a wide [`FieldMask`]. When `wide` is
    /// `false` (the narrow-schema case, at most 32 fields) only the low 32
    /// bits are meaningful and the mask is walked with the faster `u32`
    /// bit-walk; when `wide` is `true` the full [`FieldMask`] width
    /// (`u64`/`u128`, chosen by the C build) is walked.
    ///
    /// `ft_id_to_field_index[bit]` translates a bit position in the mask
    /// into the `FieldIndex` recorded in the table; it must contain at
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
    pub fn field_mask_satisfies_predicate(
        &self,
        doc_id: DocId,
        field_mask: FieldMask,
        predicate: FieldExpirationPredicate,
        expiration_point: &timespec,
        ft_id_to_field_index: &[u16],
        wide: bool,
    ) -> bool {
        // Lock-free read section: keeps the entry's bucket alive across the
        // mask walk (no borrow escapes this call).
        let _guard = reclaim::pin();
        let entry = self.find_entry(doc_id);
        if wide {
            verify_mask::<FieldMask>(
                entry,
                field_mask,
                predicate,
                expiration_point,
                ft_id_to_field_index,
            )
        } else {
            verify_mask::<u32>(
                entry,
                field_mask as u32,
                predicate,
                expiration_point,
                ft_id_to_field_index,
            )
        }
    }

    /// Direct-modulo slot formula
    const fn slot(&self, doc_id: DocId) -> usize {
        if doc_id < self.max_size as u64 {
            doc_id as usize
        } else {
            (doc_id % self.max_size as u64) as usize
        }
    }

    /// Ensures the published array has a slot for `slot`.
    ///
    /// The first grow seeds at `TTL_BUCKET_INITIAL_CAP`, subsequent grows are
    /// `+1 + min(cap/2, TTL_BUCKET_MAX_GROW_STEP)`, all clamped to `max_size`
    /// and rounded up to cover the requested `slot`. Growth allocates a fresh
    /// array, copies the (shared) slot pointers, publishes it with a release
    /// store, and retires the old array — never `realloc`, so a concurrent
    /// reader is never left holding a freed array.
    ///
    /// Writer-only (single writer under the spec write lock).
    fn grow_to(&self, slot: usize) {
        debug_assert!(slot < self.max_size);
        let old_array = self.array();
        let cap = old_array.slots.len();
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

        // Copy existing slot pointers (buckets are shared with the old array),
        // null-fill the tail.
        let mut new_slots: Vec<AtomicPtr<Bucket>> = Vec::with_capacity(newcap);
        for i in 0..newcap {
            let p = if i < cap {
                old_array.slots[i].load(Ordering::Acquire)
            } else {
                ptr::null_mut()
            };
            new_slots.push(AtomicPtr::new(p));
        }
        let new_array = Box::into_raw(Box::new(BucketArray {
            slots: new_slots.into_boxed_slice(),
        }));
        let old = self.array.swap(new_array, Ordering::AcqRel);
        // SAFETY: the old array is now unreachable to new readers
        // (RETIRE-AFTER-UNLINK); its slot storage is retired here exactly once.
        // The buckets it referenced are shared with `new_array` and are NOT
        // freed by `drop_bucket_array`.
        unsafe { reclaim::retire(old as usize, drop_bucket_array) };
    }

    /// Locate the entry for `doc_id`. The returned reference borrows a published
    /// bucket; the caller must hold a [`reclaim`] pin (readers) or be the single
    /// writer for it to stay valid.
    fn find_entry(&self, doc_id: DocId) -> Option<&TimeToLiveEntry> {
        let array = self.array();
        let slot = self.slot(doc_id);
        let bucket_ptr = array.slots.get(slot)?.load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }
        // SAFETY: a non-null slot points to a published immutable `Bucket`, kept
        // alive by the caller's pin / the single-writer invariant.
        let bucket = unsafe { &*bucket_ptr };
        bucket.entries.iter().find(|e| e.doc_id == doc_id)
    }
}

impl Drop for TimeToLiveTable {
    fn drop(&mut self) {
        // Teardown runs with no lock-free readers in flight; flush anything this
        // table retired, then free the live array and its buckets.
        reclaim::try_reclaim();
        let array_ptr = *self.array.get_mut();
        if array_ptr.is_null() {
            return;
        }
        // SAFETY: `&mut self` gives exclusive access; `array_ptr` was published
        // by this table via `Box::into_raw` and is freed exactly once here.
        let array = unsafe { Box::from_raw(array_ptr) };
        for slot in array.slots.iter() {
            let b = slot.load(Ordering::Acquire);
            if !b.is_null() {
                // SAFETY: each non-null slot owns exactly one `Box<Bucket>`.
                drop(unsafe { Box::from_raw(b) });
            }
        }
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
    let field_with_expiration_length = field_expirations.len() as u16;

    let field_count: u16 = mask.count_ones() as u16;
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
    unsafe fn get_unchecked<T>(arr: &[T], index: u32) -> &T {
        debug_assert!(
            index < arr.len() as u32,
            "Safety violation: try to access to an out-of-bounds index, {index}. Length {}",
            arr.len(),
        );
        // SAFETY: Function safety guarantees
        unsafe { arr.get_unchecked(index as usize) }
    }

    let mut predicate_misses: u16 = 0;
    let mut current_field_index: u16 = 0;

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
        let field_index_to_check = unsafe { *get_unchecked(ft_id_to_field_index, bit_index) };

        // Advance the cursor over fields strictly less than the one
        // we are checking.
        while current_field_index < field_with_expiration_length {
            // SAFETY: the `while` condition above proves it
            let candidate_index =
                unsafe { get_unchecked(field_expirations, current_field_index as u32).index };
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
        let entry_field = unsafe { get_unchecked(field_expirations, current_field_index as u32) };
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
mod tests {
    use super::test_utils::*;
    use super::*;

    #[test]
    fn field_expirations_capacity_and_shrink_to_fit() {
        let mut fields = FieldExpirations::new();
        assert_eq!(fields.len(), 0);
        assert_eq!(fields.capacity(), 0);

        // The first push seeds the backing store's native exponential growth,
        // leaving spare capacity beyond `len`.
        fields.push(fe(FIELD_INDEX_1, FUTURE));
        assert_eq!(fields.len(), 1);
        assert!(fields.capacity() > fields.len());

        // Shrinking releases the spare capacity so it matches `len`.
        fields.shrink_to_fit();
        assert_eq!(fields.capacity(), fields.len());
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn verify_field_returns_true_for_doc_colliding_with_a_known_doc() {
        // doc_id 1 is in the table; doc_id 9 also hashes to slot 1 but is
        // absent. Per docs: if the document has no entry, both predicates
        // return true.
        const MAX: usize = 8;
        let t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));

        let unknown_collider: u64 = DOC_ID_1 + MAX as u64;
        // Be sure it is a collider
        assert_eq!(t.slot(DOC_ID_1), t.slot(unknown_collider));

        assert!(t.field_satisfies_predicate(
            unknown_collider,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        assert!(t.field_satisfies_predicate(
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

        let t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());

        // ensure collisions
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER_1));
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER_2));

        t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
        t.add(DOC_ID_1_COLLIDER_1, fes([fe(FIELD_INDEX_1, PAST)]));
        t.add(DOC_ID_1_COLLIDER_2, fes([fe(FIELD_INDEX_1, FUTURE)]));
        t.remove(DOC_ID_1);
        // Doc 9: PAST ⇒ Default false.
        assert!(!t.field_satisfies_predicate(
            DOC_ID_1_COLLIDER_1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        // Doc 17: FUTURE ⇒ Default true.
        assert!(t.field_satisfies_predicate(
            DOC_ID_1_COLLIDER_2,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        t.remove(DOC_ID_1_COLLIDER_1);
        assert!(t.field_satisfies_predicate(
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

        let t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));

        // Slot collider
        assert_eq!(t.slot(DOC_ID_1), t.slot(DOC_ID_1_COLLIDER));
        t.remove(DOC_ID_1_COLLIDER);

        assert!(!t.is_empty());
        assert!(t.field_satisfies_predicate(
            DOC_ID_1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
    }

    #[test]
    fn max_size_one_collapses_every_doc_to_slot_zero() {
        const MAX: usize = 1;
        let t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        t.add(0, fes([fe(FIELD_INDEX_1, FUTURE)]));
        t.add(1, fes([fe(FIELD_INDEX_1, PAST)]));
        t.add(2, fes([fe(FIELD_INDEX_1, FUTURE)]));
        assert_eq!(t.n_allocated_buckets(), 1);
        assert!(t.field_satisfies_predicate(
            0,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        assert!(!t.field_satisfies_predicate(
            1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
        assert!(t.field_satisfies_predicate(
            2,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));

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
        let t = TimeToLiveTable::new(NonZeroUsize::new(CAP as usize).unwrap());

        for x in 1u64..8 {
            assert_eq!(t.slot(x), t.slot(x + CAP));
            assert_eq!(t.slot(x), t.slot(x + 2 * CAP));
            t.add(x, fes([fe(0, PAST)]));
            t.add(x + CAP, fes([fe(0, FUTURE)]));
            t.add(x + 2 * CAP, fes([fe(0, PAST)]));
        }
        for x in 1u64..8 {
            assert!(!t.field_satisfies_predicate(x, 0, FieldExpirationPredicate::Default, &NOW));
            assert!(t.field_satisfies_predicate(
                x + CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW
            ));
            assert!(!t.field_satisfies_predicate(
                x + 2 * CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW,
            ));
            // A never-inserted docId hashing to the same slot must report "no TTL".
            assert!(t.field_satisfies_predicate(
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
            assert!(!t.field_satisfies_predicate(x, 0, FieldExpirationPredicate::Default, &NOW));
            // Removed docs report "no TTL" ⇒ Default true.
            assert!(t.field_satisfies_predicate(
                x + CAP,
                0,
                FieldExpirationPredicate::Default,
                &NOW
            ));
            assert!(!t.field_satisfies_predicate(
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

    #[test]
    fn push_appends_entries_with_strictly_increasing_index() {
        let mut list = FieldExpirations::new();
        list.push(fe(1, PAST));
        list.push(fe(2, NOW));
        list.push(fe(3, FUTURE));

        let indices: Vec<u16> = list.iter().map(|fe| fe.index).collect();
        assert_eq!(indices, [1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "must be strictly greater than the last index")]
    fn push_panics_when_index_not_greater_than_last() {
        let mut list = FieldExpirations::new();
        list.push(fe(5, PAST));
        // Deliberately violates the precondition (3 < 5) to exercise the panic
        // branch; the call is expected to abort the test via panic.
        list.push(fe(3, FUTURE));
    }

    // Four lock-free readers hammer `field_satisfies_predicate` (no lock, only
    // the reclamation pin inside each call) while a single writer inserts and
    // deletes, driving per-bucket copy-on-write, deferred reclamation, and
    // array growth. Must never crash / use-after-free; the boolean result may
    // legitimately vary as the writer mutates concurrently.
    #[test]
    fn concurrent_lock_free_reads_during_writes() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::thread;

        const MAX: usize = 256;
        const N: u64 = 20_000;

        let table = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
        let stop = AtomicBool::new(false);
        let hi = AtomicU64::new(0);

        let table = &table;
        let stop = &stop;
        let hi = &hi;

        thread::scope(|s| {
            for r in 0..4u64 {
                s.spawn(move || {
                    let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ r;
                    while !stop.load(Ordering::Acquire) {
                        let n = hi.load(Ordering::Acquire);
                        if n == 0 {
                            thread::yield_now();
                            continue;
                        }
                        for _ in 0..128 {
                            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let id = 1 + (seed % n);
                            let _ = table.field_satisfies_predicate(
                                id,
                                FIELD_INDEX_1,
                                FieldExpirationPredicate::Default,
                                &NOW,
                            );
                        }
                    }
                });
            }
            s.spawn(move || {
                for i in 1..=N {
                    table.add(i, fes([fe(FIELD_INDEX_1, FUTURE)]));
                    hi.store(i, Ordering::Release);
                    if i > 40 && i % 3 == 0 {
                        let _ = table.remove(i - 23);
                    }
                }
                stop.store(true, Ordering::Release);
            });
        });

        // A survivor that was never deleted still resolves after all the churn.
        assert!(table.field_satisfies_predicate(
            1,
            FIELD_INDEX_1,
            FieldExpirationPredicate::Default,
            &NOW,
        ));
    }
}
