/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable bindings for [`ttl_table::TimeToLiveTable`].

use std::{marker::PhantomData, num::NonZeroUsize, ptr, slice};

use ffi::t_expirationTimePoint;
use field::FieldExpirationPredicate;
use rqe_core::{DocId, FieldMask};
pub use ttl_table::TimeToLiveTable;
use ttl_table::{FieldExpiration, FieldExpirations};

/// Borrowed view of a contiguous run of [`FieldExpiration`] entries.
///
/// The returned pointer aliases storage owned by the table — it is invalidated
/// by any subsequent `_Add` / `_Remove` / `_Destroy` for this table and must
/// not be freed by the caller. A miss is encoded as `ptr == NULL` and
/// `len == 0`.
#[repr(C)]
pub struct FieldExpirationSlice<'a> {
    pub ptr: *const FieldExpiration,
    pub len: usize,
    /// Ties the view to the storage it borrows. Zero-sized, so the C ABI is
    /// still `{ ptr, len }`.
    _marker: PhantomData<&'a [FieldExpiration]>,
}

impl<'a> FieldExpirationSlice<'a> {
    const EMPTY: Self = Self {
        ptr: ptr::null(),
        len: 0,
        _marker: PhantomData,
    };

    fn from_fields(fields: &'a FieldExpirations) -> Self {
        Self {
            ptr: fields.as_ptr(),
            len: fields.len(),
            _marker: PhantomData,
        }
    }
}

/// Lazy-initialize a [`TimeToLiveTable`] at `*table`. No-op if `*table` is
/// already non-null. `max_size` is the fixed modulus for the slot formula
/// so it must be ≥ 1.
///
/// # Panics
/// `max_size` must be ≥ 1, otherwise the function panics
///
/// # Safety
///  - `table` must be a valid, writable `*mut *mut TimeToLiveTable`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_VerifyInit(
    table: *mut *mut TimeToLiveTable,
    max_size: usize,
) {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees `table` is a valid writable pointer.
    let slot = unsafe { &mut *table };
    if !slot.is_null() {
        return;
    }
    let max = NonZeroUsize::new(max_size).expect("TTL table maxSize must be >= 1");
    *slot = Box::into_raw(Box::new(TimeToLiveTable::new(max)));
}

/// Destroy the [`TimeToLiveTable`] at `*table` and write `NULL` back to it.
/// No-op if `*table` is already null.
///
/// # Safety
///  - `table` must be a valid, writable `*mut *mut TimeToLiveTable`.
///  - If `*table` is non-null, it must point to a value previously returned
///    by [`TimeToLiveTable_VerifyInit`] and not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_Destroy(table: *mut *mut TimeToLiveTable) {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees `table` is a valid writable pointer.
    let slot = unsafe { &mut *table };
    if slot.is_null() {
        return;
    }
    // SAFETY: `*slot` was obtained from `Box::into_raw` in `_VerifyInit`
    // and has not been freed since.
    drop(unsafe { Box::from_raw(*slot) });
    *slot = ptr::null_mut();
}

/// Insert a document's per-field expirations. Ownership of `field_expirations`
/// transfers to the table; the caller must not touch its storage
/// afterwards.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`] (no
///    other reference to it must exist for the duration of the call).
///  - `field_expirations` must have been produced by [`FieldExpirations_Empty`]
///    or [`FieldExpirations_WithCapacity`] and must be non-empty.
///    Sortedness and uniqueness-by-`index` are carried by the type itself.
///  - `doc_id` must not already be present in the table.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_Add(
    table: *mut TimeToLiveTable,
    doc_id: DocId,
    field_expirations: FieldExpirations,
) {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees pointer validity and exclusive access.
    let inner = unsafe { &mut *table };
    // SAFETY: caller upholds `add`'s preconditions (documented above).
    unsafe { inner.add_unchecked(doc_id, field_expirations) };
}

/// Remove the entry for `doc_id`, if any. No-op if absent.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`] with
///    no other live references.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_Remove(table: *mut TimeToLiveTable, doc_id: DocId) {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees pointer validity and exclusive access.
    let inner = unsafe { &mut *table };
    let _ = inner.remove(doc_id);
}

/// Returns whether the table holds no entries.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_IsEmpty(table: *const TimeToLiveTable) -> bool {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees pointer validity.
    unsafe { &*table }.is_empty()
}

/// Borrow the per-field expiration list stored for `doc_id`.
///
/// Returns a [`FieldExpirationSlice`] borrowing storage owned by the table.
/// On miss, both `ptr` and `len` are zero. The returned pointer is invalidated
/// by any subsequent `_Add` / `_Remove` / `_Destroy` for this table and must
/// not be freed by the caller.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_GetFieldExpirations<'a>(
    table: *const TimeToLiveTable,
    doc_id: DocId,
) -> FieldExpirationSlice<'a> {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    // SAFETY: caller guarantees pointer validity.
    let inner = unsafe { &*table };
    match inner.field_expirations(doc_id) {
        Some(s) => FieldExpirationSlice::from_fields(s),
        None => FieldExpirationSlice::EMPTY,
    }
}

/// Single-field expiration check.
///
/// # Returns
/// `true` if the field's state satisfies `predicate` at `expiration_point`,
/// `false` otherwise. Specifically:
/// - With [`FieldExpirationPredicate::Default`], returns `true` iff the
///   field is not expired (untracked fields and documents with no entry are
///   treated as not expired).
/// - With [`FieldExpirationPredicate::Missing`], returns `true` iff the
///   field is expired.
///
/// Documents with no entry trivially satisfy any predicate and return `true`.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`].
///  - `expiration_point` must be a valid `*const t_expirationTimePoint`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_VerifyDocAndField(
    table: *const TimeToLiveTable,
    doc_id: DocId,
    field_index: u16,
    predicate: FieldExpirationPredicate,
    expiration_point: *const t_expirationTimePoint,
) -> bool {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    debug_assert!(
        !expiration_point.is_null(),
        "expiration_point cannot be NULL"
    );
    // SAFETY: caller guarantees pointer validity.
    let inner = unsafe { &*table };
    // SAFETY: caller guarantees pointer validity.
    let ep = unsafe { &*expiration_point };
    inner.verify_doc_and_field(doc_id, field_index, predicate, ep)
}

/// 32-bit field-mask expiration check.
///
/// `ft_id_to_field_index` must point to at least
/// `highest_set_bit(field_mask) + 1` valid `u16` entries.
/// May be `NULL` when `field_mask == 0`.
///
/// # Returns
/// `true` if the set of fields selected by `field_mask` satisfies
/// `predicate` at `expiration_point`:
/// - With [`FieldExpirationPredicate::Default`], returns `true` iff at
///   least one selected field is not expired.
/// - With [`FieldExpirationPredicate::Missing`], returns `true` iff at
///   least one selected field is expired.
///
/// Documents with no entry trivially return `true` under either predicate.
///
/// # Safety
///  - `table` must point to a valid, initialized [`TimeToLiveTable`].
///  - `expiration_point` must be a valid `*const t_expirationTimePoint`.
///  - `ft_id_to_field_index` must satisfy the bound above.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_VerifyDocAndFieldMask(
    table: *const TimeToLiveTable,
    doc_id: DocId,
    field_mask: u32,
    predicate: FieldExpirationPredicate,
    expiration_point: *const t_expirationTimePoint,
    ft_id_to_field_index: *const u16,
) -> bool {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    debug_assert!(
        !expiration_point.is_null(),
        "expiration_point cannot be NULL"
    );
    // SAFETY: caller guarantees pointer validity.
    let inner = unsafe { &*table };
    // SAFETY: caller guarantees pointer validity.
    let ep = unsafe { &*expiration_point };
    let len = highest_bit_plus_one(field_mask);
    // SAFETY: caller upholds `build_ft_slice`'s contract (length covers the
    // highest set bit of the mask).
    let ft = unsafe { build_ft_slice(ft_id_to_field_index, len) };
    inner.verify_doc_and_field_mask(doc_id, field_mask, predicate, ep, ft)
}

/// Wide field-mask version of [`TimeToLiveTable_VerifyDocAndFieldMask`].
///
/// # Returns
/// Same semantics as [`TimeToLiveTable_VerifyDocAndFieldMask`] — see that
/// function for the predicate / no-entry cases.
///
/// # Safety
/// Same contract as [`TimeToLiveTable_VerifyDocAndFieldMask`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TimeToLiveTable_VerifyDocAndWideFieldMask(
    table: *const TimeToLiveTable,
    doc_id: DocId,
    field_mask: FieldMask,
    predicate: FieldExpirationPredicate,
    expiration_point: *const t_expirationTimePoint,
    ft_id_to_field_index: *const u16,
) -> bool {
    debug_assert!(!table.is_null(), "table cannot be NULL");
    debug_assert!(
        !expiration_point.is_null(),
        "expiration_point cannot be NULL"
    );
    // SAFETY: caller guarantees pointer validity.
    let inner = unsafe { &*table };
    // SAFETY: caller guarantees pointer validity.
    let ep = unsafe { &*expiration_point };

    let len = highest_bit_plus_one_wide(field_mask);
    // SAFETY: caller upholds `build_ft_slice`'s contract (length covers the
    // highest set bit of the mask).
    let ft = unsafe { build_ft_slice(ft_id_to_field_index, len) };
    inner.verify_doc_and_wide_field_mask(doc_id, field_mask, predicate, ep, ft)
}

/// Test-only: number of buckets currently allocated.
///
/// Number of buckets currently allocated (lazy-growth high-water mark).
/// Returns 0 if `table` is `NULL`. Exposed for the C `test_cpp_expire`
/// lazy-growth tests.
///
/// # Returns
/// The current bucket-array length, or `0` if `table` is `NULL`.
///
/// # Safety
///  - If non-null, `table` must point to a valid, initialized
///    [`TimeToLiveTable`].
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TimeToLiveTable_NAllocatedBuckets(
    table: *const TimeToLiveTable,
) -> usize {
    if table.is_null() {
        return 0;
    }
    // SAFETY: caller guarantees non-null `table` is initialized.
    unsafe { &*table }.n_allocated_buckets()
}

/// Construct an empty [`FieldExpirations`].
///
/// No heap allocation is performed until the first insertion.
#[unsafe(no_mangle)]
pub const extern "C" fn FieldExpirations_Empty() -> FieldExpirations {
    FieldExpirations::new()
}

/// Construct a [`FieldExpirations`] with backing storage pre-sized for at
/// least `cap` entries.
#[unsafe(no_mangle)]
pub extern "C" fn FieldExpirations_WithCapacity(cap: usize) -> FieldExpirations {
    FieldExpirations::with_capacity(cap)
}

/// Append `fe` to the end of `*v` without searching for the insertion
/// point.
///
/// Use when the caller already knows that `fe.index` is strictly greater
/// than every index currently in `*v` — typically when building the
/// list from an already-sorted source iterator.
///
/// # Safety
///  - `v` must be a non-null pointer to an initialized [`FieldExpirations`].
///  - `fe.index` must be strictly greater than the `index` of every entry
///    already present in `*v`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FieldExpirations_Push(v: *mut FieldExpirations, fe: FieldExpiration) {
    debug_assert!(!v.is_null(), "v cannot be NULL");
    // SAFETY: caller upholds the contract above.
    let v = unsafe { &mut *v };
    // SAFETY: caller guarantees `fe.index` is strictly greater than every
    // index already present in `*v`, satisfying `push_unchecked`'s contract.
    unsafe {
        v.push_unchecked(fe);
    }
}

/// Number of [`FieldExpiration`] entries currently stored in `*v`.
///
/// # Safety
///  - `v` must be non-null.
///  - `*v` must be either an initialized [`FieldExpirations`] (returned by a
///    constructor in this module).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FieldExpirations_Len(v: *const FieldExpirations) -> usize {
    debug_assert!(!v.is_null(), "v cannot be NULL");

    // SAFETY: not uninitialized, so `*v` is a properly constructed
    // `FieldExpirations` per the caller's contract.
    let v = unsafe { &*v };
    v.len()
}

/// Borrow the contents of `*v` as a [`FieldExpirationSlice`].
///
/// The returned pointer aliases storage owned by `*v`.
///
/// # Safety
///  - `v` must be non-null.
///  - `*v` must be either an initialized [`FieldExpirations`] (returned by a
///    constructor in this module).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FieldExpirations_AsSlice<'a>(
    v: *const FieldExpirations,
) -> FieldExpirationSlice<'a> {
    debug_assert!(!v.is_null(), "v cannot be NULL");

    // SAFETY: not uninitialized, so `*v` is a properly constructed
    // `FieldExpirations` per the caller's contract.
    let v = unsafe { &*v };

    FieldExpirationSlice::from_fields(v)
}

/// Drop the [`FieldExpirations`] at `*v` and leave it in an empty,
/// reusable state.
///
/// Use on the abandon path — when a `FieldExpirations` is built but
/// never handed to [`TimeToLiveTable_Add`]. Two such paths exist on the
/// C side today:
/// - `Document_Free` (`src/document_basic.c`) releasing a discarded
///   document.
/// - `DocTable_UpdateExpiration` (`src/doc_table.c`) discarding an
///   empty list rather than registering it.
///
/// After this call `*v` is logically the same as a fresh
/// [`FieldExpirations_Empty`] result and is safe to reuse or to drop
/// again.
///
/// # Safety
///  - `v` must be non-null.
///  - `*v` must be either an initialized [`FieldExpirations`] (returned by a
///    constructor in this module).
///  - No other reference to `*v` may exist for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FieldExpirations_Free(v: *mut FieldExpirations) {
    debug_assert!(!v.is_null(), "v cannot be NULL");

    // SAFETY: the caller guarantees exclusive access.
    let v = unsafe { &mut *v };
    // `mem::take` replaces `*v` with `FieldExpirations::default()` (an
    // empty wrapper that doesn't allocate) and drops the old value,
    // releasing its `ThinVec` heap allocation.
    let _ = std::mem::take(v);
}

#[inline]
const fn highest_bit_plus_one(mask: u32) -> usize {
    if mask == 0 {
        0
    } else {
        (u32::BITS - mask.leading_zeros()) as usize
    }
}

#[inline]
const fn highest_bit_plus_one_wide(mask: FieldMask) -> usize {
    if mask == 0 {
        0
    } else if size_of::<FieldMask>() == size_of::<u128>() {
        (u128::BITS - mask.leading_zeros()) as usize
    } else {
        (u64::BITS - mask.leading_zeros()) as usize
    }
}

/// Build the borrowed `ft_id_to_field_index` slice the underlying methods
/// expect. The pointer may be `NULL` only when `len == 0`.
///
/// # Safety
///
/// The caller guarantees `ptr` covers at least `len` valid `u16`
/// entries.
#[inline]
unsafe fn build_ft_slice<'a>(ptr: *const u16, len: usize) -> &'a [u16] {
    if len == 0 {
        &[]
    } else {
        debug_assert!(
            !ptr.is_null(),
            "ft_id_to_field_index cannot be NULL when field_mask != 0"
        );
        // SAFETY: the caller guarantees `ptr` is non-null (asserted above),
        // properly aligned, and points to at least `len` initialized `u16`
        // entries. The returned reference borrows from the C-owned array and
        // no mutable alias exists for the duration of the borrow.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}
