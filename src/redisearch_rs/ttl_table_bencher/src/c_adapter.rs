/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Thin Rust wrappers around the C `TimeToLiveTable_*` functions.
//!
//! Mirrors the call boundary used by `doc_table.c`: the table pointer is
//! lazily initialized via `TimeToLiveTable_VerifyInit` and torn down
//! through `TimeToLiveTable_Destroy`. Only the operations supported by
//! the C public API are exposed — see the parent crate's module
//! documentation for the rationale.
//!
//! The unique wiring step is [`c_add`]: `TimeToLiveTable_Add` consumes an
//! `arrayof(FieldExpiration)` (an `arr.h` fat-pointer array, not a plain
//! malloc), so we allocate the buffer through `array_new_sz` and copy
//! the Rust-side payload in before handing it off — ownership transfers
//! to the table, which will free it from `Destroy`/`Remove`.

// All wrappers in this module take a raw `*mut TimeToLiveTable` produced
// by `c_init`; callers within this benchmark crate uphold the contract
// (table is live, ftid translation has enough entries). Clippy would
// otherwise force every wrapper to be `unsafe fn`, which buys nothing
// here — the bench file already treats these as trusted helpers.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use field::FieldExpirationPredicate;
use libc::timespec;
use std::ptr;

/// Reinterpret a `libc::timespec` as the bindgen-generated `ffi::timespec`.
///
/// Both are nominally distinct Rust types but have identical layout
/// (`time_t` + `c_long`) on every Unix target the project supports.
const fn libc_to_ffi_timespec(ts: &timespec) -> ffi::timespec {
    ffi::timespec {
        tv_sec: ts.tv_sec,
        tv_nsec: ts.tv_nsec,
    }
}

const fn predicate_to_c(p: FieldExpirationPredicate) -> ffi::FieldExpirationPredicate {
    match p {
        FieldExpirationPredicate::Default => {
            ffi::FieldExpirationPredicate_FIELD_EXPIRATION_PREDICATE_DEFAULT
        }
        FieldExpirationPredicate::Missing => {
            ffi::FieldExpirationPredicate_FIELD_EXPIRATION_PREDICATE_MISSING
        }
    }
}

/// Lazily initializes a fresh C [`ffi::TimeToLiveTable`] with the given
/// modulus and returns the heap pointer. Caller is responsible for
/// passing the returned pointer to [`c_destroy`] when finished.
#[must_use]
pub fn c_init(max_size: usize) -> *mut ffi::TimeToLiveTable {
    let mut table: *mut ffi::TimeToLiveTable = ptr::null_mut();
    // SAFETY: `&mut table` is a valid `**TimeToLiveTable`. The C function
    // writes the freshly allocated table pointer into it.
    unsafe {
        ffi::TimeToLiveTable_VerifyInit(&mut table, max_size);
    }
    debug_assert!(!table.is_null());
    table
}

/// Releases the C-side table and all owned per-document arrays.
pub fn c_destroy(table: *mut ffi::TimeToLiveTable) {
    let mut table = table;
    // SAFETY: `&mut table` is a valid `**TimeToLiveTable`. The C function
    // accepts NULL and walks the bucket array freeing each entry's
    // `arrayof(FieldExpiration)` before freeing the table itself.
    unsafe {
        ffi::TimeToLiveTable_Destroy(&mut table);
    }
}

/// Inserts a document's per-field expirations into the C table.
///
/// `fields` is materialized as an `arrayof(FieldExpiration)` via
/// `array_new_sz` and a `memcpy`; ownership of the resulting array
/// transfers to the table, which will free it from `Destroy`/`Remove`.
///
/// # Panics
///
/// Panics if `fields` is empty (the C `TimeToLiveTable_Add` asserts the
/// same precondition) or if the per-element size exceeds [`u16::MAX`].
pub fn c_add(table: *mut ffi::TimeToLiveTable, doc_id: u64, fields: &[ffi::FieldExpiration]) {
    assert!(
        !fields.is_empty(),
        "C TimeToLiveTable_Add requires at least one field"
    );

    let elem_sz = std::mem::size_of::<ffi::FieldExpiration>();
    assert!(
        elem_sz <= u16::MAX as usize,
        "FieldExpiration too large for arr.h"
    );
    let len = fields.len();
    assert!(len <= u32::MAX as usize, "field count exceeds arr.h limit");

    // SAFETY: `array_new_sz` returns a pointer to a fat-pointer array of
    // `len` elements with `remain_cap = 0`.
    let raw = unsafe { ffi::array_new_sz(elem_sz as u16, 0, len as u32) };
    debug_assert!(!raw.is_null());
    // SAFETY: `raw` points to `len * elem_sz` bytes of writable memory
    // (just allocated, disjoint from `fields`).
    unsafe {
        ptr::copy_nonoverlapping(
            fields.as_ptr().cast::<ffi::FieldExpiration>(),
            raw.cast::<ffi::FieldExpiration>(),
            len,
        );
    }
    // SAFETY: `table` is a valid pointer obtained from `c_init`; ownership
    // of `raw` (an `arrayof(FieldExpiration)` of length `len`) transfers
    // into the table.
    unsafe {
        ffi::TimeToLiveTable_Add(table, doc_id, raw.cast::<ffi::FieldExpiration>());
    }
}

/// Removes a document's entry from the C table. No-op if absent.
pub fn c_remove(table: *mut ffi::TimeToLiveTable, doc_id: u64) {
    // SAFETY: `table` is a valid pointer obtained from `c_init`. The C
    // function tolerates an unknown doc id.
    unsafe {
        ffi::TimeToLiveTable_Remove(table, doc_id);
    }
}

/// Calls `TimeToLiveTable_VerifyDocAndField`.
#[must_use]
pub fn c_verify_doc_and_field(
    table: *mut ffi::TimeToLiveTable,
    doc_id: u64,
    field: u16,
    predicate: FieldExpirationPredicate,
    now: &timespec,
) -> bool {
    // SAFETY: `table` is a valid pointer; `now` lives for the duration
    // of the call.
    unsafe {
        ffi::TimeToLiveTable_VerifyDocAndField(
            table,
            doc_id,
            field,
            predicate_to_c(predicate),
            &libc_to_ffi_timespec(now),
        )
    }
}

/// Calls `TimeToLiveTable_VerifyDocAndFieldMask` (32-bit mask).
#[must_use]
pub fn c_verify_doc_and_field_mask(
    table: *mut ffi::TimeToLiveTable,
    doc_id: u64,
    mask: u32,
    predicate: FieldExpirationPredicate,
    now: &timespec,
    ftid: &[u16],
) -> bool {
    // SAFETY: `ftid` is a valid Rust slice; we hand its pointer to C
    // along with no length info, but the contract is that the slice has
    // at least 32 entries (caller passes [`crate::ftid_table`]
    // `(u32::BITS)`).
    unsafe {
        ffi::TimeToLiveTable_VerifyDocAndFieldMask(
            table,
            doc_id,
            mask,
            predicate_to_c(predicate),
            &libc_to_ffi_timespec(now),
            ftid.as_ptr(),
        )
    }
}

/// Calls `TimeToLiveTable_VerifyDocAndWideFieldMask` (128-bit mask on
/// 64-bit targets).
#[must_use]
pub fn c_verify_doc_and_wide_field_mask(
    table: *mut ffi::TimeToLiveTable,
    doc_id: u64,
    mask: u128,
    predicate: FieldExpirationPredicate,
    now: &timespec,
    ftid: &[u16],
) -> bool {
    // SAFETY: `ftid` is a valid Rust slice with at least 128 entries
    // (caller passes [`crate::ftid_table`]`(u128::BITS)`). On 64-bit
    // targets `ffi::t_fieldMask = __uint128_t = u128`.
    unsafe {
        ffi::TimeToLiveTable_VerifyDocAndWideFieldMask(
            table,
            doc_id,
            mask,
            predicate_to_c(predicate),
            &libc_to_ffi_timespec(now),
            ftid.as_ptr(),
        )
    }
}
