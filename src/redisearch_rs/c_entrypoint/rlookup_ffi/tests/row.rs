/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

// // Link both Rust-provided and C-provided symbols
// extern crate redisearch_rs;
// // Mock or stub the ones that aren't provided by the line above
// redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::CString;
use std::ptr::NonNull;
use std::{mem, ptr};

use c_ffi_utils::opaque::IntoOpaque;
use rlookup::{RLookup, RLookupKeyFlags, RLookupRow};
use rlookup_ffi::row::{
    RLookupRow_MoveFieldsFrom, RLookupRow_WriteByName, RLookupRow_WriteByNameOwned,
    RLookupRow_WriteFieldsFrom,
};
use value::RSValueFFI;
use value::RSValueTrait;

#[test]
#[should_panic(expected = "`src` and `dst` must not be the same")]
fn rlookuprow_movefieldsfrom_same_row() {
    let lookup = RLookup::new();
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(row.as_opaque_non_null()),
            Some(row.as_opaque_non_null()),
        );
    }
}

#[test]
fn rlookuprow_movefieldsfrom_different_rows() {
    let lookup = RLookup::new();
    let mut src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(src_row.as_opaque_non_null()),
            Some(dst_row.as_opaque_non_null()),
        );
    }

    // No panic was triggered.
}

#[test]
#[should_panic(expected = "`src_row` and `dst_row` must not be the same")]
fn rlookuprow_writefieldsfrom_same_row() {
    let src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&src_lookup);

    unsafe {
        RLookupRow_WriteFieldsFrom(
            row.as_opaque_ptr(),
            ptr::from_ref(&src_lookup),
            Some(row.as_opaque_non_null()),
            Some(NonNull::from(&mut dst_lookup)),
            false,
        )
    };
}

#[test]
#[should_panic(expected = "`src_lookup` and `dst_lookup` must not be the same")]
fn rlookuprow_writefieldsfrom_same_lookup() {
    let mut lookup = RLookup::new();
    let src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);

    unsafe {
        RLookupRow_WriteFieldsFrom(
            src_row.as_opaque_ptr(),
            ptr::from_ref(&lookup),
            Some(dst_row.as_opaque_non_null()),
            Some(NonNull::from(&mut lookup)),
            false,
        )
    };
}

#[test]
fn rlookuprow_writefieldsfrom_different_lookups_and_rows() {
    let src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();
    let src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&src_lookup);
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&dst_lookup);

    unsafe {
        RLookupRow_WriteFieldsFrom(
            src_row.as_opaque_ptr(),
            ptr::from_ref(&src_lookup),
            Some(dst_row.as_opaque_non_null()),
            Some(NonNull::from(&mut dst_lookup)),
            false,
        );
    }

    // No panic was triggered.
}

#[test]
fn rlookuprow_move() {
    let mut lookup = RLookup::new();

    let mut src: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);
    let mut dst: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);

    let key = lookup
        .get_key_write(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    src.write_key(key, RSValueFFI::create_num(42.0));

    #[cfg(debug_assertions)]
    {
        src.assert_valid("tests::row::rlookuprow_move");
        dst.assert_valid("tests::row::rlookuprow_move");
    }

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(src.as_opaque_non_null()),
            Some(dst.as_opaque_non_null()),
        )
    }

    assert!(src.num_dyn_values() == 0);
    let key = lookup
        .get_key_read(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    assert!(dst.get(key).is_some());
}

#[test]
fn rlookuprow_writebyname() {
    let mut lookup = RLookup::new();
    let name = CString::new("name").unwrap();
    let len = 4;
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);
    let value =
        unsafe { RSValueFFI::from_raw(NonNull::new(ffi::RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByName(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(row.as_opaque_non_null()),
            NonNull::new(value.as_ptr()),
        );
    }

    assert_eq!(value.refcount(), 2);
}

#[test]
fn rlookuprow_writebynameowned() {
    let mut lookup = RLookup::new();
    let name = CString::new("name").unwrap();
    let len = 4;
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new(&lookup);
    let value =
        unsafe { RSValueFFI::from_raw(NonNull::new(ffi::RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByNameOwned(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(row.as_opaque_non_null()),
            NonNull::new(value.as_ptr()),
        );
    }

    assert_eq!(value.refcount(), 1);

    // See the comment regarding `mem::forget()` at the end of `RLookupRow_WriteByName()` for more info.
    mem::forget(value);
}
