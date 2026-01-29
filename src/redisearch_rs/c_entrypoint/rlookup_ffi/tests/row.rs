/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use std::ffi::CString;
use std::ptr::NonNull;
use std::{mem, ptr};

use rlookup::RLookup;
use rlookup::RLookupKeyFlags;
use rlookup::RLookupRow;
use rlookup_ffi::row::{
    RLookupRow_MoveFieldsFrom, RLookupRow_WriteByName, RLookupRow_WriteByNameOwned,
};
use value::RSValueFFI;
use value::RSValueTrait;

#[test]
fn rlookuprow_move() {
    let mut lookup = RLookup::new();

    let mut src = RLookupRow::new(&lookup);
    let mut dst = RLookupRow::new(&lookup);

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
            Some(NonNull::from(&mut src)),
            Some(NonNull::from(&mut dst)),
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
    let mut row = RLookupRow::new(&lookup);
    let value =
        unsafe { RSValueFFI::from_raw(NonNull::new(ffi::RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByName(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(NonNull::from(&mut row)),
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
    let mut row = RLookupRow::new(&lookup);
    let value =
        unsafe { RSValueFFI::from_raw(NonNull::new(ffi::RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByNameOwned(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(NonNull::from(&mut row)),
            NonNull::new(value.as_ptr()),
        );
    }

    assert_eq!(value.refcount(), 1);

    // See the comment regarding `mem::forget()` at the end of `RLookupRow_WriteByName()` for more info.
    mem::forget(value);
}
