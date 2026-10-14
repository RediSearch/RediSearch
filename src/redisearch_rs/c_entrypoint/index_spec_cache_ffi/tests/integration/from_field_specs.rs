/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_spec_cache_ffi::{IndexSpecCache_Decref, IndexSpecCache_New};
use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::ptr;

/// A C `FieldSpec` named `name`. Without a `path`, the name's `HiddenString`
/// doubles as the path, as the schema does for a field without `AS`.
fn field_spec(name: &CStr, path: Option<&CStr>) -> ffi::FieldSpec {
    // SAFETY: an all-zero `FieldSpec` is a valid value of the C struct.
    let mut fs: ffi::FieldSpec = unsafe { MaybeUninit::zeroed().assume_init() };
    // SAFETY: `name` is readable for its length, and the string copies it.
    fs.fieldName = unsafe { ffi::NewHiddenString(name.as_ptr(), name.count_bytes(), true) };
    fs.fieldPath = match path {
        // SAFETY: as for the name.
        Some(path) => unsafe { ffi::NewHiddenString(path.as_ptr(), path.count_bytes(), true) },
        None => fs.fieldName,
    };
    fs
}

fn free_field_spec(fs: ffi::FieldSpec) {
    if fs.fieldPath != fs.fieldName {
        // SAFETY: created by `field_spec` with its own copy of the bytes.
        unsafe { ffi::HiddenString_Free(fs.fieldPath, true) };
    }
    // SAFETY: as above.
    unsafe { ffi::HiddenString_Free(fs.fieldName, true) };
}

/// The cache copies each field: a field without a path of its own resolves to
/// its name, and the copies outlive the C fields they were made from.
#[test]
#[cfg_attr(miri, ignore = "creates C `HiddenString`s")]
fn copies_each_field_from_the_schema() {
    let mut text = field_spec(c"title", None);
    text.set_types(ffi::FieldType_INDEXFLD_T_FULLTEXT);
    text.ftId = 3;
    let mut sortable = field_spec(c"price", Some(c"$.price"));
    sortable.set_types(ffi::FieldType_INDEXFLD_T_NUMERIC);
    sortable.set_options(ffi::FieldSpecOptions_FieldSpec_Sortable);
    sortable.sortIdx = 2;
    let fields = [text, sortable];

    // SAFETY: `fields` holds two `FieldSpec`s whose strings `field_spec`
    // created, and there are no rule names.
    let handle = unsafe {
        IndexSpecCache_New(
            fields.as_ptr(),
            fields.len(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
        )
    };
    fields.into_iter().for_each(free_field_spec);

    // SAFETY: `handle` has not been released.
    let cache = unsafe { &*handle };
    let title = cache.find_field(c"title").expect("`title` is cached");
    assert_eq!(title.path(), c"title");
    assert_eq!(title.types(), ffi::FieldType_INDEXFLD_T_FULLTEXT);
    assert_eq!(title.ft_id(), 3);

    let price = cache.find_field(c"price").expect("`price` is cached");
    assert_eq!(price.path(), c"$.price");
    assert_eq!(price.types(), ffi::FieldType_INDEXFLD_T_NUMERIC);
    assert_eq!(price.options(), ffi::FieldSpecOptions_FieldSpec_Sortable);
    assert_eq!(price.sort_idx(), 2);
    assert!(cache.find_field(c"$.price").is_none());

    // SAFETY: `handle` has not been released, and is not used again.
    unsafe { IndexSpecCache_Decref(handle) };
}
