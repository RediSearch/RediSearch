/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{ffi::CStr, mem, ptr};

use index_spec::IndexSpec;
use pretty_assertions::assert_eq;

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn field_specs() {
    let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    let fs0 = field_spec(c"aaa", c"bbb", 0);
    let fs1 = field_spec(c"ccc", c"ddd", 1);
    let mut fields = [fs0, fs1];
    index_spec.fields = ptr::from_mut(&mut fields).cast::<ffi::FieldSpec>();
    index_spec.numFields = fields.len().try_into().unwrap();
    let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

    let fss = sut.field_specs();

    assert_eq!(fss.len(), fields.len());
    assert_eq!(
        unsafe { fss[0].to_raw().as_ref() }.unwrap().index,
        fs0.index
    );
    assert_eq!(
        unsafe { fss[1].to_raw().as_ref() }.unwrap().index,
        fs1.index
    );

    unsafe {
        ffi::HiddenString_Free(fs0.fieldName, false);
        ffi::HiddenString_Free(fs0.fieldPath, false);
        ffi::HiddenString_Free(fs1.fieldName, false);
        ffi::HiddenString_Free(fs1.fieldPath, false);
    }
}

#[test]
fn rule() {
    let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    let mut schema_rule = unsafe { mem::zeroed::<ffi::SchemaRule>() };
    schema_rule.type_ = document::DocumentType::Json;
    index_spec.rule = ptr::from_mut(&mut schema_rule);
    let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

    let rule = sut.rule();

    assert_eq!(rule.type_(), document::DocumentType::Json);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn display_name_truncates_at_interior_nul() {
    let original = b"idx\0tail\0";
    let obfuscated = c"Index@123";
    // SAFETY: a zeroed `ffi::IndexSpec` is valid for this test setup, which
    // initializes every field subsequently accessed through `IndexSpec`.
    let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };
    // SAFETY: `original` remains live and readable for the provided byte
    // length; `takeOwnership = false` leaves its backing buffer with this test.
    index_spec.specName =
        unsafe { ffi::NewHiddenString(original.as_ptr().cast(), original.len() - 1, false) };
    index_spec.obfuscatedName = obfuscated.as_ptr().cast_mut();
    // SAFETY: `index_spec` and the fields read by `display_name` are initialized
    // and remain live and immutable while `sut` is used.
    let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

    assert_eq!(sut.display_name(false), c"idx");
    assert_eq!(sut.display_name(true), obfuscated);

    // SAFETY: `specName` is still the live wrapper allocated above and is freed
    // exactly once; `false` matches its non-owning construction.
    unsafe { ffi::HiddenString_Free(index_spec.specName, false) };
}

fn field_spec(field_name: &CStr, field_path: &CStr, index: u16) -> ffi::FieldSpec {
    let mut res = unsafe { mem::zeroed::<ffi::FieldSpec>() };
    res.fieldName =
        unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
    res.fieldPath =
        unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
    res.index = index;
    res
}
