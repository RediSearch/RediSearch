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

use std::ptr;

use field_spec::{FieldSpec, FieldSpecBuilder};
use pretty_assertions::assert_eq;

#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn field_name_and_path() {
    let name = c"name";
    let path = c"path";
    let fs = FieldSpecBuilder::new(path).with_field_name(name).finish();

    let sut = unsafe { FieldSpec::from_raw(ptr::from_ref(&fs)) };

    assert_eq!(sut.field_name().into_secret_value(), name);
    assert_eq!(sut.field_path().into_secret_value(), path);

    unsafe {
        ffi::HiddenString_Free(fs.fieldName, true);
        ffi::HiddenString_Free(fs.fieldPath, true);
    }
}
