/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::slice;

use crate::{SchemaRule, bindings::field_spec::FieldSpec};

/// A safe wrapper around an `ffi::IndexSpec`.
#[repr(transparent)]
pub struct IndexSpec(ffi::IndexSpec);

impl IndexSpec {
    /// Create a safe `IndexSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::IndexSpec) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get the underlying schema rule.
    pub const fn rule(&self) -> &SchemaRule {
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { SchemaRule::from_raw(self.0.rule) }
    }

    /// Get the underlying field specs as a slice of `FieldSpec`s.
    pub fn field_specs(&self) -> &[FieldSpec] {
        debug_assert!(!self.0.fields.is_null(), "fields must not be null");
        let data = self.0.fields.cast::<FieldSpec>();
        let len = self
            .0
            .numFields
            .try_into()
            .expect("numFields must fit into usize");
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { slice::from_raw_parts(data, len) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{ffi::CStr, mem, ptr};

    use pretty_assertions::assert_eq;

    #[test]
    #[cfg_attr(miri, ignore = "miri does not support FFI functions")]
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
        schema_rule.type_ = ffi::DocumentType::Json;
        index_spec.rule = ptr::from_mut(&mut schema_rule);
        let sut = unsafe { IndexSpec::from_raw(ptr::from_ref(&index_spec)) };

        let rule = sut.rule();

        assert_eq!(rule.type_(), ffi::DocumentType::Json);
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
}
