/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::bindings::HiddenStringRef;
use enumflags2::BitFlags;
use enumflags2::bitflags;
#[cfg(test)]
use std::ffi::CStr;
use std::fmt;
#[cfg(test)]
use std::mem::MaybeUninit;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecOption {
    Sortable = 0x01,
    NoStemming = 0x02,
    NotIndexable = 0x04,
    Phonetics = 0x08,
    Dynamic = 0x10,
    Unf = 0x20,
    WithSuffixTrie = 0x40,
    UndefinedOrder = 0x80,
    IndexEmpty = 0x100,   // Index empty values (i.e., empty strings)
    IndexMissing = 0x200, // Index missing values (non-existing field)
}
pub type FieldSpecOptions = BitFlags<FieldSpecOption>;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecType {
    Fulltext = 1,
    Numeric = 2,
    Geo = 4,
    Tag = 8,
    Vector = 16,
    Geometry = 32,
}
pub type FieldSpecTypes = BitFlags<FieldSpecType>;

/// A safe wrapper around an `ffi::FieldSpec`.
#[repr(transparent)]
pub struct FieldSpec(ffi::FieldSpec);

impl FieldSpec {
    /// Create a `FieldSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::FieldSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::FieldSpec) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Get a reference to the underlying non-null pointer.
    #[cfg(test)]
    pub const fn to_raw(&self) -> *const ffi::FieldSpec {
        std::ptr::from_ref(&self.0)
    }

    /// Get the underlying field name as a `HiddenStringRef`.
    pub const fn field_name(&self) -> HiddenStringRef<'_> {
        // Safety: (1.) due to creation with `FieldSpec::from_raw`
        unsafe { HiddenStringRef::from_raw(self.0.fieldName) }
    }

    /// Get the underlying field path as a `HiddenStringRef`.
    pub const fn field_path(&self) -> HiddenStringRef<'_> {
        // Safety: (1.) due to creation with `FieldSpec::from_raw`
        unsafe { HiddenStringRef::from_raw(self.0.fieldPath) }
    }
}

impl fmt::Debug for FieldSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldSpec")
            .field("fieldName", &self.0.fieldName)
            .field("fieldPath", &self.0.fieldPath)
            .field("sortIdx", &self.0.sortIdx)
            .field("index", &self.0.index)
            .field("tree", &self.0.tree)
            .field("ftWeight", &self.0.ftWeight)
            .field("ftId", &self.0.ftId)
            .field("indexError", &self.0.indexError)
            .finish()
    }
}

#[cfg(test)]
pub struct FieldSpecBuilder {
    result: ffi::FieldSpec,
}

#[cfg(test)]
impl FieldSpecBuilder {
    pub fn new(field_path: &CStr) -> Self {
        let mut result = unsafe { MaybeUninit::<ffi::FieldSpec>::zeroed().assume_init() };

        let field_path =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), true) };

        result.fieldPath = field_path;
        result.fieldName = field_path;

        Self { result }
    }

    pub fn with_field_name(mut self, field_name: &CStr) -> Self {
        self.result.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), true) };
        self
    }

    #[cfg_attr(miri, allow(unused))]
    pub fn with_options(mut self, options: FieldSpecOptions) -> Self {
        self.result.set_options(options.bits());
        self
    }

    /// If this field is sortable, the sortable index. Otherwise -1
    #[cfg_attr(miri, allow(unused))]
    pub fn with_sort_idx(mut self, sort_idx: i16) -> Self {
        self.result.sortIdx = sort_idx;
        self
    }

    pub fn finish(self) -> ffi::FieldSpec {
        self.result
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::ptr;

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
}
