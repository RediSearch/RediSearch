use std::ptr;

use crate::hidden_string::HiddenString;

#[repr(transparent)]
pub struct FieldSpec(ffi::FieldSpec);

impl FieldSpec {
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::FieldSpec) -> &'a Self {
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    pub const fn to_raw(&self) -> *const ffi::FieldSpec {
        ptr::from_ref(&self.0)
    }

    #[cfg(test)]
    pub fn from_ffi(value: ffi::FieldSpec) -> Self {
        Self(value)
    }

    pub fn field_name(&self) -> &HiddenString {
        unsafe { HiddenString::from_raw(self.0.fieldName) }
    }

    pub fn field_path(&self) -> &HiddenString {
        unsafe { HiddenString::from_raw(self.0.fieldPath) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{ffi::CStr, mem};

    use pretty_assertions::assert_eq;

    #[test]
    fn field_name_and_path() {
        let name = c"name";
        let path = c"path";
        let fs = field_spec(name, path);

        let sut = FieldSpec::from_ffi(fs);

        assert_eq!(sut.field_name().get_secret_value(), name);
        assert_eq!(sut.field_path().get_secret_value(), path);
    }

    fn field_spec(field_name: &CStr, field_path: &CStr) -> ffi::FieldSpec {
        let mut res = unsafe { mem::zeroed::<ffi::FieldSpec>() };
        res.fieldName =
            unsafe { ffi::NewHiddenString(field_name.as_ptr(), field_name.count_bytes(), false) };
        res.fieldPath =
            unsafe { ffi::NewHiddenString(field_path.as_ptr(), field_path.count_bytes(), false) };
        res
    }
}
