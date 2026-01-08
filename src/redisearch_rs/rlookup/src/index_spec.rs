use std::{ptr, slice};

use crate::{SchemaRule, field_spec::FieldSpec};

#[derive(Debug)]
#[repr(transparent)]
pub struct IndexSpec(ffi::IndexSpec);

impl IndexSpec {
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::IndexSpec) -> &'a Self {
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    pub const fn to_raw(&self) -> *const ffi::IndexSpec {
        ptr::from_ref(&self.0)
    }

    #[cfg(test)]
    pub fn from_ffi(value: ffi::IndexSpec) -> Self {
        Self(value)
    }

    pub const fn rule(&self) -> &SchemaRule {
        unsafe { SchemaRule::from_raw(self.0.rule) }
    }

    pub fn field_specs<'a>(&self) -> &'a [FieldSpec] {
        unsafe {
            slice::from_raw_parts(
                self.0.fields.cast::<FieldSpec>(),
                self.0
                    // Although `numFields` is not being read in the C implementation,
                    // we are going to assume that it's set correctly.
                    .numFields
                    .try_into()
                    .expect("numFields must fit into usize"),
            )
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{ffi::CStr, mem};

    use pretty_assertions::assert_eq;

    #[test]
    fn field_specs_and_rule() {
        // Arrange
        let mut index_spec = unsafe { mem::zeroed::<ffi::IndexSpec>() };

        let fs0 = field_spec(c"aaa", c"bbb", 0);
        let fs1 = field_spec(c"ccc", c"ddd", 1);
        let mut fields = [fs0, fs1];

        index_spec.fields = &raw mut fields as _;
        index_spec.numFields = fields.len().try_into().unwrap();

        let mut schema_rule = unsafe { mem::zeroed::<ffi::SchemaRule>() };
        schema_rule.type_ = ffi::DocumentType::Json;

        index_spec.rule = ptr::from_mut(&mut schema_rule);

        let sut = unsafe { IndexSpec::from_raw(&raw const index_spec) };

        // Act
        let fss = sut.field_specs();
        let rule = sut.rule();

        // Assert
        assert_eq!(fss.len(), fields.len());
        assert_eq!(
            unsafe { fss[0].to_raw().as_ref() }.unwrap().index,
            fs0.index
        );
        assert_eq!(
            unsafe { fss[1].to_raw().as_ref() }.unwrap().index,
            fs1.index
        );

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
