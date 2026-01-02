use std::{ffi::c_char, ptr};

pub struct HiddenString(*const ffi::HiddenString);

impl HiddenString {
    pub unsafe fn new(value: *const ffi::HiddenString) -> Self {
        assert!(!value.is_null());
        Self(value)
    }

    pub fn get_unsafe(&self) -> (*const c_char, usize) {
        let mut length = 0;
        // Safety: Ensured by new()
        let name_ptr = unsafe { ffi::HiddenString_GetUnsafe(self.0, ptr::from_mut(&mut length)) };
        (name_ptr, length)
    }
}
