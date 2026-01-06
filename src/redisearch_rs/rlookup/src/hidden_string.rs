use std::{ffi::c_char, ptr};

#[repr(transparent)]
pub struct HiddenString(*const ffi::HiddenString);

impl HiddenString {
    pub unsafe fn from_raw(ptr: *const ffi::HiddenString) -> Self {
        assert!(!ptr.is_null());
        Self(ptr)
    }

    pub fn get_unsafe(&self) -> (*const c_char, usize) {
        let mut len = 0;
        // Safety: Ensured by from_raw()
        let name_ptr = unsafe { ffi::HiddenString_GetUnsafe(self.0, ptr::from_mut(&mut len)) };
        (name_ptr, len)
    }
}
