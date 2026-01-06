use std::{ffi::CStr, ptr, slice};

#[derive(Debug)]
#[repr(transparent)]
pub struct HiddenString(ffi::HiddenString);

impl HiddenString {
    pub unsafe fn from_raw<'a>(ptr: *const ffi::HiddenString) -> &'a Self {
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    pub fn to_raw(&self) -> *const ffi::HiddenString {
        ptr::from_ref(&self.0)
    }

    /// Get the secret (aka. "unsafe" in C land) value from the underlying [`ffi::HiddenString`].
    pub fn get_secret_value(&self) -> &CStr {
        let mut len = 0;
        let data =
            unsafe { ffi::HiddenString_GetUnsafe(ptr::from_ref(&self.0), ptr::from_mut(&mut len)) };
        // The len doesn't include the \0 so we need to do +1.
        let bytes = unsafe { slice::from_raw_parts(data.cast::<u8>(), len + 1) };
        CStr::from_bytes_with_nul(bytes).expect("string must not be malformed")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn secret_value() {
        let c = c"ABC";
        let fhs = unsafe { ffi::NewHiddenString(c.as_ptr(), c.count_bytes(), false) };
        let hs = unsafe { HiddenString::from_raw(fhs) };
        let s = hs.get_secret_value();

        assert_eq!(s, c);
    }
}
