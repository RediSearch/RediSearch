use std::fmt::Debug;

#[repr(C)]
pub struct FfiSafeString {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl From<String> for FfiSafeString {
    fn from(value: String) -> Self {
        let mut bytes = String::into_bytes(value);
        let ptr = bytes.as_mut_ptr();
        let len = bytes.len();
        let cap = bytes.capacity();
        std::mem::forget(bytes);
        Self { ptr, len, cap }
    }
}

impl Into<String> for FfiSafeString {
    fn into(self) -> String {
        unsafe { String::from_raw_parts(self.ptr as *mut u8, self.len, self.cap) }
    }
}

impl Debug for FfiSafeString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slice = unsafe { std::slice::from_raw_parts(self.ptr, self.len) };
        Debug::fmt(&slice, f)
    }
}

impl Clone for FfiSafeString {
    fn clone(&self) -> Self {
        let s = unsafe { String::from_raw_parts(self.ptr, self.len, self.cap) };
        let cloned = s.clone().into();
        std::mem::forget(s);
        cloned
    }
}

impl Drop for FfiSafeString {
    fn drop(&mut self) {
        drop(unsafe { String::from_raw_parts(self.ptr as *mut u8, self.len, self.cap) });
    }
}

unsafe impl Send for FfiSafeString {}
unsafe impl Sync for FfiSafeString {}
