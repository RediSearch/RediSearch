use std::ffi::c_char;
use std::ptr::NonNull;
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: Option<NonNull<c_char>>, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewConstString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *mut RsValue, lenp: *mut u32) -> *mut c_char {
    unimplemented!("RSValue_String_Get")
}
