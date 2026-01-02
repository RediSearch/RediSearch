use c_ffi_utils::expect_unchecked;
use std::ffi::c_char;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use value::{RsValue, Value, shared::SharedRsValue};

use crate::AsRsValueType;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: Option<NonNull<c_char>>, len: u32) -> *mut RsValue {
    let str = unsafe { expect_unchecked!(str) };
    let value = unsafe { RsValue::take_rm_alloc_string(str, len) };
    SharedRsValue::from_value(value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    let value = unsafe { RsValue::const_string(str, len) };
    SharedRsValue::from_value(value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *mut RsValue, lenp: *mut u32) -> *mut c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    let (ptr, len) = match value {
        RsValue::RmAllocString(str) => (str.as_ptr() as *mut _, str.len()),
        RsValue::ConstString(str) => (str.as_ptr() as *mut _, str.len()),
        _ => panic!("WHAT?!?!?: {:?}", value.as_value_type()),
    };

    if let Some(lenp) = unsafe { lenp.as_mut() } {
        *lenp = len;
    }

    ptr
}
