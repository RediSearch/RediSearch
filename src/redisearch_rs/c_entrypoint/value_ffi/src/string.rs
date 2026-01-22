use std::ffi::c_char;
use std::mem::ManuallyDrop;
use value::strings::{ConstString, RmAllocString};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: *mut c_char, len: u32) -> *mut RsValue {
    let string = unsafe { RmAllocString::from_raw(str, len) };
    let value = RsValue::RmAllocString(string);
    let shared_value = SharedRsValue::from_value(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    let string = unsafe { ConstString::from_raw(str, len) };
    let value = RsValue::ConstString(string);
    let shared_value = SharedRsValue::from_value(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *const RsValue, lenp: *mut u32) -> *mut c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    let Some((ptr, len)) = crate::util::rsvalue_as_str_ptr_len(value) else {
        panic!("unsupported RSValue_String_Get type")
    };

    // let (ptr, len) = match value {
    //     RsValue::RmAllocString(str) => str.as_ptr_len(),
    //     RsValue::ConstString(str) => str.as_ptr_len(),
    //     RsValue::String2(str) => (str.as_ptr(), str.count_bytes() as u32),
    //     _ => panic!("unsupported RSValue_String_Get type"),
    // };

    // tracing::info!("RSValue_String_Get: ptr={ptr:?}, len={len}");
    // if len > 0 {
    //     let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize + 1) };
    //     tracing::info!("RSValue_String_Get: {:?}", slice);
    //     let string = String::from_utf8_lossy(slice).into_owned();
    //     tracing::info!("RSValue_String_Get: {:?}", string);
    // }

    if let Some(lenp) = unsafe { lenp.as_mut() } {
        *lenp = len;
    }

    ptr as *mut _
}
