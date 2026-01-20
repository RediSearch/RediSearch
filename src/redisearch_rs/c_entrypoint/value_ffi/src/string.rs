use c_ffi_utils::expect_unchecked;
use std::ffi::c_char;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use value::strings::{ConstString, RmAllocString};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: Option<NonNull<c_char>>, len: u32) -> *mut RsValue {
    let str = unsafe { expect_unchecked!(str) };
    let string = unsafe { RmAllocString::take_unchecked(str, len) };
    let value = RsValue::RmAllocString(string);
    SharedRsValue::from_value(value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    debug_assert!(!str.is_null(), "`str` must not be NULL");
    // Safety: caller must uphold the safety requirements of
    // [`ConstString::new`].
    let string = unsafe { ConstString::new(str, len) };
    let value = RsValue::ConstString(string);
    SharedRsValue::from_value(value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *const RsValue, lenp: *mut u32) -> *mut c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    let (ptr, len) = match value {
        RsValue::RmAllocString(str) => {
            // tracing::info!("RSValue_String_Get: RsValue::RmAllocString({str:?})");
            let slice = str.as_bytes();
            (slice.as_ptr() as _, str.len())
            // (str.as_ptr() as *mut _, str.len())
        }
        RsValue::ConstString(str) => {
            // tracing::info!("RSValue_String_Get: RsValue::ConstString({str:?})");
            let slice = str.as_bytes();
            (slice.as_ptr() as _, str.len())
            // (str.as_ptr() as *mut _, str.len())
        }
        RsValue::String2(str) => {
            // tracing::info!("RSValue_String_Get: RsValue::String2({str:?})");
            (str.as_ptr() as *mut _, str.count_bytes() as u32)
        }
        _ => panic!("unsupported RSValue_String_Get type"),
    };

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

    ptr
}
