use std::ffi::c_char;
use std::mem::ManuallyDrop;
use std::ptr::copy_nonoverlapping;

use ffi::{RedisModule_Alloc, RedisModuleString};
use value::strings::{ConstString, RedisString, RmAllocString};
use value::{RsString, RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: *mut c_char, len: u32) -> *mut RsValue {
    let string = unsafe { RsString::rm_alloc_string(str, len) };
    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    let string = unsafe { RsString::const_string(str, len) };
    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(str: *const RedisModuleString) -> *mut RsValue {
    let redis_string = unsafe { RedisString::from_raw(str) };
    let value = RsValue::RedisString(redis_string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(str: *const c_char, len: u32) -> *mut RsValue {
    let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
    let buf = unsafe { rm_alloc((len + 1) as usize) } as *mut c_char;
    unsafe { copy_nonoverlapping(str, buf, len as usize) };
    unsafe { buf.add(len as usize).write(0) };
    let string = unsafe { RsString::rm_alloc_string(buf, len) };
    let value = RsValue::String(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_String_Get(value: *const RsValue, lenp: *mut u32) -> *mut c_char {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    let RsValue::String(str) = value else {
        panic!("unsupported RSValue_String_Get type")
    };

    let (ptr, len) = str.as_ptr_len();

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
