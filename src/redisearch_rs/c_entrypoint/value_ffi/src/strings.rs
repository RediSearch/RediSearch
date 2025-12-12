use std::ffi::c_char;

use ffi::RedisModuleString;
use value::strings::{ConstString, RedisString, RmAllocString};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewString(str: *mut c_char, len: u32) -> *mut RsValue {
    let string = unsafe { RmAllocString::from_raw(str, len) };
    let value = RsValue::RmAllocString(string);
    let shared_value = SharedRsValue::new(value);
    shared_value.into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewConstString(str: *const c_char, len: u32) -> *mut RsValue {
    let string = unsafe { ConstString::from_raw(str, len) };
    let value = RsValue::ConstString(string);
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
