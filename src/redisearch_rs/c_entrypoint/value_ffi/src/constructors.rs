use ffi::RedisModuleString;
use std::ffi::{c_char, c_double};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::LazyLock;
use value::strings::{RedisString, RmAllocString};
use value::trio::RsValueTrio;
use value::{RsValue, shared::SharedRsValue};

use crate::util::rsvalue_str_to_float;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    SharedRsValue::from_value(RsValue::Undefined).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNull() -> *mut RsValue {
    SharedRsValue::from_value(RsValue::Null).into_raw() as *mut _
}

pub static RSVALUE_NULL: LazyLock<SharedRsValue> =
    LazyLock::new(|| SharedRsValue::from_value(RsValue::Null));

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NullStatic() -> *mut RsValue {
    RSVALUE_NULL.as_ptr() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    SharedRsValue::from_value(RsValue::Number(value)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(str: *const RedisModuleString) -> *mut RsValue {
    let redis_string = unsafe { RedisString::take(NonNull::new(str as *mut _).unwrap()) };
    SharedRsValue::from_value(RsValue::RedisString(redis_string)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(str: *const c_char, len: u32) -> *mut RsValue {
    debug_assert!(!str.is_null(), "`str` must not be NULL");
    // Safety: caller must uphold the safety requirements of
    // [`RmAllocString::copy_from_string`].
    let string = unsafe { RmAllocString::copy_from_string(str, len) };
    let value = RsValue::RmAllocString(string);
    SharedRsValue::from_value(value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewReference(src: *const RsValue) -> *mut RsValue {
    let shared_src = unsafe { SharedRsValue::from_raw(src) };
    let shared_src = ManuallyDrop::new(shared_src);
    let ref_value = RsValue::Ref(shared_src.deref().clone());
    SharedRsValue::from_value(ref_value).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewParsedNumber(value: *const c_char, len: u32) -> *mut RsValue {
    // C uses fast_float_strtod
    let slice = unsafe { std::slice::from_raw_parts(value as *const u8, len as usize) };
    let Some(number) = rsvalue_str_to_float(slice) else {
        return std::ptr::null_mut();
    };

    SharedRsValue::from_value(RsValue::Number(number)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumberFromInt64(number: i64) -> *mut RsValue {
    SharedRsValue::from_value(RsValue::Number(number as f64)).into_raw() as *mut _
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewTrio(
    left: *mut RsValue,
    middle: *mut RsValue,
    right: *mut RsValue,
) -> *mut RsValue {
    let shared_left = unsafe { SharedRsValue::from_raw(left) };
    let shared_middle = unsafe { SharedRsValue::from_raw(middle) };
    let shared_right = unsafe { SharedRsValue::from_raw(right) };

    SharedRsValue::from_value(RsValue::Trio(RsValueTrio::new(
        shared_left,
        shared_middle,
        shared_right,
    )))
    .into_raw() as *mut _
}
