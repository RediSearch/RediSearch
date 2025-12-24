use ffi::RedisModuleString;
use libc::size_t;
use std::ffi::{c_char, c_double, c_int};
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RsValue) -> *mut RsValue {
    unimplemented!("RSValue_Dereference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: &RsValue) {
    unimplemented!("RSValue_Clear")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RsValue) -> *mut RsValue {
    unimplemented!("RSValue_IncrRef")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RsValue, src: *const RsValue) {
    unimplemented!("RSValue_MakeReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RsValue, src: *const RsValue) {
    unimplemented!("RSValue_MakeOwnReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RsValue, src: *const RsValue) {
    unimplemented!("RSValue_Replace")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RsValue) -> u16 {
    unimplemented!("RSValue_Refcount")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_RedisString_Get(
    value: *const RsValue,
) -> *const RedisModuleString {
    unimplemented!("RSValue_RedisString_Get")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToNumber(v: *const RsValue, d: *mut c_double) -> c_int {
    unimplemented!("RSValue_ToNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ConvertStringPtrLen(
    v: *const RsValue,
    lenp: *mut size_t,
    buf: *mut c_char,
    buflen: size_t,
) -> *const c_char {
    unimplemented!("RSValue_ConvertStringPtrLen")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_StringPtrLen(
    value: *const RsValue,
    lenp: *mut size_t,
) -> *const c_char {
    unimplemented!("RSValue_StringPtrLen")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_ToString(dst: *const RsValue, value: *const RsValue) {
    unimplemented!("RSValue_ToString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NumToString(
    v: *const RsValue,
    buf: *mut c_char,
    buflen: size_t,
) -> size_t {
    unimplemented!("RSValue_NumToString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetString(v: *const RsValue, str: *mut c_char, len: size_t) {
    unimplemented!("RSValue_SetString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_SetConstString(
    v: *const RsValue,
    str: *const c_char,
    len: size_t,
) {
    unimplemented!("RSValue_SetConstString")
}
