use ffi::RedisModuleString;
use std::ffi::{c_char, c_double};
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewUndefined() -> *mut RsValue {
    unimplemented!("RSValue_NewUndefined")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNull() -> *mut RsValue {
    unimplemented!("RSValue_NewNull")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NullStatic() -> *mut RsValue {
    unimplemented!("RSValue_NullStatic")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumber(value: c_double) -> *mut RsValue {
    unimplemented!("RSValue_NewNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewRedisString(s: *const RedisModuleString) -> *mut RsValue {
    unimplemented!("RSValue_NewRedisString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewCopiedString(value: *const c_char, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewCopiedString")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewReference(src: *const RsValue) -> *mut RsValue {
    unimplemented!("RSValue_NewReference")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewParsedNumber(value: *const c_char, len: u32) -> *mut RsValue {
    unimplemented!("RSValue_NewParsedNumber")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewNumberFromInt64(ii: i64) -> *mut RsValue {
    unimplemented!("RSValue_NewNumberFromInt64")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewTrio(
    val: *mut RsValue,
    otherval: *mut RsValue,
    other2val: *mut RsValue,
) -> *mut RsValue {
    unimplemented!("RSValue_NewTrio")
}
